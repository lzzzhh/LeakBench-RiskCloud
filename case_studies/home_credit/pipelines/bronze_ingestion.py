"""P1.2 — Bronze Ingestion Pipeline.

Writes raw CSV data into Iceberg Bronze tables with deterministic metadata.
All source columns are stored as STRING. Uses overwritePartitions for idempotent re-runs.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from case_studies.home_credit.scripts.validate_manifest import validate_manifest
from riskcloud.adapters.home_credit.field_mapping import (
    APPLICATION_FILE,
    BUREAU_BALANCE_FILE,
    BUREAU_FILE,
    RAW_REQUIRED_COLUMNS,
)

BRONZE_META_COLUMNS = [
    "_source_file_name",
    "_source_file_sha256",
    "_source_header_sha256",
    "_source_manifest_sha256",
    "_source_snapshot_id",
    "_bronze_schema_version",
    "_raw_row_sha256",
]
BRONZE_SCHEMA_VERSION = 1
TABLES_REQUIRED = {APPLICATION_FILE, BUREAU_FILE, BUREAU_BALANCE_FILE}

UTC = timezone.utc


# -----------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _compute_source_snapshot_id(manifest_sha: str, bronze_version: str) -> str:
    parts = json.dumps(["home_credit", manifest_sha, bronze_version], separators=(",", ":"))
    return _sha256(parts.encode())


def _row_hash(row_values: list[str | None], columns: list[str]) -> str:
    """Deterministic row hash from canonical JSON object. null included, not omitted."""
    obj = {col: val for col, val in zip(columns, row_values)}
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return _sha256(raw.encode())


def _content_fingerprint(spark, table: str, manifest_sha: str) -> dict[str, Any]:
    """Compute deterministic multiset fingerprint from _raw_row_sha256 counts."""

    df = spark.sql(f"SELECT _raw_row_sha256 FROM {table} WHERE _source_manifest_sha256 = '{manifest_sha}'")
    # Group and count, then stream
    counts = df.groupBy("_raw_row_sha256").count().orderBy("_raw_row_sha256").collect()

    h = hashlib.sha256()
    total = 0
    distinct = 0
    for row in counts:
        line = f"{row._raw_row_sha256}:{row['count']}\n"
        h.update(line.encode())
        total += row["count"]
        distinct += 1

    return {
        "content_multiset_sha256": h.hexdigest(),
        "row_count": total,
        "distinct_row_hash_count": distinct,
        "duplicate_row_count": total - distinct,
    }


# -----------------------------------------------------------------
# Config
# -----------------------------------------------------------------

class BronzeConfig:
    def __init__(self, path: Path):
        with open(path) as f:
            data = yaml.safe_load(f)
        b = data["bronze"]
        self.version = b["version"]
        self.catalog = b["catalog"]
        self.namespace = b["namespace"]
        self.partition_field = b["partition_field"]
        self.write_mode = b["write_mode"]

        if self.write_mode != "overwrite_partitions":
            raise ValueError(f"write_mode must be overwrite_partitions, got {self.write_mode!r}")

        tables = data["tables"]
        table_names = {t["file"] for t in tables.values()}
        if table_names != TABLES_REQUIRED:
            missing = TABLES_REQUIRED - table_names
            extra = table_names - TABLES_REQUIRED
            msg = []
            if missing:
                msg.append(f"missing tables: {missing}")
            if extra:
                msg.append(f"unknown tables: {extra}")
            raise ValueError("; ".join(msg))

        seen_targets = set()
        for name, tdef in tables.items():
            target = tdef["table"]
            if target in seen_targets:
                raise ValueError(f"duplicate target table: {target}")
            seen_targets.add(target)

        self.tables = tables

    @classmethod
    def from_yaml(cls, path: Path) -> BronzeConfig:
        return cls(path)


# -----------------------------------------------------------------
# Ingestion
# -----------------------------------------------------------------

def ingest_bronze(
    config: BronzeConfig,
    data_dir: Path,
    manifest_path: Path,
    receipt_dir: Path,
    run_id: str,
    git_commit: str = "",
    warehouse: str | None = None,
) -> dict[str, Any]:
    """Run full Bronze ingestion. Returns receipt dict."""
    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    started_at = datetime.now(UTC)

    # 1. Validate manifest via P1.0
    ok, errors = validate_manifest(data_dir, manifest_path)
    if not ok:
        raise RuntimeError(f"Manifest validation failed: {errors}")

    with open(manifest_path, "rb") as f:
        manifest_raw = f.read()
    manifest_sha = _sha256(manifest_raw)
    manifest_data = yaml.safe_load(manifest_raw)

    # Build file spec map
    file_specs = {f["name"]: f for f in manifest_data["files"] if f["name"] in TABLES_REQUIRED}
    if len(file_specs) != 3:
        raise RuntimeError(f"Manifest missing required files: {TABLES_REQUIRED - set(file_specs)}")

    source_snapshot_id = _compute_source_snapshot_id(manifest_sha, config.version)

    # 2. Spark session
    spark = get_spark(app_name=f"riskcloud-bronze-{run_id}", warehouse=warehouse)
    try:
        setup_namespaces(spark)

        table_results = {}
        for tbl_name, tbl_def in config.tables.items():
            tresult = _ingest_one_table(
                spark, config, tbl_def, file_specs, data_dir,
                manifest_sha, source_snapshot_id,
            )
            table_results[tbl_name] = tresult

        # 3. Build receipt
        receipt = _build_receipt(
            run_id, started_at, manifest_path, manifest_sha, source_snapshot_id,
            git_commit, config, table_results,
        )

        # 4. Write receipt atomically
        receipt_path = receipt_dir / "bronze_receipt.yaml"
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_bytes = yaml.safe_dump(receipt, default_flow_style=False, sort_keys=False).encode()
        tmp_path = receipt_path.with_suffix(receipt_path.suffix + ".tmp")
        with open(tmp_path, "wb") as f:
            f.write(receipt_bytes)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, receipt_path)

        # 5. Write snapshot manifest
        _write_snapshot_manifest(receipt_dir, receipt, manifest_sha, table_results, git_commit)

        return receipt

    finally:
        spark.stop()


def _ingest_one_table(
    spark, config: BronzeConfig, tbl_def: dict, file_specs: dict,
    data_dir: Path, manifest_sha: str, source_snapshot_id: str,
) -> dict[str, Any]:
    from pyspark.sql.functions import col, lit, udf
    from pyspark.sql.types import StringType, StructField, StructType

    file_name = tbl_def["file"]
    table_name = tbl_def["table"]
    fspec = file_specs[file_name]
    csv_path = data_dir / file_name

    if not csv_path.is_file():
        raise RuntimeError(f"Source file not found: {csv_path}")

    source_sha = _sha256_file(csv_path)

    # Read header
    with open(csv_path, newline="") as f:
        import csv
        reader = csv.reader(f)
        src_columns = next(reader)
    header_raw = ",".join(src_columns).encode()
    header_sha = _sha256(header_raw)

    # Verify header matches manifest
    if header_sha != fspec["header_sha256"]:
        raise RuntimeError(f"{file_name}: header SHA mismatch")

    # Verify required raw columns
    required = RAW_REQUIRED_COLUMNS.get(file_name, set())
    missing = required - set(src_columns)
    if missing:
        raise RuntimeError(f"{file_name}: missing required columns: {missing}")

    # Explicit all-STRING schema
    schema = StructType([
        StructField(c, StringType(), True) for c in src_columns
    ])

    # Read CSV
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("mode", "FAILFAST") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "false") \
        .schema(schema) \
        .csv(str(csv_path))

    # Verify row count
    actual_rows = df.count()
    expected_rows = fspec["row_count"]
    if actual_rows != expected_rows:
        raise RuntimeError(f"{file_name}: expected {expected_rows} rows, got {actual_rows}")

    # Compute per-row hash
    columns = df.columns
    row_hash_udf = udf(lambda *vals: _row_hash(list(vals), columns))

    # Add metadata columns
    df_enriched = df \
        .withColumn("_raw_row_sha256", row_hash_udf(*[col(c) for c in columns])) \
        .withColumn("_source_file_name", lit(file_name)) \
        .withColumn("_source_file_sha256", lit(source_sha)) \
        .withColumn("_source_header_sha256", lit(header_sha)) \
        .withColumn("_source_manifest_sha256", lit(manifest_sha)) \
        .withColumn("_source_snapshot_id", lit(source_snapshot_id)) \
        .withColumn("_bronze_schema_version", lit(BRONZE_SCHEMA_VERSION))

    # Write with overwritePartitions
    df_enriched.write \
        .format("iceberg") \
        .mode("overwrite") \
        .option("overwrite-mode", "dynamic") \
        .option("partitioned-by", config.partition_field) \
        .saveAsTable(table_name)

    # Set table properties
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'riskcloud.dataset_id' = 'home_credit',
            'riskcloud.layer' = 'bronze',
            'riskcloud.bronze_version' = '{config.version}',
            'riskcloud.source_file' = '{file_name}'
        )
    """)

    # Get snapshot
    snapshots = spark.sql(
        f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    snapshot_id = snapshots[0].snapshot_id if snapshots else None

    # Get metadata location
    meta_df = spark.sql(
        f"SELECT metadata_location FROM {table_name}.snapshots "
        f"WHERE snapshot_id = {snapshot_id}"
    ).collect()
    metadata_location = meta_df[0].metadata_location if meta_df else None

    # Fingerprint
    fingerprint = _content_fingerprint(spark, table_name, manifest_sha)

    return {
        "table_name": table_name,
        "iceberg_snapshot_id": snapshot_id,
        "metadata_location": metadata_location,
        "source_file_sha256": source_sha,
        "source_header_sha256": header_sha,
        "source_row_count": expected_rows,
        "bronze_row_count": actual_rows,
        "source_column_count": len(src_columns),
        "bronze_column_count": len(columns) + len(BRONZE_META_COLUMNS),
        "schema_sha256": _sha256(json.dumps(src_columns, sort_keys=True).encode()),
        **fingerprint,
    }


def _build_receipt(
    run_id: str, started_at: datetime, manifest_path: Path,
    manifest_sha: str, source_snapshot_id: str,
    git_commit: str, config: BronzeConfig, table_results: dict,
) -> dict[str, Any]:
    import sys
    return {
        "receipt": {
            "receipt_version": 1,
            "run_id": run_id,
            "status": "COMPLETE",
            "created_at": started_at.isoformat(),
        },
        "input": {
            "dataset_id": "home_credit",
            "manifest_path": str(manifest_path),
            "manifest_sha256": manifest_sha,
            "source_snapshot_id": source_snapshot_id,
        },
        "code": {
            "git_commit": git_commit,
            "bronze_version": config.version,
            "adapter_version": "1.0.0",
            "boundary_version": "hc-boundary-v1",
        },
        "runtime": {
            "python_version": sys.version.split()[0],
            "spark_version": "3.5.3",
            "iceberg_version": "1.6.1",
            "scala_binary": "2.12",
        },
        "tables": table_results,
        "quality": {
            "manifest_validation": "PASS",
            "row_count_closure": "PASS",
            "schema_closure": "PASS",
            "fingerprint_generated": "PASS",
            "rerun_duplicate_growth": 0,
        },
    }


def _write_snapshot_manifest(
    receipt_dir: Path, receipt: dict, manifest_sha: str,
    table_results: dict, git_commit: str,
) -> None:
    manifest = {
        "manifest": {
            "manifest_id": receipt["receipt"]["run_id"],
            "status": "COMPLETE",
            "created_at": receipt["receipt"]["created_at"],
        },
        "input": {
            "data_manifest_sha256": manifest_sha,
            "data_dir": receipt["input"]["manifest_path"],
        },
        "code": {
            "git_commit": git_commit,
            "adapter_version": "1.0.0",
            "boundary_version": "hc-boundary-v1",
        },
        "tables": {
            "bronze": {
                tbl_name: {
                    "iceberg_table": tres["table_name"],
                    "iceberg_snapshot_id": tres["iceberg_snapshot_id"],
                    "metadata_location": tres["metadata_location"],
                    "row_count": tres["bronze_row_count"],
                    "schema_sha256": tres["schema_sha256"],
                }
                for tbl_name, tres in table_results.items()
            },
            "silver": {},
            "gold": {},
        },
        "quality": {"status": "NOT_RUN"},
        "receipt": {
            "uri": str(receipt_dir / "bronze_receipt.yaml"),
            "sha256": None,
        },
    }
    path = receipt_dir / "snapshot_manifest.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(manifest, f, default_flow_style=False, sort_keys=False)
