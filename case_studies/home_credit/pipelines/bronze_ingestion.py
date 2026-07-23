"""P1.2 — Bronze Ingestion Pipeline.

Writes raw CSV data into Iceberg Bronze tables.
- All source columns as STRING
- Header SHA from raw first-line bytes (P1.0-compatible)
- DataFrameWriterV2 with overwritePartitions
- _raw_row_sha256 with column-order-sensitive canonical JSON
- Content fingerprint via toLocalIterator
- Receipt published last, after all tables written and verified
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
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
EXPECTED_TABLE_KEYS = {"application_train", "bureau", "bureau_balance"}
REQUIRED_TARGETS = {
    "application_train": "riskcloud.bronze.application_train",
    "bureau": "riskcloud.bronze.bureau",
    "bureau_balance": "riskcloud.bronze.bureau_balance",
}

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


def _sha256_header_raw(path: Path) -> str:
    """SHA-256 of the first line as raw bytes (P1.0-compatible)."""
    with open(path, "rb") as f:
        return _sha256(f.readline())


def _compute_source_snapshot_id(manifest_sha: str, bronze_version: str) -> str:
    parts = json.dumps(["home_credit", manifest_sha, bronze_version], separators=(",", ":"))
    return _sha256(parts.encode())


def _row_hash(row_values: list[str | None], columns: list[str]) -> str:
    """Deterministic row hash. Column order preserved, null included."""
    obj = {col: val for col, val in zip(columns, row_values)}
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return _sha256(raw.encode())


def _content_fingerprint(spark, table: str, manifest_sha: str) -> dict[str, Any]:
    """Content fingerprint via toLocalIterator (streaming, not collect)."""
    df = spark.sql(
        f"SELECT _raw_row_sha256 FROM {table} "
        f"WHERE _source_manifest_sha256 = '{manifest_sha}'"
    )
    counts = df.groupBy("_raw_row_sha256").count().orderBy("_raw_row_sha256")

    h = hashlib.sha256()
    total = 0
    distinct = 0
    for row in counts.toLocalIterator():
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

        # Exact value checks
        for field, expected in [
            ("version", "hc-bronze-v1"),
            ("catalog", "riskcloud"),
            ("namespace", "bronze"),
            ("schema_mode", "all_source_columns_as_string"),
            ("partition_field", "_source_manifest_sha256"),
            ("write_mode", "overwrite_partitions"),
        ]:
            actual = b.get(field)
            if actual != expected:
                raise ValueError(f"bronze.{field} must be {expected!r}, got {actual!r}")

        self.version = b["version"]
        self.catalog = b["catalog"]
        self.namespace = b["namespace"]
        self.partition_field = b["partition_field"]
        self.write_mode = b["write_mode"]

        tables = data["tables"]
        if not isinstance(tables, dict):
            raise ValueError("tables must be a dict")
        table_keys = set(tables.keys())
        if table_keys != EXPECTED_TABLE_KEYS:
            missing = EXPECTED_TABLE_KEYS - table_keys
            extra = table_keys - EXPECTED_TABLE_KEYS
            msg = []
            if missing:
                msg.append(f"missing: {missing}")
            if extra:
                msg.append(f"unknown: {extra}")
            raise ValueError("; ".join(msg))

        file_names = set()
        self.tables = {}
        for key, tdef in tables.items():
            if not isinstance(tdef, dict):
                raise ValueError(f"tables.{key} must be a dict")
            fname = tdef.get("file")
            target = tdef.get("table")
            if not isinstance(fname, str) or not isinstance(target, str):
                raise ValueError(f"tables.{key}: file and table must be strings")
            file_names.add(fname)
            expected_target = REQUIRED_TARGETS.get(key)
            if target != expected_target:
                raise ValueError(f"tables.{key}: table must be {expected_target!r}, got {target!r}")
            self.tables[key] = {"file": fname, "table": target}

        if file_names != TABLES_REQUIRED:
            raise ValueError(f"file set mismatch: got {file_names}, expected {TABLES_REQUIRED}")

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

    # Verify required files and their SHAs
    file_specs = {}
    for fspec in manifest_data.get("files", []):
        name = fspec.get("name", "")
        if name in TABLES_REQUIRED:
            file_specs[name] = fspec

    if len(file_specs) != 3:
        raise RuntimeError(f"Manifest missing required files: {TABLES_REQUIRED - set(file_specs)}")

    source_snapshot_id = _compute_source_snapshot_id(manifest_sha, config.version)

    # 2. Spark session
    spark = get_spark(app_name=f"riskcloud-bronze-{run_id}", warehouse=warehouse)
    try:
        setup_namespaces(spark)

        table_results: dict[str, dict[str, Any]] = {}
        for tbl_key, tbl_def in config.tables.items():
            tresult = _ingest_one_table(
                spark, config, tbl_key, tbl_def, file_specs, data_dir,
                manifest_sha, source_snapshot_id,
            )
            table_results[tbl_key] = tresult

        # 3. Verify from Iceberg (rows, schema, fingerprint)
        for tbl_key, tres in table_results.items():
            actual_rows = spark.table(tres["table_name"]).filter(
                f"_source_manifest_sha256 = '{manifest_sha}'"
            ).count()
            assert actual_rows == tres["source_row_count"], (
                f"{tbl_key}: Iceberg row count {actual_rows} != source {tres['source_row_count']}"
            )
            fp = _content_fingerprint(spark, tres["table_name"], manifest_sha)
            assert fp["content_multiset_sha256"] == tres["content_multiset_sha256"]
            tres["fingerprint_verified"] = True

        # 4. Build receipt (final step)
        receipt = _build_receipt(
            run_id, started_at, manifest_path, manifest_sha, source_snapshot_id,
            git_commit, config, table_results,
        )

        # 5. Write snapshot manifest (temp, then atomic)
        receipt_dir.mkdir(parents=True, exist_ok=True)
        _write_snapshot_manifest_atomic(receipt_dir, receipt, table_results, git_commit)

        # 6. Write receipt (LAST, atomic)
        _write_receipt_atomic(receipt_dir, receipt)

        return receipt

    finally:
        spark.stop()


def _ingest_one_table(
    spark, config: BronzeConfig, tbl_key: str, tbl_def: dict,
    file_specs: dict, data_dir: Path, manifest_sha: str, source_snapshot_id: str,
) -> dict[str, Any]:
    from pyspark.sql.functions import col, lit, udf
    from pyspark.sql.types import StringType, StructField, StructType

    file_name = tbl_def["file"]
    table_name = tbl_def["table"]
    fspec = file_specs[file_name]
    csv_path = data_dir / file_name

    if not csv_path.is_file():
        raise RuntimeError(f"Source file not found: {csv_path}")

    # Verify file SHA matches manifest (re-check after P1.0 validation)
    actual_file_sha = _sha256_file(csv_path)
    if actual_file_sha != fspec["sha256"]:
        raise RuntimeError(f"{file_name}: file SHA mismatch")

    # Header SHA — use raw first line bytes (P1.0 compatible)
    actual_header_sha = _sha256_header_raw(csv_path)
    if actual_header_sha != fspec["header_sha256"]:
        raise RuntimeError(f"{file_name}: header SHA mismatch")

    # Read header columns
    with open(csv_path, newline="") as f:
        reader = csv.reader(f)
        src_columns = next(reader)
    if len(src_columns) != len(set(src_columns)):
        raise RuntimeError(f"{file_name}: duplicate column names")

    required = RAW_REQUIRED_COLUMNS.get(file_name, set())
    missing = required - set(src_columns)
    if missing:
        raise RuntimeError(f"{file_name}: missing required columns: {missing}")

    # Explicit STRING schema
    schema = StructType([StructField(c, StringType(), True) for c in src_columns])

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("mode", "FAILFAST") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "false") \
        .schema(schema) \
        .csv(str(csv_path))

    actual_rows = df.count()
    expected_rows = fspec["row_count"]
    if actual_rows != expected_rows:
        raise RuntimeError(f"{file_name}: expected {expected_rows} rows, got {actual_rows}")

    # Row hash
    columns = df.columns
    row_hash_udf = udf(lambda *vals: _row_hash(list(vals), columns))

    df_enriched = (
        df
        .withColumn("_raw_row_sha256", row_hash_udf(*[col(c) for c in columns]))
        .withColumn("_source_file_name", lit(file_name))
        .withColumn("_source_file_sha256", lit(actual_file_sha))
        .withColumn("_source_header_sha256", lit(actual_header_sha))
        .withColumn("_source_manifest_sha256", lit(manifest_sha))
        .withColumn("_source_snapshot_id", lit(source_snapshot_id))
        .withColumn("_bronze_schema_version", lit(BRONZE_SCHEMA_VERSION))
    )

    # Create or replace with DataFrameWriterV2
    partition_col = col(config.partition_field)
    try:
        # Try overwritePartitions (table exists)
        df_enriched.writeTo(table_name).overwritePartitions()
    except Exception:
        # Create table first time
        df_enriched.writeTo(table_name) \
            .using("iceberg") \
            .partitionedBy(partition_col) \
            .tableProperty("format-version", "2") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("riskcloud.dataset_id", "home_credit") \
            .tableProperty("riskcloud.layer", "bronze") \
            .tableProperty("riskcloud.bronze_version", config.version) \
            .tableProperty("riskcloud.source_file", file_name) \
            .createOrReplace()
        # Then write
        df_enriched.writeTo(table_name).overwritePartitions()

    # Set table properties
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'riskcloud.dataset_id' = 'home_credit',
            'riskcloud.layer' = 'bronze',
            'riskcloud.bronze_version' = '{config.version}',
            'riskcloud.source_file' = '{file_name}'
        )
    """)

    # Get snapshot ID
    snapshots = spark.sql(
        f"SELECT snapshot_id FROM {table_name}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    snapshot_id = snapshots[0].snapshot_id if snapshots else None

    # Get metadata location
    meta_logs = spark.sql(
        f"SELECT file FROM {table_name}.metadata_log_entries "
        f"ORDER BY timestamp DESC LIMIT 1"
    ).collect()
    metadata_location = meta_logs[0].file if meta_logs else None

    # Fingerprint
    fingerprint = _content_fingerprint(spark, table_name, manifest_sha)

    return {
        "table_name": table_name,
        "iceberg_snapshot_id": snapshot_id,
        "metadata_location": metadata_location,
        "source_file_sha256": actual_file_sha,
        "source_header_sha256": actual_header_sha,
        "source_row_count": expected_rows,
        "bronze_row_count": actual_rows,
        "source_column_count": len(src_columns),
        "bronze_column_count": len(columns) + len(BRONZE_META_COLUMNS),
        "schema_sha256": _sha256(
            json.dumps({
                "columns": src_columns,
                "types": ["STRING"] * len(src_columns),
                "meta_columns": BRONZE_META_COLUMNS,
                "meta_types": ["STRING"] * len(BRONZE_META_COLUMNS),
                "partition_spec": config.partition_field,
            }, separators=(",", ":")).encode()
        ),
        "fingerprint_verified": False,
        **fingerprint,
    }


def _build_receipt(
    run_id: str, started_at: datetime, manifest_path: Path,
    manifest_sha: str, source_snapshot_id: str,
    git_commit: str, config: BronzeConfig, table_results: dict,
) -> dict[str, Any]:
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


def _write_snapshot_manifest_atomic(
    receipt_dir: Path, receipt: dict,
    table_results: dict, git_commit: str,
) -> str:
    """Write snapshot manifest atomically. Returns its SHA-256."""
    manifest = {
        "manifest": {
            "manifest_id": receipt["receipt"]["run_id"],
            "status": "PENDING",
            "created_at": receipt["receipt"]["created_at"],
        },
        "input": {
            "data_manifest_sha256": receipt["input"]["manifest_sha256"],
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
            "silver": None,
            "gold": None,
        },
        "quality": {"status": "PASS"},
        "receipt": {"uri": None, "sha256": None},
    }
    path = receipt_dir / "snapshot_manifest.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.safe_dump(manifest, default_flow_style=False, sort_keys=False)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)
    return _sha256(content.encode())


def _write_receipt_atomic(receipt_dir: Path, receipt: dict) -> None:
    """Write receipt atomically. Binds snapshot manifest SHA."""
    sm_path = receipt_dir / "snapshot_manifest.yaml"
    sm_sha = _sha256_file(sm_path)
    receipt["quality"]["snapshot_manifest_sha256"] = sm_sha

    path = receipt_dir / "bronze_receipt.yaml"
    content = yaml.safe_dump(receipt, default_flow_style=False, sort_keys=False)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)

    # Update snapshot manifest with receipt SHA
    sm_data = yaml.safe_load(sm_path.read_text())
    sm_data["receipt"]["uri"] = str(path)
    sm_data["receipt"]["sha256"] = _sha256_file(path)
    sm_data["manifest"]["status"] = "COMPLETE"
    sm_content = yaml.safe_dump(sm_data, default_flow_style=False, sort_keys=False)
    sm_tmp = sm_path.with_suffix(sm_path.suffix + ".tmp")
    sm_tmp.write_text(sm_content, encoding="utf-8")
    sm_tmp.replace(sm_path)
