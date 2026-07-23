"""P1.2 — Bronze Ingestion Pipeline.

Writes raw CSV data into Iceberg Bronze tables.
- DDL CREATE TABLE for V2 metadata init
- DataFrameWriterV2 overwritePartitions
- fsync+replace atomic publication
- Iceberg partition spec verified via Java API
- Existing table contract fully validated
- Failure artifact on error
- Spark lifecycle: only stops sessions it created
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
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
REQUIRED_FILES_BY_KEY = {
    "application_train": APPLICATION_FILE,
    "bureau": BUREAU_FILE,
    "bureau_balance": BUREAU_BALANCE_FILE,
}
REQUIRED_PROPERTIES_TEMPLATE = {
    "format-version": "2",
    "write.format.default": "parquet",
    "riskcloud.dataset_id": "home_credit",
    "riskcloud.layer": "bronze",
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
    with open(path, "rb") as f:
        return _sha256(f.readline())


def _compute_source_snapshot_id(manifest_sha: str, bronze_version: str) -> str:
    return _sha256(json.dumps(["home_credit", manifest_sha, bronze_version], separators=(",", ":")).encode())


def _row_hash(row_values: list[str | None], columns: list[str]) -> str:
    obj = {col: val for col, val in zip(columns, row_values)}
    return _sha256(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode())


def _content_fingerprint(spark, table: str, manifest_sha: str) -> dict[str, Any]:
    df = spark.sql(f"SELECT _raw_row_sha256 FROM {table} WHERE _source_manifest_sha256 = '{manifest_sha}'")
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


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with open(tmp_path, "wb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        if hasattr(os, "O_DIRECTORY"):
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _atomic_write_yaml(path: Path, payload: dict) -> str:
    content = yaml.safe_dump(payload, default_flow_style=False, sort_keys=False).encode("utf-8")
    _atomic_write_bytes(path, content)
    return _sha256(content)


# -----------------------------------------------------------------
# Config
# -----------------------------------------------------------------


class BronzeConfig:
    def __init__(self, path: Path):
        with open(path) as f:
            data = yaml.safe_load(f)
        b = data["bronze"]
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
            raise ValueError(f"table keys: expected {EXPECTED_TABLE_KEYS}, got {table_keys}")
        self.tables = {}
        for key, tdef in tables.items():
            if not isinstance(tdef, dict):
                raise ValueError(f"tables.{key} must be a dict")
            fname, target = tdef.get("file"), tdef.get("table")
            ef = REQUIRED_FILES_BY_KEY.get(key)
            if fname != ef:
                raise ValueError(f"tables.{key}.file must be {ef!r}, got {fname!r}")
            et = REQUIRED_TARGETS.get(key)
            if target != et:
                raise ValueError(f"tables.{key}.table must be {et!r}, got {target!r}")
            self.tables[key] = {"file": fname, "table": target}

    @classmethod
    def from_yaml(cls, path: Path) -> BronzeConfig:
        return cls(path)


# -----------------------------------------------------------------
# Table lifecycle
# -----------------------------------------------------------------


def _table_exists(spark, table_name: str) -> bool:
    parts = table_name.split(".", 2)
    rows = spark.sql(f"SHOW TABLES IN `{parts[0]}`.`{parts[1]}`").collect()
    return any(r.tableName == parts[2] and not bool(r.isTemporary) for r in rows)


def _create_bronze_table(spark, table_name: str, config: BronzeConfig, file_name: str, source_columns: list[str]):
    cols = [f"`{c}` STRING" for c in source_columns]
    cols += [
        "_source_file_name STRING",
        "_source_file_sha256 STRING",
        "_source_header_sha256 STRING",
        "_source_manifest_sha256 STRING",
        "_source_snapshot_id STRING",
        "_bronze_schema_version INT",
        "_raw_row_sha256 STRING",
    ]
    ddl = (
        f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(cols) + "\n) "
        f"USING iceberg PARTITIONED BY (_source_manifest_sha256) "
        f"TBLPROPERTIES ('format-version'='2','write.format.default'='parquet',"
        f"'riskcloud.dataset_id'='home_credit','riskcloud.layer'='bronze',"
        f"'riskcloud.bronze_version'='{config.version}','riskcloud.source_file'='{file_name}')"
    )
    spark.sql(ddl)


def _validate_existing_table_contract(
    spark,
    table_name: str,
    config: BronzeConfig,
    file_name: str,
    source_columns: list[str],
):
    """Full contract validation. Raises RuntimeError on any mismatch."""
    from pyspark.sql.types import IntegerType, StringType

    # Schema
    schema = spark.table(table_name).schema
    expected_names = source_columns + BRONZE_META_COLUMNS
    actual_names = [f.name for f in schema.fields]
    if actual_names != expected_names:
        raise RuntimeError(f"{table_name}: field order mismatch; expected={expected_names}, actual={actual_names}")
    source_set = set(source_columns)
    for fld in schema.fields:
        if fld.name in source_set:
            if not isinstance(fld.dataType, StringType):
                raise RuntimeError(f"{table_name}.{fld.name}: source column must be STRING")
        elif fld.name == "_bronze_schema_version":
            if not isinstance(fld.dataType, IntegerType):
                raise RuntimeError(f"{table_name}._bronze_schema_version: must be INT")
        elif not isinstance(fld.dataType, StringType):
            raise RuntimeError(f"{table_name}.{fld.name}: metadata must be STRING")

    # Properties
    props_sql = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    props = {r.key: r.value for r in props_sql}
    required = dict(REQUIRED_PROPERTIES_TEMPLATE)
    required["riskcloud.bronze_version"] = config.version
    required["riskcloud.source_file"] = file_name
    for k, v in required.items():
        actual = props.get(k)
        if actual != v:
            raise RuntimeError(f"{table_name}: property {k} must be {v!r}, got {actual!r}")

    # Partition spec via Iceberg API
    try:
        jtable = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table_name)
        pfields = jtable.spec().fields()
        if pfields.size() != 1:
            raise RuntimeError(f"{table_name}: expected 1 partition field, got {pfields.size()}")
        pf = pfields.get(0)
        if pf.name() != "_source_manifest_sha256":
            raise RuntimeError(f"{table_name}: partition field must be _source_manifest_sha256, got {pf.name()}")
        if str(pf.transform()) != "identity":
            raise RuntimeError(f"{table_name}: partition transform must be identity, got {pf.transform()}")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"{table_name}: failed to validate partition spec: {exc}") from exc


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
    spark=None,
) -> dict[str, Any]:
    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    started_at = datetime.now(UTC)
    own_spark = spark is None

    ok, errors = validate_manifest(data_dir, manifest_path)
    if not ok:
        raise RuntimeError(f"Manifest validation failed: {errors}")

    with open(manifest_path, "rb") as f:
        manifest_raw = f.read()
    manifest_sha = _sha256(manifest_raw)
    manifest_data = yaml.safe_load(manifest_raw)

    file_specs = {fs["name"]: fs for fs in manifest_data.get("files", []) if fs.get("name", "") in TABLES_REQUIRED}
    if len(file_specs) != 3:
        raise RuntimeError(f"Manifest missing required files: {TABLES_REQUIRED - set(file_specs)}")

    source_snapshot_id = _compute_source_snapshot_id(manifest_sha, config.version)

    if receipt_dir.exists() and any(receipt_dir.iterdir()):
        raise RuntimeError(f"run directory must be empty: {receipt_dir}")

    sess = get_spark(app_name=f"riskcloud-bronze-{run_id}", warehouse=warehouse) if own_spark else spark
    primary_error: BaseException | None = None
    completed_tables: list[str] = []
    current_table: str | None = None

    try:
        setup_namespaces(sess)
        table_results: dict[str, dict[str, Any]] = {}
        for tbl_key, tbl_def in config.tables.items():
            current_table = tbl_key
            table_results[tbl_key] = _ingest_one_table(
                sess,
                config,
                tbl_key,
                tbl_def,
                file_specs,
                data_dir,
                manifest_sha,
                source_snapshot_id,
            )
            completed_tables.append(tbl_key)
        current_table = None

        receipt = _build_receipt(
            run_id, started_at, manifest_path, manifest_sha, source_snapshot_id, git_commit, config, table_results
        )
        _publish_success_artifacts(receipt_dir, receipt, table_results, git_commit, data_dir)
        return receipt

    except BaseException as exc:
        primary_error = exc
        try:
            _write_failure_artifact(
                receipt_dir, run_id, manifest_sha, manifest_path,
                data_dir, git_commit, config, completed_tables, current_table, exc,
            )
        except BaseException as artifact_err:
            # Don't mask the original error
            note = f"Additionally failed to write bronze_failure.yaml: {type(artifact_err).__name__}"
            add_note_fn = getattr(exc, "add_note", None)
            if callable(add_note_fn):
                add_note_fn(note)
        raise
    finally:
        if own_spark:
            try:
                sess.stop()
            except BaseException as stop_err:
                if primary_error is None:
                    raise RuntimeError("Spark shutdown failed after successful ingestion") from stop_err


def _ingest_one_table(
    spark,
    config: BronzeConfig,
    tbl_key: str,
    tbl_def: dict,
    file_specs: dict,
    data_dir: Path,
    manifest_sha: str,
    source_snapshot_id: str,
) -> dict[str, Any]:
    from pyspark.sql.functions import col, lit, udf
    from pyspark.sql.types import StringType, StructField, StructType

    file_name = tbl_def["file"]
    table_name = tbl_def["table"]
    fspec = file_specs[file_name]
    csv_path = data_dir / file_name

    if not csv_path.is_file():
        raise RuntimeError(f"Source file not found: {csv_path}")
    actual_file_sha = _sha256_file(csv_path)
    if actual_file_sha != fspec["sha256"]:
        raise RuntimeError(f"{file_name}: file SHA mismatch")
    actual_header_sha = _sha256_header_raw(csv_path)
    if actual_header_sha != fspec["header_sha256"]:
        raise RuntimeError(f"{file_name}: header SHA mismatch")

    with open(csv_path, newline="") as f:
        src_columns = next(csv.reader(f))
    if len(src_columns) != len(set(src_columns)):
        raise RuntimeError(f"{file_name}: duplicate column names")
    missing = RAW_REQUIRED_COLUMNS.get(file_name, set()) - set(src_columns)
    if missing:
        raise RuntimeError(f"{file_name}: missing required columns: {missing}")

    schema = StructType([StructField(c, StringType(), True) for c in src_columns])
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .option("multiLine", "false")
        .schema(schema)
        .csv(str(csv_path))
    )

    actual_rows = df.count()
    expected_rows = fspec["row_count"]
    if actual_rows != expected_rows:
        raise RuntimeError(f"{file_name}: expected {expected_rows} rows, got {actual_rows}")

    columns = df.columns
    row_hash_udf = udf(lambda *vals: _row_hash(list(vals), columns))

    if _table_exists(spark, table_name):
        _validate_existing_table_contract(spark, table_name, config, file_name, src_columns)
        before_rows = spark.sql(
            f"SELECT COUNT(*) FROM {table_name} WHERE _source_manifest_sha256 = '{manifest_sha}'"
        ).collect()[0][0]
    else:
        _create_bronze_table(spark, table_name, config, file_name, src_columns)
        before_rows = 0

    df_enriched = (
        df.withColumn("_raw_row_sha256", row_hash_udf(*[col(c) for c in columns]))
        .withColumn("_source_file_name", lit(file_name))
        .withColumn("_source_file_sha256", lit(actual_file_sha))
        .withColumn("_source_header_sha256", lit(actual_header_sha))
        .withColumn("_source_manifest_sha256", lit(manifest_sha))
        .withColumn("_source_snapshot_id", lit(source_snapshot_id))
        .withColumn("_bronze_schema_version", lit(BRONZE_SCHEMA_VERSION))
        # Explicit field order matching DDL
        .select(
            *src_columns,
            "_source_file_name",
            "_source_file_sha256",
            "_source_header_sha256",
            "_source_manifest_sha256",
            "_source_snapshot_id",
            "_bronze_schema_version",
            "_raw_row_sha256",
        )
    )

    df_enriched.writeTo(table_name).overwritePartitions()

    after_rows = spark.sql(
        f"SELECT COUNT(*) FROM {table_name} WHERE _source_manifest_sha256 = '{manifest_sha}'"
    ).collect()[0][0]
    if after_rows != expected_rows:
        raise RuntimeError(f"{table_name}: after write {after_rows} rows != source {expected_rows}")

    snapshots = spark.sql(
        f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    snapshot_id = snapshots[0].snapshot_id if snapshots else None
    if snapshot_id is None:
        raise RuntimeError(f"{table_name}: no snapshot after write")

    # Current snapshot and metadata via Iceberg Java API
    try:
        jtable = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table_name
        )
        jtable.refresh()
        java_snapshot = jtable.currentSnapshot()
        if java_snapshot is None:
            raise RuntimeError(f"{table_name}: Java current snapshot is empty")
        java_snapshot_id = java_snapshot.snapshotId()
        if java_snapshot_id != snapshot_id:
            raise RuntimeError(
                f"{table_name}: Java snapshot {java_snapshot_id} != SQL snapshot {snapshot_id}"
            )
        ops = jtable.operations()
        current_meta = ops.refresh()
        metadata_snap = current_meta.currentSnapshot()
        if metadata_snap is None:
            raise RuntimeError(f"{table_name}: TableMetadata has no current snapshot")
        if metadata_snap.snapshotId() != snapshot_id:
            raise RuntimeError(
                f"{table_name}: TableMetadata snapshot {metadata_snap.snapshotId()} "
                f"!= committed snapshot {snapshot_id}"
            )
        metadata_location = current_meta.metadataFileLocation()
        if not metadata_location:
            raise RuntimeError(f"{table_name}: current metadata location is empty")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"{table_name}: failed to load current metadata: {exc}") from exc

    # Verify metadata file exists via Hadoop FS
    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        mp = jvm.org.apache.hadoop.fs.Path(metadata_location)
        fs = mp.getFileSystem(conf)
        if not fs.exists(mp):
            raise RuntimeError(f"{table_name}: metadata file does not exist: {metadata_location}")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"{table_name}: failed to verify metadata: {exc}") from exc

    # Schema SHA from actual state (re-verify contract after write)
    _validate_existing_table_contract(spark, table_name, config, file_name, src_columns)
    actual_schema = spark.table(table_name).schema
    schema_payload = {
        "spark_schema_json": actual_schema.json(),
        "partition_spec": [{"name": "_source_manifest_sha256", "transform": "identity"}],
        "table_properties": {
            "format-version": "2",
            "write.format.default": "parquet",
            "riskcloud.dataset_id": "home_credit",
            "riskcloud.layer": "bronze",
            "riskcloud.bronze_version": config.version,
            "riskcloud.source_file": file_name,
        },
    }
    schema_sha = _sha256(json.dumps(schema_payload, sort_keys=True, separators=(",", ":")).encode())

    fingerprint = _content_fingerprint(spark, table_name, manifest_sha)
    rerun_growth = after_rows - expected_rows

    return {
        "table_name": table_name,
        "iceberg_snapshot_id": snapshot_id,
        "metadata_location": metadata_location,
        "source_file_sha256": actual_file_sha,
        "source_header_sha256": actual_header_sha,
        "source_row_count": expected_rows,
        "bronze_row_count": after_rows,
        "partition_row_count_before": before_rows,
        "partition_row_count_after": after_rows,
        "rerun_duplicate_growth": rerun_growth,
        "source_column_count": len(src_columns),
        "bronze_column_count": len(columns) + len(BRONZE_META_COLUMNS),
        "schema_sha256": schema_sha,
        "fingerprint_verified": True,
        **fingerprint,
    }


def _build_receipt(
    run_id, started_at, manifest_path, manifest_sha, source_snapshot_id, git_commit, config, table_results
):
    total_growth = sum(t["rerun_duplicate_growth"] for t in table_results.values())
    return {
        "receipt": {"receipt_version": 1, "run_id": run_id, "status": "COMPLETE", "created_at": started_at.isoformat()},
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
            "rerun_duplicate_growth": total_growth,
        },
    }


def _publish_success_artifacts(receipt_dir, receipt, table_results, git_commit, data_dir):
    """Atomically publish snapshot manifest + receipt via staging directory."""
    if receipt_dir.exists():
        raise RuntimeError(f"run directory already exists: {receipt_dir}")

    snapshot_manifest = {
        "manifest": {
            "manifest_id": receipt["receipt"]["run_id"],
            "status": "COMPLETE",
            "created_at": receipt["receipt"]["created_at"],
        },
        "input": {"data_manifest_sha256": receipt["input"]["manifest_sha256"], "data_dir": str(data_dir)},
        "code": {"git_commit": git_commit, "adapter_version": "1.0.0", "boundary_version": "hc-boundary-v1"},
        "tables": {
            "bronze": {
                tn: {
                    "iceberg_table": tr["table_name"],
                    "iceberg_snapshot_id": tr["iceberg_snapshot_id"],
                    "metadata_location": tr["metadata_location"],
                    "row_count": tr["bronze_row_count"],
                    "schema_sha256": tr["schema_sha256"],
                }
                for tn, tr in table_results.items()
            },
            "silver": {
                k: {"iceberg_table": None, "iceberg_snapshot_id": None, "row_count": None}
                for k in ["application_train", "bureau", "bureau_balance"]
            },
            "gold": {
                k: {"iceberg_table": None, "iceberg_snapshot_id": None, "row_count": None}
                for k in ["prediction_points", "feature_values", "strict_view", "full_view"]
            },
        },
        "quality": {"status": "NOT_RUN"},
        "receipt": {"uri": str(receipt_dir / "bronze_receipt.yaml"), "sha256": None},
    }

    stage_dir = receipt_dir.with_name(f".{receipt_dir.name}.{os.getpid()}.staging")
    if stage_dir.exists():
        shutil.rmtree(stage_dir)

    try:
        stage_dir.mkdir(parents=True)
        snapshot_sha = _atomic_write_yaml(stage_dir / "snapshot_manifest.yaml", snapshot_manifest)
        receipt["quality"]["snapshot_manifest_sha256"] = snapshot_sha
        _atomic_write_yaml(stage_dir / "bronze_receipt.yaml", receipt)
        _fsync_directory(stage_dir)
        os.replace(stage_dir, receipt_dir)
        _fsync_directory(receipt_dir.parent)
    finally:
        if stage_dir.exists():
            shutil.rmtree(stage_dir)


def _fsync_directory(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_failure_artifact(
    receipt_dir, run_id, manifest_sha, manifest_path, data_dir, git_commit, config, completed_tables, current_table, exc
):
    failure_dir = receipt_dir.with_name(f"{receipt_dir.name}.failed")
    failure_dir.mkdir(parents=True, exist_ok=True)
    failure = {
        "failure": {
            "version": 1,
            "run_id": run_id,
            "status": "FAILED",
            "failed_at": datetime.now(UTC).isoformat(),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        },
        "input": {"data_dir": str(data_dir), "manifest_path": str(manifest_path), "manifest_sha256": manifest_sha},
        "code": {"git_commit": git_commit, "bronze_version": config.version},
        "progress": {"completed_tables": completed_tables, "failed_table": current_table},
    }
    _atomic_write_yaml(failure_dir / "bronze_failure.yaml", failure)
