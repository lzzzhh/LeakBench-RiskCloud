"""Phase 1 shared governance module — Iceberg lifecycle, publication, lineage.

All P1.x stages use these helpers for:
  - CREATE TABLE DDL with full contract
  - SHOW TABLES fail-closed table existence
  - Schema/partition/properties validation
  - Current snapshot/metadata verification
  - Atomic publication with staging directory
  - Failure artifact isolation
"""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path
from typing import Any

import yaml


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with open(tmp, "wb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        if hasattr(os, "O_DIRECTORY"):
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    finally:
        if tmp.exists():
            tmp.unlink()


def atomic_write_yaml(path: Path, payload: dict) -> str:
    content = yaml.safe_dump(payload, default_flow_style=False, sort_keys=False).encode("utf-8")
    atomic_write_bytes(path, content)
    return _sha256(content)


def fsync_directory(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# -----------------------------------------------------------------
# Table lifecycle
# -----------------------------------------------------------------


def table_exists(spark, table_name: str) -> bool:
    parts = table_name.split(".", 2)
    rows = spark.sql(f"SHOW TABLES IN `{parts[0]}`.`{parts[1]}`").collect()
    return any(r.tableName == parts[2] and not bool(r.isTemporary) for r in rows)


def create_iceberg_table(spark, table_name: str, column_defs: list[str], properties: dict[str, str]):
    cols = ",\n  ".join(column_defs)
    props = ", ".join(f"'{k}'='{v}'" for k, v in properties.items())
    ddl = f"CREATE TABLE {table_name} (\n  {cols}\n) USING iceberg TBLPROPERTIES ({props})"
    spark.sql(ddl)


def validate_table_contract(
    spark,
    table_name: str,
    expected_names: list[str],
    expected_types: dict[str, str],
    required_properties: dict[str, str],
):
    """Full contract validation: schema names/types/order, table properties."""
    from pyspark.sql.types import IntegerType, StringType

    schema = spark.table(table_name).schema
    actual_names = [f.name for f in schema.fields]
    if actual_names != expected_names:
        raise RuntimeError(f"{table_name}: field order mismatch; expected={expected_names}, actual={actual_names}")

    for fld in schema.fields:
        expected = expected_types.get(fld.name)
        if expected == "STRING" and not isinstance(fld.dataType, StringType):
            raise RuntimeError(f"{table_name}.{fld.name}: expected STRING")
        if expected == "INT" and not isinstance(fld.dataType, IntegerType):
            raise RuntimeError(f"{table_name}.{fld.name}: expected INT")

    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()}
    for k, v in required_properties.items():
        if props.get(k) != v:
            raise RuntimeError(f"{table_name}: property {k} must be {v!r}, got {props.get(k)!r}")


def get_current_snapshot_metadata(spark, table_name: str) -> dict[str, Any]:
    """Get current snapshot, metadata location, and cross-validate via Iceberg API."""
    jvm = spark._jvm
    jtable = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table_name)
    cls_name = str(jtable.getClass().getName())
    cl = jtable.getClass().getClassLoader()
    if cl is None:
        raise RuntimeError(f"{table_name}: no ClassLoader")
    has_ops = cl.loadClass("org.apache.iceberg.HasTableOperations")
    if not has_ops.isAssignableFrom(jtable.getClass()):
        raise RuntimeError(f"{table_name}: no HasTableOperations")
    ops = jtable.operations()
    meta = ops.refresh()
    jtable.refresh()
    snap = jtable.currentSnapshot()
    if snap is None:
        raise RuntimeError(f"{table_name}: no current snapshot")
    snap_id = snap.snapshotId()
    loc = meta.metadataFileLocation()
    if not loc:
        raise RuntimeError(f"{table_name}: no metadata location")
    # Verify file exists
    mp = jvm.org.apache.hadoop.fs.Path(loc)
    fs = mp.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(mp):
        raise RuntimeError(f"{table_name}: metadata file not found: {loc}")
    return {"snapshot_id": snap_id, "metadata_location": loc, "runtime_class": cls_name}


# -----------------------------------------------------------------
# Publication
# -----------------------------------------------------------------


def publish_artifacts(
    receipt_dir: Path,
    snapshot_manifest: dict,
    receipt: dict,
) -> tuple[str, str]:
    if receipt_dir.exists():
        raise RuntimeError(f"run directory exists: {receipt_dir}")
    stage = receipt_dir.with_name(f".{receipt_dir.name}.{os.getpid()}.staging")
    if stage.exists():
        shutil.rmtree(stage)
    try:
        stage.mkdir(parents=True)
        sm_sha = atomic_write_yaml(stage / "snapshot_manifest.yaml", snapshot_manifest)
        receipt["quality"]["snapshot_manifest_sha256"] = sm_sha
        r_sha = atomic_write_yaml(stage / "receipt.yaml", receipt)
        fsync_directory(stage)
        os.replace(stage, receipt_dir)
        fsync_directory(receipt_dir.parent)
        return sm_sha, r_sha
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def write_failure_artifact(
    receipt_dir: Path,
    run_id: str,
    exc: BaseException,
    completed_tables: list[str],
    current_table: str | None,
    input_info: dict,
    code_info: dict,
) -> None:
    failure_dir = receipt_dir.with_name(f"{receipt_dir.name}.failed")
    if failure_dir.exists():
        raise RuntimeError(f"failure dir exists: {failure_dir}")
    failure_dir.mkdir(parents=True)
    failure = {
        "failure": {
            "version": 1,
            "run_id": run_id,
            "status": "FAILED",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        },
        "input": input_info,
        "code": code_info,
        "progress": {"completed_tables": completed_tables, "failed_table": current_table},
    }
    atomic_write_yaml(failure_dir / "bronze_failure.yaml", failure)
