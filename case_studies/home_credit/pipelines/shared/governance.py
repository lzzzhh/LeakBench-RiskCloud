"""Phase 1 shared governance — Iceberg lifecycle, publication, lineage."""

from __future__ import annotations

import hashlib
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import yaml

UTC = timezone.utc


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write(path: Path, content: bytes) -> None:
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
    atomic_write(path, content)
    return sha256(content)


def fsync_dir(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def table_exists(spark, table_name: str) -> bool:
    parts = table_name.split(".", 2)
    rows = spark.sql(f"SHOW TABLES IN `{parts[0]}`.`{parts[1]}`").collect()
    return any(r.tableName == parts[2] and not bool(r.isTemporary) for r in rows)


def create_table(spark, table_name: str, col_defs: list[str], partition_field: str, props: dict[str, str]):
    cols = ",\n  ".join(col_defs)
    prop_str = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
    spark.sql(
        f"CREATE TABLE {table_name} (\n  {cols}\n) USING iceberg "
        f"PARTITIONED BY ({partition_field}) TBLPROPERTIES ({prop_str})"
    )


def validate_contract(
    spark, table_name: str, expected_names: list[str], type_map: dict[str, str], required_props: dict[str, str]
):
    from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

    schema = spark.table(table_name).schema
    actual_names = [f.name for f in schema.fields]
    if actual_names != expected_names:
        raise RuntimeError(f"{table_name}: field order mismatch")
    type_lookup = {"STRING": StringType, "INT": IntegerType, "DOUBLE": DoubleType, "TIMESTAMP": TimestampType}
    for fld in schema.fields:
        exp = type_map.get(fld.name, "STRING")
        exp_cls = type_lookup.get(exp)
        if exp_cls and not isinstance(fld.dataType, exp_cls):
            raise RuntimeError(f"{table_name}.{fld.name}: expected {exp}")
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()}
    for k, v in required_props.items():
        if props.get(k) != v:
            raise RuntimeError(f"{table_name}: property {k} must be {v!r}")


def get_snapshot_meta(spark, table_name: str) -> dict:
    jvm = spark._jvm
    jt = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table_name)
    jt.refresh()
    snap = jt.currentSnapshot()
    if snap is None:
        raise RuntimeError(f"{table_name}: no snapshot")
    sid = snap.snapshotId()
    cl = jt.getClass().getClassLoader()
    if cl is None:
        raise RuntimeError(f"{table_name}: no ClassLoader")
    ops_cls = cl.loadClass("org.apache.iceberg.HasTableOperations")
    if not ops_cls.isAssignableFrom(jt.getClass()):
        raise RuntimeError(f"{table_name}: no HasTableOperations")
    meta = jt.operations().refresh()
    loc = meta.metadataFileLocation()
    if not loc:
        raise RuntimeError(f"{table_name}: no metadata location")
    mp = jvm.org.apache.hadoop.fs.Path(loc)
    if not mp.getFileSystem(spark._jsc.hadoopConfiguration()).exists(mp):
        raise RuntimeError(f"{table_name}: metadata file missing")
    return {"snapshot_id": sid, "metadata_location": loc, "runtime_class": str(jt.getClass().getName())}


def publish(receipt_dir: Path, stage: str, snapshot_manifest: dict, receipt: dict) -> tuple[str, str]:
    if receipt_dir.exists():
        raise RuntimeError(f"run directory exists: {receipt_dir}")
    stage_dir = receipt_dir.with_name(f".{receipt_dir.name}.{os.getpid()}.staging")
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    try:
        stage_dir.mkdir(parents=True)
        sm_sha = atomic_write_yaml(stage_dir / "snapshot_manifest.yaml", snapshot_manifest)
        receipt["quality"]["snapshot_manifest_sha256"] = sm_sha
        r_sha = atomic_write_yaml(stage_dir / f"{stage}_receipt.yaml", receipt)
        fsync_dir(stage_dir)
        os.replace(stage_dir, receipt_dir)
        fsync_dir(receipt_dir.parent)
        return sm_sha, r_sha
    finally:
        if stage_dir.exists():
            shutil.rmtree(stage_dir)


def write_failure(
    receipt_dir: Path,
    stage: str,
    run_id: str,
    exc: BaseException,
    completed: list[str],
    current: str | None,
    input_info: dict,
    code_info: dict,
):
    fd = receipt_dir.with_name(f"{receipt_dir.name}.failed")
    if fd.exists():
        raise RuntimeError(f"failure dir exists: {fd}")
    fd.mkdir(parents=True)
    failure = {
        "failure": {
            "version": 1,
            "run_id": run_id,
            "status": "FAILED",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "failed_at": datetime.now(UTC).isoformat(),
        },
        "input": input_info,
        "code": code_info,
        "progress": {"completed_tables": completed, "failed_table": current},
    }
    atomic_write_yaml(fd / f"{stage}_failure.yaml", failure)
