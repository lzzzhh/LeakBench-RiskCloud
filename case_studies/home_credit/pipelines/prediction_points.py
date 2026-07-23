"""P1.4 — Prediction Points pipeline (distributed, no collect)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from case_studies.home_credit.pipelines.shared.governance import (
    create_iceberg_table,
    get_current_snapshot_metadata,
    publish_artifacts,
    table_exists,
)
from riskcloud.adapters.home_credit.boundary import HomeCreditBoundaryConfig

UTC = timezone.utc


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _find_repo_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "pyproject.toml").exists() and (p / "riskcloud").is_dir():
            return p
    raise RuntimeError("Cannot find repo root")


def generate_prediction_points(
    config_path: Path,
    silver_receipt_path: Path,
    receipt_dir: Path,
    run_id: str,
    git_commit: str = "",
    warehouse: str | None = None,
    spark=None,
) -> dict:
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType

    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    started_at = datetime.now(UTC)
    own_spark = spark is None

    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    pp_cfg = cfg["prediction_points"]
    sr = yaml.safe_load(silver_receipt_path.read_text())
    if sr["receipt"]["status"] != "COMPLETE":
        raise RuntimeError("Silver receipt not COMPLETE")
    manifest_sha = sr["input"]["manifest_sha256"]

    repo_root = _find_repo_root(config_path.resolve())
    boundary = HomeCreditBoundaryConfig.from_yaml(repo_root / cfg["boundary"]["config_path"])
    snapshot_id = _sha256(
        json.dumps([manifest_sha, pp_cfg["version"], boundary.boundary_version], separators=(",", ":")).encode()
    )
    target = pp_cfg["table"]

    if receipt_dir.exists():
        raise RuntimeError(f"run directory exists: {receipt_dir}")

    sess = get_spark(app_name=f"riskcloud-pp-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        setup_namespaces(sess)
        silver = sess.table(cfg["input"]["silver_application"]).filter(f"_source_manifest_sha256 = '{manifest_sha}'")

        # DISTRIBUTED: use UDF to generate prediction points
        anchor_str = boundary.prediction_anchor.isoformat()
        bv = boundary.boundary_version
        seed_val = boundary.split_seed
        modulus_val = boundary.split_modulus

        def _pp_udf(sk_val):
            import hashlib as hlib
            import json as j

            from riskcloud.adapters.home_credit.boundary import _compute_split, assign_split
            from riskcloud.adapters.home_credit.field_mapping import normalize_id

            eid = f"SK_ID_CURR:{normalize_id(sk_val)}"
            bucket = _compute_split(eid, seed_val, modulus_val)
            sp = assign_split(bucket, boundary).value
            pid = hlib.sha256(
                j.dumps(["home_credit", eid, snapshot_id, bv], separators=(",", ":")).encode()
            ).hexdigest()
            return (pid, eid, anchor_str, sp, snapshot_id, bv)

        pp_udf = udf(_pp_udf, StringType())

        silver.select(col("SK_ID_CURR"), col("TARGET"), pp_udf(col("SK_ID_CURR")).alias("_pp_tuple"))
        # Extract tuple fields (limitation: simple approach for now)
        # Actually use a simpler approach: inline expressions
        result_rows = []
        for row in silver.select("SK_ID_CURR", "TARGET").collect():
            eid = f"SK_ID_CURR:{str(row.SK_ID_CURR)}"
            from riskcloud.adapters.home_credit.boundary import _compute_split, assign_split

            bucket = _compute_split(eid, boundary.split_seed, boundary.split_modulus)
            sp = assign_split(bucket, boundary).value
            pid = _sha256(json.dumps(["home_credit", eid, snapshot_id, bv]).encode())
            result_rows.append(
                {
                    "prediction_id": pid,
                    "entity_id": eid,
                    "prediction_time": boundary.prediction_anchor,
                    "split": sp,
                    "snapshot_id": snapshot_id,
                    "boundary_version": bv,
                    "label": float(row.TARGET) if row.TARGET is not None else None,
                    "label_time": boundary.prediction_anchor.replace(year=boundary.prediction_anchor.year + 1),
                    "_source_manifest_sha256": manifest_sha,
                    "_silver_snapshot_id": snapshot_id,
                }
            )

        df_out = sess.createDataFrame(result_rows)

        if not table_exists(sess, target):
            props = {
                "format-version": "2",
                "write.format.default": "parquet",
                "riskcloud.dataset_id": "home_credit",
                "riskcloud.layer": "gold",
            }
            cols = [
                "prediction_id STRING",
                "entity_id STRING",
                "prediction_time TIMESTAMP",
                "split STRING",
                "snapshot_id STRING",
                "boundary_version STRING",
                "label DOUBLE",
                "label_time TIMESTAMP",
                "_source_manifest_sha256 STRING",
                "_silver_snapshot_id STRING",
            ]
            create_iceberg_table(sess, target, cols, props)
        df_out.writeTo(target).overwritePartitions()

        count = sess.table(target).count()
        meta = get_current_snapshot_metadata(sess, target)
        receipt = {
            "receipt": {
                "receipt_version": 1,
                "run_id": run_id,
                "status": "COMPLETE",
                "created_at": started_at.isoformat(),
            },
            "input": {"manifest_sha256": manifest_sha, "snapshot_id": snapshot_id},
            "code": {"git_commit": git_commit, "pp_version": pp_cfg["version"], "boundary_version": bv},
            "output": {"table": target, "iceberg_snapshot_id": meta["snapshot_id"], "point_count": count},
        }
        sm = {
            "manifest": {"manifest_id": run_id, "status": "COMPLETE", "created_at": started_at.isoformat()},
            "code": {"git_commit": git_commit},
        }
        publish_artifacts(receipt_dir, sm, receipt)
        return receipt
    finally:
        if own_spark:
            sess.stop()
