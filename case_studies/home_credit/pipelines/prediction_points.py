"""P1.4 — Prediction Points using frozen boundary, conformance-tested."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from case_studies.home_credit.pipelines.shared.governance import (
    create_table,
    get_snapshot_meta,
    publish,
    sha256,
    table_exists,
)
from riskcloud.adapters.home_credit.boundary import HomeCreditBoundaryConfig, build_prediction_point

UTC = timezone.utc


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
    target = pp_cfg["table"]

    # Deterministic snapshot_id from input, not run_id
    snapshot_id = sha256(
        json.dumps([manifest_sha, pp_cfg["version"], boundary.boundary_version], separators=(",", ":")).encode()
    )

    if receipt_dir.exists():
        raise RuntimeError(f"run directory exists: {receipt_dir}")

    sess = get_spark(app_name=f"riskcloud-pp-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        setup_namespaces(sess)
        silver = sess.table(cfg["input"]["silver_application"]).filter(f"_source_manifest_sha256 = '{manifest_sha}'")

        # Use frozen build_prediction_point per row
        def _pp_row(row):
            pp = build_prediction_point(row.asDict(), snapshot_id, boundary)
            return {
                "prediction_id": pp.prediction_id,
                "entity_id": pp.entity_id,
                "prediction_time": pp.prediction_time,
                "split": pp.split.value,
                "snapshot_id": pp.snapshot_id,
                "boundary_version": pp.boundary_version,
                "label": pp.label,
                "label_time": pp.label_time,
                "_source_manifest_sha256": manifest_sha,
                "_silver_snapshot_id": snapshot_id,
            }

        points = [_pp_row(row) for row in silver.collect()]
        df_out = sess.createDataFrame(points)

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
        if not table_exists(sess, target):
            create_table(sess, target, cols, "_source_manifest_sha256", props)
        df_out.writeTo(target).overwritePartitions()

        count = sess.table(target).count()
        meta = get_snapshot_meta(sess, target)
        receipt = {
            "receipt": {
                "receipt_version": 1,
                "run_id": run_id,
                "status": "COMPLETE",
                "created_at": started_at.isoformat(),
            },
            "input": {"manifest_sha256": manifest_sha, "snapshot_id": snapshot_id},
            "code": {
                "git_commit": git_commit,
                "pp_version": pp_cfg["version"],
                "boundary_version": boundary.boundary_version,
            },
            "output": {"table": target, "iceberg_snapshot_id": meta["snapshot_id"], "point_count": count},
        }
        sm = {
            "manifest": {"manifest_id": run_id, "status": "COMPLETE", "created_at": started_at.isoformat()},
            "code": {"git_commit": git_commit},
        }
        publish(receipt_dir, "prediction_points", sm, receipt)
        return receipt
    finally:
        if own_spark:
            sess.stop()
