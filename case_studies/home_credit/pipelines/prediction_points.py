"""P1.4 — Prediction Points Pipeline.

Generates gold.prediction_points from silver.application_train using
the Phase 1 Adapter/Boundary. Each prediction point binds a snapshot,
boundary version, split, label, and label_time.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from riskcloud.adapters.home_credit.adapter import HomeCreditAdapter
from riskcloud.adapters.home_credit.boundary import HomeCreditBoundaryConfig

UTC = timezone.utc


def generate_prediction_points(
    config_path: Path,
    silver_receipt_path: Path,
    receipt_dir: Path,
    run_id: str,
    git_commit: str = "",
    warehouse: str | None = None,
    spark=None,
) -> dict[str, Any]:
    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    started_at = datetime.now(UTC)
    own_spark = spark is None

    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    pp_cfg = cfg["prediction_points"]
    boundary_cfg = cfg["boundary"]
    input_cfg = cfg["input"]

    silver_table = input_cfg["silver_application"]
    target_table = pp_cfg["table"]
    snapshot_id = run_id

    # Load boundary config
    repo_root = config_path.parents[2]
    boundary_config = HomeCreditBoundaryConfig.from_yaml(repo_root / boundary_cfg["config_path"])

    # Validate silver receipt
    silver_receipt = yaml.safe_load(silver_receipt_path.read_text())
    if silver_receipt["receipt"]["status"] != "COMPLETE":
        raise RuntimeError("Silver receipt not COMPLETE")
    manifest_sha = silver_receipt["input"]["manifest_sha256"]

    sess = get_spark(app_name=f"riskcloud-pp-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        setup_namespaces(sess)

        # Create adapter
        adapter = HomeCreditAdapter(
            snapshot_id=snapshot_id,
            manifest_path=silver_receipt_path,
            data_dir=Path("."),
            ingested_at=started_at,
            boundary_config=boundary_config,
        )

        # Read silver data
        df_silver = sess.table(silver_table)
        rows = df_silver.collect()

        # Generate prediction points
        points = []
        for row in rows:
            rec = row.asDict()
            rec["__source_table__"] = "application_train"
            pp = adapter.define_prediction_boundary(rec)
            points.append(
                {
                    "prediction_id": pp.prediction_id,
                    "entity_id": pp.entity_id,
                    "prediction_time": pp.prediction_time.isoformat(),
                    "split": pp.split.value,
                    "snapshot_id": pp.snapshot_id,
                    "boundary_version": pp.boundary_version,
                    "label": pp.label,
                    "label_time": pp.label_time.isoformat() if pp.label_time else None,
                    "_source_manifest_sha256": manifest_sha,
                }
            )

        # Write to Iceberg
        df_pp = sess.createDataFrame(points)
        try:
            df_pp.writeTo(target_table).using("iceberg").createOrReplace()
        except Exception:
            pass
        df_pp.writeTo(target_table).overwritePartitions()

        count = sess.table(target_table).count()
        snapshots = sess.sql(
            f"SELECT snapshot_id FROM {target_table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        snap_id = snapshots[0].snapshot_id if snapshots else None

        receipt = {
            "receipt": {
                "receipt_version": 1,
                "run_id": run_id,
                "status": "COMPLETE",
                "created_at": started_at.isoformat(),
            },
            "input": {"manifest_sha256": manifest_sha, "silver_receipt": str(silver_receipt_path)},
            "code": {
                "git_commit": git_commit,
                "pp_version": pp_cfg["version"],
                "boundary_version": boundary_cfg["version"],
            },
            "output": {"table": target_table, "iceberg_snapshot_id": snap_id, "point_count": count},
        }

        _publish(receipt_dir, receipt)
        return receipt
    finally:
        if own_spark:
            sess.stop()


def _publish(receipt_dir, receipt):
    stage_dir = receipt_dir.with_name(f".{receipt_dir.name}.staging")
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True)
    content = yaml.safe_dump(receipt, default_flow_style=False, sort_keys=False)
    tmp = stage_dir / "prediction_points_receipt.yaml"
    tmp.write_text(content, encoding="utf-8")
    os.replace(stage_dir, receipt_dir)
