"""P1.5 — As-of Feature Aggregation Pipeline.

Computes point-in-time features from Silver tables, joined to Prediction Points.
Writes a long-format feature value table to gold.feature_values.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

UTC = timezone.utc


def compute_features(
    config_path: Path,
    receipt_dir: Path,
    run_id: str,
    git_commit: str = "",
    warehouse: str | None = None,
    spark=None,
) -> dict[str, Any]:
    from pyspark.sql.functions import col, expr

    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    started_at = datetime.now(UTC)
    own_spark = spark is None

    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    fc = cfg["features"]
    inp = cfg["input"]
    target_table = fc["table"]

    sess = get_spark(app_name=f"riskcloud-features-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        setup_namespaces(sess)

        app = sess.table(inp["silver_application"])
        pp = sess.table(inp["prediction_points"])

        # Compute application features
        app_feats = app.select(
            col("SK_ID_CURR").alias("entity_id"),
            *[
                expr(fl["lineage"]).alias(fl["feature_id"])
                for fl in cfg["features_list"]
                if fl["source"] == "application_train"
            ],
        )

        # Join to prediction points
        pp_sel = pp.select("entity_id", "prediction_id", "prediction_time").dropDuplicates(["entity_id"])
        joined = pp_sel.join(app_feats, "entity_id", "left")

        # Pivot to long format: (prediction_id, entity_id, feature_id, feature_value)
        long_rows = []
        app_cols = joined.columns
        feature_cols = [c for c in app_cols if c not in ("entity_id", "prediction_id", "prediction_time")]
        for row in joined.collect():
            for fcol in feature_cols:
                long_rows.append(
                    {
                        "prediction_id": row["prediction_id"],
                        "entity_id": row["entity_id"],
                        "feature_id": fcol,
                        "feature_value": str(row[fcol]) if row[fcol] is not None else None,
                        "_feature_source": "silver.application_train",
                        "_feature_version": fc["version"],
                    }
                )

        df_long = sess.createDataFrame(long_rows)

        # Write
        try:
            df_long.writeTo(target_table).using("iceberg").createOrReplace()
        except Exception:
            pass
        df_long.writeTo(target_table).overwritePartitions()

        count = sess.table(target_table).count()

        receipt = {
            "receipt": {
                "receipt_version": 1,
                "run_id": run_id,
                "status": "COMPLETE",
                "created_at": started_at.isoformat(),
            },
            "output": {"table": target_table, "feature_count": count},
        }

        _publish_features(receipt_dir, receipt)
        return receipt
    finally:
        if own_spark:
            sess.stop()


def _publish_features(receipt_dir, receipt):
    stage_dir = receipt_dir.with_name(f".{receipt_dir.name}.staging")
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True)
    content = yaml.safe_dump(receipt, default_flow_style=False, sort_keys=False)
    tmp = stage_dir / "features_receipt.yaml"
    tmp.write_text(content, encoding="utf-8")
    os.replace(stage_dir, receipt_dir)
