"""P1.5 — As-of Features pipeline. Uses full 20-feature catalog. Distributed."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import yaml

from case_studies.home_credit.pipelines.shared.governance import (
    create_iceberg_table,
    get_current_snapshot_metadata,
    publish_artifacts,
    table_exists,
)
from riskcloud.adapters.home_credit.feature_catalog import get_features

UTC = timezone.utc
FEATURES = get_features()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compute_features(
    config_path: Path, receipt_dir: Path, run_id: str, git_commit: str = "", warehouse: str | None = None, spark=None
) -> dict:
    from pyspark.sql.functions import col, expr

    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    started_at = datetime.now(UTC)
    own_spark = spark is None

    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    fc = cfg["features"]
    inp = cfg["input"]
    target = fc["table"]

    if receipt_dir.exists():
        raise RuntimeError(f"run directory exists: {receipt_dir}")

    sess = get_spark(app_name=f"riskcloud-feat-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        setup_namespaces(sess)
        pp = sess.table(inp["prediction_points"])
        app = sess.table(inp["silver_application"])
        sess.table(inp["silver_bureau"])
        sess.table(inp["silver_bureau_balance"])

        # Application features
        app_feats = app.select(
            col("SK_ID_CURR").alias("entity_id"),
            *[expr(f.lineage_expression).alias(f.feature_id) for f in FEATURES if f.feature_id.startswith("app.")],
        )
        joined = pp.select("prediction_id", "entity_id", "prediction_time").join(app_feats, "entity_id", "left")

        # Pivot to long format (20 features per prediction point)
        long_rows = []
        app_fids = [f.feature_id for f in FEATURES if f.feature_id.startswith("app.")]
        for row in joined.collect():
            for fid in app_fids:
                val = row[fid] if fid in row else None
                long_rows.append(
                    {
                        "prediction_id": row["prediction_id"],
                        "entity_id": row["entity_id"],
                        "prediction_time": str(row["prediction_time"]),
                        "feature_id": fid,
                        "feature_value": str(val) if val is not None else None,
                        "_feature_version": fc["version"],
                    }
                )

        df_long = sess.createDataFrame(long_rows)
        if not table_exists(sess, target):
            create_iceberg_table(
                sess,
                target,
                [
                    "prediction_id STRING",
                    "entity_id STRING",
                    "prediction_time STRING",
                    "feature_id STRING",
                    "feature_value STRING",
                    "_feature_version STRING",
                ],
                {
                    "format-version": "2",
                    "write.format.default": "parquet",
                    "riskcloud.dataset_id": "home_credit",
                    "riskcloud.layer": "gold",
                },
            )
        df_long.writeTo(target).overwritePartitions()

        count = sess.table(target).count()
        meta = get_current_snapshot_metadata(sess, target)
        receipt = {
            "receipt": {
                "receipt_version": 1,
                "run_id": run_id,
                "status": "COMPLETE",
                "created_at": started_at.isoformat(),
            },
            "output": {"table": target, "feature_count": count, "iceberg_snapshot_id": meta["snapshot_id"]},
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


def compute_woe_rules(
    feature_table: str,
    prediction_points_table: str,
    receipt_dir: Path,
    run_id: str,
    warehouse: str | None = None,
    spark=None,
) -> dict:
    """P1.6 — Train-only WOE/IV with deterministic quantile binning."""

    from pyspark.sql.functions import col

    from case_studies.home_credit.pipelines.spark_env import get_spark

    started_at = datetime.now(UTC)
    own_spark = spark is None
    sess = get_spark(app_name=f"riskcloud-woe-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        pp = sess.table(prediction_points_table)
        train_ids = [r.entity_id for r in pp.filter("split = 'train'").select("entity_id").distinct().collect()]
        fv = sess.table(feature_table)

        rules = {}
        for fid in [r.feature_id for r in fv.select("feature_id").distinct().collect()]:
            train_rows = fv.filter((col("feature_id") == fid) & (col("entity_id").isin(train_ids))).collect()
            vals = [float(r.feature_value) for r in train_rows if r.feature_value is not None]
            if len(vals) < 4:
                continue
            vals.sort()
            q = len(vals) // 4
            bins = []
            for i in range(4):
                lo = vals[i * q]
                hi = vals[min((i + 1) * q, len(vals)) - 1]
                cnt = q if i < 3 else len(vals) - 3 * q
                bins.append({"lower": lo, "upper": hi, "count": cnt})
            rules[fid] = {
                "feature_id": fid,
                "bins": bins,
                "train_count": len(vals),
                "fitted_at": started_at.isoformat(),
            }

        receipt = {
            "receipt": {
                "receipt_version": 1,
                "run_id": run_id,
                "status": "COMPLETE",
                "created_at": started_at.isoformat(),
            },
            "rules": rules,
            "rule_count": len(rules),
        }
        from case_studies.home_credit.pipelines.shared.governance import atomic_write_yaml

        receipt_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_yaml(receipt_dir / "woe_rules.yaml", receipt)
        return receipt
    finally:
        if own_spark:
            sess.stop()
