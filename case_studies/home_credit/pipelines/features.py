"""P1.5 As-of Features + P1.6 WOE/IV + Strict/Full Views."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path

import yaml

from case_studies.home_credit.pipelines.shared.governance import (
    atomic_write_yaml,
    create_table,
    get_snapshot_meta,
    publish,
    table_exists,
)
from riskcloud.adapters.home_credit.feature_catalog import get_features

UTC = timezone.utc
FEATURES = get_features()


def compute_features(
    config_path: Path, receipt_dir: Path, run_id: str, git_commit: str = "", warehouse: str | None = None, spark=None
) -> dict:
    from pyspark.sql.functions import col, count, expr

    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    started_at = datetime.now(UTC)
    own_spark = spark is None

    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    inp = cfg["input"]
    target = cfg["features"]["table"]

    if receipt_dir.exists():
        raise RuntimeError(f"run directory exists: {receipt_dir}")

    sess = get_spark(app_name=f"riskcloud-feat-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        setup_namespaces(sess)
        pp = sess.table(inp["prediction_points"])
        app = sess.table(inp["silver_application"])

        # Application features (all 8 app.*)
        app_feats = app.select(
            col("SK_ID_CURR").alias("entity_id"),
            *[expr(f.lineage_expression).alias(f.feature_id) for f in FEATURES if f.feature_id.startswith("app.")],
        )
        joined = pp.select("prediction_id", "entity_id").join(app_feats, "entity_id", "left")

        # Pivot to long format: collect() is acceptable for fixture-scale; production uses explode+stack
        long_rows = []
        app_ids = [f.feature_id for f in FEATURES if f.feature_id.startswith("app.")]
        for row in joined.collect():
            eid = row["entity_id"] if row["entity_id"] else ""
            for fid in app_ids:
                long_rows.append(
                    {
                        "prediction_id": row["prediction_id"],
                        "entity_id": eid,
                        "feature_id": fid,
                        "feature_value": str(row[fid]) if row[fid] is not None else None,
                    }
                )

        df_long = sess.createDataFrame(long_rows)
        if not table_exists(sess, target):
            create_table(
                sess,
                target,
                [
                    "prediction_id STRING",
                    "entity_id STRING",
                    "feature_id STRING",
                    "feature_value STRING",
                    "_feature_version STRING",
                ],
                "_feature_version",
                {
                    "format-version": "2",
                    "write.format.default": "parquet",
                    "riskcloud.dataset_id": "home_credit",
                    "riskcloud.layer": "gold",
                },
            )
        df_long.writeTo(target).overwritePartitions()

        count = sess.table(target).count()
        meta = get_snapshot_meta(sess, target)
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
        publish(receipt_dir, "features", sm, receipt)
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
    """P1.6 — Train-only WOE/IV with deterministic quantile binning + additive smoothing."""
    from pyspark.sql.functions import col

    from case_studies.home_credit.pipelines.spark_env import get_spark

    started_at = datetime.now(UTC)
    own_spark = spark is None
    sess = get_spark(app_name=f"riskcloud-woe-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        pp = sess.table(prediction_points_table)
        fv = sess.table(feature_table)

        # Join features to prediction points, filter to train split
        train = pp.filter("split = 'train'").select("prediction_id", "entity_id", "label")
        fv_train = fv.join(train, "prediction_id", "inner")

        rules = {}
        feature_ids = [r.feature_id for r in fv.select("feature_id").distinct().collect()]
        for fid in feature_ids:
            rows = fv_train.filter(col("feature_id") == fid).select("feature_value", "label").collect()
            vals = [(float(r.feature_value), float(r.label)) for r in rows if r.feature_value is not None]
            if len(vals) < 8:
                continue
            vals.sort(key=lambda x: x[0])
            q = len(vals) // 4  # quartile bins
            if q < 2:
                continue
            bins = []
            total_good = sum(1 for v, lbl in vals if lbl == 0)
            total_bad = sum(1 for v, lbl in vals if lbl == 1)
            if total_good == 0 or total_bad == 0:
                continue
            for i in range(4):
                lo = vals[i * q][0]
                hi = vals[min((i + 1) * q, len(vals)) - 1][0]
                bin_vals = vals[i * q : min((i + 1) * q, len(vals))]
                g = sum(1 for v, lbl in bin_vals if lbl == 0) + 0.5
                b = sum(1 for v, lbl in bin_vals if lbl == 1) + 0.5
                gd = g / (total_good + 2.0)
                bd = b / (total_bad + 2.0)
                woe = math.log(gd / bd) if gd > 0 and bd > 0 else 0.0
                iv = (gd - bd) * woe
                bins.append(
                    {
                        "lower": lo,
                        "upper": hi,
                        "good_count": g,
                        "bad_count": b,
                        "good_distribution": gd,
                        "bad_distribution": bd,
                        "woe": woe,
                        "iv_component": iv,
                    }
                )
            total_iv = sum(b["iv_component"] for b in bins)
            rules[fid] = {
                "feature_id": fid,
                "bins": bins,
                "total_iv": total_iv,
                "train_count": len(vals),
                "train_good": total_good,
                "train_bad": total_bad,
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
        receipt_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_yaml(receipt_dir / "woe_rules.yaml", receipt)
        return receipt
    finally:
        if own_spark:
            sess.stop()
