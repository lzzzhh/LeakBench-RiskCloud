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
    from pyspark.sql.functions import col, concat, count, expr, lit

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
        pp_df = sess.table(inp["prediction_points"]).select("prediction_id", "entity_id", "prediction_time")
        app = sess.table(inp["silver_application"])
        bur = sess.table(inp["silver_bureau"]).filter("DAYS_CREDIT <= 0")
        bub = sess.table(inp["silver_bureau_balance"]).filter("MONTHS_BALANCE <= 0")

        long_rows = []

        # Application features (8)
        app_feats = app.select(
            concat(lit("SK_ID_CURR:"), col("SK_ID_CURR").cast("string")).alias("entity_id"),
            *[expr(f.lineage_expression).alias(f.feature_id) for f in FEATURES if f.feature_id.startswith("app.")],
        )
        joined_app = pp_df.join(app_feats, "entity_id", "left")
        app_ids = [f.feature_id for f in FEATURES if f.feature_id.startswith("app.")]
        for row in joined_app.collect():
            for fid in app_ids:
                val = row[fid] if row[fid] is not None else None
                long_rows.append(
                    {
                        "prediction_id": row["prediction_id"],
                        "entity_id": row["entity_id"],
                        "prediction_time": str(row["prediction_time"]),
                        "feature_id": fid,
                        "feature_value": str(val) if val is not None else None,
                        "feature_version": "1",
                        "feature_catalog_version": "hc-features-v1",
                        "semantic_group_id": _get_semantic_group(fid),
                        "leakage_risk": _get_leakage_risk(fid),
                        "_source_manifest_sha256": "",
                        "_prediction_snapshot_id": "",
                    }
                )

        # Bureau features (8) — aggregate then join
        bur_aggs = []
        for f in FEATURES:
            if f.feature_id.startswith("bureau.") and not f.feature_id.startswith("bureau_balance."):
                bur_aggs.append(expr(f.lineage_expression).alias(f.feature_id))
        if bur_aggs:
            bur_feats = (
                bur.groupBy("SK_ID_CURR")
                .agg(*bur_aggs)
                .select(
                    concat(lit("SK_ID_CURR:"), col("SK_ID_CURR").cast("string")).alias("entity_id"),
                    *[
                        col(f.feature_id)
                        for f in FEATURES
                        if f.feature_id.startswith("bureau.") and not f.feature_id.startswith("bureau_balance.")
                    ],
                )
            )
            joined_bur = pp_df.join(bur_feats, "entity_id", "left")
            bur_ids = [
                f.feature_id
                for f in FEATURES
                if f.feature_id.startswith("bureau.") and not f.feature_id.startswith("bureau_balance.")
            ]
            for row in joined_bur.collect():
                for fid in bur_ids:
                    val = row[fid] if fid in row and row[fid] is not None else None
                    long_rows.append(
                        {
                            "prediction_id": row["prediction_id"],
                            "entity_id": row["entity_id"],
                            "prediction_time": str(row["prediction_time"]),
                            "feature_id": fid,
                            "feature_value": str(val) if val is not None else None,
                            "feature_version": "1",
                            "feature_catalog_version": "hc-features-v1",
                            "semantic_group_id": _get_semantic_group(fid),
                            "leakage_risk": _get_leakage_risk(fid),
                            "_source_manifest_sha256": "",
                            "_prediction_snapshot_id": "",
                        }
                    )

        # Bureau balance features (4)
        bub_aggs = []
        for f in FEATURES:
            if f.feature_id.startswith("bureau_balance."):
                bub_aggs.append(expr(f.lineage_expression).alias(f.feature_id))
        if bub_aggs:
            bub_feats = bub.groupBy("SK_ID_CURR").agg(*bub_aggs)
            joined_bub = pp_df.join(bub_feats, pp_df["entity_id"] == bub_feats["SK_ID_CURR"], "left")
            bub_ids = [f.feature_id for f in FEATURES if f.feature_id.startswith("bureau_balance.")]
            for row in joined_bub.collect():
                for fid in bub_ids:
                    val = row[fid] if fid in row and row[fid] is not None else None
                    long_rows.append(
                        {
                            "prediction_id": row["prediction_id"],
                            "entity_id": row["entity_id"],
                            "prediction_time": str(row["prediction_time"]),
                            "feature_id": fid,
                            "feature_value": str(val) if val is not None else None,
                            "feature_version": "1",
                            "feature_catalog_version": "hc-features-v1",
                            "semantic_group_id": _get_semantic_group(fid),
                            "leakage_risk": _get_leakage_risk(fid),
                            "_source_manifest_sha256": "",
                            "_prediction_snapshot_id": "",
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


def _get_semantic_group(feature_id: str) -> str:
    for f in FEATURES:
        if f.feature_id == feature_id:
            return f.semantic_group_id or ""
    return ""


def _get_leakage_risk(feature_id: str) -> str:
    for f in FEATURES:
        if f.feature_id == feature_id:
            return f.leakage_risk.value
    return "unknown"


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
