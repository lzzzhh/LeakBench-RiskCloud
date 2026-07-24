"""P1.5 As-of Features + P1.6 WOE/IV with explicit aggregation."""

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
    from pyspark.sql.functions import (
        array,
        asc,
        avg,
        col,
        concat,
        count,
        desc,
        explode,
        expr,
        lit,
        row_number,
        struct,
        when,
    )
    from pyspark.sql.functions import sum as spark_sum
    from pyspark.sql.window import Window

    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

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
        bur = sess.table(inp["silver_bureau"]).filter(col("DAYS_CREDIT") <= 0)
        bub = sess.table(inp["silver_bureau_balance"]).filter(col("MONTHS_BALANCE") <= 0)

        canonical_id = concat(lit("SK_ID_CURR:"), col("SK_ID_CURR").cast("string"))

        # Application features — use catalog expressions (controlled subset)
        app_feats = app.select(
            canonical_id.alias("entity_id"),
            *[expr(f.lineage_expression).alias(f.feature_id) for f in FEATURES if f.feature_id.startswith("app.")],
        )

        # Bureau features — explicit Spark aggregation
        bur_feats = (
            bur.groupBy("SK_ID_CURR")
            .agg(
                count(lit(1)).cast("double").alias("bureau.record_count"),
                spark_sum(when(col("CREDIT_ACTIVE") == "Active", 1).otherwise(0))
                .cast("double")
                .alias("bureau.active_count"),
                spark_sum(when(col("CREDIT_ACTIVE") == "Closed", 1).otherwise(0))
                .cast("double")
                .alias("bureau.closed_count"),
                spark_sum("AMT_CREDIT_SUM").cast("double").alias("bureau.credit_sum_total"),
                spark_sum("AMT_CREDIT_SUM_DEBT").cast("double").alias("bureau.debt_sum_total"),
                spark_sum("AMT_CREDIT_SUM_OVERDUE").cast("double").alias("bureau.overdue_sum_total"),
                avg("DAYS_CREDIT").cast("double").alias("bureau.days_credit_mean"),
                spark_sum(when(col("DAYS_CREDIT").between(-365, 0), 1).otherwise(0))
                .cast("double")
                .alias("bureau.recent_12m_count"),
            )
            .select(canonical_id.alias("entity_id"), "*")
        )

        # Bureau balance features — explicit aggregation
        delinq_level = (
            when(col("STATUS") == "0", 0)
            .when(col("STATUS") == "1", 1)
            .when(col("STATUS") == "2", 2)
            .when(col("STATUS") == "3", 3)
            .when(col("STATUS") == "4", 4)
            .when(col("STATUS") == "5", 5)
            .when(col("STATUS").isin("C", "X"), 0)
        )
        bub_base = bub.groupBy("SK_ID_CURR").agg(
            count(lit(1)).cast("double").alias("bureau_balance.month_count"),
            spark_sum(when(col("STATUS").isin("1", "2", "3", "4", "5"), 1).otherwise(0))
            .cast("double")
            .alias("bureau_balance.delinquent_month_count"),
            spark_sum(delinq_level).alias("bureau_balance.max_delinquency_level"),
        )
        # Latest status
        w = Window.partitionBy("SK_ID_CURR").orderBy(desc("MONTHS_BALANCE"), asc("SK_ID_BUREAU"))
        bub_latest = (
            bub.withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select(
                "SK_ID_CURR",
                when(col("STATUS").isin("1", "2", "3", "4", "5"), 1.0)
                .otherwise(0.0)
                .alias("bureau_balance.latest_status_delinquent"),
            )
        )
        bub_feats = bub_base.join(bub_latest, "SK_ID_CURR", "left").select(canonical_id.alias("entity_id"), "*")

        # Build wide table and pivot to long format
        wide = (
            pp_df.join(app_feats, "entity_id", "left")
            .join(bur_feats, "entity_id", "left")
            .join(bub_feats, "entity_id", "left")
        )

        # Long format via explode
        fid_list = [f.feature_id for f in FEATURES]
        structs = [
            struct(
                lit(fid).alias("feature_id"),
                col(fid).cast("double").alias("feature_value"),
            )
            for fid in fid_list
        ]
        flat = wide.withColumn("_feat", explode(array(*structs))).select(
            "prediction_id",
            "entity_id",
            "prediction_time",
            col("_feat.feature_id"),
            col("_feat.feature_value"),
        )

        # Create or write table
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
            "feature_id STRING",
            "feature_value DOUBLE",
            "_source_manifest_sha256 STRING",
        ]
        if not table_exists(sess, target):
            create_table(sess, target, cols, "_source_manifest_sha256", props)
        flat.withColumn("_source_manifest_sha256", lit("")).writeTo(target).overwritePartitions()

        meta = get_snapshot_meta(sess, target)
        receipt = {
            "receipt": {
                "receipt_version": 1,
                "run_id": run_id,
                "status": "COMPLETE",
                "created_at": datetime.now(UTC).isoformat(),
            },
            "output": {
                "table": target,
                "feature_count": sess.table(target).count(),
                "iceberg_snapshot_id": meta["snapshot_id"],
            },
        }
        sm = {"manifest": {"manifest_id": run_id, "status": "COMPLETE"}, "code": {"git_commit": git_commit}}
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
    from pyspark.sql.functions import col

    from case_studies.home_credit.pipelines.spark_env import get_spark

    own_spark = spark is None
    sess = get_spark(app_name=f"riskcloud-woe-{run_id}", warehouse=warehouse) if own_spark else spark
    try:
        pp = sess.table(prediction_points_table)
        fv = sess.table(feature_table)
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
            q = max(len(vals) // 4, 1)
            bins = []
            total_good = sum(1 for _, lbl in vals if lbl == 0)
            total_bad = sum(1 for _, lbl in vals if lbl == 1)
            if total_good == 0 or total_bad == 0:
                continue
            for i in range(4):
                lo = vals[i * q][0]
                hi = vals[min((i + 1) * q, len(vals)) - 1][0]
                bin_vals = vals[i * q : min((i + 1) * q, len(vals))]
                g = sum(1 for _, lbl in bin_vals if lbl == 0) + 0.5
                b = sum(1 for _, lbl in bin_vals if lbl == 1) + 0.5
                gd = g / (total_good + 2.0)
                bd = b / (total_bad + 2.0)
                woe = math.log(gd / bd) if gd > 0 and bd > 0 else 0.0
                iv = (gd - bd) * woe
                bins.append(
                    {
                        "lower": lo,
                        "upper": hi,
                        "raw_good_count": sum(1 for _, lbl in bin_vals if lbl == 0),
                        "raw_bad_count": sum(1 for _, lbl in bin_vals if lbl == 1),
                        "smoothed_good_count": g,
                        "smoothed_bad_count": b,
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
                "training_sample_count": len(vals),
                "training_good_count": total_good,
                "training_bad_count": total_bad,
            }
        receipt = {
            "receipt": {"receipt_version": 1, "run_id": run_id, "status": "COMPLETE"},
            "rules": rules,
            "rule_count": len(rules),
        }
        receipt_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_yaml(receipt_dir / "woe_rules.yaml", receipt)
        return receipt
    finally:
        if own_spark:
            sess.stop()
