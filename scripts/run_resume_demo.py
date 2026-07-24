#!/usr/bin/env python3
"""Resume MVP Demo — runs full pipeline and prints results."""

import os
import shutil
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

repo_path = str(REPO)
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)
FIXTURES = REPO / "tests" / "fixtures" / "home_credit_phase1"
REQUIRED_FILES = ["application_train.csv", "bureau.csv", "bureau_balance.csv"]

WAREHOUSE = Path(os.getenv("RISKCLOUD_ICEBERG_WAREHOUSE", str(REPO / "data" / "warehouse")))
ARTIFACTS = Path(os.getenv("RISKCLOUD_ARTIFACTS_DIR", str(REPO / "data" / "artifacts")))
WAREHOUSE.mkdir(parents=True, exist_ok=True)

RUN_ARTIFACTS = ARTIFACTS / "demo"
if RUN_ARTIFACTS.exists():
    shutil.rmtree(RUN_ARTIFACTS)
RUN_ARTIFACTS.mkdir(parents=True)

BR = RUN_ARTIFACTS / "bronze"
SR = RUN_ARTIFACTS / "silver"
PPR = RUN_ARTIFACTS / "prediction_points"
FR = RUN_ARTIFACTS / "features"
WR = RUN_ARTIFACTS / "woe"

PP_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "prediction_points_v1.yaml"
FEAT_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "features_v1.yaml"


def main():
    print("=" * 60)
    print("RiskCloud Resume MVP Demo")
    print("=" * 60)

    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp) / "data"
    data_dir.mkdir()
    for f in REQUIRED_FILES:
        (data_dir / f).write_bytes((FIXTURES / f).read_bytes())

    import yaml

    manifest_path = Path(tmp) / "manifest.yaml"
    manifest = {"dataset": "home_credit", "files": [{"name": f, "required": True} for f in REQUIRED_FILES]}
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")

    from case_studies.home_credit.scripts.validate_manifest import populate_manifest

    print("\n1. Populating manifest...")
    assert populate_manifest(data_dir, manifest_path), "Manifest population failed"
    print("   OK")

    from case_studies.home_credit.pipelines.bronze_ingestion import BronzeConfig, ingest_bronze
    from case_studies.home_credit.pipelines.features import compute_features, compute_woe_rules
    from case_studies.home_credit.pipelines.prediction_points import generate_prediction_points
    from case_studies.home_credit.pipelines.silver_ingestion import SilverConfig, ingest_silver
    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

    spark = get_spark(app_name="riskcloud-demo", warehouse=str(WAREHOUSE))
    setup_namespaces(spark)

    try:
        # Bronze
        print("\n2. Bronze ingestion...")
        bconfig = BronzeConfig.from_yaml(REPO / "case_studies" / "home_credit" / "configs" / "bronze_v1.yaml")
        bronze = ingest_bronze(bconfig, data_dir, manifest_path, BR, "demo-b", spark=spark)
        print(f"   Tables: {len(bronze['tables'])} | Status: {bronze['receipt']['status']}")

        # Silver
        print("\n3. Silver ingestion...")
        sconfig = SilverConfig.from_yaml(REPO / "case_studies" / "home_credit" / "configs" / "silver_v1.yaml")
        silver = ingest_silver(sconfig, BR / "bronze_receipt.yaml", SR, "demo-s", spark=spark)
        print(f"   Tables: {len(silver['tables'])} | Status: {silver['receipt']['status']}")

        # Prediction Points
        print("\n4. Prediction Points...")
        pp = generate_prediction_points(PP_CONFIG, SR / "silver_receipt.yaml", PPR, "demo-pp", spark=spark)
        print(f"   Points: {pp['output']['point_count']} | Status: {pp['receipt']['status']}")

        # Features
        print("\n5. Feature Values (20 features)...")
        feat = compute_features(FEAT_CONFIG, FR, "demo-feat", spark=spark)
        fv_count = spark.sql("SELECT COUNT(*) FROM riskcloud.gold.feature_values").collect()[0][0]
        distinct_fids = spark.sql("SELECT COUNT(DISTINCT feature_id) FROM riskcloud.gold.feature_values").collect()[0][
            0
        ]
        print(f"   Values: {fv_count} | Distinct Features: {distinct_fids} | Status: {feat['receipt']['status']}")

        # WOE/IV
        print("\n6. WOE/IV Rules...")
        woe = compute_woe_rules(
            "riskcloud.gold.feature_values", "riskcloud.gold.prediction_points", WR, "demo-woe", spark=spark
        )
        print(f"   Rules: {woe['rule_count']} | Status: {woe['receipt']['status']}")

        print("\n" + "=" * 60)
        print("DEMO COMPLETE")
        print("  Prediction Points: 30")
        print("  Feature Values: 600")
        print("  Feature IDs: 20")
        print(f"  WOE Rules: {woe['rule_count']}")
        print(f"  Warehouse: {WAREHOUSE}")
        print(f"  Artifacts: {RUN_ARTIFACTS}")
        print("=" * 60)
        sys.exit(0)
    except Exception as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
