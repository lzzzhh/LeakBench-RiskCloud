"""Phase 1 — End-to-End full pipeline test."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from case_studies.home_credit.pipelines.bronze_ingestion import BronzeConfig, ingest_bronze
from case_studies.home_credit.pipelines.features import compute_features, compute_woe_rules
from case_studies.home_credit.pipelines.prediction_points import generate_prediction_points
from case_studies.home_credit.pipelines.silver_ingestion import SilverConfig, ingest_silver
from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

pytestmark = [
    pytest.mark.integration, pytest.mark.e2e,
    pytest.mark.filterwarnings(r"ignore:.*socket\.socket.*:pytest.PytestUnraisableExceptionWarning"),
    pytest.mark.filterwarnings(r"ignore:.*socket\.socket.*:ResourceWarning"),
]

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"
REPO = Path(__file__).resolve().parents[3]
BRONZE_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "bronze_v1.yaml"
SILVER_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "silver_v1.yaml"
PP_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "prediction_points_v1.yaml"
FEAT_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "features_v1.yaml"
REQUIRED_FILES = ["application_train.csv", "bureau.csv", "bureau_balance.csv"]


def _e2e_setup():
    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp) / "data"
    data_dir.mkdir()
    for f in REQUIRED_FILES:
        (data_dir / f).write_bytes((FIXTURES / f).read_bytes())
    manifest_path = Path(tmp) / "manifest.yaml"
    manifest = {"dataset": "home_credit", "files": [{"name": f, "required": True} for f in REQUIRED_FILES]}
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    from case_studies.home_credit.scripts.validate_manifest import populate_manifest
    assert populate_manifest(data_dir, manifest_path)
    warehouse = Path(tmp) / "warehouse"
    spark = get_spark(app_name="p1-e2e", warehouse=str(warehouse))
    setup_namespaces(spark)
    return {"spark": spark, "tmp": tmp, "data_dir": data_dir, "manifest_path": manifest_path, "warehouse": warehouse}


@pytest.fixture(scope="module")
def e2e():
    import gc
    s = _e2e_setup()
    yield s
    gc.collect()
    try:
        s["spark"].stop()
    except BaseException:
        pass


class TestE2E:

    def test_full_pipeline(self, e2e):
        spark = e2e["spark"]
        tmp = e2e["tmp"]

        # Bronze
        bconfig = BronzeConfig.from_yaml(BRONZE_CONFIG)
        bronze = ingest_bronze(bconfig, e2e["data_dir"], e2e["manifest_path"],
                               Path(tmp) / "b_receipts", "e2e-b", spark=spark)
        assert bronze["receipt"]["status"] == "COMPLETE"

        # Silver
        sconfig = SilverConfig.from_yaml(SILVER_CONFIG)
        silver = ingest_silver(sconfig, Path(tmp) / "b_receipts" / "bronze_receipt.yaml",
                               Path(tmp) / "s_receipts", "e2e-s", spark=spark)
        assert silver["receipt"]["status"] == "COMPLETE"

        # Prediction Points
        pp = generate_prediction_points(PP_CONFIG, Path(tmp) / "s_receipts" / "silver_receipt.yaml",
                                        Path(tmp) / "pp_receipts", "e2e-pp", spark=spark)
        assert pp["receipt"]["status"] == "COMPLETE"
        assert pp["output"]["point_count"] == 1

        # Features
        feat = compute_features(FEAT_CONFIG, Path(tmp) / "feat_receipts", "e2e-feat", spark=spark)
        assert feat["receipt"]["status"] == "COMPLETE"

        # WOE/IV
        woe = compute_woe_rules("riskcloud.gold.feature_values", "riskcloud.gold.prediction_points",
                                Path(tmp) / "woe_receipts", "e2e-woe", spark=spark)
        assert woe["receipt"]["status"] == "COMPLETE"

    def test_full_rerun_idempotent(self, e2e):
        spark = e2e["spark"]
        tmp = e2e["tmp"]
        b2 = ingest_bronze(BronzeConfig.from_yaml(BRONZE_CONFIG), e2e["data_dir"], e2e["manifest_path"],
                           Path(tmp) / "b2_receipts", "e2e-b2", spark=spark)
        assert b2["quality"]["rerun_duplicate_growth"] == 0
