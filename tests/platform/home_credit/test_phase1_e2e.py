"""Phase 1 — End-to-End pipeline test.

Runs full pipeline:
  Raw fixture → Bronze → Silver → Prediction Points → Features
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from case_studies.home_credit.pipelines.bronze_ingestion import BronzeConfig, ingest_bronze
from case_studies.home_credit.pipelines.features import compute_features
from case_studies.home_credit.pipelines.prediction_points import generate_prediction_points
from case_studies.home_credit.pipelines.silver_ingestion import SilverConfig, ingest_silver
from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

pytestmark = [
    pytest.mark.e2e,
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


@pytest.fixture(scope="module")
def e2e_setup():
    import gc

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

    try:
        # Bronze
        bconfig = BronzeConfig.from_yaml(BRONZE_CONFIG)
        bronze = ingest_bronze(
            bconfig, data_dir, manifest_path, Path(tmp) / "b_receipts", "e2e-b", git_commit="test", spark=spark
        )
        assert bronze["receipt"]["status"] == "COMPLETE"

        # Silver
        sconfig = SilverConfig.from_yaml(SILVER_CONFIG)
        silver = ingest_silver(
            sconfig, Path(tmp) / "b_receipts" / "bronze_receipt.yaml", Path(tmp) / "s_receipts", "e2e-s", spark=spark
        )
        assert silver["receipt"]["status"] == "COMPLETE"

        yield {"spark": spark, "warehouse": warehouse, "bronze": bronze, "silver": silver, "tmp": tmp}
    finally:
        gc.collect()
        try:
            spark.stop()
        except BaseException:
            pass


class TestE2E:
    def test_bronze_to_silver_lineage(self, e2e_setup):
        b = e2e_setup["bronze"]
        s = e2e_setup["silver"]
        assert b["input"]["manifest_sha256"] == s["input"]["manifest_sha256"]

    def test_silver_tables_exist(self, e2e_setup):
        spark = e2e_setup["spark"]
        for tn in ["riskcloud.silver.application_train", "riskcloud.silver.bureau", "riskcloud.silver.bureau_balance"]:
            assert spark.catalog.tableExists(tn)

    def test_prediction_points(self, e2e_setup):
        pp = generate_prediction_points(
            PP_CONFIG,
            Path(e2e_setup["tmp"]) / "s_receipts" / "silver_receipt.yaml",
            Path(tempfile.mkdtemp()) / "pp_receipt",
            "e2e-pp",
            spark=e2e_setup["spark"],
        )
        assert pp["receipt"]["status"] == "COMPLETE"
        assert pp["output"]["point_count"] == 1  # fixture has 1 row

    def test_features(self, e2e_setup):
        # Need prediction points first
        generate_prediction_points(
            PP_CONFIG,
            Path(e2e_setup["tmp"]) / "s_receipts" / "silver_receipt.yaml",
            Path(e2e_setup["tmp"]) / "pp_receipt2",
            "e2e-pp2",
            spark=e2e_setup["spark"],
        )
        features = compute_features(
            FEAT_CONFIG,
            Path(tempfile.mkdtemp()) / "feat_receipt",
            "e2e-feat",
            spark=e2e_setup["spark"],
        )
        assert features["receipt"]["status"] == "COMPLETE"
        assert features["output"]["feature_count"] > 0

    def test_rerun_idempotent(self, e2e_setup):
        """Full pipeline rerun produces consistent results."""
        tmp = e2e_setup["tmp"]
        b2 = ingest_bronze(
            BronzeConfig.from_yaml(BRONZE_CONFIG),
            Path(tmp) / "data",
            Path(tmp) / "manifest.yaml",
            Path(tmp) / "b2_receipts",
            "e2e-b2",
            spark=e2e_setup["spark"],
        )
        assert b2["quality"]["rerun_duplicate_growth"] == 0
