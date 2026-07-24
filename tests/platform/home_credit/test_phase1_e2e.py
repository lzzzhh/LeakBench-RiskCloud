"""Phase 1 E2E — full pipeline with richer fixture."""

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
    pytest.mark.e2e,
    pytest.mark.filterwarnings(r"ignore:.*socket\.socket.*:pytest.PytestUnraisableExceptionWarning"),
    pytest.mark.filterwarnings(r"ignore:.*socket\.socket.*:ResourceWarning"),
]

PHASE1_FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit_phase1"
REPO = Path(__file__).resolve().parents[3]
BRONZE_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "bronze_v1.yaml"
SILVER_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "silver_v1.yaml"
PP_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "prediction_points_v1.yaml"
FEAT_CONFIG = REPO / "case_studies" / "home_credit" / "configs" / "features_v1.yaml"
REQUIRED_FILES = ["application_train.csv", "bureau.csv", "bureau_balance.csv"]


@pytest.fixture(scope="module")
def e2e():
    import gc

    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp) / "data"
    data_dir.mkdir()
    for f in REQUIRED_FILES:
        (data_dir / f).write_bytes((PHASE1_FIXTURES / f).read_bytes())
    manifest_path = Path(tmp) / "manifest.yaml"
    manifest = {"dataset": "home_credit", "files": [{"name": f, "required": True} for f in REQUIRED_FILES]}
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    from case_studies.home_credit.scripts.validate_manifest import populate_manifest

    assert populate_manifest(data_dir, manifest_path)
    warehouse = Path(tmp) / "warehouse"
    spark = get_spark(app_name="p1-e2e", warehouse=str(warehouse))
    setup_namespaces(spark)

    primary_error = None
    try:
        # Bronze
        bconfig = BronzeConfig.from_yaml(BRONZE_CONFIG)
        bronze = ingest_bronze(bconfig, data_dir, manifest_path, Path(tmp) / "b_receipts", "e2e-b", spark=spark)
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

        # Features
        feat = compute_features(FEAT_CONFIG, Path(tmp) / "feat_receipts", "e2e-feat", spark=spark)
        assert feat["receipt"]["status"] == "COMPLETE"

        # WOE/IV
        woe = compute_woe_rules("riskcloud.gold.feature_values", "riskcloud.gold.prediction_points",
                                Path(tmp) / "woe_receipts", "e2e-woe", spark=spark)
        assert woe["receipt"]["status"] == "COMPLETE"

        yield {"spark": spark, "tmp": tmp, "bronze": bronze, "silver": silver, "pp": pp, "feat": feat, "woe": woe}
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        gc.collect()
        try:
            spark.stop()
        except BaseException as stop_err:
            if primary_error is None:
                raise
            add_note = getattr(primary_error, "add_note", None)
            if callable(add_note):
                add_note(f"Spark teardown: {type(stop_err).__name__}")


class TestE2E:

    def test_full_pipeline_succeeds(self, e2e):
        assert e2e["bronze"]["receipt"]["status"] == "COMPLETE"
        assert e2e["silver"]["receipt"]["status"] == "COMPLETE"
        assert e2e["pp"]["receipt"]["status"] == "COMPLETE"
        assert e2e["feat"]["receipt"]["status"] == "COMPLETE"
        assert e2e["woe"]["receipt"]["status"] == "COMPLETE"

    def test_features_have_rows(self, e2e):
        assert e2e["feat"]["output"]["feature_count"] > 0

    def test_woe_has_rules(self, e2e):
        assert e2e["woe"]["rule_count"] > 0

    def test_features_20_feature_closure(self, e2e):
        """Feature Values table must contain all 20 catalog features."""
        from riskcloud.adapters.home_credit.feature_catalog import get_features

        spark = e2e["spark"]
        actual_ids = {
            r.feature_id
            for r in spark.sql(
                "SELECT DISTINCT feature_id FROM riskcloud.gold.feature_values"
            ).collect()
        }
        expected_ids = {f.feature_id for f in get_features()}
        missing = expected_ids - actual_ids
        extra = actual_ids - expected_ids
        assert not missing, f"Missing feature IDs: {missing}"
        assert not extra, f"Unexpected feature IDs: {extra}"

    def test_entity_ids_are_canonical(self, e2e):
        """All entity_ids in Prediction Points must be SK_ID_CURR:NNN format."""
        spark = e2e["spark"]
        rows = spark.sql(
            "SELECT DISTINCT entity_id FROM riskcloud.gold.prediction_points"
        ).collect()
        for r in rows:
            assert r.entity_id.startswith("SK_ID_CURR:"), f"Bad entity_id: {r.entity_id}"
            suffix = r.entity_id.split(":", 1)[1]
            assert suffix.isdigit(), f"Non-numeric suffix: {suffix}"
