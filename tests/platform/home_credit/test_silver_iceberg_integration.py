"""P1.3 — Silver integration tests."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from case_studies.home_credit.pipelines.bronze_ingestion import BronzeConfig, ingest_bronze
from case_studies.home_credit.pipelines.silver_ingestion import SilverConfig, ingest_silver
from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces

pytestmark = [
    pytest.mark.silver_integration,
    pytest.mark.filterwarnings(r"ignore:.*socket\.socket.*:pytest.PytestUnraisableExceptionWarning"),
    pytest.mark.filterwarnings(r"ignore:.*socket\.socket.*:ResourceWarning"),
]

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"
BRONZE_CONFIG = Path(__file__).resolve().parents[3] / "case_studies" / "home_credit" / "configs" / "bronze_v1.yaml"
SILVER_CONFIG = Path(__file__).resolve().parents[3] / "case_studies" / "home_credit" / "configs" / "silver_v1.yaml"
REQUIRED_FILES = ["application_train.csv", "bureau.csv", "bureau_balance.csv"]


def _setup_bronze():
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
    spark = get_spark(app_name="p13-test", warehouse=str(warehouse))
    setup_namespaces(spark)
    bconfig = BronzeConfig.from_yaml(BRONZE_CONFIG)
    receipt = ingest_bronze(
        bconfig, data_dir, manifest_path, Path(tmp) / "bronze_receipts", "p13-bronze", git_commit="test", spark=spark
    )
    return {
        "spark": spark,
        "warehouse": warehouse,
        "bronze_receipt_dir": Path(tmp) / "bronze_receipts",
        "bronze_receipt": receipt,
        "config": SilverConfig.from_yaml(SILVER_CONFIG),
    }


@pytest.fixture(scope="module")
def module_setup():
    import gc

    result = _setup_bronze()
    yield result
    gc.collect()
    try:
        result["spark"].stop()
    except BaseException:
        pass


class TestSilverIngestion:
    def test_ingest_succeeds(self, module_setup):
        sconfig = module_setup["config"]
        receipt = ingest_silver(
            sconfig,
            module_setup["bronze_receipt_dir"] / "bronze_receipt.yaml",
            Path(tempfile.mkdtemp()) / "silver_receipts",
            "p13-silver",
            spark=module_setup["spark"],
        )
        assert receipt["receipt"]["status"] == "COMPLETE"
        assert len(receipt["tables"]) == 3

    def test_bureau_balance_has_sk_id_curr(self, module_setup):
        sconfig = module_setup["config"]
        receipt = ingest_silver(
            sconfig,
            module_setup["bronze_receipt_dir"] / "bronze_receipt.yaml",
            Path(tempfile.mkdtemp()) / "silver_receipts2",
            "p13-silver2",
            spark=module_setup["spark"],
        )
        tbl = receipt["tables"]["bureau_balance"]["table_name"]
        cols = module_setup["spark"].table(tbl).columns
        assert "SK_ID_CURR" in cols, "bureau_balance must have enriched SK_ID_CURR"

    def test_types_casted(self, module_setup):
        sconfig = module_setup["config"]
        receipt = ingest_silver(
            sconfig,
            module_setup["bronze_receipt_dir"] / "bronze_receipt.yaml",
            Path(tempfile.mkdtemp()) / "silver_receipts3",
            "p13-silver3",
            spark=module_setup["spark"],
        )
        tbl = receipt["tables"]["application_train"]["table_name"]
        schema = module_setup["spark"].table(tbl).schema
        from pyspark.sql.types import IntegerType

        fields = {f.name: f for f in schema.fields}
        assert isinstance(fields["SK_ID_CURR"].dataType, IntegerType)
        assert isinstance(fields["TARGET"].dataType, IntegerType)
