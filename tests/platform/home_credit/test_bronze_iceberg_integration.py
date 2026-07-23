"""P1.2 — Bronze Iceberg integration tests (requires Java + PySpark)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from case_studies.home_credit.pipelines.bronze_ingestion import (
    BronzeConfig,
    _sha256,
    ingest_bronze,
)
from riskcloud.adapters.home_credit.field_mapping import RAW_REQUIRED_COLUMNS

pytestmark = pytest.mark.bronze_integration

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"
CONFIG_PATH = (
    Path(__file__).resolve().parents[3]
    / "case_studies"
    / "home_credit"
    / "configs"
    / "bronze_v1.yaml"
)


def _populate_manifest(data_dir: Path, manifest_path: Path) -> str:
    for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
        (data_dir / f).write_text((FIXTURES / f).read_text())
    manifest = {
        "dataset": "home_credit",
        "files": [
            {"name": "application_train.csv", "required": True},
            {"name": "bureau.csv", "required": True},
            {"name": "bureau_balance.csv", "required": True},
        ],
    }
    with open(manifest_path, "w") as f:
        yaml.safe_dump(manifest, f)
    from case_studies.home_credit.scripts.validate_manifest import populate_manifest
    ok = populate_manifest(data_dir, manifest_path)
    assert ok, "populate failed"
    return _sha256(manifest_path.read_bytes())


@pytest.fixture(scope="module")
def module_setup():
    """Run full bronze ingestion once per module. Returns result dict."""
    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp) / "data"
    data_dir.mkdir()
    manifest_path = Path(tmp) / "manifest.yaml"
    receipt_dir = Path(tmp) / "receipts"
    warehouse = Path(tmp) / "warehouse"

    manifest_sha = _populate_manifest(data_dir, manifest_path)
    config = BronzeConfig.from_yaml(CONFIG_PATH)

    run_id = "p12-test-001"
    try:
        receipt = ingest_bronze(config, data_dir, manifest_path, receipt_dir, run_id,
                                git_commit="test", warehouse=str(warehouse))
    except Exception as exc:
        pytest.fail(f"ingest_bronze failed: {type(exc).__name__}: {exc}")

    return {
        "data_dir": data_dir,
        "manifest_path": manifest_path,
        "receipt_dir": receipt_dir,
        "warehouse": warehouse,
        "manifest_sha": manifest_sha,
        "config": config,
        "receipt": receipt,
        "run_id": run_id,
    }


def _inspect_spark(warehouse: str):
    """Create a Spark session for inspecting Iceberg tables."""
    from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces
    spark = get_spark(app_name="p12-inspect", warehouse=warehouse)
    setup_namespaces(spark)
    return spark


# -----------------------------------------------------------------
# Basic write tests
# -----------------------------------------------------------------

class TestBasicWrite:

    def test_receipt_complete(self, module_setup):
        assert module_setup["receipt"]["receipt"]["status"] == "COMPLETE"

    def test_three_tables_in_receipt(self, module_setup):
        tables = module_setup["receipt"]["tables"]
        assert len(tables) == 3
        for name in ["application_train", "bureau", "bureau_balance"]:
            assert name in tables

    def test_row_count_closure(self, module_setup):
        for tbl in module_setup["receipt"]["tables"].values():
            assert tbl["source_row_count"] == tbl["partition_row_count_after"]

    def test_each_table_has_snapshot(self, module_setup):
        for tbl in module_setup["receipt"]["tables"].values():
            assert tbl["iceberg_snapshot_id"] is not None

    def test_source_snapshot_id_present(self, module_setup):
        assert module_setup["receipt"]["input"]["source_snapshot_id"]

    def test_manifest_sha_in_receipt(self, module_setup):
        assert module_setup["receipt"]["input"]["manifest_sha256"] == module_setup["manifest_sha"]

    def test_receipt_file_exists(self, module_setup):
        p = module_setup["receipt_dir"] / "bronze_receipt.yaml"
        assert p.is_file()
        data = yaml.safe_load(p.read_text())
        assert data["receipt"]["status"] == "COMPLETE"

    def test_snapshot_manifest_exists(self, module_setup):
        p = module_setup["receipt_dir"] / "snapshot_manifest.yaml"
        assert p.is_file()
        data = yaml.safe_load(p.read_text())
        assert data["manifest"]["status"] == "COMPLETE"

    def test_bureau_balance_no_sk_id_curr(self, module_setup):
        spark = _inspect_spark(str(module_setup["warehouse"]))
        try:
            cols = spark.table("riskcloud.bronze.bureau_balance").columns
            assert "SK_ID_CURR" not in cols, "Bronze must not contain enriched SK_ID_CURR"
            for rc in RAW_REQUIRED_COLUMNS["bureau_balance.csv"]:
                assert rc in cols, f"Missing raw column: {rc}"
        finally:
            spark.stop()

    def test_metadata_columns_present(self, module_setup):
        spark = _inspect_spark(str(module_setup["warehouse"]))
        try:
            cols = spark.table("riskcloud.bronze.application_train").columns
            for mc in ["_source_manifest_sha256", "_source_snapshot_id", "_raw_row_sha256",
                       "_source_file_name", "_source_file_sha256", "_source_header_sha256",
                       "_bronze_schema_version"]:
                assert mc in cols, f"Missing metadata column: {mc}"
        finally:
            spark.stop()

    def test_all_source_columns_string(self, module_setup):
        spark = _inspect_spark(str(module_setup["warehouse"]))
        try:
            from pyspark.sql.types import StringType
            for table_name in ["riskcloud.bronze.application_train",
                               "riskcloud.bronze.bureau",
                               "riskcloud.bronze.bureau_balance"]:
                for fld in spark.table(table_name).schema.fields:
                    if not fld.name.startswith("_"):
                        assert isinstance(fld.dataType, StringType), (
                            f"{table_name}.{fld.name}: expected StringType, got {fld.dataType}"
                        )
        finally:
            spark.stop()


# -----------------------------------------------------------------
# Rerun test
# -----------------------------------------------------------------

class TestRerun:

    def test_same_manifest_rerun(self, module_setup):
        run_id2 = "p12-test-rerun"
        receipt2 = ingest_bronze(
            module_setup["config"], module_setup["data_dir"], module_setup["manifest_path"],
            module_setup["receipt_dir"] / "run2", run_id2,
            git_commit="test", warehouse=str(module_setup["warehouse"]),
        )
        for tbl_name in ["application_train", "bureau", "bureau_balance"]:
            t1 = module_setup["receipt"]["tables"][tbl_name]
            t2 = receipt2["tables"][tbl_name]
            assert t2["partition_row_count_before"] == t1["source_row_count"], (
                f"{tbl_name}: rerun before={t2['partition_row_count_before']}"
            )
            assert t2["partition_row_count_after"] == t2["source_row_count"]
            assert t2["rerun_duplicate_growth"] == 0
            assert t2["content_multiset_sha256"] == t1["content_multiset_sha256"]


# -----------------------------------------------------------------
# Different manifest test
# -----------------------------------------------------------------

class TestDifferentManifest:

    def test_changed_manifest_preserves_old_partition(self, module_setup):
        # Create manifest B with modified value
        import shutil
        tmp = tempfile.mkdtemp()
        data_dir_b = Path(tmp) / "data"
        data_dir_b.mkdir()
        for f in ["bureau.csv", "bureau_balance.csv"]:
            shutil.copy(module_setup["data_dir"] / f, data_dir_b / f)
        # Modify application_train
        (data_dir_b / "application_train.csv").write_text("SK_ID_CURR,TARGET\n1,1\n")

        manifest_b = Path(tmp) / "manifest_b.yaml"
        manifest_sha_b = _populate_manifest(data_dir_b, manifest_b)

        receipt_b = ingest_bronze(
            module_setup["config"], data_dir_b, manifest_b,
            module_setup["receipt_dir"] / "run_b", "p12-test-b",
            git_commit="test", warehouse=str(module_setup["warehouse"]),
        )

        # B should have different values
        assert manifest_sha_b != module_setup["manifest_sha"]
        assert receipt_b["input"]["source_snapshot_id"] != module_setup["receipt"]["input"]["source_snapshot_id"]

        # A partition should still exist
        spark = _inspect_spark(str(module_setup["warehouse"]))
        try:
            for tbl_name in ["riskcloud.bronze.application_train",
                             "riskcloud.bronze.bureau",
                             "riskcloud.bronze.bureau_balance"]:
                count_a = spark.sql(
                    f"SELECT COUNT(*) FROM {tbl_name} "
                    f"WHERE _source_manifest_sha256 = '{module_setup['manifest_sha']}'"
                ).collect()[0][0]
                count_b = spark.sql(
                    f"SELECT COUNT(*) FROM {tbl_name} "
                    f"WHERE _source_manifest_sha256 = '{manifest_sha_b}'"
                ).collect()[0][0]
                assert count_a > 0, f"{tbl_name}: manifest A partition gone"
                assert count_b > 0, f"{tbl_name}: manifest B partition missing"
        finally:
            spark.stop()
