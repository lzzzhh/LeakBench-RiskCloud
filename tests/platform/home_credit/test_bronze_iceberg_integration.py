"""P1.2 — Bronze Iceberg integration tests (requires Java + PySpark)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from case_studies.home_credit.pipelines.bronze_ingestion import (
    BronzeConfig,
    _row_hash,
    _sha256,
    ingest_bronze,
)
from riskcloud.adapters.home_credit.field_mapping import RAW_REQUIRED_COLUMNS

pytestmark = pytest.mark.bronz_int

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"
CONFIG_PATH = (
    Path(__file__).resolve().parents[3]
    / "case_studies"
    / "home_credit"
    / "configs"
    / "bronze_v1.yaml"
)


def _populate_manifest(data_dir: Path, manifest_path: Path) -> str:
    """Populate manifest from fixtures, return manifest SHA."""
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


@pytest.fixture
def setup():
    """Create temp dirs with populated manifest, run ingestion once."""
    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp) / "data"
    data_dir.mkdir()
    manifest_path = Path(tmp) / "manifest.yaml"
    receipt_dir = Path(tmp) / "receipts"
    warehouse = Path(tmp) / "warehouse"

    manifest_sha = _populate_manifest(data_dir, manifest_path)
    config = BronzeConfig.from_yaml(CONFIG_PATH)

    run_id = "p12-test-001"
    receipt = ingest_bronze(config, data_dir, manifest_path, receipt_dir, run_id,
                            git_commit="test", warehouse=str(warehouse))
    result = {
        "data_dir": data_dir,
        "manifest_path": manifest_path,
        "receipt_dir": receipt_dir,
        "warehouse": warehouse,
        "manifest_sha": manifest_sha,
        "config": config,
        "receipt": receipt,
        "run_id": run_id,
    }
    yield result


class TestBronzeIngestion:

    def test_receipt_complete(self, setup):
        assert setup["receipt"]["receipt"]["status"] == "COMPLETE"

    def test_three_tables_in_receipt(self, setup):
        tables = setup["receipt"]["tables"]
        assert len(tables) == 3
        for name in ["application_train", "bureau", "bureau_balance"]:
            assert name in tables

    def test_row_count_closure(self, setup):
        tables = setup["receipt"]["tables"]
        for tbl in tables.values():
            assert tbl["source_row_count"] == tbl["bronze_row_count"]

    def test_each_table_has_snapshot(self, setup):
        for tbl in setup["receipt"]["tables"].values():
            assert tbl["iceberg_snapshot_id"] is not None

    def test_source_snapshot_id_present(self, setup):
        assert setup["receipt"]["input"]["source_snapshot_id"]

    def test_manifest_sha_in_receipt(self, setup):
        assert setup["receipt"]["input"]["manifest_sha256"] == setup["manifest_sha"]

    def test_receipt_file_exists(self, setup):
        p = setup["receipt_dir"] / "bronze_receipt.yaml"
        assert p.is_file()
        data = yaml.safe_load(p.read_text())
        assert data["receipt"]["status"] == "COMPLETE"

    def test_snapshot_manifest_exists(self, setup):
        p = setup["receipt_dir"] / "snapshot_manifest.yaml"
        assert p.is_file()
        data = yaml.safe_load(p.read_text())
        assert data["manifest"]["status"] == "COMPLETE"
        for tbl_name in ["application_train", "bureau", "bureau_balance"]:
            assert tbl_name in data["tables"]["bronze"]

    def test_bronze_bureau_balance_columns(self, setup):
        """Bronze bureau_balance must have raw schema columns, not enriched."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[1]") \
            .config("spark.sql.catalog.riskcloud", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.riskcloud.type", "hadoop") \
            .config("spark.sql.catalog.riskcloud.warehouse", str(setup["warehouse"])) \
            .getOrCreate()
        try:
            cols = spark.table("riskcloud.bronze.bureau_balance").columns
            # Raw columns must be present
            for rc in RAW_REQUIRED_COLUMNS["bureau_balance.csv"]:
                assert rc in cols, f"Missing raw column: {rc}"
            # Enriched column (SK_ID_CURR) must NOT be present
            assert "SK_ID_CURR" not in cols, "Bronze must not contain enriched SK_ID_CURR"
        finally:
            spark.stop()

    def test_metadata_columns_present(self, setup):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[1]") \
            .config("spark.sql.catalog.riskcloud", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.riskcloud.type", "hadoop") \
            .config("spark.sql.catalog.riskcloud.warehouse", str(setup["warehouse"])) \
            .getOrCreate()
        try:
            cols = spark.table("riskcloud.bronze.application_train").columns
            for mc in ["_source_manifest_sha256", "_source_snapshot_id", "_raw_row_sha256",
                       "_source_file_name", "_source_file_sha256", "_source_header_sha256",
                       "_bronze_schema_version"]:
                assert mc in cols, f"Missing metadata column: {mc}"
        finally:
            spark.stop()

    def test_rerun_idempotent(self, setup):
        """Re-running with same manifest must not duplicate rows."""
        receipt1 = setup["receipt"]
        run_id2 = "p12-test-002"
        receipt2 = ingest_bronze(
            setup["config"], setup["data_dir"], setup["manifest_path"],
            setup["receipt_dir"] / "run2", run_id2,
            git_commit="test", warehouse=str(setup["warehouse"]),
        )
        for tbl_name in ["application_train", "bureau", "bureau_balance"]:
            t1 = receipt1["tables"][tbl_name]
            t2 = receipt2["tables"][tbl_name]
            assert t2["bronze_row_count"] == t1["bronze_row_count"], (
                f"{tbl_name}: rerun grew from {t1['bronze_row_count']} to {t2['bronze_row_count']}"
            )
            assert t2["content_multiset_sha256"] == t1["content_multiset_sha256"], (
                f"{tbl_name}: fingerprint changed on rerun"
            )

    def test_row_hash_deterministic_across_runs(self, setup):
        """Same raw data → same _raw_row_sha256."""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[1]") \
            .config("spark.sql.catalog.riskcloud", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.riskcloud.type", "hadoop") \
            .config("spark.sql.catalog.riskcloud.warehouse", str(setup["warehouse"])) \
            .getOrCreate()
        try:
            rows = spark.sql(
                "SELECT _raw_row_sha256 FROM riskcloud.bronze.application_train "
                "WHERE _source_manifest_sha256 = '" + setup["manifest_sha"] + "'"
            ).collect()
            assert len(rows) > 0
            expected = _row_hash(["1", "0"], ["SK_ID_CURR", "TARGET"])
            assert rows[0]._raw_row_sha256 == expected
        finally:
            spark.stop()

    def test_fingerprint_in_receipt(self, setup):
        for tbl in setup["receipt"]["tables"].values():
            assert "content_multiset_sha256" in tbl
            assert len(tbl["content_multiset_sha256"]) == 64
