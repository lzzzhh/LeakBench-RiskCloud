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
from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces
from riskcloud.adapters.home_credit.field_mapping import RAW_REQUIRED_COLUMNS

pytestmark = [
    pytest.mark.integration, pytest.mark.bronze_integration,
    # PySpark 3.5.3 local socket readers may leave raw socket
    # cleanup to garbage collection. Filter restricted to
    # socket.socket unraisable/resource warnings in this module.
    pytest.mark.filterwarnings(
        r"ignore:.*socket\.socket.*:pytest.PytestUnraisableExceptionWarning"
    ),
    pytest.mark.filterwarnings(
        r"ignore:.*socket\.socket.*:ResourceWarning"
    ),
]

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"
CONFIG_PATH = (
    Path(__file__).resolve().parents[3]
    / "case_studies" / "home_credit" / "configs" / "bronze_v1.yaml"
)
REQUIRED_FILES = ["application_train.csv", "bureau.csv", "bureau_balance.csv"]

_ALLOWED_SPARK_SHUTDOWN = (ConnectionResetError, ConnectionRefusedError, BrokenPipeError, EOFError)


def _is_known_shutdown_noise(error: BaseException) -> bool:
    import errno
    if isinstance(error, _ALLOWED_SPARK_SHUTDOWN):
        return True
    if isinstance(error, OSError):
        return error.errno in {errno.ECONNRESET, errno.ECONNREFUSED, errno.EPIPE, errno.EBADF}
    return False


def _flatten_exc_group(exc: BaseException) -> list[BaseException]:
    import builtins
    group_type = getattr(builtins, "BaseExceptionGroup", None)
    if group_type is not None and isinstance(exc, group_type):
        leaves: list[BaseException] = []
        for child in exc.exceptions:
            leaves.extend(_flatten_exc_group(child))
        return leaves
    return [exc]


def _stop_test_spark(spark) -> None:
    try:
        spark.stop()
    except BaseException as exc:
        leaves = _flatten_exc_group(exc)
        if leaves and all(_is_known_shutdown_noise(e) for e in leaves):
            return  # PySpark 3.5.3 local socket cleanup noise
        raise


def _copy_fixture_files(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    for f in REQUIRED_FILES:
        (data_dir / f).write_bytes((FIXTURES / f).read_bytes())


def _populate_existing_manifest(data_dir: Path, manifest_path: Path) -> str:
    manifest = {
        "dataset": "home_credit",
        "files": [{"name": f, "required": True} for f in REQUIRED_FILES],
    }
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    from case_studies.home_credit.scripts.validate_manifest import populate_manifest
    ok = populate_manifest(data_dir, manifest_path)
    assert ok, "populate failed"
    return _sha256(manifest_path.read_bytes())


# -----------------------------------------------------------------
# Module fixture — shared Spark session for all tests
# -----------------------------------------------------------------

@pytest.fixture(scope="module")
def module_setup():
    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp) / "data"
    warehouse = Path(tmp) / "warehouse"
    manifest_path = Path(tmp) / "manifest.yaml"

    _copy_fixture_files(data_dir)
    manifest_sha = _populate_existing_manifest(data_dir, manifest_path)
    config = BronzeConfig.from_yaml(CONFIG_PATH)

    spark = get_spark(app_name="p12-integration", warehouse=str(warehouse))
    setup_namespaces(spark)

    primary_error = None
    try:
        receipt = ingest_bronze(config, data_dir, manifest_path,
                                Path(tmp) / "receipts", "p12-a",
                                git_commit="test", spark=spark)
        yield {
            "data_dir": data_dir, "manifest_path": manifest_path,
            "warehouse": warehouse, "manifest_sha": manifest_sha,
            "config": config, "receipt": receipt, "spark": spark,
            "receipt_dir": Path(tmp) / "receipts",
        }
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        import gc
        gc.collect()
        try:
            _stop_test_spark(spark)
            gc.collect()
        except BaseException as stop_err:
            if primary_error is None:
                raise
            add_note = getattr(primary_error, "add_note", None)
            if callable(add_note):
                add_note(f"Spark teardown also failed: {type(stop_err).__name__}")


# -----------------------------------------------------------------
# Basic
# -----------------------------------------------------------------

class TestBasicWrite:

    def test_receipt_complete(self, module_setup):
        assert module_setup["receipt"]["receipt"]["status"] == "COMPLETE"

    def test_three_tables(self, module_setup):
        assert len(module_setup["receipt"]["tables"]) == 3

    def test_row_count_closure(self, module_setup):
        for tbl in module_setup["receipt"]["tables"].values():
            assert tbl["source_row_count"] == tbl["partition_row_count_after"]

    def test_snapshots_non_null(self, module_setup):
        for tbl in module_setup["receipt"]["tables"].values():
            assert tbl["iceberg_snapshot_id"] is not None

    def test_metadata_location_non_null(self, module_setup):
        for tbl in module_setup["receipt"]["tables"].values():
            assert tbl["metadata_location"] is not None

    def test_manifest_sha_in_receipt(self, module_setup):
        assert module_setup["receipt"]["input"]["manifest_sha256"] == module_setup["manifest_sha"]

    def test_snapshot_manifest_complete(self, module_setup):
        assert "snapshot_manifest_sha256" in module_setup["receipt"]["quality"]

    def test_bureau_balance_no_sk_id_curr(self, module_setup):
        cols = module_setup["spark"].table("riskcloud.bronze.bureau_balance").columns
        assert "SK_ID_CURR" not in cols
        for rc in RAW_REQUIRED_COLUMNS["bureau_balance.csv"]:
            assert rc in cols

    def test_source_columns_string(self, module_setup):
        from pyspark.sql.types import StringType
        for tn in ["riskcloud.bronze.application_train", "riskcloud.bronze.bureau",
                   "riskcloud.bronze.bureau_balance"]:
            for fld in module_setup["spark"].table(tn).schema.fields:
                if not fld.name.startswith("_"):
                    assert isinstance(fld.dataType, StringType), f"{tn}.{fld.name}"


# -----------------------------------------------------------------
# Rerun
# -----------------------------------------------------------------

class TestRerun:

    def test_same_manifest_rerun(self, module_setup):
        receipt2 = ingest_bronze(
            module_setup["config"], module_setup["data_dir"], module_setup["manifest_path"],
            Path(tempfile.mkdtemp()) / "receipts", "p12-rerun",
            git_commit="test", spark=module_setup["spark"],
        )
        for tbl_name in ["application_train", "bureau", "bureau_balance"]:
            t1 = module_setup["receipt"]["tables"][tbl_name]
            t2 = receipt2["tables"][tbl_name]
            assert t2["partition_row_count_before"] == t1["source_row_count"]
            assert t2["rerun_duplicate_growth"] == 0
            assert t2["content_multiset_sha256"] == t1["content_multiset_sha256"]


# -----------------------------------------------------------------
# Different manifest
# -----------------------------------------------------------------

class TestDifferentManifest:

    def test_manifest_b_preserves_a_partition(self, module_setup):
        tmp = tempfile.mkdtemp()
        data_dir_b = Path(tmp) / "data"
        manifest_b = Path(tmp) / "manifest_b.yaml"

        _copy_fixture_files(data_dir_b)
        app_b = data_dir_b / "application_train.csv"
        app_b.write_text("SK_ID_CURR,TARGET\n1,1\n", encoding="utf-8")
        manifest_sha_b = _populate_existing_manifest(data_dir_b, manifest_b)

        assert manifest_sha_b != module_setup["manifest_sha"]

        receipt_b = ingest_bronze(
            module_setup["config"], data_dir_b, manifest_b,
            Path(tmp) / "receipts", "p12-b",
            git_commit="test", spark=module_setup["spark"],
        )

        assert receipt_b["input"]["source_snapshot_id"] != module_setup["receipt"]["input"]["source_snapshot_id"]

        ta = module_setup["receipt"]["tables"]["application_train"]
        tb = receipt_b["tables"]["application_train"]
        assert tb["content_multiset_sha256"] != ta["content_multiset_sha256"]

        for tbl_name in ["bureau", "bureau_balance"]:
            assert receipt_b["tables"][tbl_name]["content_multiset_sha256"] == \
                   module_setup["receipt"]["tables"][tbl_name]["content_multiset_sha256"]

        spark = module_setup["spark"]
        for tbl_key, tbl_name in [
            ("application_train", "riskcloud.bronze.application_train"),
            ("bureau", "riskcloud.bronze.bureau"),
            ("bureau_balance", "riskcloud.bronze.bureau_balance"),
        ]:
            count_a = spark.sql(
                f"SELECT COUNT(*) FROM {tbl_name} "
                f"WHERE _source_manifest_sha256 = '{module_setup['manifest_sha']}'"
            ).collect()[0][0]
            count_b = spark.sql(
                f"SELECT COUNT(*) FROM {tbl_name} "
                f"WHERE _source_manifest_sha256 = '{manifest_sha_b}'"
            ).collect()[0][0]
            assert count_a == module_setup["receipt"]["tables"][tbl_key]["source_row_count"]
            assert count_b == receipt_b["tables"][tbl_key]["source_row_count"]


# -----------------------------------------------------------------
# Publication and metadata gate
# -----------------------------------------------------------------

class TestPublicationGate:

    def test_runtime_metadata_and_disk_publication(self, module_setup):
        receipt = module_setup["receipt"]
        for tres in receipt["tables"].values():
            assert tres["iceberg_snapshot_id"] == tres["java_snapshot_id"] == tres["metadata_snapshot_id"]
            assert tres["iceberg_runtime_class"]
            assert tres["iceberg_class_loader"]
            assert tres["iceberg_class_loader"] != "null"
            assert tres["metadata_location"]

        rd = module_setup["receipt_dir"]
        disk_receipt = yaml.safe_load((rd / "bronze_receipt.yaml").read_text(encoding="utf-8"))
        disk_manifest = yaml.safe_load((rd / "snapshot_manifest.yaml").read_text(encoding="utf-8"))
        assert disk_receipt["receipt"]["status"] == "COMPLETE"
        assert disk_manifest["manifest"]["status"] == "COMPLETE"
        sm_path = rd / "snapshot_manifest.yaml"
        assert disk_receipt["quality"]["snapshot_manifest_sha256"] == _sha256(sm_path.read_bytes())
        assert {p.name for p in rd.iterdir()} == {"snapshot_manifest.yaml", "bronze_receipt.yaml"}
