"""P1.1 — Home Credit Adapter constructor and structure tests."""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from riskcloud.adapters.home_credit.adapter import HomeCreditAdapter
from riskcloud.adapters.home_credit.boundary import HomeCreditBoundaryConfig
from riskcloud.contracts.validation import ContractValidationError

UTC = timezone.utc
CONFIG_PATH = Path(__file__).resolve().parents[3] / "case_studies" / "home_credit" / "configs" / "boundary_v1.yaml"
FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"
SNAPSHOT_ID = "snap-001"
INGESTED = datetime(2026, 7, 23, 12, 0, 0, tzinfo=UTC)


def _populated_manifest() -> tuple[Path, Path]:
    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp) / "data"
    data_dir.mkdir()
    for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
        (data_dir / f).write_text((FIXTURES / f).read_text())
    manifest_path = Path(tmp) / "manifest.yaml"
    import yaml

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
    return manifest_path, data_dir


@pytest.fixture
def config():
    return HomeCreditBoundaryConfig.from_yaml(CONFIG_PATH)


@pytest.fixture
def manifest_path_and_data_dir():
    return _populated_manifest()


@pytest.fixture
def adapter(config, manifest_path_and_data_dir):
    mf, data_dir = manifest_path_and_data_dir
    return HomeCreditAdapter(SNAPSHOT_ID, mf, data_dir, INGESTED, config)


class TestAdapterClosure:
    def test_validate_adapter_passes(self, adapter):
        assert adapter.validate_adapter() == []

    def test_identity(self, adapter):
        assert adapter.dataset_id == "home_credit"
        assert "Home Credit" in adapter.display_name
        assert adapter.adapter_version == "1.0.0"

    def test_columns(self, adapter):
        assert adapter.prediction_time_column() == "__proxy_application_time__"
        assert adapter.label_column() == "TARGET"
        assert adapter.label_time_column() == "__proxy_label_time__"

    def test_define_prediction_boundary(self, adapter):
        pp = adapter.define_prediction_boundary(
            {
                "__source_table__": "application_train",
                "SK_ID_CURR": 100001,
                "TARGET": 0,
            }
        )
        assert pp.entity_id == "SK_ID_CURR:100001"
        assert pp.snapshot_id == SNAPSHOT_ID
        assert pp.boundary_version == "hc-boundary-v1"

    def test_all_abstract_members(self, adapter):
        for attr in ["dataset_id", "display_name", "adapter_version"]:
            getattr(adapter, attr)
        for method in [
            "define_prediction_boundary",
            "prediction_time_column",
            "label_column",
            "label_time_column",
            "generate_events",
            "build_feature_catalog",
            "semantic_group_mapping",
        ]:
            getattr(adapter, method)


class TestConstructor:
    def test_rejects_empty_snapshot(self, config, manifest_path_and_data_dir):
        mf, data_dir = manifest_path_and_data_dir
        with pytest.raises(ContractValidationError):
            HomeCreditAdapter("", mf, data_dir, INGESTED, config)

    def test_rejects_none_snapshot(self, config, manifest_path_and_data_dir):
        mf, data_dir = manifest_path_and_data_dir
        with pytest.raises(ContractValidationError):
            HomeCreditAdapter(None, mf, data_dir, INGESTED, config)

    def test_rejects_missing_manifest(self, config):
        with pytest.raises(ContractValidationError, match="not found"):
            HomeCreditAdapter(SNAPSHOT_ID, Path("/nonexistent/manifest.yaml"), Path("/nonexistent"), INGESTED, config)

    def test_rejects_null_manifest(self, config):
        p, data_dir = _populated_manifest()
        import yaml

        # Overwrite with null metadata
        data = yaml.safe_load(p.read_text())
        for f in data["files"]:
            if f.get("required"):
                f["sha256"] = None
                f["row_count"] = None
        with open(p, "w") as fp:
            yaml.safe_dump(data, fp)
        with pytest.raises(ContractValidationError, match="null"):
            HomeCreditAdapter(SNAPSHOT_ID, p, data_dir, INGESTED, config)

    def test_rejects_non_utc_ingested(self, config, manifest_path_and_data_dir):
        mf, data_dir = manifest_path_and_data_dir
        from datetime import timedelta

        with pytest.raises(ContractValidationError, match="UTC"):
            HomeCreditAdapter(
                SNAPSHOT_ID, mf, data_dir, datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=10))), config
            )

    def test_rejects_ingested_before_prediction(self, config, manifest_path_and_data_dir):
        mf, data_dir = manifest_path_and_data_dir
        with pytest.raises(ContractValidationError, match="prediction_anchor"):
            HomeCreditAdapter(SNAPSHOT_ID, mf, data_dir, datetime(1999, 1, 1, tzinfo=UTC), config)

    def test_rejects_none_boundary(self, manifest_path_and_data_dir):
        mf, data_dir = manifest_path_and_data_dir
        with pytest.raises(ContractValidationError):
            HomeCreditAdapter(SNAPSHOT_ID, mf, data_dir, INGESTED, None)

    def test_source_system_is_honored(self, adapter):
        events = list(
            adapter.generate_events(
                {"__source_table__": "application_train", "SK_ID_CURR": 100001, "TARGET": 0},
                source_system="bronze.home_credit",
            )
        )
        assert events[0].source_system == "bronze.home_credit"

    def test_rejects_empty_manifest(self, config):
        import tempfile

        import yaml

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.safe_dump({"dataset": "home_credit", "files": []}, f)
            path = Path(f.name)
        try:
            with pytest.raises(ContractValidationError):
                HomeCreditAdapter(SNAPSHOT_ID, path, Path(tempfile.gettempdir()), INGESTED, config)
        finally:
            path.unlink()

    def test_rejects_missing_required_file_in_manifest(self, config, manifest_path_and_data_dir):
        mf, data_dir = manifest_path_and_data_dir
        import yaml

        data = yaml.safe_load(mf.read_text())
        data["files"] = [f for f in data["files"] if f["name"] != "bureau.csv"]
        p2 = mf.with_name("no_bureau.yaml")
        with open(p2, "w") as fp:
            yaml.safe_dump(data, fp)
        with pytest.raises(ContractValidationError, match="bureau"):
            HomeCreditAdapter(SNAPSHOT_ID, p2, data_dir, INGESTED, config)


class TestPredictionBoundaryGuard:
    def test_rejects_application_test_with_target(self, adapter):
        with pytest.raises(ContractValidationError, match="application_train"):
            adapter.define_prediction_boundary(
                {
                    "__source_table__": "application_test",
                    "SK_ID_CURR": 100001,
                    "TARGET": 0,
                }
            )

    def test_rejects_no_target(self, adapter):
        with pytest.raises(ContractValidationError, match="TARGET"):
            adapter.define_prediction_boundary(
                {
                    "__source_table__": "application_train",
                    "SK_ID_CURR": 100001,
                }
            )
