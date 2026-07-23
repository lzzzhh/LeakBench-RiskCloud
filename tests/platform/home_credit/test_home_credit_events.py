"""P1.1 — Home Credit Event + Adapter + Binding tests."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from riskcloud.adapters.home_credit.adapter import HomeCreditAdapter
from riskcloud.adapters.home_credit.boundary import HomeCreditBoundaryConfig
from riskcloud.contracts.validation import ContractValidationError

UTC = timezone.utc
CONFIG_PATH = (
    Path(__file__).resolve().parents[3]
    / "case_studies"
    / "home_credit"
    / "configs"
    / "boundary_v1.yaml"
)
FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"
SNAPSHOT_ID = "snap-001"
INGESTED = datetime(2026, 7, 23, 12, 0, 0, tzinfo=UTC)


def _populated_manifest() -> Path:
    """Create a temporary manifest from fixtures and populate it."""
    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp) / "data"
    data_dir.mkdir()
    for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
        (data_dir / f).write_text((FIXTURES / f).read_text())
    manifest_path = Path(tmp) / "manifest.yaml"
    import yaml
    manifest = {
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
    assert ok, "Failed to populate test manifest"
    return manifest_path


@pytest.fixture
def config():
    return HomeCreditBoundaryConfig.from_yaml(CONFIG_PATH)


@pytest.fixture
def adapter(config):
    mf = _populated_manifest()
    return HomeCreditAdapter(SNAPSHOT_ID, mf, INGESTED, config)


class TestApplicationEvent:

    def test_valid(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))
        assert len(events) == 1
        assert events[0].entity_id == "SK_ID_CURR:100001"

    def test_strict_roundtrip(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))
        js = events[0].to_json()
        restored = type(events[0]).parse(json.loads(js))
        assert restored.event_id == events[0].event_id

    def test_event_time_le_available_at(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))
        assert events[0].event_time <= events[0].available_at

    def test_missing_sk_id_curr(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({"__source_table__": "application_train", "TARGET": 0}))


class TestBureauEvent:

    def test_valid(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "bureau",
            "SK_ID_CURR": 100001,
            "SK_ID_BUREAU": 500,
            "DAYS_CREDIT": -365,
        }))
        assert len(events) == 1

    def test_strict_roundtrip(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "bureau",
            "SK_ID_CURR": 100001,
            "SK_ID_BUREAU": 500,
            "DAYS_CREDIT": -365,
        }))
        js = events[0].to_json()
        restored = type(events[0]).parse(json.loads(js))
        assert restored.event_id == events[0].event_id

    def test_rejects_positive_days(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({
                "__source_table__": "bureau",
                "SK_ID_CURR": 100001,
                "SK_ID_BUREAU": 500,
                "DAYS_CREDIT": 10,
            }))

    def test_rejects_bool_days(self, adapter):
        with pytest.raises(ContractValidationError, match="bool"):
            list(adapter.generate_events({
                "__source_table__": "bureau",
                "SK_ID_CURR": 100001,
                "SK_ID_BUREAU": 500,
                "DAYS_CREDIT": True,
            }))

    def test_rejects_float_days(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({
                "__source_table__": "bureau",
                "SK_ID_CURR": 100001,
                "SK_ID_BUREAU": 500,
                "DAYS_CREDIT": -365.5,
            }))


class TestBureauBalanceEvent:

    def test_valid(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "bureau_balance",
            "SK_ID_CURR": 100001,
            "SK_ID_BUREAU": 500,
            "MONTHS_BALANCE": -6,
            "STATUS": "C",
        }))
        assert len(events) == 1

    def test_strict_roundtrip(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "bureau_balance",
            "SK_ID_CURR": 100001,
            "SK_ID_BUREAU": 500,
            "MONTHS_BALANCE": -6,
            "STATUS": "C",
        }))
        js = events[0].to_json()
        restored = type(events[0]).parse(json.loads(js))
        assert restored.event_id == events[0].event_id

    def test_rejects_missing_sk_id_curr(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({
                "__source_table__": "bureau_balance",
                "SK_ID_BUREAU": 500,
                "MONTHS_BALANCE": -6,
                "STATUS": "C",
            }))

    def test_rejects_positive_months(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({
                "__source_table__": "bureau_balance",
                "SK_ID_CURR": 100001,
                "SK_ID_BUREAU": 500,
                "MONTHS_BALANCE": 3,
                "STATUS": "C",
            }))

    def test_rejects_bool_months(self, adapter):
        with pytest.raises(ContractValidationError, match="bool"):
            list(adapter.generate_events({
                "__source_table__": "bureau_balance",
                "SK_ID_CURR": 100001,
                "SK_ID_BUREAU": 500,
                "MONTHS_BALANCE": False,
                "STATUS": "C",
            }))

    def test_rejects_float_months(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({
                "__source_table__": "bureau_balance",
                "SK_ID_CURR": 100001,
                "SK_ID_BUREAU": 500,
                "MONTHS_BALANCE": -6.8,
                "STATUS": "C",
            }))


class TestEventIdentity:

    def test_same_input_same_id(self, adapter):
        rec = {"__source_table__": "application_train", "SK_ID_CURR": 100001, "TARGET": 0}
        e1 = list(adapter.generate_events(rec))[0]
        e2 = list(adapter.generate_events(rec))[0]
        assert e1.event_id == e2.event_id

    def test_changed_manifest_changes_identity(self, config):
        import yaml
        mf_path = _populated_manifest()
        a1 = HomeCreditAdapter(SNAPSHOT_ID, mf_path, INGESTED, config)
        e1 = list(a1.generate_events({"__source_table__": "application_train", "SK_ID_CURR": 100001, "TARGET": 0}))[0]

        # Modify manifest content → different SHA → different event identity
        manifest = yaml.safe_load(mf_path.read_text())
        for f in manifest["files"]:
            if f["name"] == "application_train.csv":
                f["row_count"] = 999  # change row count
        mf2_path = mf_path.with_name("manifest2.yaml")
        with open(mf2_path, "w") as f2:
            yaml.safe_dump(manifest, f2)
        a2 = HomeCreditAdapter(SNAPSHOT_ID, mf2_path, INGESTED, config)
        e2 = list(a2.generate_events({"__source_table__": "application_train", "SK_ID_CURR": 100001, "TARGET": 0}))[0]

        assert e1.event_id != e2.event_id


class TestEventErrors:

    def test_missing_source_table(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({"SK_ID_CURR": 1}))

    def test_unknown_source_table(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({"__source_table__": "unknown", "SK_ID_CURR": 1}))


class TestAdapterClosure:

    def test_validate_adapter(self, adapter):
        assert adapter.validate_adapter() == []

    def test_identity(self, adapter):
        assert adapter.dataset_id == "home_credit"
        assert adapter.adapter_version == "1.0.0"

    def test_prediction_boundary_requires_application_train(self, adapter):
        with pytest.raises(ContractValidationError, match="application_train"):
            adapter.define_prediction_boundary({
                "__source_table__": "application_test",
                "SK_ID_CURR": 100001,
                "TARGET": 0,
            })

    def test_prediction_boundary_rejects_no_target(self, adapter):
        with pytest.raises(ContractValidationError, match="TARGET"):
            adapter.define_prediction_boundary({
                "__source_table__": "application_train",
                "SK_ID_CURR": 100001,
            })


class TestConstructor:

    def test_rejects_non_utc_ingested(self, config):
        mf = _populated_manifest()
        with pytest.raises(ContractValidationError, match="UTC"):
            HomeCreditAdapter(SNAPSHOT_ID, mf,
                              datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=10))),
                              config)

    def test_rejects_ingested_before_prediction(self, config):
        mf = _populated_manifest()
        with pytest.raises(ContractValidationError, match="prediction_anchor"):
            HomeCreditAdapter(SNAPSHOT_ID, mf, datetime(1999, 1, 1, tzinfo=UTC), config)

    def test_rejects_null_manifest(self, config):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            import yaml
            yaml.safe_dump({"files": [
                {"name": "application_train.csv", "required": True,
                 "sha256": None, "row_count": None, "columns": None},
            ]}, f)
            path = Path(f.name)
        try:
            with pytest.raises(ContractValidationError, match="null"):
                HomeCreditAdapter(SNAPSHOT_ID, path, INGESTED, config)
        finally:
            path.unlink()
