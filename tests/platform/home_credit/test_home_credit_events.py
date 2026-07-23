"""P1.1 — Home Credit Event tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
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
MANIFEST_SHA = "a" * 64
SNAPSHOT_ID = "snap-001"
INGESTED = datetime(2026, 7, 23, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def adapter():
    cfg = HomeCreditBoundaryConfig.from_yaml(CONFIG_PATH)
    return HomeCreditAdapter(SNAPSHOT_ID, MANIFEST_SHA, INGESTED, cfg)


class TestApplicationEvent:

    def test_valid(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))
        assert len(events) == 1
        evt = events[0]
        assert evt.entity_id == "SK_ID_CURR:100001"
        assert evt.customer_id == "customer:100001"
        assert evt.event_type.value == "loan_application"

    def test_roundtrip_json(self, adapter):
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
            list(adapter.generate_events({
                "__source_table__": "application_train",
                "TARGET": 0,
            }))


class TestBureauEvent:

    def test_valid(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "bureau",
            "SK_ID_CURR": 100001,
            "SK_ID_BUREAU": 500,
            "DAYS_CREDIT": -365,
        }))
        assert len(events) == 1
        assert events[0].entity_id == "SK_ID_CURR:100001"

    def test_rejects_positive_days_credit(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({
                "__source_table__": "bureau",
                "SK_ID_CURR": 100001,
                "SK_ID_BUREAU": 500,
                "DAYS_CREDIT": 10,
            }))

    def test_event_time_before_prediction(self, adapter):
        events = list(adapter.generate_events({
            "__source_table__": "bureau",
            "SK_ID_CURR": 100001,
            "SK_ID_BUREAU": 500,
            "DAYS_CREDIT": -365,
        }))
        assert events[0].event_time < adapter._boundary.prediction_anchor


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
        assert events[0].entity_id == "SK_ID_CURR:100001"

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

    def test_rejects_invalid_status(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({
                "__source_table__": "bureau_balance",
                "SK_ID_CURR": 100001,
                "SK_ID_BUREAU": 500,
                "MONTHS_BALANCE": -6,
                "STATUS": "",
            }))


class TestEventIdentity:

    def test_same_input_same_event_id(self, adapter):
        e1 = list(adapter.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))[0]
        e2 = list(adapter.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))[0]
        assert e1.event_id == e2.event_id

    def test_changed_ingested_at_same_event_id(self, adapter):
        cfg = HomeCreditBoundaryConfig.from_yaml(CONFIG_PATH)
        a2 = HomeCreditAdapter(SNAPSHOT_ID, MANIFEST_SHA, datetime(2027, 1, 1, tzinfo=UTC), cfg)
        e1 = list(adapter.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))[0]
        e2 = list(a2.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))[0]
        assert e1.event_id == e2.event_id

    def test_changed_manifest_changes_identity(self, adapter):
        cfg = HomeCreditBoundaryConfig.from_yaml(CONFIG_PATH)
        a2 = HomeCreditAdapter(SNAPSHOT_ID, "b" * 64, INGESTED, cfg)
        e1 = list(adapter.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))[0]
        e2 = list(a2.generate_events({
            "__source_table__": "application_train",
            "SK_ID_CURR": 100001,
            "TARGET": 0,
        }))[0]
        assert e1.event_id != e2.event_id


class TestEventErrorHandling:

    def test_missing_source_table(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({"SK_ID_CURR": 1}))

    def test_unknown_source_table(self, adapter):
        with pytest.raises(ContractValidationError):
            list(adapter.generate_events({
                "__source_table__": "unknown_table",
                "SK_ID_CURR": 1,
            }))
