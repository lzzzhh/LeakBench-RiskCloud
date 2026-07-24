"""PR #7 — Realtime event contract tests (positive + negative)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from case_studies.home_credit.streaming.contracts.event_envelope import (
    ALL_SOURCE_TOPICS,
    SCHEMA_VERSION,
    TOPIC_BUREAU,
    TOPIC_DLQ,
    TOPIC_FEATURE_UPDATES,
    TOPIC_SPECS,
    ApplicationEventPayload,
    BureauBalanceEventPayload,
    BureauEventPayload,
    EventEnvelope,
    EventType,
    FeatureUpdate,
    Operation,
)
from case_studies.home_credit.streaming.contracts.feature_computation import (
    build_computation_specs,
    validate_closure,
)

UTC = timezone.utc
NOW = datetime(2000, 1, 1, 12, 0, 0, tzinfo=UTC)


def _app_dict() -> dict:
    return {
        "SK_ID_CURR": 100001,
        "AMT_CREDIT": 500000.0,
        "AMT_INCOME_TOTAL": 200000.0,
        "AMT_ANNUITY": 25000.0,
        "AMT_GOODS_PRICE": 450000.0,
        "DAYS_BIRTH": -12000,
        "EXT_SOURCE_1": 0.5,
        "EXT_SOURCE_2": 0.6,
        "EXT_SOURCE_3": 0.4,
    }


def _make_event(
    event_type: EventType, source_table: str, source_pk: str, entity_id: str, payload_dict: dict
) -> EventEnvelope:
    payload_cls = {
        EventType.APPLICATION_UPSERT: ApplicationEventPayload,
        EventType.BUREAU_UPSERT: BureauEventPayload,
        EventType.BUREAU_BALANCE_UPSERT: BureauBalanceEventPayload,
    }[event_type]
    payload_obj = payload_cls.from_dict(payload_dict)
    eid = EventEnvelope.compute_event_id(source_table, source_pk, payload_obj.to_dict(), NOW, Operation.UPSERT)
    return EventEnvelope(
        event_id=eid,
        schema_version=SCHEMA_VERSION,
        event_type=event_type,
        source_table=source_table,
        source_pk=source_pk,
        entity_id=entity_id,
        op=Operation.UPSERT,
        event_time=NOW,
        produced_at=NOW,
        payload=payload_obj,
    )


# =================================================================
# Kafka topics
# =================================================================


class TestKafkaTopics:
    def test_five_topics(self):
        assert len(TOPIC_SPECS) == 5

    def test_all_partitions_3(self):
        for spec in TOPIC_SPECS.values():
            assert spec.partitions == 3

    def test_source_delete_only(self):
        for t in ALL_SOURCE_TOPICS:
            assert TOPIC_SPECS[t].cleanup_policy == ("delete",)

    def test_feature_updates_compact_delete(self):
        assert TOPIC_SPECS[TOPIC_FEATURE_UPDATES].cleanup_policy == ("compact", "delete")

    def test_dlq_isolated(self):
        assert TOPIC_DLQ not in ALL_SOURCE_TOPICS


# =================================================================
# Source payloads
# =================================================================


class TestSourcePayloads:
    def test_app_from_dict(self):
        p = ApplicationEventPayload.from_dict(_app_dict())
        assert p.SK_ID_CURR == 100001

    def test_app_rejects_missing_sk(self):
        with pytest.raises(ValueError, match="SK_ID_CURR"):
            ApplicationEventPayload.from_dict({})

    def test_app_rejects_bad_flag(self):
        with pytest.raises(ValueError, match="FLAG_DOCUMENT_2"):
            ApplicationEventPayload.from_dict({**_app_dict(), "FLAG_DOCUMENT_2": 2})

    def test_bureau_from_dict(self):
        p = BureauEventPayload.from_dict({"SK_ID_CURR": 100001, "SK_ID_BUREAU": 500, "DAYS_CREDIT": -365})
        assert p.SK_ID_BUREAU == 500

    def test_bureau_balance_from_dict(self):
        p = BureauBalanceEventPayload.from_dict(
            {"SK_ID_CURR": 100001, "SK_ID_BUREAU": 500, "MONTHS_BALANCE": -6, "STATUS": "C"}
        )
        assert p.STATUS == "C"

    def test_bureau_balance_rejects_bad_status(self):
        with pytest.raises(ValueError, match="STATUS"):
            BureauBalanceEventPayload.from_dict({"SK_ID_CURR": 100001, "SK_ID_BUREAU": 500, "STATUS": "Z"})


# =================================================================
# EventEnvelope
# =================================================================


class TestEventEnvelope:
    def test_valid_app_event(self):
        evt = _make_event(EventType.APPLICATION_UPSERT, "application_train", "100001", "SK_ID_CURR:100001", _app_dict())
        assert evt.is_valid(), evt.validate()

    def test_json_roundtrip(self):
        evt = _make_event(EventType.APPLICATION_UPSERT, "application_train", "100001", "SK_ID_CURR:100001", _app_dict())
        restored = EventEnvelope.from_json(evt.to_json())
        assert restored.event_id == evt.event_id
        assert restored.payload.SK_ID_CURR == evt.payload.SK_ID_CURR

    def test_rejects_event_type_source_mismatch(self):
        evt = _make_event(
            EventType.BUREAU_UPSERT,
            "application_train",
            "500",
            "SK_ID_CURR:100001",
            {"SK_ID_CURR": 100001, "SK_ID_BUREAU": 500},
        )
        assert not evt.is_valid()

    def test_rejects_payload_type_mismatch(self):
        eid = EventEnvelope.compute_event_id("application_train", "100001", _app_dict(), NOW, Operation.UPSERT)
        bur_payload = BureauEventPayload.from_dict({"SK_ID_CURR": 100001, "SK_ID_BUREAU": 500})
        evt = EventEnvelope(
            event_id=eid,
            schema_version=SCHEMA_VERSION,
            event_type=EventType.APPLICATION_UPSERT,
            source_table="application_train",
            source_pk="100001",
            entity_id="SK_ID_CURR:100001",
            op=Operation.UPSERT,
            event_time=NOW,
            produced_at=NOW,
            payload=bur_payload,
        )  # type: ignore[arg-type]
        assert not evt.is_valid()

    def test_rejects_tampered_event_id(self):
        evt = _make_event(EventType.APPLICATION_UPSERT, "application_train", "100001", "SK_ID_CURR:100001", _app_dict())
        bad = EventEnvelope(
            event_id="sha256:" + "f" * 64,
            schema_version=evt.schema_version,
            event_type=evt.event_type,
            source_table=evt.source_table,
            source_pk=evt.source_pk,
            entity_id=evt.entity_id,
            op=evt.op,
            event_time=evt.event_time,
            produced_at=evt.produced_at,
            payload=evt.payload,
        )
        assert not bad.is_valid()

    def test_rejects_changed_payload_old_id(self):
        evt = _make_event(EventType.APPLICATION_UPSERT, "application_train", "100001", "SK_ID_CURR:100001", _app_dict())
        # Same event_id but different payload
        eid = evt.event_id
        different_payload = ApplicationEventPayload.from_dict({**_app_dict(), "AMT_CREDIT": 999.0})
        bad = EventEnvelope(
            event_id=eid,
            schema_version=evt.schema_version,
            event_type=evt.event_type,
            source_table=evt.source_table,
            source_pk=evt.source_pk,
            entity_id=evt.entity_id,
            op=evt.op,
            event_time=evt.event_time,
            produced_at=evt.produced_at,
            payload=different_payload,
        )
        assert not bad.is_valid()


# =================================================================
# FeatureUpdate
# =================================================================


class TestFeatureUpdate:
    def test_valid(self):
        uid = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "bureau.record_count", 1, NOW, 3.0)
        fu = FeatureUpdate(
            feature_update_id=uid,
            entity_id="SK_ID_CURR:100001",
            feature_id="bureau.record_count",
            feature_value=3.0,
            feature_version=1,
            event_time=NOW,
            computed_at=NOW,
            source_event_id="sha256:" + "a" * 64,
            source_topic=TOPIC_BUREAU,
        )
        assert fu.is_valid()

    def test_rejects_unknown_feature_id(self):
        uid = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "bureau.record_count", 1, NOW, 0.0)
        fu = FeatureUpdate(
            feature_update_id=uid,
            entity_id="SK_ID_CURR:100001",
            feature_id="unknown.feature",
            feature_value=0.0,
            feature_version=1,
            event_time=NOW,
            computed_at=NOW,
            source_event_id="sha256:" + "a" * 64,
            source_topic=TOPIC_BUREAU,
        )
        assert not fu.is_valid()

    def test_rejects_tampered_value_old_id(self):
        uid = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "bureau.record_count", 1, NOW, 3.0)
        fu = FeatureUpdate(
            feature_update_id=uid,
            entity_id="SK_ID_CURR:100001",
            feature_id="bureau.record_count",
            feature_value=99.0,
            feature_version=1,
            event_time=NOW,
            computed_at=NOW,
            source_event_id="sha256:" + "a" * 64,
            source_topic=TOPIC_BUREAU,
        )
        assert not fu.is_valid()

    def test_rejects_nan(self):
        with pytest.raises(ValueError):
            FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "bureau.record_count", 1, NOW, float("nan"))


# =================================================================
# Feature computation specs
# =================================================================


class TestFeatureComputationSpecs:
    def test_20_specs(self):
        assert len(build_computation_specs()) == 20

    def test_closure_passes(self):
        ok, errors = validate_closure()
        assert ok, errors

    def test_all_batch_refs_non_empty(self):
        for s in build_computation_specs():
            assert s.batch_implementation_ref, s.feature_id

    def test_bureau_temporal_filter(self):
        for s in build_computation_specs():
            if s.source_table == "bureau":
                assert s.input_filter_sql and "DAYS_CREDIT" in s.input_filter_sql, s.feature_id

    def test_bureau_balance_temporal_filter(self):
        for s in build_computation_specs():
            if s.source_table == "bureau_balance":
                assert s.input_filter_sql and "MONTHS_BALANCE" in s.input_filter_sql, s.feature_id

    def test_batch_refs_resolve(self):
        import importlib

        for s in build_computation_specs():
            ref = s.batch_implementation_ref
            mod_name, func_name = ref.split(":", 1)
            mod = importlib.import_module(mod_name)
            assert callable(getattr(mod, func_name)), f"{ref} not callable"
