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
    FeatureUpdate,
    Operation,
)
from case_studies.home_credit.streaming.contracts.feature_computation import (
    build_computation_specs,
    validate_closure,
)

UTC = timezone.utc
NOW = datetime(2000, 1, 1, 12, 0, 0, tzinfo=UTC)


def _valid_event_dict() -> dict:
    eid = EventEnvelope.compute_event_id("application_train", "100001", {"TARGET": "0"}, NOW, Operation.UPSERT)
    return {
        "event_id": eid, "schema_version": SCHEMA_VERSION,
        "event_type": "application_upsert", "source_table": "application_train",
        "source_pk": "100001", "entity_id": "SK_ID_CURR:100001",
        "op": "UPSERT", "event_time": NOW.isoformat(),
        "produced_at": NOW.isoformat(), "payload": {"TARGET": "0"},
    }


# -----------------------------------------------------------------
# Kafka topics
# -----------------------------------------------------------------

class TestKafkaTopics:

    def test_five_topics(self):
        assert len(TOPIC_SPECS) == 5

    def test_all_partitions_3(self):
        for spec in TOPIC_SPECS.values():
            assert spec.partitions == 3

    def test_replication_factor_1(self):
        for spec in TOPIC_SPECS.values():
            assert spec.replication_factor == 1

    def test_source_topics_delete_only(self):
        for t in ALL_SOURCE_TOPICS:
            assert TOPIC_SPECS[t].cleanup_policy == ("delete",)

    def test_feature_updates_compact_delete(self):
        assert TOPIC_SPECS[TOPIC_FEATURE_UPDATES].cleanup_policy == ("compact", "delete")

    def test_dlq_delete_only(self):
        assert TOPIC_SPECS[TOPIC_DLQ].cleanup_policy == ("delete",)

    def test_dlq_not_in_source_topics(self):
        assert TOPIC_DLQ not in ALL_SOURCE_TOPICS


# -----------------------------------------------------------------
# Source payloads
# -----------------------------------------------------------------

class TestSourcePayloads:

    def test_application_payload(self):
        p = ApplicationEventPayload(SK_ID_CURR=100001, AMT_CREDIT=500000.0)
        d = p.to_dict()
        assert d["SK_ID_CURR"] == 100001
        assert d["AMT_CREDIT"] == 500000.0

    def test_bureau_payload(self):
        p = BureauEventPayload(SK_ID_CURR=100001, SK_ID_BUREAU=500, DAYS_CREDIT=-365)
        d = p.to_dict()
        assert d["SK_ID_BUREAU"] == 500

    def test_bureau_balance_payload(self):
        p = BureauBalanceEventPayload(SK_ID_CURR=100001, SK_ID_BUREAU=500, MONTHS_BALANCE=-6, STATUS="C")
        d = p.to_dict()
        assert d["STATUS"] == "C"


# -----------------------------------------------------------------
# EventEnvelope positive
# -----------------------------------------------------------------

class TestEventEnvelopePositive:

    def test_valid(self):
        evt = EventEnvelope.from_dict(_valid_event_dict())
        assert evt.is_valid()

    def test_json_roundtrip(self):
        evt = EventEnvelope.from_dict(_valid_event_dict())
        restored = EventEnvelope.from_json(evt.to_json())
        assert restored.event_id == evt.event_id
        assert restored.entity_id == evt.entity_id

    def test_event_id_deterministic(self):
        eid1 = EventEnvelope.compute_event_id("application_train", "100001", {"TARGET": "0"}, NOW, Operation.UPSERT)
        eid2 = EventEnvelope.compute_event_id("application_train", "100001", {"TARGET": "0"}, NOW, Operation.UPSERT)
        assert eid1 == eid2

    def test_payload_order_does_not_affect_id(self):
        eid1 = EventEnvelope.compute_event_id("a", "1", {"x": "1", "y": "2"}, NOW, Operation.UPSERT)
        eid2 = EventEnvelope.compute_event_id("a", "1", {"y": "2", "x": "1"}, NOW, Operation.UPSERT)
        assert eid1 == eid2

    def test_payload_change_changes_id(self):
        eid1 = EventEnvelope.compute_event_id("a", "1", {"x": "1"}, NOW, Operation.UPSERT)
        eid2 = EventEnvelope.compute_event_id("a", "1", {"x": "2"}, NOW, Operation.UPSERT)
        assert eid1 != eid2

    def test_bureau_event_type_matches_source(self):
        eid = EventEnvelope.compute_event_id("bureau", "500", {}, NOW, Operation.UPSERT)
        d = _valid_event_dict()
        d.update(event_id=eid, event_type="bureau_upsert", source_table="bureau", source_pk="500")
        assert EventEnvelope.from_dict(d).is_valid()

    def test_bureau_balance_event(self):
        eid = EventEnvelope.compute_event_id("bureau_balance", "500", {}, NOW, Operation.UPSERT)
        d = _valid_event_dict()
        d.update(event_id=eid, event_type="bureau_balance_upsert", source_table="bureau_balance", source_pk="500")
        assert EventEnvelope.from_dict(d).is_valid()


# -----------------------------------------------------------------
# EventEnvelope negative
# -----------------------------------------------------------------

class TestEventEnvelopeNegative:

    def test_rejects_non_canonical_entity_id(self):
        d = _valid_event_dict()
        d["entity_id"] = "100001"
        assert not EventEnvelope.from_dict(d).is_valid()

    def test_rejects_wrong_schema_version(self):
        d = _valid_event_dict()
        d["schema_version"] = 99
        assert not EventEnvelope.from_dict(d).is_valid()

    def test_rejects_naive_event_time(self):
        d = _valid_event_dict()
        d["event_time"] = "2000-01-01T12:00:00"
        evt = EventEnvelope.from_dict(d)
        assert not evt.is_valid()

    def test_rejects_type_source_mismatch(self):
        eid = EventEnvelope.compute_event_id("application_train", "1", {}, NOW, Operation.UPSERT)
        d = _valid_event_dict()
        d.update(event_id=eid, event_type="bureau_upsert", source_table="application_train")
        assert not EventEnvelope.from_dict(d).is_valid()

    def test_rejects_unknown_source_table(self):
        eid = EventEnvelope.compute_event_id("unknown", "1", {}, NOW, Operation.UPSERT)
        d = _valid_event_dict()
        d.update(event_id=eid, source_table="unknown")
        assert not EventEnvelope.from_dict(d).is_valid()

    def test_rejects_bad_event_id_format(self):
        d = _valid_event_dict()
        d["event_id"] = "not-a-hash"
        assert not EventEnvelope.from_dict(d).is_valid()


# -----------------------------------------------------------------
# FeatureUpdate positive
# -----------------------------------------------------------------

class TestFeatureUpdatePositive:

    def test_valid(self):
        uid = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "bureau.record_count", 1, NOW, 3.0)
        fu = FeatureUpdate(
            feature_update_id=uid, entity_id="SK_ID_CURR:100001",
            feature_id="bureau.record_count", feature_value=3.0, feature_version=1,
            event_time=NOW, computed_at=NOW, source_event_id="sha256:" + "a" * 64,
            source_topic=TOPIC_BUREAU,
        )
        assert fu.is_valid()

    def test_update_id_deterministic(self):
        uid1 = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "bureau.record_count", 1, NOW, 3.0)
        uid2 = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "bureau.record_count", 1, NOW, 3.0)
        assert uid1 == uid2

    def test_null_value_allowed(self):
        uid = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "bureau.record_count", 1, NOW, None)
        fu = FeatureUpdate(
            feature_update_id=uid, entity_id="SK_ID_CURR:100001",
            feature_id="bureau.record_count", feature_value=None, feature_version=1,
            event_time=NOW, computed_at=NOW, source_event_id="sha256:" + "a" * 64,
            source_topic=TOPIC_BUREAU,
        )
        assert fu.is_valid()


# -----------------------------------------------------------------
# FeatureUpdate negative
# -----------------------------------------------------------------

class TestFeatureUpdateNegative:

    def test_rejects_nan_value(self):
        with pytest.raises(ValueError):
            FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "f", 1, NOW, float("nan"))

    def test_rejects_inf_value(self):
        with pytest.raises(ValueError):
            FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "f", 1, NOW, float("inf"))

    def test_rejects_non_canonical_entity(self):
        uid = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "f", 1, NOW, 0.0)
        fu = FeatureUpdate(
            feature_update_id=uid, entity_id="bad", feature_id="f",
            feature_value=0.0, feature_version=1, event_time=NOW, computed_at=NOW,
            source_event_id="sha256:" + "a" * 64, source_topic=TOPIC_BUREAU,
        )
        assert not fu.is_valid()

    def test_rejects_unknown_source_topic(self):
        uid = FeatureUpdate.compute_update_id("SK_ID_CURR:100001", "f", 1, NOW, 0.0)
        fu = FeatureUpdate(
            feature_update_id=uid, entity_id="SK_ID_CURR:100001", feature_id="f",
            feature_value=0.0, feature_version=1, event_time=NOW, computed_at=NOW,
            source_event_id="sha256:" + "a" * 64, source_topic="unknown.topic",
        )
        assert not fu.is_valid()


# -----------------------------------------------------------------
# Feature computation specs
# -----------------------------------------------------------------

class TestFeatureComputationSpecs:

    def test_20_specs(self):
        assert len(build_computation_specs()) == 20

    def test_closure_passes(self):
        ok, errors = validate_closure()
        assert ok, errors

    def test_all_batch_refs_non_empty(self):
        for s in build_computation_specs():
            assert s.batch_implementation_ref, s.feature_id

    def test_all_stream_expressions_non_empty(self):
        for s in build_computation_specs():
            assert s.stream_expression_sql, s.feature_id

    def test_bureau_has_temporal_filter(self):
        for s in build_computation_specs():
            if s.source_table == "bureau":
                assert s.input_filter_sql is not None, s.feature_id
                assert "DAYS_CREDIT" in s.input_filter_sql, s.feature_id

    def test_bureau_balance_has_temporal_filter(self):
        for s in build_computation_specs():
            if s.source_table == "bureau_balance":
                assert s.input_filter_sql is not None, s.feature_id
                assert "MONTHS_BALANCE" in s.input_filter_sql, s.feature_id
