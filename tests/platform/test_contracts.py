"""Schema validation tests for all four platform contracts.

Coverage:
  - Event: required fields, time ordering, serialization round-trip
  - PredictionPoint: time ordering, label constraints, split enum
  - FeatureCatalogEntry: required fields, stage/risk enums
  - DocumentParseResult: credit entity linkage, quality score ranges
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import pytest

from riskcloud.contracts.event import Event, EventType, EntityType
from riskcloud.contracts.prediction_point import PredictionPoint, Split
from riskcloud.contracts.feature_catalog import (
    FeatureCatalogEntry,
    FeatureStage,
    LeakageRisk,
)
from riskcloud.contracts.document import DocumentParseResult


# -----------------------------------------------------------------
# helpers
# -----------------------------------------------------------------

UTC = timezone.utc
NOW = datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC)


def make_valid_event(**overrides) -> Event:
    """Return a valid Event for tests."""
    event_time = NOW
    kwargs = dict(
        dataset_id="test_ds",
        event_id="evt-001",
        entity_type=EntityType.LOAN_APPLICATION,
        entity_id="SK_ID_CURR:100001",
        customer_id="customer:abc",
        event_type=EventType.LOAN_APPLICATION,
        event_time=event_time,
        available_at=event_time + timedelta(seconds=10),
        ingested_at=event_time + timedelta(seconds=20),
        source_system="test_adapter",
    )
    kwargs.update(overrides)
    return Event(**kwargs)


def make_valid_prediction_point(**overrides) -> PredictionPoint:
    kwargs = dict(
        prediction_id="pp-001",
        entity_id="SK_ID_CURR:100001",
        prediction_time=NOW,
        label=1.0,
        label_time=NOW + timedelta(days=365),
        split=Split.TRAIN,
    )
    kwargs.update(overrides)
    return PredictionPoint(**kwargs)


def make_valid_feature_entry(**overrides) -> FeatureCatalogEntry:
    kwargs = dict(
        feature_id="bureau_total_credit",
        feature_name="Total Credit (Bureau)",
        entity_type="application",
        feature_group="bureau",
        source_system="bureau",
        event_time_rule="bureau observation date",
        availability_rule="bureau reporting date <= prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
    )
    kwargs.update(overrides)
    return FeatureCatalogEntry(**kwargs)


def make_valid_doc_result(**overrides) -> DocumentParseResult:
    kwargs = dict(
        document_id="doc-001",
        entity_id="SK_ID_CURR:100001",
        object_uri="s3://bucket/doc001.png",
        content_sha256="a" * 64,
        document_type="id_card",
    )
    kwargs.update(overrides)
    return DocumentParseResult(**kwargs)


# =================================================================
# Event Contract
# =================================================================

class TestEventContract:

    def test_valid_event_passes(self):
        evt = make_valid_event()
        assert evt.is_valid()
        assert evt.validate() == []

    def test_valid_event_json_roundtrip(self):
        evt = make_valid_event()
        json_str = evt.to_json()
        restored = Event.from_json(json_str)
        assert restored == evt
        # check that the JSON is parseable
        d = json.loads(json_str)
        assert d["dataset_id"] == "test_ds"

    def test_event_id_computation_is_deterministic(self):
        eid1 = Event.compute_event_id(
            "test_ds", EntityType.LOAN_APPLICATION, "SK_ID_CURR:1",
            EventType.LOAN_APPLICATION, NOW, "s3://test"
        )
        eid2 = Event.compute_event_id(
            "test_ds", EntityType.LOAN_APPLICATION, "SK_ID_CURR:1",
            EventType.LOAN_APPLICATION, NOW, "s3://test"
        )
        assert eid1 == eid2
        # different payload → different id
        eid3 = Event.compute_event_id(
            "test_ds", EntityType.LOAN_APPLICATION, "SK_ID_CURR:1",
            EventType.LOAN_APPLICATION, NOW, "s3://other"
        )
        assert eid1 != eid3

    def test_missing_dataset_id(self):
        evt = make_valid_event(dataset_id="")
        errs = evt.validate()
        assert any("dataset_id" in e for e in errs)

    def test_missing_event_id(self):
        evt = make_valid_event(event_id="")
        errs = evt.validate()
        assert any("event_id" in e for e in errs)

    def test_missing_entity_id(self):
        evt = make_valid_event(entity_id="")
        errs = evt.validate()
        assert any("entity_id" in e for e in errs)

    def test_missing_customer_id(self):
        evt = make_valid_event(customer_id="")
        errs = evt.validate()
        assert any("customer_id" in e for e in errs)

    def test_missing_source_system(self):
        evt = make_valid_event(source_system="")
        errs = evt.validate()
        assert any("source_system" in e for e in errs)

    def test_event_time_must_be_timezone_aware(self):
        evt = make_valid_event(
            event_time=datetime(2024, 7, 1, 12, 0, 0),
            available_at=datetime(2024, 7, 1, 12, 0, 10, tzinfo=UTC),
            ingested_at=datetime(2024, 7, 1, 12, 0, 20, tzinfo=UTC),
        )
        errs = evt.validate()
        assert any("event_time" in e for e in errs)

    def test_available_at_must_be_timezone_aware(self):
        evt = make_valid_event(
            event_time=datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC),
            available_at=datetime(2024, 7, 1, 12, 0, 10),
            ingested_at=datetime(2024, 7, 1, 12, 0, 20, tzinfo=UTC),
        )
        errs = evt.validate()
        assert any("available_at" in e for e in errs)

    def test_ingested_at_must_be_timezone_aware(self):
        evt = make_valid_event(
            event_time=datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC),
            available_at=datetime(2024, 7, 1, 12, 0, 10, tzinfo=UTC),
            ingested_at=datetime(2024, 7, 1, 12, 0, 20),
        )
        errs = evt.validate()
        assert any("ingested_at" in e for e in errs)

    def test_event_time_must_not_exceed_available_at(self):
        evt = make_valid_event(
            event_time=NOW + timedelta(hours=1),
            available_at=NOW,
            ingested_at=NOW + timedelta(hours=1),
        )
        errs = evt.validate()
        assert any("event_time" in e for e in errs)

    def test_invalid_payload_sha256_length(self):
        evt = make_valid_event(payload_sha256="abc")
        errs = evt.validate()
        assert any("payload_sha256" in e for e in errs)

    def test_invalid_schema_version(self):
        evt = make_valid_event(schema_version=0)
        errs = evt.validate()
        assert any("schema_version" in e for e in errs)

    def test_multiple_errors_accumulated(self):
        evt = make_valid_event(dataset_id="", event_id="", entity_id="")
        errs = evt.validate()
        assert len(errs) >= 3

    def test_datetime_parsing_with_z_suffix(self):
        evt = Event._parse_dt("2024-07-01T12:00:00Z")
        assert evt.tzinfo is not None
        assert evt == datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC)

    def test_datetime_parsing_with_offset(self):
        evt = Event._parse_dt("2024-07-01T22:00:00+10:00")
        assert evt.tzinfo is not None

    def test_datetime_parsing_naive_becomes_utc(self):
        evt = Event._parse_dt("2024-07-01T12:00:00")
        assert evt.tzinfo == UTC


# =================================================================
# PredictionPoint Contract
# =================================================================

class TestPredictionPointContract:

    def test_valid_point_passes(self):
        pp = make_valid_prediction_point()
        assert pp.is_valid()
        assert pp.validate() == []

    def test_json_roundtrip(self):
        pp = make_valid_prediction_point()
        json_str = pp.to_json()
        restored = PredictionPoint.from_json(json_str)
        assert restored.prediction_id == pp.prediction_id
        assert restored.prediction_time == pp.prediction_time
        assert restored.label == pp.label
        assert restored.label_time == pp.label_time
        assert restored.split == pp.split

    def test_missing_prediction_id(self):
        pp = make_valid_prediction_point(prediction_id="")
        errs = pp.validate()
        assert any("prediction_id" in e for e in errs)

    def test_missing_entity_id(self):
        pp = make_valid_prediction_point(entity_id="")
        errs = pp.validate()
        assert any("entity_id" in e for e in errs)

    def test_prediction_time_must_be_timezone_aware(self):
        pp = make_valid_prediction_point(
            prediction_time=datetime(2024, 7, 1, 12, 0, 0)
        )
        errs = pp.validate()
        assert any("prediction_time" in e for e in errs)

    def test_label_must_be_in_range(self):
        pp = make_valid_prediction_point(label=1.5)
        errs = pp.validate()
        assert any("label" in e for e in errs)

    def test_label_negative(self):
        pp = make_valid_prediction_point(label=-0.1)
        errs = pp.validate()
        assert any("label" in e for e in errs)

    def test_label_time_required_when_label_set(self):
        pp = make_valid_prediction_point(label=1.0, label_time=None)
        errs = pp.validate()
        assert any("label_time" in e for e in errs)

    def test_label_time_must_be_after_prediction_time(self):
        pp = make_valid_prediction_point(
            label=1.0,
            label_time=NOW - timedelta(days=1),
        )
        errs = pp.validate()
        assert any("label_time" in e for e in errs)

    def test_oot_split(self):
        pp = make_valid_prediction_point(split=Split.OOT)
        assert pp.is_valid()

    def test_online_split(self):
        pp = make_valid_prediction_point(split=Split.ONLINE)
        assert pp.is_valid()


# =================================================================
# FeatureCatalogEntry Contract
# =================================================================

class TestFeatureCatalogEntryContract:

    def test_valid_entry_passes(self):
        entry = make_valid_feature_entry()
        assert entry.is_valid()
        assert entry.validate() == []

    def test_json_roundtrip(self):
        entry = make_valid_feature_entry()
        json_str = entry.to_json()
        restored = FeatureCatalogEntry.from_json(json_str)
        assert restored.feature_id == entry.feature_id
        assert restored.stage == entry.stage
        assert restored.leakage_risk == entry.leakage_risk

    def test_missing_feature_id(self):
        entry = make_valid_feature_entry(feature_id="")
        errs = entry.validate()
        assert any("feature_id" in e for e in errs)

    def test_missing_feature_name(self):
        entry = make_valid_feature_entry(feature_name="")
        errs = entry.validate()
        assert any("feature_name" in e for e in errs)

    def test_missing_entity_type(self):
        entry = make_valid_feature_entry(entity_type="")
        errs = entry.validate()
        assert any("entity_type" in e for e in errs)

    def test_missing_feature_group(self):
        entry = make_valid_feature_entry(feature_group="")
        errs = entry.validate()
        assert any("feature_group" in e for e in errs)

    def test_missing_source_system(self):
        entry = make_valid_feature_entry(source_system="")
        errs = entry.validate()
        assert any("source_system" in e for e in errs)

    def test_missing_event_time_rule(self):
        entry = make_valid_feature_entry(event_time_rule="")
        errs = entry.validate()
        assert any("event_time_rule" in e for e in errs)

    def test_missing_availability_rule(self):
        entry = make_valid_feature_entry(availability_rule="")
        errs = entry.validate()
        assert any("availability_rule" in e for e in errs)

    def test_invalid_version(self):
        entry = make_valid_feature_entry(version=0)
        errs = entry.validate()
        assert any("version" in e for e in errs)

    def test_negative_ttl(self):
        entry = make_valid_feature_entry(ttl=-1)
        errs = entry.validate()
        assert any("ttl" in e for e in errs)

    def test_negative_cost_unit(self):
        entry = make_valid_feature_entry(cost_unit=-0.5)
        errs = entry.validate()
        assert any("cost_unit" in e for e in errs)

    def test_unknown_leakage_risk_default(self):
        entry = make_valid_feature_entry()
        assert entry.leakage_risk == LeakageRisk.UNKNOWN

    def test_post_outcome_stage_with_risk(self):
        entry = make_valid_feature_entry(
            stage=FeatureStage.POST_OUTCOME,
            leakage_risk=LeakageRisk.POST_OUTCOME,
        )
        assert entry.is_valid()


# =================================================================
# DocumentParseResult Contract
# =================================================================

class TestDocumentParseResultContract:

    def test_valid_result_passes(self):
        doc = make_valid_doc_result()
        assert doc.is_valid()
        assert doc.validate() == []

    def test_json_roundtrip(self):
        doc = make_valid_doc_result()
        json_str = doc.to_json()
        restored = DocumentParseResult.from_json(json_str)
        assert restored.document_id == doc.document_id
        assert restored.content_sha256 == doc.content_sha256
        assert restored.document_type == doc.document_type

    def test_missing_document_id(self):
        doc = make_valid_doc_result(document_id="")
        errs = doc.validate()
        assert any("document_id" in e for e in errs)

    def test_missing_object_uri(self):
        doc = make_valid_doc_result(object_uri="")
        errs = doc.validate()
        assert any("object_uri" in e for e in errs)

    def test_invalid_content_sha256_length(self):
        doc = make_valid_doc_result(content_sha256="short")
        errs = doc.validate()
        assert any("content_sha256" in e for e in errs)

    def test_missing_document_type(self):
        doc = make_valid_doc_result(document_type="")
        errs = doc.validate()
        assert any("document_type" in e for e in errs)

    def test_ocr_confidence_range(self):
        doc = make_valid_doc_result(ocr_confidence=1.5)
        errs = doc.validate()
        assert any("ocr_confidence" in e for e in errs)

    def test_ocr_confidence_negative(self):
        doc = make_valid_doc_result(ocr_confidence=-0.1)
        errs = doc.validate()
        assert any("ocr_confidence" in e for e in errs)

    def test_field_coverage_range(self):
        doc = make_valid_doc_result(field_coverage=2.0)
        errs = doc.validate()
        assert any("field_coverage" in e for e in errs)

    def test_image_quality_score_range(self):
        doc = make_valid_doc_result(image_quality_score=-0.5)
        errs = doc.validate()
        assert any("image_quality_score" in e for e in errs)

    def test_has_credit_entity_with_valid_entity(self):
        doc = make_valid_doc_result(entity_id="SK_ID_CURR:100001")
        assert doc.has_credit_entity()

    def test_has_credit_entity_with_none(self):
        doc = make_valid_doc_result(entity_id=None)
        assert not doc.has_credit_entity()

    def test_has_credit_entity_with_empty(self):
        doc = make_valid_doc_result(entity_id="")
        assert not doc.has_credit_entity()

    def test_entity_id_none_does_not_link_to_credit(self):
        """Document datasets without entity linkage stay independent."""
        doc = make_valid_doc_result(entity_id=None)
        assert not doc.has_credit_entity()
        assert doc.is_valid()
