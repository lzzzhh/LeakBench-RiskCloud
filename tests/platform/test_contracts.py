"""Schema validation tests for all four contracts — strict entry points.

Tests use:
  - Contract.parse()    → should succeed for valid data, raise for invalid
  - Contract.from_dict_unchecked() → should always construct (even invalid)
  - Direct construction  → only for deep immutability tests
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import pytest

from riskcloud.contracts.validation import ContractValidationError, FieldError
from riskcloud.contracts.event import Event, EventType, EntityType, compute_event_id
from riskcloud.contracts.prediction_point import PredictionPoint, Split
from riskcloud.contracts.feature_catalog import (
    FeatureCatalogEntry,
    FeatureStage,
    LeakageRisk,
)
from riskcloud.contracts.document import DocumentParseResult, LinkageStatus

UTC = timezone.utc
NOW = datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC)


def _valid_event_dict(**overrides) -> dict:
    d = {
        "dataset_id": "test_ds",
        "event_id": compute_event_id("test_ds", EntityType.LOAN_APPLICATION, "SK_001", EventType.LOAN_APPLICATION, NOW),
        "entity_type": "loan_application",
        "entity_id": "SK_ID_CURR:100001",
        "customer_id": "customer:abc",
        "event_type": "loan_application",
        "event_time": NOW.isoformat(),
        "available_at": (NOW + timedelta(seconds=10)).isoformat(),
        "ingested_at": (NOW + timedelta(seconds=20)).isoformat(),
        "source_system": "test_adapter",
    }
    d.update(overrides)
    return d


def _valid_pred_point_dict(**overrides) -> dict:
    d = {
        "prediction_id": "pp-001",
        "entity_id": "SK_ID_CURR:100001",
        "prediction_time": NOW.isoformat(),
        "split": "train",
        "snapshot_id": "snap-001",
        "boundary_version": "v1.0",
        "label": 1.0,
        "label_time": (NOW + timedelta(days=365)).isoformat(),
    }
    d.update(overrides)
    return d


def _valid_feature_dict(**overrides) -> dict:
    d = {
        "feature_id": "bureau_total_credit",
        "feature_name": "Total Credit (Bureau)",
        "entity_type": "application",
        "feature_group": "bureau",
        "source_system": "bureau",
        "event_time_rule": "bureau observation date <= prediction_time",
        "availability_rule": "bureau reporting date <= prediction_time",
        "stage": "pre_application",
        "owner": "credit_team",
        "leakage_risk": "none",
        "semantic_group_id": "bureau_history",
        "lineage_expression": "SELECT SUM(credit) FROM bureau WHERE date <= prediction_time",
    }
    d.update(overrides)
    return d


def _valid_doc_dict(**overrides) -> dict:
    d = {
        "document_id": "doc-001",
        "object_uri": "s3://bucket/doc001.png",
        "content_sha256": "a" * 64,
        "document_type": "id_card",
    }
    d.update(overrides)
    return d


# =================================================================
# Event Contract
# =================================================================

class TestEventContract:

    # -- parse() happy path --

    def test_parse_valid_event(self):
        evt = Event.parse(_valid_event_dict())
        assert evt.dataset_id == "test_ds"
        assert evt.entity_type == EntityType.LOAN_APPLICATION

    def test_json_roundtrip(self):
        evt = Event.parse(_valid_event_dict())
        json_str = evt.to_json()
        restored = Event.parse(json.loads(json_str))
        assert restored.dataset_id == evt.dataset_id
        assert restored.event_id == evt.event_id
        assert restored.event_time == evt.event_time
        assert restored.headers == evt.headers

    # -- parse() failures --

    def test_parse_empty_dataset_id_raises(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(dataset_id=""))
        assert any("dataset_id" in e.field_path for e in exc.value.errors)

    def test_parse_empty_event_id_raises(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(event_id=""))
        assert any("event_id" in e.field_path for e in exc.value.errors)

    def test_parse_naive_datetime_rejected(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(event_time="2024-07-01T12:00:00"))
        assert any("event_time" in e.field_path for e in exc.value.errors)

    def test_parse_event_time_after_available_at(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(
                event_time=(NOW + timedelta(hours=1)).isoformat(),
                available_at=NOW.isoformat(),
            ))
        assert any("event_time" in e.field_path for e in exc.value.errors)

    def test_parse_wrong_type_raises(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(dataset_id=None))
        assert any("dataset_id" in e.field_path for e in exc.value.errors)

    def test_parse_invalid_enum_raises(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(entity_type="invalid_entity"))
        assert any("entity_type" in e.field_path for e in exc.value.errors)

    def test_parse_invalid_schema_version(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(schema_version=0))
        assert any("schema_version" in e.field_path for e in exc.value.errors)

    def test_parse_multiple_errors_accumulated(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(dataset_id="", event_id="", source_system=""))
        assert len(exc.value.errors) >= 3

    # -- from_dict_unchecked (always succeeds) --

    def test_unchecked_never_raises(self):
        evt = Event.from_dict_unchecked({"dataset_id": None, "event_id": None})
        assert evt is not None
        assert isinstance(evt, Event)

    # -- event identity --

    def test_compute_event_id_deterministic(self):
        eid1 = compute_event_id("ds", EntityType.LOAN_APPLICATION, "SK_1", EventType.LOAN_APPLICATION, NOW)
        eid2 = compute_event_id("ds", EntityType.LOAN_APPLICATION, "SK_1", EventType.LOAN_APPLICATION, NOW)
        assert eid1 == eid2

    def test_compute_event_id_same_instant_different_offset(self):
        """Same instant with different UTC offset → same ID."""
        t1 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone(timedelta(hours=10)))
        t2 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        eid1 = compute_event_id("ds", EntityType.LOAN_APPLICATION, "SK_1", EventType.LOAN_APPLICATION, t1)
        eid2 = compute_event_id("ds", EntityType.LOAN_APPLICATION, "SK_1", EventType.LOAN_APPLICATION, t2)
        assert eid1 == eid2

    def test_compute_event_id_different_source_record(self):
        eid1 = compute_event_id("ds", EntityType.LOAN_APPLICATION, "SK_1", EventType.LOAN_APPLICATION, NOW, source_record_id="r1")
        eid2 = compute_event_id("ds", EntityType.LOAN_APPLICATION, "SK_1", EventType.LOAN_APPLICATION, NOW, source_record_id="r2")
        assert eid1 != eid2

    # -- deep immutability --

    def test_headers_are_frozen_copy(self):
        orig = {"x": "1"}
        evt = Event.parse(_valid_event_dict(headers=orig))
        # Mutating original dict does not affect event
        orig["x"] = "2"
        assert evt.headers["x"] == "1"
        # Cannot mutate via event either (TypeError on setitem if MappingProxy, or AttributeError)
        with pytest.raises(TypeError) if hasattr(evt.headers, "_mapping") else pytest.raises(Exception):
            evt.headers["y"] = "3"  # type: ignore[index]


# =================================================================
# PredictionPoint Contract
# =================================================================

class TestPredictionPointContract:

    def test_parse_valid_point(self):
        pp = PredictionPoint.parse(_valid_pred_point_dict())
        assert pp.prediction_id == "pp-001"
        assert pp.split == Split.TRAIN

    def test_json_roundtrip(self):
        pp = PredictionPoint.parse(_valid_pred_point_dict())
        restored = PredictionPoint.parse(json.loads(pp.to_json()))
        assert restored.prediction_id == pp.prediction_id
        assert restored.snapshot_id == pp.snapshot_id
        assert restored.boundary_version == pp.boundary_version

    def test_parse_missing_prediction_id_raises(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(prediction_id=""))

    def test_parse_naive_datetime_rejected(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(prediction_time="2024-07-01T12:00:00"))

    def test_parse_label_out_of_range(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(label=1.5))

    def test_parse_label_without_label_time(self):
        with pytest.raises(ContractValidationError) as exc:
            PredictionPoint.parse(_valid_pred_point_dict(label=1.0, label_time=None))
        assert any("label_time" in e.field_path for e in exc.value.errors)

    def test_parse_label_time_before_prediction_time(self):
        with pytest.raises(ContractValidationError) as exc:
            PredictionPoint.parse(_valid_pred_point_dict(
                label=1.0,
                label_time=(NOW - timedelta(days=1)).isoformat(),
            ))
        assert any("label_time" in e.field_path for e in exc.value.errors)

    def test_parse_train_missing_snapshot_raises(self):
        with pytest.raises(ContractValidationError) as exc:
            PredictionPoint.parse(_valid_pred_point_dict(split="train", snapshot_id=None))
        assert any("snapshot_id" in e.field_path for e in exc.value.errors)

    def test_parse_oot_missing_boundary_raises(self):
        with pytest.raises(ContractValidationError) as exc:
            PredictionPoint.parse(_valid_pred_point_dict(split="oot", boundary_version=None))
        assert any("boundary_version" in e.field_path for e in exc.value.errors)

    def test_parse_online_with_label_rejected(self):
        with pytest.raises(ContractValidationError) as exc:
            PredictionPoint.parse(_valid_pred_point_dict(
                split="online",
                snapshot_id=None,
                label=1.0,
            ))
        assert any("label" in e.field_path for e in exc.value.errors)

    def test_parse_label_time_without_label(self):
        with pytest.raises(ContractValidationError) as exc:
            PredictionPoint.parse(_valid_pred_point_dict(
                label=None,
                label_time=(NOW + timedelta(days=1)).isoformat(),
            ))
        assert any("label" in e.field_path for e in exc.value.errors)

    def test_unchecked_never_raises(self):
        pp = PredictionPoint.from_dict_unchecked({})
        assert isinstance(pp, PredictionPoint)


# =================================================================
# FeatureCatalog Contract
# =================================================================

class TestFeatureCatalogEntryContract:

    def test_parse_valid_entry(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict())
        assert entry.feature_id == "bureau_total_credit"
        assert entry.is_publishable()

    def test_json_roundtrip(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict())
        restored = FeatureCatalogEntry.parse(json.loads(entry.to_json()))
        assert restored.feature_id == entry.feature_id
        assert restored.tags == entry.tags

    def test_parse_missing_feature_id_raises(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(feature_id=""))

    def test_parse_missing_feature_name_raises(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(feature_name=""))

    def test_parse_invalid_version(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(version=0))

    def test_parse_negative_ttl(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(ttl=-1))

    def test_parse_negative_cost_unit(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(cost_unit=-0.5))

    # -- publishable check --

    def test_draft_entry_not_publishable(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(
            owner="", leakage_risk="unknown", lineage_expression=None, semantic_group_id=None,
        ))
        assert not entry.is_publishable()
        errs = entry.publishable_errors()
        assert len(errs) >= 2

    def test_publishable_rejects_unknown_risk(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(leakage_risk="unknown"))
        assert not entry.is_publishable()

    def test_publishable_rejects_empty_owner(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(owner=""))
        assert not entry.is_publishable()

    def test_publishable_rejects_empty_lineage(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(lineage_expression=None))
        assert not entry.is_publishable()

    def test_publishable_rejects_missing_semantic_group(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(semantic_group_id=None))
        assert not entry.is_publishable()

    def test_online_feature_requires_ttl(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(online_available=True, ttl=None))
        assert not entry.is_publishable()

    # -- deep immutability: tags are tuple --

    def test_tags_are_immutable_tuple(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(tags=["a", "b"]))
        assert isinstance(entry.tags, tuple)
        assert entry.tags == ("a", "b")
        with pytest.raises(AttributeError):
            entry.tags.append("c")  # type: ignore[union-attr]


# =================================================================
# DocumentParseResult Contract
# =================================================================

class TestDocumentParseResultContract:

    def test_parse_valid_result(self):
        doc = DocumentParseResult.parse(_valid_doc_dict())
        assert doc.document_id == "doc-001"
        assert not doc.has_entity_reference()
        assert not doc.is_credit_model_eligible()

    def test_json_roundtrip(self):
        doc = DocumentParseResult.parse(_valid_doc_dict())
        restored = DocumentParseResult.parse(json.loads(doc.to_json()))
        assert restored.document_id == doc.document_id
        assert restored.content_sha256 == doc.content_sha256

    def test_parse_missing_document_id_raises(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(document_id=""))

    def test_parse_missing_object_uri_raises(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(object_uri=""))

    def test_parse_invalid_sha256_raises(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(content_sha256="short"))

    def test_parse_missing_document_type_raises(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(document_type=""))

    def test_parse_ocr_confidence_out_of_range(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(ocr_confidence=1.5))

    def test_parse_ocr_confidence_negative(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(ocr_confidence=-0.1))

    def test_parse_field_coverage_out_of_range(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(field_coverage=2.0))

    def test_parse_image_quality_out_of_range(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(image_quality_score=-0.5))

    # -- linkage --

    def test_unlinked_not_credit_eligible(self):
        doc = DocumentParseResult.parse(_valid_doc_dict(
            entity_id="SK_001", linkage_status="unlinked",
        ))
        assert doc.has_entity_reference()
        assert not doc.is_credit_model_eligible()

    def test_verified_is_credit_eligible(self):
        doc = DocumentParseResult.parse(_valid_doc_dict(
            entity_id="SK_001", linkage_status="verified",
        ))
        assert doc.has_entity_reference()
        assert doc.is_credit_model_eligible()

    def test_synthetic_is_credit_eligible(self):
        doc = DocumentParseResult.parse(_valid_doc_dict(
            entity_id="SK_001", linkage_status="synthetic",
        ))
        assert doc.is_credit_model_eligible()

    def test_verified_without_entity_id_raises(self):
        with pytest.raises(ContractValidationError) as exc:
            DocumentParseResult.parse(_valid_doc_dict(
                entity_id="", linkage_status="verified",
            ))
        assert any("entity_id" in e.field_path for e in exc.value.errors)

    # -- deep immutability: metadata --

    def test_metadata_is_frozen_copy(self):
        orig = {"quality": 0.9}
        doc = DocumentParseResult.parse(_valid_doc_dict(metadata=orig))
        orig["quality"] = 0.1
        assert doc.metadata["quality"] == 0.9
