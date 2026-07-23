"""Schema validation tests for all four contracts — strict entry points.

Tests cover:
  - parse() success and failure
  - from_dict_unchecked() tolerance
  - event_id enforcement
  - deep immutability (nested mutation)
  - split-aware PredictionPoint rules
  - publishable checks
  - document linkage gate
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from riskcloud.contracts.document import DocumentParseResult
from riskcloud.contracts.event import EntityType, Event, EventType, compute_event_id
from riskcloud.contracts.feature_catalog import (
    FeatureCatalogEntry,
)
from riskcloud.contracts.prediction_point import PredictionPoint
from riskcloud.contracts.validation import ContractValidationError

UTC = timezone.utc
NOW = datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC)


def _canonical_event_id(**fields) -> str:
    d = dict(
        dataset_id="test_ds",
        entity_type=EntityType.LOAN_APPLICATION,
        entity_id="SK_ID_CURR:100001",
        event_type=EventType.LOAN_APPLICATION,
        event_time_utc=NOW,
        source_record_id="src-rec-1",
        source_record_revision="v1",
    )
    d.update(fields)
    return compute_event_id(**d)


def _valid_event_dict(**overrides) -> dict:
    d = {
        "dataset_id": "test_ds",
        "event_id": _canonical_event_id(),
        "entity_type": "loan_application",
        "entity_id": "SK_ID_CURR:100001",
        "customer_id": "customer:abc",
        "event_type": "loan_application",
        "event_time": NOW.isoformat(),
        "available_at": (NOW + timedelta(seconds=10)).isoformat(),
        "ingested_at": (NOW + timedelta(seconds=20)).isoformat(),
        "source_system": "test_adapter",
        "source_record_id": "src-rec-1",
        "source_record_revision": "v1",
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
        "processed_at": NOW.isoformat(),
    }
    d.update(overrides)
    return d


# =================================================================
# Event Contract
# =================================================================

class TestEventContract:

    def test_parse_valid(self):
        evt = Event.parse(_valid_event_dict())
        assert evt.dataset_id == "test_ds"

    def test_json_roundtrip(self):
        evt = Event.parse(_valid_event_dict())
        restored = Event.parse(json.loads(evt.to_json()))
        assert restored.dataset_id == evt.dataset_id
        assert restored.event_id == evt.event_id

    def test_reject_wrong_event_id(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(event_id="completely-wrong"))
        assert any("event_id" in e.field_path for e in exc.value.errors)

    def test_reject_naive_datetime(self):
        with pytest.raises(ContractValidationError):
            Event.parse(_valid_event_dict(event_time="2024-07-01T12:00:00",
                                          event_id=_canonical_event_id(event_time_utc=NOW)))

    def test_reject_empty_dataset_id(self):
        with pytest.raises(ContractValidationError):
            Event.parse(_valid_event_dict(dataset_id="",
                                          event_id=_canonical_event_id(dataset_id="")))

    def test_reject_missing_source_record_and_sha(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse({
                "dataset_id": "test_ds",
                "event_id": "any-id",
                "entity_type": "loan_application",
                "entity_id": "SK_ID_CURR:100001",
                "customer_id": "customer:abc",
                "event_type": "loan_application",
                "event_time": NOW.isoformat(),
                "available_at": (NOW + timedelta(seconds=10)).isoformat(),
                "ingested_at": (NOW + timedelta(seconds=20)).isoformat(),
                "source_system": "test_adapter",
                "source_record_id": "",
                "source_record_revision": "",
            })
        assert any("source_record_id" in e.field_path for e in exc.value.errors)

    def test_reject_wrong_type(self):
        with pytest.raises(ContractValidationError):
            Event.parse(_valid_event_dict(dataset_id=None,
                                          event_id=_canonical_event_id(dataset_id="")))

    def test_reject_invalid_enum(self):
        with pytest.raises(ContractValidationError):
            Event.parse(_valid_event_dict(entity_type="invalid_entity"))

    def test_accept_same_instant_diff_offset(self):
        t1 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone(timedelta(hours=10)))
        eid = compute_event_id("ds", EntityType.LOAN_APPLICATION, "SK_1", EventType.LOAN_APPLICATION, t1, "r1")
        d = _valid_event_dict(
            dataset_id="ds", entity_id="SK_1", event_type="loan_application",
            event_time=t1.isoformat(), event_id=eid,
            source_record_id="r1", source_record_revision="",
        )
        evt = Event.parse(d)
        assert evt.event_id == eid

    # -- deep immutability: nested headers --

    def test_nested_headers_are_frozen(self):
        """Headers values must be strings. Deep immutability tested via metadata."""
        evt = Event.parse(_valid_event_dict(headers={"trace-id": "abc123", "span-id": "def456"}))
        with pytest.raises((TypeError, AttributeError)):
            evt.headers["trace-id"] = "mutated"  # type: ignore[index]

    def test_headers_reject_non_string_value(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(headers={"bad": 123}))
        assert any("headers" in e.field_path for e in exc.value.errors)

    def test_unchecked_never_raises_for_missing_fields(self):
        evt = Event.from_dict_unchecked({})
        assert isinstance(evt, Event)


# =================================================================
# PredictionPoint Contract
# =================================================================

class TestPredictionPointContract:

    def test_parse_valid(self):
        pp = PredictionPoint.parse(_valid_pred_point_dict())
        assert pp.prediction_id == "pp-001"

    def test_json_roundtrip(self):
        pp = PredictionPoint.parse(_valid_pred_point_dict())
        restored = PredictionPoint.parse(json.loads(pp.to_json()))
        assert restored.snapshot_id == "snap-001"

    def test_reject_missing_snapshot_for_train(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(snapshot_id=None))

    def test_reject_missing_boundary_for_oot(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(split="oot", boundary_version=None))

    def test_reject_label_for_online(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(split="online", label=1.0, snapshot_id=None))

    def test_reject_naive_datetime(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(prediction_time="2024-07-01T12:00:00"))

    def test_reject_label_out_of_range(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(label=1.5))

    def test_reject_label_without_label_time(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(label=1.0, label_time=None))

    def test_reject_label_time_without_label(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(label=None, label_time=(NOW + timedelta(days=1)).isoformat()))

    def test_reject_wrong_type_snapshot(self):
        with pytest.raises(ContractValidationError):
            PredictionPoint.parse(_valid_pred_point_dict(snapshot_id=123))


# =================================================================
# FeatureCatalog Contract
# =================================================================

class TestFeatureCatalogEntryContract:

    def test_parse_valid(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict())
        assert entry.is_publishable()

    def test_json_roundtrip(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict())
        restored = FeatureCatalogEntry.parse(json.loads(entry.to_json()))
        assert restored.feature_id == entry.feature_id

    def test_reject_empty_feature_id(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(feature_id=""))

    def test_draft_not_publishable(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(
            owner="", leakage_risk="unknown", lineage_expression=None, semantic_group_id=None,
        ))
        assert not entry.is_publishable()

    def test_publishable_rejects_unknown_risk(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(leakage_risk="unknown"))
        assert not entry.is_publishable()

    def test_online_feature_requires_ttl(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(online_available=True, ttl=None))
        assert not entry.is_publishable()

    def test_reject_invalid_version(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(version=0))

    def test_reject_wrong_type_tags(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(tags="not-a-list"))

    def test_reject_wrong_type_owner(self):
        with pytest.raises(ContractValidationError):
            FeatureCatalogEntry.parse(_valid_feature_dict(owner=123))

    # -- deep immutability: tags --

    def test_tags_are_deeply_immutable(self):
        """Tags are flat strings; nested lists are rejected. Immutability tested via tuple."""
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(tags=["alpha", "beta"]))
        assert isinstance(entry.tags, tuple)
        with pytest.raises(AttributeError):
            entry.tags.append("new")  # type: ignore[union-attr]


# =================================================================
# DocumentParseResult Contract
# =================================================================

class TestDocumentParseResultContract:

    def test_parse_valid(self):
        doc = DocumentParseResult.parse(_valid_doc_dict())
        assert doc.document_id == "doc-001"

    def test_json_roundtrip(self):
        doc = DocumentParseResult.parse(_valid_doc_dict())
        restored = DocumentParseResult.parse(json.loads(doc.to_json()))
        assert restored.content_sha256 == doc.content_sha256

    def test_reject_empty_document_id(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(document_id=""))

    def test_reject_missing_processed_at(self):
        with pytest.raises(ContractValidationError) as exc:
            DocumentParseResult.parse(_valid_doc_dict(processed_at=None))
        assert any("processed_at" in e.field_path for e in exc.value.errors)

    def test_reject_wrong_type_entity_id(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(entity_id=123))

    # -- linkage gating --

    def test_unlinked_not_eligible(self):
        doc = DocumentParseResult.parse(_valid_doc_dict())
        assert not doc.is_credit_model_eligible()

    def test_verified_without_evidence_not_eligible(self):
        doc = DocumentParseResult.parse(_valid_doc_dict(
            entity_id="SK_001", linkage_status="verified",
        ))
        assert not doc.is_credit_model_eligible()

    def test_verified_with_full_evidence_is_eligible(self):
        doc = DocumentParseResult.parse(_valid_doc_dict(
            entity_id="SK_001",
            linkage_status="verified",
            linkage_source="manual_review",
            linkage_version="v1",
            linkage_evidence_uri="s3://audit/link-001.json",
            linked_at=NOW.isoformat(),
        ))
        assert doc.is_credit_model_eligible()

    def test_synthetic_without_allow_not_eligible(self):
        doc = DocumentParseResult.parse(_valid_doc_dict(
            entity_id="SK_001", linkage_status="synthetic",
        ))
        assert not doc.is_credit_model_eligible()
        assert doc.is_credit_model_eligible(allow_synthetic=True)

    def test_verified_without_entity_raises(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(
                entity_id="", linkage_status="verified",
            ))

    # -- deep immutability: nested metadata --

    def test_nested_metadata_is_frozen(self):
        doc = DocumentParseResult.parse(_valid_doc_dict(metadata={"quality": {"warnings": ["w1"]}}))
        with pytest.raises((TypeError, AttributeError)):
            doc.metadata["quality"]["warnings"].append("mutated")  # type: ignore[union-attr,index]

    def test_metadata_rejects_bytearray(self):
        with pytest.raises(ContractValidationError) as exc:
            DocumentParseResult.parse(_valid_doc_dict(metadata={"bad": bytearray(b"abc")}))
        errors_text = "|".join(str(e) for e in exc.value.errors)
        assert "non-json" in errors_text.lower() or "metadata" in errors_text.lower()

    def test_metadata_rejects_set(self):
        with pytest.raises(ContractValidationError) as exc:
            DocumentParseResult.parse(_valid_doc_dict(metadata={"s": {"a", "b"}}))
        errors_text = "|".join(str(e) for e in exc.value.errors)
        assert "non-json" in errors_text.lower()

    def test_to_json_succeeds_for_valid_doc(self):
        doc = DocumentParseResult.parse(_valid_doc_dict(metadata={"quality": 0.9}))
        s = doc.to_json()
        assert json.loads(s)["document_id"] == "doc-001"


# =================================================================
# SHA-256 hex validation
# =================================================================

class TestSha256Validation:

    def test_event_rejects_non_hex_sha(self):
        with pytest.raises(ContractValidationError):
            Event.parse(_valid_event_dict(
                source_record_id="",
                payload_sha256="z" * 64,
                event_id=compute_event_id(
                    "test_ds", EntityType.LOAN_APPLICATION, "SK_ID_CURR:100001",
                    EventType.LOAN_APPLICATION, NOW,
                    payload_sha256="z" * 64,
                ),
            ))

    def test_document_rejects_non_hex_sha(self):
        with pytest.raises(ContractValidationError):
            DocumentParseResult.parse(_valid_doc_dict(content_sha256="z" * 64))


# =================================================================
# Payload-only event identity
# =================================================================

class TestPayloadOnlyIdentity:

    def test_payload_only_event_is_valid(self):
        sha = "a" * 64
        eid = compute_event_id(
            "test_ds", EntityType.LOAN_APPLICATION, "SK_ID_CURR:100001",
            EventType.LOAN_APPLICATION, NOW,
            payload_sha256=sha,
        )
        evt = Event.parse(_valid_event_dict(
            source_record_id="",
            source_record_revision="",
            payload_sha256=sha,
            event_id=eid,
        ))
        assert evt.payload_sha256 == sha

    def test_different_payload_sha_produces_different_event_id(self):
        sha1 = "a" * 64
        sha2 = "b" * 64
        eid1 = compute_event_id(
            "test_ds", EntityType.LOAN_APPLICATION, "SK_ID_CURR:100001",
            EventType.LOAN_APPLICATION, NOW,
            payload_sha256=sha1,
        )
        eid2 = compute_event_id(
            "test_ds", EntityType.LOAN_APPLICATION, "SK_ID_CURR:100001",
            EventType.LOAN_APPLICATION, NOW,
            payload_sha256=sha2,
        )
        assert eid1 != eid2


# =================================================================
# Regression: event_time <= available_at
# =================================================================

class TestEventTimeOrdering:

    def test_event_time_must_not_exceed_available_at(self):
        with pytest.raises(ContractValidationError) as exc:
            Event.parse(_valid_event_dict(
                event_time=(NOW + timedelta(hours=1)).isoformat(),
                available_at=NOW.isoformat(),
                event_id=compute_event_id(
                    "test_ds", EntityType.LOAN_APPLICATION, "SK_ID_CURR:100001",
                    EventType.LOAN_APPLICATION, NOW + timedelta(hours=1),
                    source_record_id="src-rec-1",
                ),
            ))
        assert any("event_time" in e.field_path for e in exc.value.errors)


# =================================================================
# Tags must be flat strings
# =================================================================

class TestTagsValidation:

    def test_tags_rejects_non_string_element(self):
        with pytest.raises(ContractValidationError) as exc:
            FeatureCatalogEntry.parse(_valid_feature_dict(tags=[123]))
        assert any("tags" in e.field_path for e in exc.value.errors)

    def test_tags_accepts_flat_strings(self):
        entry = FeatureCatalogEntry.parse(_valid_feature_dict(tags=["alpha", "beta"]))
        assert entry.tags == ("alpha", "beta")

    def test_tags_rejects_nested_lists(self):
        with pytest.raises(ContractValidationError) as exc:
            FeatureCatalogEntry.parse(_valid_feature_dict(tags=[["nested"]]))
        assert any("tags" in e.field_path for e in exc.value.errors)
