"""Document Parse Result Contract (Section 6.4) — strict, linkage-aware.

Entity linkage is now modeled as a separate concept from entity references:
  - has_entity_reference() → True if any entity_id string is present
  - linkage_status       → unlinked / verified / synthetic
  - Document features can ONLY enter credit risk models when
    linkage_status is "verified" or (explicitly allowed) "synthetic".
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from riskcloud.contracts.validation import (
    ContractValidationError,
    FieldError,
    coerce_str_nonempty,
    coerce_float_opt,
    coerce_bool_opt,
    coerce_enum,
    coerce_datetime_utc,
    immutable_dict,
)


class LinkageStatus(str, Enum):
    UNLINKED = "unlinked"        # No entity association at all
    VERIFIED = "verified"         # Real or explicitly synthetic entity link
    SYNTHETIC = "synthetic"       # Explicitly constructed for testing


@dataclass(frozen=True)
class DocumentParseResult:
    document_id: str
    object_uri: str
    content_sha256: str
    document_type: str
    entity_id: Optional[str] = None
    linkage_status: LinkageStatus = LinkageStatus.UNLINKED
    linkage_source: str = ""
    linkage_version: str = ""
    linkage_evidence_uri: str = ""
    linked_at: Optional[datetime] = None
    ocr_text_uri: Optional[str] = None
    layout_json_uri: Optional[str] = None
    extracted_fields_json: Optional[str] = None
    ocr_confidence: Optional[float] = None
    field_coverage: Optional[float] = None
    image_quality_score: Optional[float] = None
    tamper_signal: Optional[bool] = None
    model_version: Optional[str] = None
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Defensive copy → MappingProxyType for deep immutability."""
        object.__setattr__(self, "metadata", immutable_dict(self.metadata))

    # -- entity linkage -------------------------------------------------

    def has_entity_reference(self) -> bool:
        """True if any entity_id string is present (weak check)."""
        return self.entity_id is not None and self.entity_id.strip() != ""

    def is_credit_model_eligible(self) -> bool:
        """Only verified or explicitly synthetic links may enter credit models."""
        return self.linkage_status in (LinkageStatus.VERIFIED, LinkageStatus.SYNTHETIC)

    # -- strict entry points -------------------------------------------

    @classmethod
    def parse(cls, d: dict[str, Any]) -> "DocumentParseResult":
        errors: list[FieldError] = []
        try:
            doc = cls._from_dict_coerce(d, errors)
        except ContractValidationError:
            raise
        if errors:
            raise ContractValidationError(errors)
        return doc

    @classmethod
    def from_dict_unchecked(cls, d: dict[str, Any]) -> "DocumentParseResult":
        errors: list[FieldError] = []
        return cls._from_dict_coerce(d, errors)

    @classmethod
    def _from_dict_coerce(cls, d: dict[str, Any], errors: list[FieldError]) -> "DocumentParseResult":
        def get_str(k: str) -> str:
            try:
                return coerce_str_nonempty(d.get(k), k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return ""

        document_id = get_str("document_id")
        object_uri = get_str("object_uri")
        content_sha256 = get_str("content_sha256")
        document_type = get_str("document_type")

        if len(content_sha256) != 64:
            errors.append(FieldError("content_sha256", "must be 64 hex characters", content_sha256))

        entity_id = d.get("entity_id")

        try:
            linkage_status = coerce_enum(d.get("linkage_status", "unlinked"), LinkageStatus, "linkage_status")
        except ContractValidationError as e:
            errors.extend(e.errors)
            linkage_status = LinkageStatus.UNLINKED

        linkage_source = d.get("linkage_source", "")
        linkage_version = d.get("linkage_version", "")
        linkage_evidence_uri = d.get("linkage_evidence_uri", "")

        linked_at_value = d.get("linked_at")
        linked_at: Optional[datetime] = None
        if linked_at_value is not None:
            try:
                linked_at = coerce_datetime_utc(linked_at_value, "linked_at")
            except ContractValidationError as e:
                errors.extend(e.errors)

        # If linkage_status is verified/synthetic but no entity_id → inconsistent
        if linkage_status in (LinkageStatus.VERIFIED, LinkageStatus.SYNTHETIC):
            if entity_id is None or (isinstance(entity_id, str) and not entity_id.strip()):
                errors.append(FieldError(
                    "entity_id",
                    f"required when linkage_status={linkage_status.value}",
                ))

        # Processed_at
        try:
            processed_at = coerce_datetime_utc(d.get("processed_at", datetime.now(timezone.utc).isoformat()), "processed_at")
        except ContractValidationError as e:
            errors.extend(e.errors)
            processed_at = datetime.now(timezone.utc)

        # Optional model version
        model_version = d.get("model_version")

        # Float quality scores
        def coerce_opt_float_range(k: str) -> Optional[float]:
            val = d.get(k)
            if val is None:
                return None
            try:
                v = coerce_float_opt(val, k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return None
            if v is not None and not (0.0 <= v <= 1.0):
                errors.append(FieldError(k, "must be in [0, 1]", v))
            return v

        ocr_confidence = coerce_opt_float_range("ocr_confidence")
        field_coverage = coerce_opt_float_range("field_coverage")
        image_quality_score = coerce_opt_float_range("image_quality_score")

        try:
            tamper_signal = coerce_bool_opt(d.get("tamper_signal"), "tamper_signal")
        except ContractValidationError as e:
            errors.extend(e.errors)
            tamper_signal = None

        ocr_text_uri = d.get("ocr_text_uri")
        layout_json_uri = d.get("layout_json_uri")
        extracted_fields_json = d.get("extracted_fields_json")

        metadata = d.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}

        return cls(
            document_id=document_id,
            object_uri=object_uri,
            content_sha256=content_sha256,
            document_type=document_type,
            entity_id=entity_id,
            linkage_status=linkage_status,
            linkage_source=linkage_source,
            linkage_version=linkage_version,
            linkage_evidence_uri=linkage_evidence_uri,
            linked_at=linked_at,
            ocr_text_uri=ocr_text_uri,
            layout_json_uri=layout_json_uri,
            extracted_fields_json=extracted_fields_json,
            ocr_confidence=ocr_confidence,
            field_coverage=field_coverage,
            image_quality_score=image_quality_score,
            tamper_signal=tamper_signal,
            model_version=model_version,
            processed_at=processed_at,
            metadata=metadata,
        )

    # -- serialization --------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "document_id": self.document_id,
            "object_uri": self.object_uri,
            "content_sha256": self.content_sha256,
            "document_type": self.document_type,
            "entity_id": self.entity_id,
            "linkage_status": self.linkage_status.value,
            "linkage_source": self.linkage_source,
            "linkage_version": self.linkage_version,
            "linkage_evidence_uri": self.linkage_evidence_uri,
            "linked_at": self.linked_at.isoformat() if self.linked_at else None,
            "ocr_text_uri": self.ocr_text_uri,
            "layout_json_uri": self.layout_json_uri,
            "extracted_fields_json": self.extracted_fields_json,
            "ocr_confidence": self.ocr_confidence,
            "field_coverage": self.field_coverage,
            "image_quality_score": self.image_quality_score,
            "tamper_signal": self.tamper_signal,
            "model_version": self.model_version,
            "processed_at": self.processed_at.isoformat(),
            "metadata": dict(self.metadata),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
