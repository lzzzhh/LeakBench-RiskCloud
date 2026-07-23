"""Document Parse Result Contract (Section 6.4) — strict, linkage-aware, deeply immutable.

Entity linkage:
  - has_entity_reference() → True if non-empty entity_id string exists
  - is_credit_model_eligible(allow_synthetic=False) →
        VERIFIED  → checks linkage_source, linkage_version, linkage_evidence_uri, linked_at
        SYNTHETIC → only if allow_synthetic=True
  - processed_at is enforced for persisted parse results
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from riskcloud.contracts.validation import (
    ContractValidationError,
    FieldError,
    coerce_bool_opt,
    coerce_datetime_utc,
    coerce_enum,
    coerce_float_opt,
    coerce_str_nonempty,
    coerce_str_opt,
    deep_freeze,
    deep_thaw,
)


class LinkageStatus(str, Enum):
    UNLINKED = "unlinked"
    VERIFIED = "verified"
    SYNTHETIC = "synthetic"


@dataclass(frozen=True)
class DocumentParseResult:
    document_id: str
    object_uri: str
    content_sha256: str
    document_type: str
    entity_id: str | None = None
    linkage_status: LinkageStatus = LinkageStatus.UNLINKED
    linkage_source: str = ""
    linkage_version: str = ""
    linkage_evidence_uri: str = ""
    linked_at: datetime | None = None
    ocr_text_uri: str | None = None
    layout_json_uri: str | None = None
    extracted_fields_json: str | None = None
    ocr_confidence: float | None = None
    field_coverage: float | None = None
    image_quality_score: float | None = None
    tamper_signal: bool | None = None
    model_version: str | None = None
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Any = field(default_factory=dict)

    def __post_init__(self):
        """Deep-freeze metadata for recursive immutability."""
        object.__setattr__(self, "metadata", deep_freeze(self.metadata))

    # -- entity linkage -------------------------------------------------

    def has_entity_reference(self) -> bool:
        """True if entity_id is a non-empty string."""
        return isinstance(self.entity_id, str) and self.entity_id.strip() != ""

    def is_credit_model_eligible(self, *, allow_synthetic: bool = False) -> bool:
        """Only verified (with full evidence) or explicitly allowed synthetic links."""
        if not self.has_entity_reference():
            return False

        if self.linkage_status == LinkageStatus.VERIFIED:
            return all([
                self.linkage_source.strip() != "",
                self.linkage_version.strip() != "",
                self.linkage_evidence_uri.strip() != "",
                self.linked_at is not None,
            ])

        if self.linkage_status == LinkageStatus.SYNTHETIC:
            return allow_synthetic

        return False

    # -- strict entry points -------------------------------------------

    @classmethod
    def parse(cls, d: dict[str, Any]) -> DocumentParseResult:
        errors: list[FieldError] = []
        try:
            doc = cls._from_dict_coerce(d, errors)
        except ContractValidationError:
            raise
        if errors:
            raise ContractValidationError(errors)
        return doc

    @classmethod
    def from_dict_unchecked(cls, d: dict[str, Any]) -> DocumentParseResult:
        errors: list[FieldError] = []
        return cls._from_dict_coerce(d, errors)

    @classmethod
    def _from_dict_coerce(cls, d: dict[str, Any], errors: list[FieldError]) -> DocumentParseResult:
        def _str(k: str) -> str:
            try:
                return coerce_str_nonempty(d.get(k), k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return ""

        def _str_opt(k: str) -> str | None:
            try:
                return coerce_str_opt(d.get(k), k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return None

        document_id = _str("document_id")
        object_uri = _str("object_uri")
        content_sha256 = _str("content_sha256")
        document_type = _str("document_type")

        if len(content_sha256) != 64:
            errors.append(FieldError("content_sha256", "must be 64 hex characters", content_sha256))

        # entity_id — type-checked
        entity_id_raw = d.get("entity_id")
        entity_id: str | None = None
        if entity_id_raw is not None:
            try:
                entity_id = coerce_str_opt(entity_id_raw, "entity_id")
            except ContractValidationError as e:
                errors.extend(e.errors)

        linkage_status = LinkageStatus.UNLINKED
        try:
            linkage_status = coerce_enum(d.get("linkage_status", "unlinked"), LinkageStatus, "linkage_status")
        except ContractValidationError as e:
            errors.extend(e.errors)

        # Linkage evidence — type-checked
        linkage_source = d.get("linkage_source", "")
        if linkage_source is not None and not isinstance(linkage_source, str):
            errors.append(FieldError("linkage_source", f"expected str, got {type(linkage_source).__name__}"))
            linkage_source = ""
        elif linkage_source is None:
            linkage_source = ""

        linkage_version = d.get("linkage_version", "")
        if linkage_version is not None and not isinstance(linkage_version, str):
            errors.append(FieldError("linkage_version", f"expected str, got {type(linkage_version).__name__}"))
            linkage_version = ""
        elif linkage_version is None:
            linkage_version = ""

        linkage_evidence_uri = d.get("linkage_evidence_uri", "")
        if linkage_evidence_uri is not None and not isinstance(linkage_evidence_uri, str):
            errors.append(FieldError(
                "linkage_evidence_uri",
                f"expected str, got {type(linkage_evidence_uri).__name__}",
            ))
            linkage_evidence_uri = ""
        elif linkage_evidence_uri is None:
            linkage_evidence_uri = ""

        linked_at = None
        lt_raw = d.get("linked_at")
        if lt_raw is not None:
            try:
                linked_at = coerce_datetime_utc(lt_raw, "linked_at")
            except ContractValidationError as e:
                errors.extend(e.errors)

        # Enforce entity_id when linkage_status is verified/synthetic
        if linkage_status in (LinkageStatus.VERIFIED, LinkageStatus.SYNTHETIC):
            if not entity_id or not entity_id.strip():
                errors.append(FieldError("entity_id", f"required when linkage_status={linkage_status.value}"))

        # processed_at — must be present for persisted results, fail on wrong type
        processed_at_raw = d.get("processed_at")
        if processed_at_raw is None:
            errors.append(FieldError("processed_at", "required for persisted parse results"))
            processed_at = datetime(1970, 1, 1, tzinfo=timezone.utc)
        else:
            try:
                processed_at = coerce_datetime_utc(processed_at_raw, "processed_at")
            except ContractValidationError as e:
                errors.extend(e.errors)
                processed_at = datetime(1970, 1, 1, tzinfo=timezone.utc)

        # Optional model_version
        model_version = _str_opt("model_version")

        # Float quality scores
        def _range_float(k: str) -> float | None:
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

        ocr_confidence = _range_float("ocr_confidence")
        field_coverage = _range_float("field_coverage")
        image_quality_score = _range_float("image_quality_score")

        tamper_signal = None
        try:
            tamper_signal = coerce_bool_opt(d.get("tamper_signal"), "tamper_signal")
        except ContractValidationError as e:
            errors.extend(e.errors)

        ocr_text_uri = _str_opt("ocr_text_uri")
        layout_json_uri = _str_opt("layout_json_uri")
        extracted_fields_json = _str_opt("extracted_fields_json")

        # Metadata — type-checked
        metadata_raw = d.get("metadata", {})
        if not isinstance(metadata_raw, dict):
            errors.append(FieldError("metadata", f"expected dict, got {type(metadata_raw).__name__}"))
            metadata_raw = {}
        metadata = metadata_raw

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
            "metadata": deep_thaw(self.metadata),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
