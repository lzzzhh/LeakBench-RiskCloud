"""Document Parse Result Contract (Section 6.4).

Structured output from document parsing pipeline (OCR, KIE, layout analysis).
Document features can ONLY enter a credit risk model when a real entity
association exists. Independent document datasets remain separate products.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass(frozen=True)
class DocumentParseResult:
    """Result of a document parsing job.

    entity_id may be None when the document dataset has no entity-level
    linkage to credit data. In that case, document features must NOT be
    consumed by credit risk models.
    """

    document_id: str
    entity_id: Optional[str]            # None = no credit entity linkage
    object_uri: str
    content_sha256: str
    document_type: str
    ocr_text_uri: Optional[str] = None
    layout_json_uri: Optional[str] = None
    extracted_fields_json: Optional[str] = None  # inline JSON or URI
    ocr_confidence: Optional[float] = None        # [0, 1]
    field_coverage: Optional[float] = None        # [0, 1]
    image_quality_score: Optional[float] = None   # [0, 1]
    tamper_signal: Optional[bool] = None
    model_version: Optional[str] = None
    processed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: dict[str, Any] = field(default_factory=dict)

    # -- validation ----------------------------------------------------

    def validate(self) -> list[str]:
        errors: list[str] = []

        if not self.document_id.strip():
            errors.append("document_id must be non-empty")
        if not self.object_uri.strip():
            errors.append("object_uri must be non-empty")
        if len(self.content_sha256) != 64:
            errors.append("content_sha256 must be 64 hex characters")
        if not self.document_type.strip():
            errors.append("document_type must be non-empty")

        if self.processed_at.tzinfo is None:
            errors.append("processed_at must be timezone-aware")

        if self.ocr_confidence is not None and not (0.0 <= self.ocr_confidence <= 1.0):
            errors.append("ocr_confidence must be in [0, 1]")
        if self.field_coverage is not None and not (0.0 <= self.field_coverage <= 1.0):
            errors.append("field_coverage must be in [0, 1]")
        if self.image_quality_score is not None and not (0.0 <= self.image_quality_score <= 1.0):
            errors.append("image_quality_score must be in [0, 1]")

        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    # -- convenience ---------------------------------------------------

    def has_credit_entity(self) -> bool:
        """Can this document be linked to a credit risk entity?"""
        return self.entity_id is not None and self.entity_id.strip() != ""

    # -- serialization --------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["processed_at"] = self.processed_at.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DocumentParseResult":
        return cls(
            document_id=d["document_id"],
            entity_id=d.get("entity_id"),
            object_uri=d["object_uri"],
            content_sha256=d["content_sha256"],
            document_type=d["document_type"],
            ocr_text_uri=d.get("ocr_text_uri"),
            layout_json_uri=d.get("layout_json_uri"),
            extracted_fields_json=d.get("extracted_fields_json"),
            ocr_confidence=d.get("ocr_confidence"),
            field_coverage=d.get("field_coverage"),
            image_quality_score=d.get("image_quality_score"),
            tamper_signal=d.get("tamper_signal"),
            model_version=d.get("model_version"),
            processed_at=cls._parse_dt(d.get("processed_at", datetime.now(timezone.utc).isoformat())),
            metadata=d.get("metadata", {}),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str) -> "DocumentParseResult":
        return cls.from_dict(json.loads(s))

    @staticmethod
    def _parse_dt(v: str) -> datetime:
        s = v.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
