"""Event Contract (Section 6.1).

The universal event envelope for all events flowing through the platform.
Every event MUST carry a stable event_id, three time fields, and lineage metadata.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class EventType(str, Enum):
    LOAN_APPLICATION = "loan_application"
    BUREAU_SNAPSHOT = "bureau_snapshot"
    PREV_APPLICATION = "prev_application"
    INSTALLMENT_PAYMENT = "installment_payment"
    CREDIT_CARD_BALANCE = "credit_card_balance"
    POS_CASH_BALANCE = "pos_cash_balance"
    DOCUMENT_UPLOADED = "document_uploaded"
    DOCUMENT_PARSED = "document_parsed"
    PREDICTION_REQUEST = "prediction_request"
    PREDICTION_RESULT = "prediction_result"
    LABEL_FEEDBACK = "label_feedback"
    FEATURE_CORRECTION = "feature_correction"


class EntityType(str, Enum):
    CUSTOMER = "customer"
    LOAN_APPLICATION = "loan_application"
    TRANSACTION = "transaction"
    DOCUMENT = "document"


@dataclass(frozen=True)
class Event:
    """Universal event envelope.

    The three time fields MUST satisfy:
        event_time <= available_at

    data_leakage detection compares available_at against prediction_time.
    """

    dataset_id: str
    event_id: str
    entity_type: EntityType
    entity_id: str
    customer_id: str
    event_type: EventType
    event_time: datetime
    available_at: datetime
    ingested_at: datetime
    source_system: str
    schema_version: int = 1
    payload_uri: Optional[str] = None
    payload_sha256: Optional[str] = None
    headers: dict[str, str] = field(default_factory=dict)

    # -- validation ----------------------------------------------------

    @staticmethod
    def compute_event_id(
        dataset_id: str,
        entity_type: EntityType,
        entity_id: str,
        event_type: EventType,
        event_time: datetime,
        payload_uri: Optional[str] = None,
    ) -> str:
        """Compute deterministic event_id from identity fields."""
        parts = [
            dataset_id,
            entity_type.value,
            entity_id,
            event_type.value,
            event_time.isoformat(),
            payload_uri or "",
        ]
        return hashlib.sha256("|".join(parts).encode()).hexdigest()

    def validate(self) -> list[str]:
        """Return list of validation errors (empty means valid)."""
        errors: list[str] = []

        if not self.dataset_id.strip():
            errors.append("dataset_id must be non-empty")
        if not self.event_id.strip():
            errors.append("event_id must be non-empty")
        if not self.entity_id.strip():
            errors.append("entity_id must be non-empty")
        if not self.customer_id.strip():
            errors.append("customer_id must be non-empty")
        if not self.source_system.strip():
            errors.append("source_system must be non-empty")

        if self.event_time.tzinfo is None:
            errors.append("event_time must be timezone-aware")
        if self.available_at.tzinfo is None:
            errors.append("available_at must be timezone-aware")
        if self.ingested_at.tzinfo is None:
            errors.append("ingested_at must be timezone-aware")

        # Only compare times if both are timezone-aware
        if self.event_time.tzinfo is not None and self.available_at.tzinfo is not None:
            if self.event_time > self.available_at:
                errors.append(
                    f"event_time ({self.event_time.isoformat()}) must be "
                    f"<= available_at ({self.available_at.isoformat()})"
                )

        if self.schema_version < 1:
            errors.append("schema_version must be >= 1")

        if self.payload_sha256 is not None and len(self.payload_sha256) != 64:
            errors.append("payload_sha256 must be 64 hex characters (SHA-256)")

        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    # -- serialization --------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["event_time"] = self.event_time.isoformat()
        d["available_at"] = self.available_at.isoformat()
        d["ingested_at"] = self.ingested_at.isoformat()
        d["entity_type"] = self.entity_type.value
        d["event_type"] = self.event_type.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Event":
        return cls(
            dataset_id=d["dataset_id"],
            event_id=d["event_id"],
            entity_type=EntityType(d["entity_type"]),
            entity_id=d["entity_id"],
            customer_id=d["customer_id"],
            event_type=EventType(d["event_type"]),
            event_time=cls._parse_dt(d["event_time"]),
            available_at=cls._parse_dt(d["available_at"]),
            ingested_at=cls._parse_dt(d["ingested_at"]),
            source_system=d["source_system"],
            schema_version=d.get("schema_version", 1),
            payload_uri=d.get("payload_uri"),
            payload_sha256=d.get("payload_sha256"),
            headers=d.get("headers", {}),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str) -> "Event":
        return cls.from_dict(json.loads(s))

    @staticmethod
    def _parse_dt(v: str) -> datetime:
        """Parse ISO datetime string, appending Z if offset-naive."""
        s = v.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
