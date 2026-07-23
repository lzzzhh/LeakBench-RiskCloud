"""Event Contract (Section 6.1) — strict, immutable, idempotent identity.

Key rules:
  - event_time <= available_at
  - event_id is enforced by strict parsing (parse_*) or identity rules
  - Deep immutability: headers is a frozen copy, no mutable containers escape
  - Two entry points:
      Event.parse(d)       → strict validation, raises ContractValidationError
      Event.from_dict_unchecked(d)  → no validation (tests only)
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from riskcloud.contracts.validation import (
    ContractValidationError,
    FieldError,
    coerce_str_nonempty,
    coerce_enum,
    coerce_datetime_utc,
    coerce_int_opt,
    immutable_dict,
)


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


# -----------------------------------------------------------------
# Event identity — stable, source-key-based
# -----------------------------------------------------------------

def compute_event_id(
    dataset_id: str,
    entity_type: EntityType,
    entity_id: str,
    event_type: EventType,
    event_time_utc: datetime,
    source_record_id: str = "",
    source_record_revision: str = "",
) -> str:
    """Compute deterministic event_id from immutable source business key.

    Uses UTC-normalized time to avoid different IDs for the same instant.
    Does NOT include payload_uri (URIs can change).
    When source_record_id is provided, payload identity is secondary.
    """
    utc_time = event_time_utc.astimezone(timezone.utc)
    parts = [
        dataset_id,
        entity_type.value,
        entity_id,
        event_type.value,
        utc_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        source_record_id,
        source_record_revision,
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


# -----------------------------------------------------------------

@dataclass(frozen=True)
class Event:
    """Universal event envelope with strict identity and deep immutability."""

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
    source_record_id: str = ""
    source_record_revision: str = ""
    payload_uri: Optional[str] = None
    payload_sha256: Optional[str] = None
    headers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Defensive copy → MappingProxyType for deep immutability."""
        object.__setattr__(self, "headers", immutable_dict(self.headers))

    # -- strict entry points -------------------------------------------

    @classmethod
    def parse(cls, d: dict[str, Any]) -> "Event":
        """Strict deserialization with full validation. Raises on failure."""
        errors: list[FieldError] = []
        try:
            evt = cls._from_dict_coerce(d, errors)
        except ContractValidationError:
            raise
        if errors:
            raise ContractValidationError(errors)
        return evt

    @classmethod
    def from_dict_unchecked(cls, d: dict[str, Any]) -> "Event":
        """Unchecked deserialization for tests. Use parse() in production."""
        errors: list[FieldError] = []
        return cls._from_dict_coerce(d, errors)

    @classmethod
    def _from_dict_coerce(cls, d: dict[str, Any], errors: list[FieldError]) -> "Event":
        """Coerce and validate, accumulating errors. Used by both parse and unchecked."""
        # Required non-empty strings
        def get_str(k: str) -> str:
            try:
                return coerce_str_nonempty(d.get(k), k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return ""

        dataset_id = get_str("dataset_id")
        event_id = get_str("event_id")
        entity_id = get_str("entity_id")
        customer_id = get_str("customer_id")
        source_system = get_str("source_system")

        # Enums
        try:
            entity_type = coerce_enum(d.get("entity_type"), EntityType, "entity_type")
        except ContractValidationError as e:
            errors.extend(e.errors)
            entity_type = EntityType.CUSTOMER

        try:
            event_type = coerce_enum(d.get("event_type"), EventType, "event_type")
        except ContractValidationError as e:
            errors.extend(e.errors)
            event_type = EventType.LOAN_APPLICATION

        # Datetimes
        try:
            event_time = coerce_datetime_utc(d.get("event_time"), "event_time")
        except ContractValidationError as e:
            errors.extend(e.errors)
            event_time = datetime(1970, 1, 1, tzinfo=timezone.utc)

        try:
            available_at = coerce_datetime_utc(d.get("available_at"), "available_at")
        except ContractValidationError as e:
            errors.extend(e.errors)
            available_at = datetime(1970, 1, 1, tzinfo=timezone.utc)

        try:
            ingested_at = coerce_datetime_utc(d.get("ingested_at"), "ingested_at")
        except ContractValidationError as e:
            errors.extend(e.errors)
            ingested_at = datetime(1970, 1, 1, tzinfo=timezone.utc)

        # Time ordering (only if both are valid)
        if event_time.tzinfo is not None and available_at.tzinfo is not None:
            if event_time > available_at:
                errors.append(FieldError(
                    "event_time",
                    f"event_time ({event_time.isoformat()}) must be <= available_at ({available_at.isoformat()})",
                ))

        # Schema version
        schema_version_raw = d.get("schema_version", 1)
        try:
            schema_version = coerce_int_opt(schema_version_raw, "schema_version")
        except ContractValidationError as e:
            errors.extend(e.errors)
            schema_version = None
        if schema_version is None:
            schema_version = 1
        if schema_version < 1:
            errors.append(FieldError("schema_version", "must be >= 1", schema_version))

        # Optional payload fields
        payload_uri = d.get("payload_uri")
        if payload_uri is not None:
            try:
                payload_uri = coerce_str_nonempty(payload_uri, "payload_uri")
            except ContractValidationError as e:
                errors.extend(e.errors)
                payload_uri = None

        payload_sha256 = d.get("payload_sha256")
        if payload_sha256 is not None:
            try:
                payload_sha256 = coerce_str_nonempty(payload_sha256, "payload_sha256")
            except ContractValidationError as e:
                errors.extend(e.errors)
                payload_sha256 = None
            else:
                if len(payload_sha256) != 64:
                    errors.append(FieldError("payload_sha256", "must be 64 hex characters", payload_sha256))

        # Optionals
        source_record_id = d.get("source_record_id", "")
        source_record_revision = d.get("source_record_revision", "")
        if not isinstance(source_record_id, str):
            source_record_id = ""
        if not isinstance(source_record_revision, str):
            source_record_revision = ""

        # Headers (defensive copy done in __post_init__)
        headers = d.get("headers", {})
        if not isinstance(headers, dict):
            headers = {}

        return cls(
            dataset_id=dataset_id,
            event_id=event_id,
            entity_type=entity_type,
            entity_id=entity_id,
            customer_id=customer_id,
            event_type=event_type,
            event_time=event_time,
            available_at=available_at,
            ingested_at=ingested_at,
            source_system=source_system,
            schema_version=schema_version,
            source_record_id=source_record_id,
            source_record_revision=source_record_revision,
            payload_uri=payload_uri,
            payload_sha256=payload_sha256,
            headers=headers,
        )

    # -- serialization --------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "event_id": self.event_id,
            "entity_type": self.entity_type.value,
            "entity_id": self.entity_id,
            "customer_id": self.customer_id,
            "event_type": self.event_type.value,
            "event_time": self.event_time.isoformat(),
            "available_at": self.available_at.isoformat(),
            "ingested_at": self.ingested_at.isoformat(),
            "source_system": self.source_system,
            "schema_version": self.schema_version,
            "source_record_id": self.source_record_id,
            "source_record_revision": self.source_record_revision,
            "payload_uri": self.payload_uri,
            "payload_sha256": self.payload_sha256,
            "headers": dict(self.headers),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
