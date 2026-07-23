"""Event Contract (Section 6.1) — strict identity enforcement, deep immutability.

Key invariants:
  - event_id MUST match the canonical identity computed from source keys
  - Either source_record_id or payload_sha256 must be non-empty (collision proof)
  - headers values are type-checked
  - Deep immutability via recursive freeze
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from riskcloud.contracts.validation import (
    ContractValidationError,
    FieldError,
    coerce_datetime_utc,
    coerce_enum,
    coerce_int_opt,
    coerce_str_nonempty,
    coerce_str_nonempty_opt,
    coerce_str_opt,
    deep_freeze,
    deep_thaw,
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
# Canonical event identity
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
    """Compute canonical event_id from immutable business key.

    Uses UTC-normalized time for same-instant-same-ID.
    Does NOT use payload_uri (URIs are not stable identity).
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
    """Universal event envelope with enforced identity and deep immutability."""

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
    payload_uri: str | None = None
    payload_sha256: str | None = None
    headers: Any = field(default_factory=dict)

    def __post_init__(self):
        """Deep-freeze headers for recursive immutability."""
        object.__setattr__(self, "headers", deep_freeze(self.headers))

    # -- strict entry points -------------------------------------------

    @classmethod
    def parse(cls, d: dict[str, Any]) -> Event:
        """Strict deserialization. Raises ContractValidationError on failure."""
        errors: list[FieldError] = []
        try:
            evt = cls._from_dict_coerce(d, errors)
        except ContractValidationError:
            raise
        if errors:
            raise ContractValidationError(errors)
        return evt

    @classmethod
    def from_dict_unchecked(cls, d: dict[str, Any]) -> Event:
        """Unchecked deserialization for tests only."""
        errors: list[FieldError] = []
        return cls._from_dict_coerce(d, errors)

    @classmethod
    def _from_dict_coerce(cls, d: dict[str, Any], errors: list[FieldError]) -> Event:
        def _str(k: str) -> str:
            try:
                return coerce_str_nonempty(d.get(k), k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return ""

        def _str_opt(k: str) -> str | None:
            try:
                return coerce_str_nonempty_opt(d.get(k), k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return None

        dataset_id = _str("dataset_id")
        event_id = _str("event_id")
        entity_id = _str("entity_id")
        customer_id = _str("customer_id")
        source_system = _str("source_system")

        # Source record ID – type-check, fail if wrong type
        source_record_id_raw = d.get("source_record_id", "")
        if source_record_id_raw is not None:
            try:
                source_record_id = coerce_str_nonempty_opt(source_record_id_raw, "source_record_id") or ""
            except ContractValidationError as e:
                errors.extend(e.errors)
                source_record_id = ""
        else:
            source_record_id = ""

        source_record_revision_raw = d.get("source_record_revision", "")
        if source_record_revision_raw is not None:
            try:
                source_record_revision = coerce_str_opt(source_record_revision_raw, "source_record_revision") or ""
            except ContractValidationError as e:
                errors.extend(e.errors)
                source_record_revision = ""
        else:
            source_record_revision = ""

        # Enums
        entity_type = EntityType.CUSTOMER
        try:
            entity_type = coerce_enum(d.get("entity_type"), EntityType, "entity_type")
        except ContractValidationError as e:
            errors.extend(e.errors)

        evt_type = EventType.LOAN_APPLICATION
        try:
            evt_type = coerce_enum(d.get("event_type"), EventType, "event_type")
        except ContractValidationError as e:
            errors.extend(e.errors)

        # Datetimes
        event_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        available_at = datetime(1970, 1, 1, tzinfo=timezone.utc)
        ingested_at = datetime(1970, 1, 1, tzinfo=timezone.utc)
        try:
            event_time = coerce_datetime_utc(d.get("event_time"), "event_time")
        except ContractValidationError as e:
            errors.extend(e.errors)
        try:
            available_at = coerce_datetime_utc(d.get("available_at"), "available_at")
        except ContractValidationError as e:
            errors.extend(e.errors)
        try:
            ingested_at = coerce_datetime_utc(d.get("ingested_at"), "ingested_at")
        except ContractValidationError as e:
            errors.extend(e.errors)

        # Time ordering
        if event_time.tzinfo is not None and available_at.tzinfo is not None:
            if event_time > available_at:
                errors.append(FieldError(
                    "event_time",
                    "event_time must be <= available_at",
                ))

        # Schema version
        schema_version = 1
        sv_raw = d.get("schema_version", 1)
        try:
            sv = coerce_int_opt(sv_raw, "schema_version")
        except ContractValidationError as e:
            errors.extend(e.errors)
            sv = None
        if sv is None:
            sv = 1
        if sv < 1:
            errors.append(FieldError("schema_version", "must be >= 1", sv))
        schema_version = sv

        # Payload fields
        payload_uri = _str_opt("payload_uri")
        payload_sha256 = _str_opt("payload_sha256")
        if payload_sha256 is not None and len(payload_sha256) != 64:
            errors.append(FieldError("payload_sha256", "must be 64 hex characters", payload_sha256))

        # Collision-proof requirement
        if not source_record_id and not payload_sha256:
            errors.append(FieldError(
                "source_record_id",
                "either source_record_id or payload_sha256 must be non-empty to prevent collision",
            ))

        # Headers: type-check keys and values
        headers_raw = d.get("headers", {})
        headers: dict = {}
        if isinstance(headers_raw, dict):
            for k, v in headers_raw.items():
                if not isinstance(k, str):
                    errors.append(FieldError(f"headers.{k}", f"key must be str, got {type(k).__name__}"))
                if not isinstance(v, str):
                    errors.append(FieldError(f"headers.{k}", f"value must be str, got {type(v).__name__}"))
            headers = headers_raw
        else:
            errors.append(FieldError("headers", f"expected dict, got {type(headers_raw).__name__}", headers_raw))

        # ---- ENFORCE canonical event_id ----
        if event_time.tzinfo is not None:
            expected_id = compute_event_id(
                dataset_id, entity_type, entity_id, evt_type, event_time,
                source_record_id, source_record_revision,
            )
            if not hmac.compare_digest(event_id, expected_id):
                errors.append(FieldError(
                    "event_id",
                    f"does not match canonical identity; expected={expected_id[:16]}..., got={event_id[:16]}...",
                    event_id,
                ))

        return cls(
            dataset_id=dataset_id,
            event_id=event_id,
            entity_type=entity_type,
            entity_id=entity_id,
            customer_id=customer_id,
            event_type=evt_type,
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
            "headers": deep_thaw(self.headers),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
