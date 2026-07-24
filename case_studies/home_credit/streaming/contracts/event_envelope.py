"""Realtime event contracts — EventEnvelope, FeatureUpdate, Kafka topics, source payloads."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

# -----------------------------------------------------------------
# Kafka topics
# -----------------------------------------------------------------

@dataclass(frozen=True)
class KafkaTopicSpec:
    name: str
    partitions: int = 3
    replication_factor: int = 1
    cleanup_policy: tuple[str, ...] = ("delete",)


TOPIC_APPLICATION = "riskcloud.home_credit.application.v1"
TOPIC_BUREAU = "riskcloud.home_credit.bureau.v1"
TOPIC_BUREAU_BALANCE = "riskcloud.home_credit.bureau_balance.v1"
TOPIC_FEATURE_UPDATES = "riskcloud.home_credit.feature_updates.v1"
TOPIC_DLQ = "riskcloud.home_credit.dlq.v1"

TOPIC_SPECS: dict[str, KafkaTopicSpec] = {
    TOPIC_APPLICATION: KafkaTopicSpec(TOPIC_APPLICATION),
    TOPIC_BUREAU: KafkaTopicSpec(TOPIC_BUREAU),
    TOPIC_BUREAU_BALANCE: KafkaTopicSpec(TOPIC_BUREAU_BALANCE),
    TOPIC_FEATURE_UPDATES: KafkaTopicSpec(TOPIC_FEATURE_UPDATES, cleanup_policy=("compact", "delete")),
    TOPIC_DLQ: KafkaTopicSpec(TOPIC_DLQ),
}

ALL_SOURCE_TOPICS = {TOPIC_APPLICATION, TOPIC_BUREAU, TOPIC_BUREAU_BALANCE}
SOURCE_TABLES = {"application_train", "bureau", "bureau_balance"}

SCHEMA_VERSION = 1
_ENTITY_ID_RE = re.compile(r"^SK_ID_CURR:[0-9]+$")
_EVENT_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


# -----------------------------------------------------------------
# Source payload types
# -----------------------------------------------------------------

@dataclass(frozen=True)
class ApplicationEventPayload:
    SK_ID_CURR: int
    AMT_INCOME_TOTAL: float | None = None
    AMT_CREDIT: float | None = None
    AMT_ANNUITY: float | None = None
    AMT_GOODS_PRICE: float | None = None
    DAYS_BIRTH: int | None = None
    EXT_SOURCE_1: float | None = None
    EXT_SOURCE_2: float | None = None
    EXT_SOURCE_3: float | None = None
    FLAG_DOCUMENT_2: int = 0
    FLAG_DOCUMENT_3: int = 0
    FLAG_DOCUMENT_4: int = 0
    FLAG_DOCUMENT_5: int = 0
    FLAG_DOCUMENT_6: int = 0
    FLAG_DOCUMENT_7: int = 0
    FLAG_DOCUMENT_8: int = 0
    FLAG_DOCUMENT_9: int = 0
    FLAG_DOCUMENT_10: int = 0
    FLAG_DOCUMENT_11: int = 0
    FLAG_DOCUMENT_12: int = 0
    FLAG_DOCUMENT_13: int = 0
    FLAG_DOCUMENT_14: int = 0
    FLAG_DOCUMENT_15: int = 0
    FLAG_DOCUMENT_16: int = 0
    FLAG_DOCUMENT_17: int = 0
    FLAG_DOCUMENT_18: int = 0
    FLAG_DOCUMENT_19: int = 0
    FLAG_DOCUMENT_20: int = 0
    FLAG_DOCUMENT_21: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass(frozen=True)
class BureauEventPayload:
    SK_ID_CURR: int
    SK_ID_BUREAU: int
    DAYS_CREDIT: int | None = None
    CREDIT_ACTIVE: str | None = None
    AMT_CREDIT_SUM: float | None = None
    AMT_CREDIT_SUM_DEBT: float | None = None
    AMT_CREDIT_SUM_OVERDUE: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass(frozen=True)
class BureauBalanceEventPayload:
    SK_ID_CURR: int
    SK_ID_BUREAU: int
    MONTHS_BALANCE: int | None = None
    STATUS: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


# -----------------------------------------------------------------
# Enums
# -----------------------------------------------------------------

class Operation(str, Enum):
    UPSERT = "UPSERT"


class EventType(str, Enum):
    APPLICATION_UPSERT = "application_upsert"
    BUREAU_UPSERT = "bureau_upsert"
    BUREAU_BALANCE_UPSERT = "bureau_balance_upsert"


class ComputationMode(str, Enum):
    STREAM = "stream"


class QualityStatus(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"


_EVENT_TYPE_TO_SOURCE: dict[EventType, str] = {
    EventType.APPLICATION_UPSERT: "application_train",
    EventType.BUREAU_UPSERT: "bureau",
    EventType.BUREAU_BALANCE_UPSERT: "bureau_balance",
}

_SOURCE_TO_PAYLOAD: dict[str, type] = {
    "application_train": ApplicationEventPayload,
    "bureau": BureauEventPayload,
    "bureau_balance": BureauBalanceEventPayload,
}


# -----------------------------------------------------------------
# EventEnvelope
# -----------------------------------------------------------------

@dataclass(frozen=True)
class EventEnvelope:
    event_id: str
    schema_version: int
    event_type: EventType
    source_table: str
    source_pk: str
    entity_id: str
    op: Operation
    event_time: datetime
    produced_at: datetime
    payload: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def _canonical_json(obj: Any) -> bytes:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()

    @staticmethod
    def compute_event_id(
        source_table: str, source_pk: str, payload: dict[str, Any],
        event_time: datetime, op: Operation,
    ) -> str:
        parts = [source_table, source_pk, event_time.isoformat(), op.value,
                 json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)]
        return "sha256:" + hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()

    def validate(self) -> list[str]:
        errors = []
        if not _EVENT_ID_RE.match(self.event_id):
            errors.append(f"event_id must match sha256:<64 hex>, got {self.event_id}")
        if self.schema_version != SCHEMA_VERSION:
            errors.append(f"schema_version must be {SCHEMA_VERSION}")
        if not _ENTITY_ID_RE.match(self.entity_id):
            errors.append(f"entity_id must match SK_ID_CURR:<digits>, got {self.entity_id}")
        if self.event_time.tzinfo is None:
            errors.append("event_time must be timezone-aware")
        if self.produced_at.tzinfo is None:
            errors.append("produced_at must be timezone-aware")
        if self.source_table not in SOURCE_TABLES:
            errors.append(f"unknown source_table: {self.source_table}")
        expected_table = _EVENT_TYPE_TO_SOURCE.get(self.event_type)
        if expected_table is None:
            errors.append(f"unknown event_type: {self.event_type}")
        elif expected_table not in self.source_table:
            errors.append(f"event_type {self.event_type} does not match source_table {self.source_table}")
        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id, "schema_version": self.schema_version,
            "event_type": self.event_type.value, "source_table": self.source_table,
            "source_pk": self.source_pk, "entity_id": self.entity_id,
            "op": self.op.value, "event_time": self.event_time.isoformat(),
            "produced_at": self.produced_at.isoformat(), "payload": self.payload,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, allow_nan=False)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> EventEnvelope:
        return cls(
            event_id=d["event_id"], schema_version=d["schema_version"],
            event_type=EventType(d["event_type"]), source_table=d["source_table"],
            source_pk=d["source_pk"], entity_id=d["entity_id"],
            op=Operation(d["op"]), event_time=datetime.fromisoformat(d["event_time"]),
            produced_at=datetime.fromisoformat(d["produced_at"]), payload=d.get("payload", {}),
        )

    @classmethod
    def from_json(cls, s: str) -> EventEnvelope:
        return cls.from_dict(json.loads(s))


# -----------------------------------------------------------------
# FeatureUpdate
# -----------------------------------------------------------------

@dataclass(frozen=True)
class FeatureUpdate:
    feature_update_id: str
    entity_id: str
    feature_id: str
    feature_value: float | None
    feature_version: int
    event_time: datetime
    computed_at: datetime
    source_event_id: str
    source_topic: str
    computation_mode: ComputationMode = ComputationMode.STREAM
    feature_catalog_version: str = "hc-features-v1"
    quality_status: QualityStatus = QualityStatus.VALID

    @staticmethod
    def compute_update_id(
        entity_id: str, feature_id: str, feature_version: int,
        event_time: datetime, feature_value: float | None,
    ) -> str:
        val = "null" if feature_value is None else (
            str(feature_value) if math.isfinite(feature_value) else
            (_ for _ in ()).throw(ValueError(f"non-finite value: {feature_value}"))
        )
        parts = [entity_id, feature_id, str(feature_version), event_time.isoformat(), val]
        return "sha256:" + hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()

    def validate(self) -> list[str]:
        errors = []
        if not _ENTITY_ID_RE.match(self.entity_id):
            errors.append(f"entity_id canonical: {self.entity_id}")
        if self.feature_version <= 0:
            errors.append("feature_version must be > 0")
        if self.event_time.tzinfo is None:
            errors.append("event_time timezone-aware")
        if self.computed_at.tzinfo is None:
            errors.append("computed_at timezone-aware")
        if self.computed_at < self.event_time:
            errors.append("computed_at < event_time")
        if not _EVENT_ID_RE.match(self.source_event_id):
            errors.append("source_event_id format")
        if self.source_topic not in ALL_SOURCE_TOPICS:
            errors.append(f"unknown source_topic: {self.source_topic}")
        if self.feature_value is not None and not math.isfinite(self.feature_value):
            errors.append("feature_value must be finite or None")
        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_update_id": self.feature_update_id, "entity_id": self.entity_id,
            "feature_id": self.feature_id, "feature_value": self.feature_value,
            "feature_version": self.feature_version, "event_time": self.event_time.isoformat(),
            "computed_at": self.computed_at.isoformat(), "source_event_id": self.source_event_id,
            "source_topic": self.source_topic, "computation_mode": self.computation_mode.value,
            "feature_catalog_version": self.feature_catalog_version,
            "quality_status": self.quality_status.value,
        }
