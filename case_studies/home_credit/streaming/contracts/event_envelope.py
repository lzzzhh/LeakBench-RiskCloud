"""Realtime event contracts — EventEnvelope, FeatureUpdate, Kafka topics, typed source payloads."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from riskcloud.adapters.home_credit.feature_catalog import get_features

# =================================================================
# Kafka topics
# =================================================================

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
_UPDATE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _canonical_feature_value(value: float | None) -> str:
    if value is None:
        return "null"
    if not math.isfinite(value):
        raise ValueError(f"non-finite feature value: {value}")
    if value == 0:
        return "0"
    return format(value, ".17g")


# =================================================================
# Typed source payloads
# =================================================================

def _validate_required_int(d: dict, key: str, errors: list[str]) -> int | None:
    v = d.get(key)
    if v is None:
        errors.append(f"{key} is required")
        return None
    if isinstance(v, bool) or not isinstance(v, int):
        errors.append(f"{key} must be int, got {type(v).__name__}")
        return None
    return v


def _validate_optional_float(d: dict, key: str, errors: list[str]) -> float | None:
    v = d.get(key)
    if v is None:
        return None
    if isinstance(v, bool):
        errors.append(f"{key} must not be bool")
        return None
    if isinstance(v, (int, float)):
        if not math.isfinite(float(v)):
            errors.append(f"{key} must be finite")
            return None
        return float(v)
    errors.append(f"{key} must be numeric, got {type(v).__name__}")
    return None


def _validate_optional_int(d: dict, key: str, errors: list[str]) -> int | None:
    v = d.get(key)
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, int):
        errors.append(f"{key} must be int, got {type(v).__name__}")
        return None
    return v


def _validate_optional_str(d: dict, key: str, errors: list[str]) -> str | None:
    v = d.get(key)
    if v is None:
        return None
    if not isinstance(v, str):
        errors.append(f"{key} must be str, got {type(v).__name__}")
        return None
    return v


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

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ApplicationEventPayload:
        errors: list[str] = []
        sk = _validate_required_int(d, "SK_ID_CURR", errors)
        if errors:
            raise ValueError("; ".join(errors))
        flag_keys = {f"FLAG_DOCUMENT_{i}" for i in range(2, 22)}
        extra = set(d.keys()) - {"SK_ID_CURR", "AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY",
                                 "AMT_GOODS_PRICE", "DAYS_BIRTH", "EXT_SOURCE_1", "EXT_SOURCE_2",
                                 "EXT_SOURCE_3"} - flag_keys
        if extra:
            errors.append(f"unknown fields: {extra}")
        flags = {}
        for fk in flag_keys:
            v = d.get(fk, 0)
            if isinstance(v, bool) or v not in (0, 1, None):
                errors.append(f"{fk} must be 0 or 1, got {v}")
            flags[fk] = v if v is not None else 0
        amt_income = _validate_optional_float(d, "AMT_INCOME_TOTAL", errors)
        amt_credit = _validate_optional_float(d, "AMT_CREDIT", errors)
        amt_annuity = _validate_optional_float(d, "AMT_ANNUITY", errors)
        amt_goods = _validate_optional_float(d, "AMT_GOODS_PRICE", errors)
        days_birth = _validate_optional_int(d, "DAYS_BIRTH", errors)
        es1 = _validate_optional_float(d, "EXT_SOURCE_1", errors)
        es2 = _validate_optional_float(d, "EXT_SOURCE_2", errors)
        es3 = _validate_optional_float(d, "EXT_SOURCE_3", errors)
        if errors:
            raise ValueError("; ".join(errors))
        return cls(
            SK_ID_CURR=sk, AMT_INCOME_TOTAL=amt_income, AMT_CREDIT=amt_credit,
            AMT_ANNUITY=amt_annuity, AMT_GOODS_PRICE=amt_goods, DAYS_BIRTH=days_birth,
            EXT_SOURCE_1=es1, EXT_SOURCE_2=es2, EXT_SOURCE_3=es3,
            **{k.replace("FLAG_DOCUMENT_", "FLAG_DOCUMENT_"): v for k, v in flags.items()},
        )


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

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> BureauEventPayload:
        errors: list[str] = []
        sk_curr = _validate_required_int(d, "SK_ID_CURR", errors)
        sk_bur = _validate_required_int(d, "SK_ID_BUREAU", errors)
        if errors:
            raise ValueError("; ".join(errors))
        days_credit = _validate_optional_int(d, "DAYS_CREDIT", errors)
        ca = d.get("CREDIT_ACTIVE")
        if ca is not None and ca not in ("Active", "Closed"):
            errors.append(f"CREDIT_ACTIVE must be Active/Closed, got {ca}")
        amt_sum = _validate_optional_float(d, "AMT_CREDIT_SUM", errors)
        amt_debt = _validate_optional_float(d, "AMT_CREDIT_SUM_DEBT", errors)
        amt_overdue = _validate_optional_float(d, "AMT_CREDIT_SUM_OVERDUE", errors)
        extra = set(d.keys()) - {"SK_ID_CURR", "SK_ID_BUREAU", "DAYS_CREDIT", "CREDIT_ACTIVE",
                                 "AMT_CREDIT_SUM", "AMT_CREDIT_SUM_DEBT", "AMT_CREDIT_SUM_OVERDUE"}
        if extra:
            errors.append(f"unknown fields: {extra}")
        if errors:
            raise ValueError("; ".join(errors))
        return cls(
            SK_ID_CURR=sk_curr, SK_ID_BUREAU=sk_bur,
            DAYS_CREDIT=days_credit, CREDIT_ACTIVE=ca,
            AMT_CREDIT_SUM=amt_sum, AMT_CREDIT_SUM_DEBT=amt_debt,
            AMT_CREDIT_SUM_OVERDUE=amt_overdue,
        )


@dataclass(frozen=True)
class BureauBalanceEventPayload:
    SK_ID_CURR: int
    SK_ID_BUREAU: int
    MONTHS_BALANCE: int | None = None
    STATUS: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> BureauBalanceEventPayload:
        errors: list[str] = []
        sk_curr = _validate_required_int(d, "SK_ID_CURR", errors)
        sk_bur = _validate_required_int(d, "SK_ID_BUREAU", errors)
        if errors:
            raise ValueError("; ".join(errors))
        st = d.get("STATUS")
        if st is not None and st not in ("0", "1", "2", "3", "4", "5", "C", "X"):
            errors.append(f"STATUS must be 0-5/C/X, got {st}")
        extra = set(d.keys()) - {"SK_ID_CURR", "SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"}
        if extra:
            raise ValueError(f"unknown fields: {extra}")
        months_balance = _validate_optional_int(d, "MONTHS_BALANCE", errors)
        if errors:
            raise ValueError("; ".join(errors))
        return cls(
            SK_ID_CURR=sk_curr, SK_ID_BUREAU=sk_bur,  # type: ignore[arg-type]
            MONTHS_BALANCE=months_balance, STATUS=st,
        )


SourcePayload = ApplicationEventPayload | BureauEventPayload | BureauBalanceEventPayload


# =================================================================
# Enums
# =================================================================

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

_EVENT_TYPE_TO_PAYLOAD: dict[EventType, type] = {
    EventType.APPLICATION_UPSERT: ApplicationEventPayload,
    EventType.BUREAU_UPSERT: BureauEventPayload,
    EventType.BUREAU_BALANCE_UPSERT: BureauBalanceEventPayload,
}


CATALOG_FEATURE_IDS = frozenset(f.feature_id for f in get_features())


# =================================================================
# EventEnvelope
# =================================================================

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
    payload: SourcePayload = field(default_factory=dict)

    @staticmethod
    def compute_event_id(
        source_table: str, source_pk: str, payload_dict: dict[str, Any],
        event_time: datetime, op: Operation,
    ) -> str:
        parts = [source_table, source_pk, event_time.isoformat(), op.value,
                 json.dumps(payload_dict, sort_keys=True, separators=(",", ":"), allow_nan=False)]
        return "sha256:" + hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()

    def expected_event_id(self) -> str:
        return self.compute_event_id(
            self.source_table, self.source_pk, self.payload.to_dict(),
            self.event_time, self.op,
        )

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not _EVENT_ID_RE.match(self.event_id):
            errors.append("event_id must match sha256:<64 hex>")
        if self.schema_version != SCHEMA_VERSION:
            errors.append(f"schema_version must be {SCHEMA_VERSION}")
        if not _ENTITY_ID_RE.match(self.entity_id):
            errors.append("entity_id must match SK_ID_CURR:<digits>")
        if self.event_time.tzinfo is None:
            errors.append("event_time must be timezone-aware")
        if self.produced_at.tzinfo is None:
            errors.append("produced_at must be timezone-aware")
        if self.source_table not in SOURCE_TABLES:
            errors.append(f"unknown source_table: {self.source_table}")
        expected_table = _EVENT_TYPE_TO_SOURCE.get(self.event_type)
        if expected_table is None:
            errors.append(f"unknown event_type: {self.event_type}")
        elif expected_table != self.source_table:
            errors.append(f"event_type {self.event_type.value} != source_table {self.source_table}")
        expected_payload_cls = _EVENT_TYPE_TO_PAYLOAD.get(self.event_type)
        if expected_payload_cls and not isinstance(self.payload, expected_payload_cls):
            errors.append(f"payload type mismatch for {self.event_type.value}")
        else:
            try:
                if self.event_id != self.expected_event_id():
                    errors.append("event_id does not match event content")
            except Exception:
                errors.append("failed to verify event_id integrity")
        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id, "schema_version": self.schema_version,
            "event_type": self.event_type.value, "source_table": self.source_table,
            "source_pk": self.source_pk, "entity_id": self.entity_id,
            "op": self.op.value, "event_time": self.event_time.isoformat(),
            "produced_at": self.produced_at.isoformat(), "payload": self.payload.to_dict(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, allow_nan=False)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> EventEnvelope:
        evt_type = EventType(d["event_type"])
        payload_cls = _EVENT_TYPE_TO_PAYLOAD.get(evt_type)
        payload = payload_cls.from_dict(d.get("payload", {})) if payload_cls else d.get("payload", {})
        return cls(
            event_id=d["event_id"], schema_version=d["schema_version"],
            event_type=evt_type, source_table=d["source_table"],
            source_pk=d["source_pk"], entity_id=d["entity_id"],
            op=Operation(d["op"]), event_time=datetime.fromisoformat(d["event_time"]),
            produced_at=datetime.fromisoformat(d["produced_at"]), payload=payload,
        )

    @classmethod
    def from_json(cls, s: str) -> EventEnvelope:
        return cls.from_dict(json.loads(s))


# =================================================================
# FeatureUpdate
# =================================================================

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
        parts = [entity_id, feature_id, str(feature_version), event_time.isoformat(),
                 _canonical_feature_value(feature_value)]
        return "sha256:" + hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()

    def expected_update_id(self) -> str:
        return self.compute_update_id(
            self.entity_id, self.feature_id, self.feature_version,
            self.event_time, self.feature_value,
        )

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not _ENTITY_ID_RE.match(self.entity_id):
            errors.append("entity_id canonical")
        if self.feature_id not in CATALOG_FEATURE_IDS:
            errors.append(f"feature_id not in catalog: {self.feature_id}")
        if self.feature_version <= 0:
            errors.append("feature_version > 0")
        if self.event_time.tzinfo is None:
            errors.append("event_time timezone-aware")
        if self.computed_at.tzinfo is None:
            errors.append("computed_at timezone-aware")
        if self.event_time.tzinfo is not None and self.computed_at.tzinfo is not None:
            if self.computed_at < self.event_time:
                errors.append("computed_at must be >= event_time")
        if not _EVENT_ID_RE.match(self.source_event_id):
            errors.append("source_event_id format")
        if self.source_topic not in ALL_SOURCE_TOPICS:
            errors.append(f"unknown source_topic: {self.source_topic}")
        if self.feature_value is not None and not math.isfinite(self.feature_value):
            errors.append("feature_value finite or None")
        if not _UPDATE_ID_RE.match(self.feature_update_id):
            errors.append("feature_update_id format")
        if not isinstance(self.computation_mode, ComputationMode):
            errors.append("computation_mode must be ComputationMode")
        if not isinstance(self.quality_status, QualityStatus):
            errors.append("quality_status must be QualityStatus")
        # Only check integrity if IDs are parseable and value is safe
        value_safe = (
            self.feature_value is None
            or (
                isinstance(self.feature_value, (int, float))
                and not isinstance(self.feature_value, bool)
                and math.isfinite(float(self.feature_value))
            )
        )
        if self.feature_id in CATALOG_FEATURE_IDS and value_safe:
            if self.feature_update_id != self.expected_update_id():
                errors.append("feature_update_id mismatch")
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
