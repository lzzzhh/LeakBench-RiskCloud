"""FeatureCatalog Contract (Section 6.3) — strict, deeply immutable.

Two states:
  - DRAFT: can have UNKNOWN risk, empty owner/lineage
  - PUBLISHABLE: requires owner, lineage, non-UNKNOWN risk

All optional string fields are type-checked; tags are recursively frozen.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any

from riskcloud.contracts.validation import (
    ContractValidationError,
    FieldError,
    coerce_bool_opt,
    coerce_enum,
    coerce_float_opt,
    coerce_int_opt,
    coerce_str_nonempty,
    coerce_str_opt,
    deep_freeze,
    deep_thaw,
)


class FeatureStage(str, Enum):
    PRE_APPLICATION = "pre_application"
    APPLICATION = "application"
    DECISION = "decision"
    POST_DECISION = "post_decision"
    POST_OUTCOME = "post_outcome"
    LABEL_DERIVED = "label_derived"


class LeakageRisk(str, Enum):
    NONE = "none"
    TEMPORAL = "temporal"
    POST_OUTCOME = "post_outcome"
    LABEL_DERIVED = "label_derived"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FeatureCatalogEntry:
    feature_id: str
    feature_name: str
    entity_type: str
    feature_group: str
    source_system: str
    event_time_rule: str
    availability_rule: str
    stage: FeatureStage
    online_available: bool = False
    ttl: int | None = None
    owner: str = ""
    version: int = 1
    leakage_risk: LeakageRisk = LeakageRisk.UNKNOWN
    semantic_group_id: str | None = None
    cost_unit: float | None = None
    lineage_expression: str | None = None
    description: str = ""
    tags: Any = ()

    def __post_init__(self):
        """Deep-freeze tags for recursive immutability."""
        object.__setattr__(self, "tags", deep_freeze(self.tags))

    # -- strict entry points -------------------------------------------

    @classmethod
    def parse(cls, d: dict[str, Any]) -> FeatureCatalogEntry:
        errors: list[FieldError] = []
        try:
            entry = cls._from_dict_coerce(d, errors)
        except ContractValidationError:
            raise
        if errors:
            raise ContractValidationError(errors)
        return entry

    @classmethod
    def from_dict_unchecked(cls, d: dict[str, Any]) -> FeatureCatalogEntry:
        errors: list[FieldError] = []
        return cls._from_dict_coerce(d, errors)

    @classmethod
    def _from_dict_coerce(cls, d: dict[str, Any], errors: list[FieldError]) -> FeatureCatalogEntry:
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

        feature_id = _str("feature_id")
        feature_name = _str("feature_name")
        entity_type = _str("entity_type")
        feature_group = _str("feature_group")
        source_system = _str("source_system")
        event_time_rule = _str("event_time_rule")
        availability_rule = _str("availability_rule")

        # Optional string fields (type-checked, may be empty)
        owner = d.get("owner", "")
        if owner is not None and not isinstance(owner, str):
            errors.append(FieldError("owner", f"expected str, got {type(owner).__name__}"))
            owner = ""
        elif owner is None:
            owner = ""

        semantic_group_id = _str_opt("semantic_group_id")

        lineage_expression = d.get("lineage_expression")
        if lineage_expression is not None and not isinstance(lineage_expression, str):
            errors.append(FieldError("lineage_expression", f"expected str, got {type(lineage_expression).__name__}"))
            lineage_expression = None

        description = d.get("description", "")
        if description is not None and not isinstance(description, str):
            errors.append(FieldError("description", f"expected str, got {type(description).__name__}"))
            description = ""
        elif description is None:
            description = ""

        # Stage
        stage = FeatureStage.PRE_APPLICATION
        try:
            stage = coerce_enum(d.get("stage"), FeatureStage, "stage")
        except ContractValidationError as e:
            errors.extend(e.errors)

        # Bools / ints / floats
        online_available = False
        try:
            online_available = coerce_bool_opt(d.get("online_available"), "online_available") or False
        except ContractValidationError as e:
            errors.extend(e.errors)

        ttl = None
        try:
            ttl = coerce_int_opt(d.get("ttl"), "ttl")
        except ContractValidationError as e:
            errors.extend(e.errors)
        if ttl is not None and ttl <= 0:
            errors.append(FieldError("ttl", "must be positive", ttl))

        version = 1
        try:
            v = coerce_int_opt(d.get("version", 1), "version")
            if v is None:
                v = 1
            version = v
        except ContractValidationError as e:
            errors.extend(e.errors)
        if version < 1:
            errors.append(FieldError("version", "must be >= 1", version))

        leakage_risk = LeakageRisk.UNKNOWN
        try:
            leakage_risk = coerce_enum(d.get("leakage_risk", "unknown"), LeakageRisk, "leakage_risk")
        except ContractValidationError as e:
            errors.extend(e.errors)

        cost_unit = None
        try:
            cost_unit = coerce_float_opt(d.get("cost_unit"), "cost_unit")
        except ContractValidationError as e:
            errors.extend(e.errors)
        if cost_unit is not None and cost_unit < 0:
            errors.append(FieldError("cost_unit", "must be non-negative", cost_unit))

        # Tags: accept list or tuple, fail on wrong type
        tags_raw = d.get("tags", ())
        if isinstance(tags_raw, (list, tuple)):
            tags = tuple(tags_raw)
        elif tags_raw is None:
            tags = ()
        else:
            errors.append(FieldError("tags", f"expected list or tuple, got {type(tags_raw).__name__}", tags_raw))
            tags = ()

        return cls(
            feature_id=feature_id,
            feature_name=feature_name,
            entity_type=entity_type,
            feature_group=feature_group,
            source_system=source_system,
            event_time_rule=event_time_rule,
            availability_rule=availability_rule,
            stage=stage,
            online_available=online_available,
            ttl=ttl,
            owner=owner,
            version=version,
            leakage_risk=leakage_risk,
            semantic_group_id=semantic_group_id,
            cost_unit=cost_unit,
            lineage_expression=lineage_expression,
            description=description,
            tags=tags,
        )

    # -- publishable check ----------------------------------------------

    PUBLISHABLE_CHECKS = [
        ("owner must be non-empty", lambda e: isinstance(e.owner, str) and bool(e.owner.strip())),
        ("leakage_risk must not be UNKNOWN", lambda e: e.leakage_risk != LeakageRisk.UNKNOWN),
        (
            "lineage_expression must be non-empty",
            lambda e: isinstance(e.lineage_expression, str) and e.lineage_expression.strip() != "",
        ),
        (
            "semantic_group_id must be set",
            lambda e: isinstance(e.semantic_group_id, str) and e.semantic_group_id.strip() != "",
        ),
    ]

    def is_publishable(self) -> bool:
        return len(self.publishable_errors()) == 0

    def publishable_errors(self) -> list[FieldError]:
        errors: list[FieldError] = []
        for msg, predicate in self.PUBLISHABLE_CHECKS:
            try:
                if not predicate(self):
                    errors.append(FieldError(self.feature_id, msg))
            except Exception:
                errors.append(FieldError(self.feature_id, f"type error during publishable check: {msg}"))
        if self.online_available and self.ttl is None:
            errors.append(FieldError(self.feature_id, "online_available requires ttl"))
        return errors

    # -- serialization --------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_id": self.feature_id,
            "feature_name": self.feature_name,
            "entity_type": self.entity_type,
            "feature_group": self.feature_group,
            "source_system": self.source_system,
            "event_time_rule": self.event_time_rule,
            "availability_rule": self.availability_rule,
            "stage": self.stage.value,
            "online_available": self.online_available,
            "ttl": self.ttl,
            "owner": self.owner,
            "version": self.version,
            "leakage_risk": self.leakage_risk.value,
            "semantic_group_id": self.semantic_group_id,
            "cost_unit": self.cost_unit,
            "lineage_expression": self.lineage_expression,
            "description": self.description,
            "tags": deep_thaw(self.tags),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
