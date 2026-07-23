"""FeatureCatalog Contract (Section 6.3) — strict, deeply immutable.

Two states:
  - DRAFT: can have UNKNOWN risk, empty owner/lineage
  - PUBLISHABLE: requires owner, lineage, non-UNKNOWN risk

Production paths (training, online serving, LeakBench gate) must use
is_publishable() check before consuming a catalog entry.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Sequence

from riskcloud.contracts.validation import (
    ContractValidationError,
    FieldError,
    coerce_str_nonempty,
    coerce_enum,
    coerce_float_opt,
    coerce_bool_opt,
    coerce_int_opt,
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
    ttl: Optional[int] = None
    owner: str = ""
    version: int = 1
    leakage_risk: LeakageRisk = LeakageRisk.UNKNOWN
    semantic_group_id: Optional[str] = None
    cost_unit: Optional[float] = None
    lineage_expression: Optional[str] = None
    description: str = ""
    tags: tuple[str, ...] = ()

    # -- strict entry points -------------------------------------------

    @classmethod
    def parse(cls, d: dict[str, Any]) -> "FeatureCatalogEntry":
        errors: list[FieldError] = []
        try:
            entry = cls._from_dict_coerce(d, errors)
        except ContractValidationError:
            raise
        if errors:
            raise ContractValidationError(errors)
        return entry

    @classmethod
    def from_dict_unchecked(cls, d: dict[str, Any]) -> "FeatureCatalogEntry":
        errors: list[FieldError] = []
        return cls._from_dict_coerce(d, errors)

    @classmethod
    def _from_dict_coerce(cls, d: dict[str, Any], errors: list[FieldError]) -> "FeatureCatalogEntry":
        def get_str(k: str) -> str:
            try:
                return coerce_str_nonempty(d.get(k), k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return ""

        feature_id = get_str("feature_id")
        feature_name = get_str("feature_name")
        entity_type = get_str("entity_type")
        feature_group = get_str("feature_group")
        source_system = get_str("source_system")
        event_time_rule = get_str("event_time_rule")
        availability_rule = get_str("availability_rule")

        try:
            stage = coerce_enum(d.get("stage"), FeatureStage, "stage")
        except ContractValidationError as e:
            errors.extend(e.errors)
            stage = FeatureStage.PRE_APPLICATION

        try:
            online_available = coerce_bool_opt(d.get("online_available"), "online_available") or False
        except ContractValidationError as e:
            errors.extend(e.errors)
            online_available = False

        try:
            ttl = coerce_int_opt(d.get("ttl"), "ttl")
        except ContractValidationError as e:
            errors.extend(e.errors)
            ttl = None
        if ttl is not None and ttl <= 0:
            errors.append(FieldError("ttl", "must be positive", ttl))

        owner = d.get("owner", "")
        if not isinstance(owner, str):
            owner = ""

        try:
            version = coerce_int_opt(d.get("version", 1), "version")
        except ContractValidationError as e:
            errors.extend(e.errors)
            version = None
        if version is None:
            version = 1
        if version < 1:
            errors.append(FieldError("version", "must be >= 1", version))

        try:
            leakage_risk = coerce_enum(d.get("leakage_risk", "unknown"), LeakageRisk, "leakage_risk")
        except ContractValidationError as e:
            errors.extend(e.errors)
            leakage_risk = LeakageRisk.UNKNOWN

        semantic_group_id = d.get("semantic_group_id")

        try:
            cost_unit = coerce_float_opt(d.get("cost_unit"), "cost_unit")
        except ContractValidationError as e:
            errors.extend(e.errors)
            cost_unit = None
        if cost_unit is not None and cost_unit < 0:
            errors.append(FieldError("cost_unit", "must be non-negative", cost_unit))

        lineage_expression = d.get("lineage_expression")

        description = d.get("description", "")
        if not isinstance(description, str):
            description = ""

        tags_raw = d.get("tags", [])
        if isinstance(tags_raw, list):
            tags = tuple(str(t) for t in tags_raw)
        elif isinstance(tags_raw, tuple):
            tags = tags_raw
        else:
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

    PUBLISHABLE_REQUIRED = {
        "owner must be non-empty": lambda e: bool(e.owner.strip()),
        "leakage_risk must not be UNKNOWN": lambda e: e.leakage_risk != LeakageRisk.UNKNOWN,
        "lineage_expression must be non-empty": lambda e: e.lineage_expression is not None and e.lineage_expression.strip() != "",
        "semantic_group_id must be set": lambda e: e.semantic_group_id is not None and e.semantic_group_id.strip() != "",
    }

    def is_publishable(self) -> bool:
        """True if this entry can be used in training, online serving, or LeakBench gate."""
        return len(self.publishable_errors()) == 0

    def publishable_errors(self) -> list[FieldError]:
        """Return errors that prevent this entry from being publishable."""
        errors: list[FieldError] = []
        for msg, predicate in self.PUBLISHABLE_REQUIRED.items():
            if not predicate(self):
                errors.append(FieldError(self.feature_id, msg))
        # Online features must have TTL
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
            "tags": list(self.tags),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
