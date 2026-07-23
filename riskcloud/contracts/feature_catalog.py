"""Feature Catalog Contract (Section 6.3).

Every feature used in training or online scoring must have a catalog entry
that records its temporal stage, availability rule, leakage risk, and
semantic grouping.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Optional


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
    """Catalog entry for a single feature.

    This is the source-of-truth definition that both Spark (batch) and
    Flink (stream) must respect.
    """

    feature_id: str
    feature_name: str
    entity_type: str                      # customer, application, transaction, document
    feature_group: str                    # bureau, repayment, document_quality, etc.
    source_system: str                    # original source table/system
    event_time_rule: str                  # how to determine when the fact occurred
    availability_rule: str                # when the fact becomes usable for prediction
    stage: FeatureStage
    online_available: bool = False
    ttl: Optional[int] = None             # seconds; None = no expiry
    owner: str = ""
    version: int = 1
    leakage_risk: LeakageRisk = LeakageRisk.UNKNOWN
    semantic_group_id: Optional[str] = None
    cost_unit: Optional[float] = None     # governance cost per unit
    lineage_expression: Optional[str] = None  # SQL or computation hash
    description: str = ""
    tags: list[str] = field(default_factory=list)

    # -- validation ----------------------------------------------------

    def validate(self) -> list[str]:
        errors: list[str] = []

        if not self.feature_id.strip():
            errors.append("feature_id must be non-empty")
        if not self.feature_name.strip():
            errors.append("feature_name must be non-empty")
        if not self.entity_type.strip():
            errors.append("entity_type must be non-empty")
        if not self.feature_group.strip():
            errors.append("feature_group must be non-empty")
        if not self.source_system.strip():
            errors.append("source_system must be non-empty")
        if not self.event_time_rule.strip():
            errors.append("event_time_rule must be non-empty")
        if not self.availability_rule.strip():
            errors.append("availability_rule must be non-empty")

        if self.version < 1:
            errors.append("version must be >= 1")

        if self.ttl is not None and self.ttl <= 0:
            errors.append("ttl must be positive")

        if self.cost_unit is not None and self.cost_unit < 0:
            errors.append("cost_unit must be non-negative")

        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    # -- serialization --------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["stage"] = self.stage.value
        d["leakage_risk"] = self.leakage_risk.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "FeatureCatalogEntry":
        return cls(
            feature_id=d["feature_id"],
            feature_name=d["feature_name"],
            entity_type=d["entity_type"],
            feature_group=d["feature_group"],
            source_system=d["source_system"],
            event_time_rule=d["event_time_rule"],
            availability_rule=d["availability_rule"],
            stage=FeatureStage(d["stage"]),
            online_available=d.get("online_available", False),
            ttl=d.get("ttl"),
            owner=d.get("owner", ""),
            version=d.get("version", 1),
            leakage_risk=LeakageRisk(d.get("leakage_risk", "unknown")),
            semantic_group_id=d.get("semantic_group_id"),
            cost_unit=d.get("cost_unit"),
            lineage_expression=d.get("lineage_expression"),
            description=d.get("description", ""),
            tags=d.get("tags", []),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str) -> "FeatureCatalogEntry":
        return cls.from_dict(json.loads(s))
