"""Prediction Point Contract (Section 6.2).

Every model training must first generate Prediction Points, then execute
point-in-time joins. Never aggregate tables to final state first, then
randomly split.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class Split(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    OOT = "oot"          # out-of-time
    ONLINE = "online"


@dataclass(frozen=True)
class PredictionPoint:
    """A single point in time at which a prediction is made.

    The prediction_time is the boundary: any feature whose available_at >
    prediction_time is temporally invalid for this prediction point.
    """

    prediction_id: str
    entity_id: str
    prediction_time: datetime
    label: Optional[float] = None
    label_time: Optional[datetime] = None
    split: Split = Split.TRAIN
    snapshot_id: Optional[str] = None
    boundary_version: Optional[str] = None

    # -- validation ----------------------------------------------------

    def validate(self) -> list[str]:
        errors: list[str] = []

        if not self.prediction_id.strip():
            errors.append("prediction_id must be non-empty")
        if not self.entity_id.strip():
            errors.append("entity_id must be non-empty")

        if self.prediction_time.tzinfo is None:
            errors.append("prediction_time must be timezone-aware")

        if self.label is not None:
            if not (0.0 <= self.label <= 1.0):
                errors.append("label must be in [0, 1]")
            if self.label_time is None:
                errors.append("label_time required when label is set")
            elif self.label_time.tzinfo is None:
                errors.append("label_time must be timezone-aware")
            elif (
                self.prediction_time.tzinfo is not None
                and self.label_time <= self.prediction_time
            ):
                errors.append(
                    f"label_time ({self.label_time.isoformat()}) must be "
                    f"> prediction_time ({self.prediction_time.isoformat()})"
                )

        return errors

    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    # -- serialization --------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["prediction_time"] = self.prediction_time.isoformat()
        d["split"] = self.split.value
        if self.label_time is not None:
            d["label_time"] = self.label_time.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PredictionPoint":
        return cls(
            prediction_id=d["prediction_id"],
            entity_id=d["entity_id"],
            prediction_time=cls._parse_dt(d["prediction_time"]),
            label=d.get("label"),
            label_time=cls._parse_dt(d["label_time"]) if d.get("label_time") else None,
            split=Split(d.get("split", "train")),
            snapshot_id=d.get("snapshot_id"),
            boundary_version=d.get("boundary_version"),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str) -> "PredictionPoint":
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
