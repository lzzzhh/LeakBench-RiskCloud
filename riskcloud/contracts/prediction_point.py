"""PredictionPoint Contract (Section 6.2) — strict, split-aware validation.

Split-aware rules:
  - TRAIN / VALIDATION / OOT  → snapshot_id + boundary_version required
  - ONLINE                    → boundary_version required, label prohibited
  - label_time > prediction_time (when both present)
  - label required when label_time is set
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from riskcloud.contracts.validation import (
    ContractValidationError,
    FieldError,
    coerce_str_nonempty,
    coerce_float_opt,
    coerce_enum,
    coerce_datetime_utc,
)


class Split(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    OOT = "oot"
    ONLINE = "online"


@dataclass(frozen=True)
class PredictionPoint:
    prediction_id: str
    entity_id: str
    prediction_time: datetime
    split: Split = Split.TRAIN
    snapshot_id: Optional[str] = None
    boundary_version: Optional[str] = None
    label: Optional[float] = None
    label_time: Optional[datetime] = None

    # -- strict entry points -------------------------------------------

    @classmethod
    def parse(cls, d: dict[str, Any]) -> "PredictionPoint":
        errors: list[FieldError] = []
        try:
            pp = cls._from_dict_coerce(d, errors)
        except ContractValidationError:
            raise
        if errors:
            raise ContractValidationError(errors)
        return pp

    @classmethod
    def from_dict_unchecked(cls, d: dict[str, Any]) -> "PredictionPoint":
        errors: list[FieldError] = []
        return cls._from_dict_coerce(d, errors)

    @classmethod
    def _from_dict_coerce(cls, d: dict[str, Any], errors: list[FieldError]) -> "PredictionPoint":
        def get_str(k: str) -> str:
            try:
                return coerce_str_nonempty(d.get(k), k)
            except ContractValidationError as e:
                errors.extend(e.errors)
                return ""

        prediction_id = get_str("prediction_id")
        entity_id = get_str("entity_id")

        try:
            prediction_time = coerce_datetime_utc(d.get("prediction_time"), "prediction_time")
        except ContractValidationError as e:
            errors.extend(e.errors)
            prediction_time = datetime(1970, 1, 1, tzinfo=timezone.utc)

        try:
            split = coerce_enum(d.get("split", "train"), Split, "split")
        except ContractValidationError as e:
            errors.extend(e.errors)
            split = Split.TRAIN

        # Split-aware required fields
        training_splits = {Split.TRAIN, Split.VALIDATION, Split.OOT}

        if split in training_splits:
            if not d.get("snapshot_id"):
                errors.append(FieldError("snapshot_id", f"required for split={split.value}"))
            if not d.get("boundary_version"):
                errors.append(FieldError("boundary_version", f"required for split={split.value}"))

        if split == Split.ONLINE:
            if not d.get("boundary_version"):
                errors.append(FieldError("boundary_version", "required for online split"))
            if d.get("label") is not None:
                errors.append(FieldError("label", "must not be set for online split"))

        snapshot_id = d.get("snapshot_id")
        boundary_version = d.get("boundary_version")

        # Label
        label_value = d.get("label")
        try:
            label = coerce_float_opt(label_value, "label") if label_value is not None else None
        except ContractValidationError as e:
            errors.extend(e.errors)
            label = None

        if label is not None and not (0.0 <= label <= 1.0):
            errors.append(FieldError("label", "must be in [0, 1]", label))

        # label_time
        label_time_value = d.get("label_time")
        label_time: Optional[datetime] = None
        if label_time_value is not None:
            try:
                label_time = coerce_datetime_utc(label_time_value, "label_time")
            except ContractValidationError as e:
                errors.extend(e.errors)

        # label ↔ label_time consistency
        if label is not None and label_time is None:
            errors.append(FieldError("label_time", "required when label is set"))
        if label is None and label_time is not None:
            errors.append(FieldError("label", "required when label_time is set"))

        # Time ordering: label_time > prediction_time
        if label_time is not None and prediction_time.tzinfo is not None and label_time.tzinfo is not None:
            if label_time <= prediction_time:
                errors.append(FieldError(
                    "label_time",
                    f"label_time ({label_time.isoformat()}) must be > prediction_time ({prediction_time.isoformat()})",
                ))

        return cls(
            prediction_id=prediction_id,
            entity_id=entity_id,
            prediction_time=prediction_time,
            split=split,
            snapshot_id=snapshot_id,
            boundary_version=boundary_version,
            label=label,
            label_time=label_time,
        )

    # -- serialization --------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["prediction_time"] = self.prediction_time.isoformat()
        d["split"] = self.split.value
        if self.label_time is not None:
            d["label_time"] = self.label_time.isoformat()
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
