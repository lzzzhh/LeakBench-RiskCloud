"""PredictionPoint Contract (Section 6.2) — strict, split-aware validation.

Split-aware rules:
  - TRAIN / VALIDATION / OOT  → snapshot_id + boundary_version (non-empty str)
  - ONLINE                    → boundary_version (non-empty str), label prohibited
  - label_time > prediction_time (when both present)
  - label ↔ label_time symmetry
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from riskcloud.contracts.validation import (
    ContractValidationError,
    FieldError,
    coerce_datetime_utc,
    coerce_enum,
    coerce_float_opt,
    coerce_str_nonempty,
    coerce_str_nonempty_opt,
)


class Split(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    OOT = "oot"
    ONLINE = "online"


_TRAINING_SPLITS = {Split.TRAIN, Split.VALIDATION, Split.OOT}


@dataclass(frozen=True)
class PredictionPoint:
    prediction_id: str
    entity_id: str
    prediction_time: datetime
    split: Split = Split.TRAIN
    snapshot_id: str | None = None
    boundary_version: str | None = None
    label: float | None = None
    label_time: datetime | None = None

    # -- strict entry points -------------------------------------------

    @classmethod
    def parse(cls, d: dict[str, Any]) -> PredictionPoint:
        errors: list[FieldError] = []
        try:
            pp = cls._from_dict_coerce(d, errors)
        except ContractValidationError:
            raise
        if errors:
            raise ContractValidationError(errors)
        return pp

    @classmethod
    def from_dict_unchecked(cls, d: dict[str, Any]) -> PredictionPoint:
        errors: list[FieldError] = []
        return cls._from_dict_coerce(d, errors)

    @classmethod
    def _from_dict_coerce(cls, d: dict[str, Any], errors: list[FieldError]) -> PredictionPoint:
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

        prediction_id = _str("prediction_id")
        entity_id = _str("entity_id")

        prediction_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        try:
            prediction_time = coerce_datetime_utc(d.get("prediction_time"), "prediction_time")
        except ContractValidationError as e:
            errors.extend(e.errors)

        split = Split.TRAIN
        try:
            split = coerce_enum(d.get("split", "train"), Split, "split")
        except ContractValidationError as e:
            errors.extend(e.errors)

        # Split-aware lineage — type-checked, non-empty
        snapshot_id = _str_opt("snapshot_id")
        boundary_version = _str_opt("boundary_version")

        if split in _TRAINING_SPLITS:
            if not snapshot_id:
                errors.append(FieldError("snapshot_id", f"required for split={split.value}"))
            if not boundary_version:
                errors.append(FieldError("boundary_version", f"required for split={split.value}"))

        if split == Split.ONLINE:
            if not boundary_version:
                errors.append(FieldError("boundary_version", "required for online split"))
            if d.get("label") is not None or d.get("label_time") is not None:
                errors.append(FieldError("label", "must not be set for online split"))

        # Label
        label_value = d.get("label")
        label = None
        if label_value is not None:
            try:
                label = coerce_float_opt(label_value, "label")
            except ContractValidationError as e:
                errors.extend(e.errors)
            if label is not None and not (0.0 <= label <= 1.0):
                errors.append(FieldError("label", "must be in [0, 1]", label))

        # label_time
        label_time = None
        lt_raw = d.get("label_time")
        if lt_raw is not None:
            try:
                label_time = coerce_datetime_utc(lt_raw, "label_time")
            except ContractValidationError as e:
                errors.extend(e.errors)

        # label ↔ label_time symmetry
        if label is not None and label_time is None:
            errors.append(FieldError("label_time", "required when label is set"))
        if label is None and label_time is not None:
            errors.append(FieldError("label", "required when label_time is set"))

        # Time ordering
        if label_time is not None and prediction_time.tzinfo is not None:
            if label_time <= prediction_time:
                errors.append(FieldError(
                    "label_time",
                    "label_time must be > prediction_time",
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

    def to_dict(self) -> dict[str, Any]:
        d = {
            "prediction_id": self.prediction_id,
            "entity_id": self.entity_id,
            "prediction_time": self.prediction_time.isoformat(),
            "split": self.split.value,
            "snapshot_id": self.snapshot_id,
            "boundary_version": self.boundary_version,
            "label": self.label,
            "label_time": self.label_time.isoformat() if self.label_time else None,
        }
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
