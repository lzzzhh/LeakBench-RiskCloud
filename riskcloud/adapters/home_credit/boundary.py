"""Home Credit Prediction Boundary — deterministic proxy holdout.

Generates PredictionPoint objects with:
  - Fixed synthetic prediction anchor
  - Configurable label maturity
  - Deterministic hash-based train/validation/proxy-OOT split
  - Rejects unlabeled records (no TARGET column)
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from riskcloud.adapters.home_credit.field_mapping import normalize_id
from riskcloud.contracts.prediction_point import PredictionPoint, Split


@dataclass(frozen=True)
class HomeCreditBoundaryConfig:
    boundary_version: str
    prediction_anchor: datetime
    label_maturity_days: int
    split_seed: int
    split_modulus: int
    train_upper: int
    validation_upper: int
    oot_upper: int

    def __post_init__(self):
        if not self.boundary_version.strip():
            raise ValueError("boundary_version must be non-empty")
        if self.prediction_anchor.tzinfo is None:
            raise ValueError("prediction_anchor must be timezone-aware")
        if self.label_maturity_days <= 0:
            raise ValueError("label_maturity_days must be positive")
        if self.split_modulus <= 0:
            raise ValueError("split_modulus must be positive")
        thresholds = (self.train_upper, self.validation_upper, self.oot_upper)
        if thresholds != tuple(sorted(thresholds)):
            raise ValueError("split thresholds must be strictly increasing")
        if self.oot_upper != self.split_modulus:
            raise ValueError("oot_upper must equal split_modulus")

    @classmethod
    def from_yaml(cls, path: Path) -> HomeCreditBoundaryConfig:
        with open(path) as f:
            data = yaml.safe_load(f)
        b = data["boundary"]
        s = data["split"]
        return cls(
            boundary_version=b["boundary_version"],
            prediction_anchor=datetime.fromisoformat(b["prediction_anchor_utc"]),
            label_maturity_days=b["label_maturity_days"],
            split_seed=s["seed"],
            split_modulus=s["modulus"],
            train_upper=s["train_upper_exclusive"],
            validation_upper=s["validation_upper_exclusive"],
            oot_upper=s["oot_upper_exclusive"],
        )


def _compute_split(
    entity_id: str,
    seed: int,
    modulus: int,
) -> int:
    """Deterministic hash-based bucket."""
    canonical = json.dumps(["home_credit", entity_id, seed], separators=(",", ":"))
    h = hashlib.sha256(canonical.encode()).digest()
    # Take first 4 bytes as unsigned int
    bucket = int.from_bytes(h[:4], "big") % modulus
    return bucket


def assign_split(bucket: int, config: HomeCreditBoundaryConfig) -> Split:
    if bucket < config.train_upper:
        return Split.TRAIN
    elif bucket < config.validation_upper:
        return Split.VALIDATION
    else:
        return Split.OOT


def build_prediction_point(
    raw_record: dict[str, Any],
    snapshot_id: str,
    config: HomeCreditBoundaryConfig,
) -> PredictionPoint:
    """Build a PredictionPoint from an application_train record."""
    sk_id_curr = raw_record.get("SK_ID_CURR")
    if sk_id_curr is None:
        raise ValueError("Missing SK_ID_CURR in application record")
    entity_id = f"SK_ID_CURR:{normalize_id(sk_id_curr)}"

    # Label
    target = raw_record.get("TARGET")
    if target is None:
        raise ValueError(f"Missing TARGET for entity {entity_id}")
    label = float(target)
    if label not in (0.0, 1.0):
        raise ValueError(f"TARGET must be 0 or 1, got {label}")

    prediction_time = config.prediction_anchor
    label_time = prediction_time + timedelta(days=config.label_maturity_days)

    bucket = _compute_split(entity_id, config.split_seed, config.split_modulus)
    split = assign_split(bucket, config)

    # Deterministic prediction_id
    pid_parts = json.dumps(
        ["home_credit", entity_id, snapshot_id, config.boundary_version],
        separators=(",", ":"),
    )
    prediction_id = hashlib.sha256(pid_parts.encode()).hexdigest()

    return PredictionPoint(
        prediction_id=prediction_id,
        entity_id=entity_id,
        prediction_time=prediction_time,
        split=split,
        snapshot_id=snapshot_id,
        boundary_version=config.boundary_version,
        label=label,
        label_time=label_time,
    )
