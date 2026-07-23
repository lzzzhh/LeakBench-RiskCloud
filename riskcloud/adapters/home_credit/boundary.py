"""Home Credit Prediction Boundary — deterministic proxy holdout.

Strict fail-closed config:
  - Requires exact UTC anchor (offset zero)
  - Thresholds must be strictly increasing with positive bounds
  - All semantic fields from YAML validated against expected values
  - Python 3.10 compatible Z-suffix handling
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


def _parse_utc_datetime(raw: str) -> datetime:
    """Parse ISO datetime string. Accepts trailing Z. Requires UTC."""
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.utcoffset() is None:
        raise ValueError(f"datetime must have timezone offset: {raw!r}")
    if dt.utcoffset() != timedelta(0):
        raise ValueError(f"datetime must be UTC (offset 0), got {dt.utcoffset()}")
    return dt


def _require_exact(value: Any, expected: Any, field: str) -> None:
    if value != expected:
        raise ValueError(f"{field} must be {expected!r}, got {value!r}")


@dataclass(frozen=True)
class HomeCreditBoundaryConfig:
    boundary_version: str
    boundary_type: str
    prediction_anchor: datetime
    label_maturity_days: int
    split_policy: str
    split_seed: int
    split_modulus: int
    train_upper: int
    validation_upper: int
    oot_upper: int
    oot_is_calendar_time: bool
    application_test_supervised: bool
    available_at_rule: str

    def __post_init__(self):
        if not self.boundary_version.strip():
            raise ValueError("boundary_version must be non-empty")
        if self.boundary_type != "synthetic_proxy":
            raise ValueError(f"boundary_type must be synthetic_proxy, got {self.boundary_type!r}")
        if self.prediction_anchor.tzinfo is None:
            raise ValueError("prediction_anchor must be timezone-aware")
        if self.prediction_anchor.utcoffset() != timedelta(0):
            raise ValueError("prediction_anchor must be UTC (offset 0)")
        if self.label_maturity_days <= 0:
            raise ValueError("label_maturity_days must be positive")

        # Strictly increasing: 0 < train < validation < oot == modulus
        if not isinstance(self.split_seed, int) or isinstance(self.split_seed, bool):
            raise ValueError("split_seed must be int (not bool)")
        if not (0 < self.train_upper < self.validation_upper < self.oot_upper == self.split_modulus):
            raise ValueError(
                f"thresholds must satisfy 0 < train({self.train_upper}) < "
                f"validation({self.validation_upper}) < oot({self.oot_upper}) == "
                f"modulus({self.split_modulus})"
            )
        if self.split_policy != "deterministic_hash_proxy_holdout":
            raise ValueError(f"split_policy must be deterministic_hash_proxy_holdout, got {self.split_policy!r}")
        if self.oot_is_calendar_time:
            raise ValueError("oot_is_calendar_time must be false")
        if self.application_test_supervised:
            raise ValueError("application_test_supervised_evaluation must be false")
        if self.available_at_rule != "application_snapshot_time":
            raise ValueError(f"available_at_rule must be application_snapshot_time, got {self.available_at_rule!r}")

    @classmethod
    def from_yaml(cls, path: Path) -> HomeCreditBoundaryConfig:
        with open(path) as f:
            data = yaml.safe_load(f)
        b = data["boundary"]
        s = data["split"]
        sem = data["semantics"]
        return cls(
            boundary_version=b["boundary_version"],
            boundary_type=b["boundary_type"],
            prediction_anchor=_parse_utc_datetime(b["prediction_anchor_utc"]),
            label_maturity_days=int(b["label_maturity_days"]),
            split_policy=s["policy"],
            split_seed=int(s["seed"]),
            split_modulus=int(s["modulus"]),
            train_upper=int(s["train_upper_exclusive"]),
            validation_upper=int(s["validation_upper_exclusive"]),
            oot_upper=int(s["oot_upper_exclusive"]),
            oot_is_calendar_time=bool(sem["oot_is_calendar_time"]),
            application_test_supervised=bool(sem["application_test_supervised_evaluation"]),
            available_at_rule=sem["available_at_rule"],
        )


def _compute_split(entity_id: str, seed: int, modulus: int) -> int:
    canonical = json.dumps(["home_credit", entity_id, seed], separators=(",", ":"))
    h = hashlib.sha256(canonical.encode()).digest()
    return int.from_bytes(h[:4], "big") % modulus


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
    sk_id_curr = raw_record.get("SK_ID_CURR")
    if sk_id_curr is None:
        raise ValueError("Missing SK_ID_CURR in application record")
    entity_id = f"SK_ID_CURR:{normalize_id(sk_id_curr)}"

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

    pid_parts = json.dumps(
        ["home_credit", entity_id, snapshot_id, config.boundary_version],
        separators=(",", ":"),
    )
    prediction_id = hashlib.sha256(pid_parts.encode()).hexdigest()

    return PredictionPoint.parse({
        "prediction_id": prediction_id,
        "entity_id": entity_id,
        "prediction_time": prediction_time.isoformat(),
        "split": split.value,
        "snapshot_id": snapshot_id,
        "boundary_version": config.boundary_version,
        "label": label,
        "label_time": label_time.isoformat(),
    })
