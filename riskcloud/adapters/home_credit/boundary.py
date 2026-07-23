"""Home Credit Prediction Boundary — deterministic proxy holdout.

Strict fail-closed:
  - No silent int()/bool()/float() coercion on config or label values
  - Exact UTC anchor (offset zero)
  - Golden split vectors frozen in tests
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
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.utcoffset() is None:
        raise ValueError(f"datetime must have timezone offset: {raw!r}")
    if dt.utcoffset() != timedelta(0):
        raise ValueError(f"datetime must be UTC (offset 0), got {dt.utcoffset()}")
    return dt


def _require_type(value: object, expected: type, field: str) -> None:
    if not isinstance(value, expected):
        raise ValueError(f"{field} must be {expected.__name__}, got {type(value).__name__}: {value!r}")
    if expected is int and isinstance(value, bool):
        raise ValueError(f"{field} must be int, not bool")


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
        _require_exact(self.boundary_type, "synthetic_proxy", "boundary_type")
        if self.prediction_anchor.tzinfo is None:
            raise ValueError("prediction_anchor must be timezone-aware")
        if self.prediction_anchor.utcoffset() != timedelta(0):
            raise ValueError("prediction_anchor must be UTC (offset 0)")
        _require_type(self.label_maturity_days, int, "label_maturity_days")
        if self.label_maturity_days <= 0:
            raise ValueError("label_maturity_days must be positive")
        _require_exact(self.split_policy, "deterministic_hash_proxy_holdout", "split_policy")
        _require_type(self.split_seed, int, "split_seed")
        _require_type(self.split_modulus, int, "split_modulus")
        _require_type(self.train_upper, int, "train_upper")
        _require_type(self.validation_upper, int, "validation_upper")
        _require_type(self.oot_upper, int, "oot_upper")
        if not (0 < self.train_upper < self.validation_upper < self.oot_upper == self.split_modulus):
            raise ValueError(
                f"thresholds must satisfy 0 < train({self.train_upper}) < "
                f"validation({self.validation_upper}) < oot({self.oot_upper}) == "
                f"modulus({self.split_modulus})"
            )
        _require_type(self.oot_is_calendar_time, bool, "oot_is_calendar_time")
        if self.oot_is_calendar_time:
            raise ValueError("oot_is_calendar_time must be False")
        _require_type(self.application_test_supervised, bool, "application_test_supervised")
        if self.application_test_supervised:
            raise ValueError("application_test_supervised_evaluation must be False")
        _require_exact(self.available_at_rule, "application_snapshot_time", "available_at_rule")

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
            label_maturity_days=b["label_maturity_days"],
            split_policy=s["policy"],
            split_seed=s["seed"],
            split_modulus=s["modulus"],
            train_upper=s["train_upper_exclusive"],
            validation_upper=s["validation_upper_exclusive"],
            oot_upper=s["oot_upper_exclusive"],
            oot_is_calendar_time=sem["oot_is_calendar_time"],
            application_test_supervised=sem["application_test_supervised_evaluation"],
            available_at_rule=sem["available_at_rule"],
        )


def _compute_split(entity_id: str, seed: int, modulus: int) -> int:
    canonical = json.dumps(["home_credit", entity_id, seed], separators=(",", ":"))
    h = hashlib.sha256(canonical.encode()).digest()
    return int.from_bytes(h[:4], "big") % modulus


def assign_split(bucket: int, config: HomeCreditBoundaryConfig) -> Split:
    if not (0 <= bucket < config.split_modulus):
        raise ValueError(f"bucket {bucket} out of range [0, {config.split_modulus})")
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
        raise ValueError("Missing SK_ID_CURR")
    entity_id = f"SK_ID_CURR:{normalize_id(sk_id_curr)}"

    target = raw_record.get("TARGET")
    if target is None:
        raise ValueError(f"Missing TARGET for {entity_id}")

    # Only accept 0, 1, "0", "1" — reject bool
    if isinstance(target, bool):
        raise ValueError(f"TARGET must not be bool, got {target!r}")
    if target in (0, 1):
        label = float(target)
    elif isinstance(target, str) and target.strip() in ("0", "1"):
        label = float(target.strip())
    else:
        raise ValueError(f"TARGET must be 0, 1, '0', or '1', got {target!r}")

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
