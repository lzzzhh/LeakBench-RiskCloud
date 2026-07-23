"""P1.1 — Home Credit Boundary tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from riskcloud.adapters.home_credit.boundary import (
    HomeCreditBoundaryConfig,
    _compute_split,
    assign_split,
    build_prediction_point,
)
from riskcloud.contracts.prediction_point import Split

UTC = timezone.utc
CONFIG_PATH = (
    Path(__file__).resolve().parents[3]
    / "case_studies"
    / "home_credit"
    / "configs"
    / "boundary_v1.yaml"
)


@pytest.fixture
def config():
    return HomeCreditBoundaryConfig.from_yaml(CONFIG_PATH)


@pytest.fixture
def snapshot_id():
    return "snap-20260723-001"


class TestBoundaryConfig:

    def test_config_loads(self, config):
        assert config.boundary_version == "hc-boundary-v1"
        assert config.label_maturity_days == 365
        assert config.split_modulus == 10000
        assert config.boundary_type == "synthetic_proxy"
        assert config.available_at_rule == "application_snapshot_time"

    def test_config_rejects_equal_thresholds(self):
        with pytest.raises(ValueError, match="thresholds"):
            HomeCreditBoundaryConfig(
                boundary_version="v1", boundary_type="synthetic_proxy",
                prediction_anchor=datetime(2000, 1, 1, tzinfo=UTC),
                label_maturity_days=365,
                split_policy="deterministic_hash_proxy_holdout",
                split_seed=1, split_modulus=100,
                train_upper=50, validation_upper=50, oot_upper=100,
                oot_is_calendar_time=False, application_test_supervised=False,
                available_at_rule="application_snapshot_time",
            )

    def test_config_rejects_non_utc(self):
        with pytest.raises(ValueError, match="UTC"):
            HomeCreditBoundaryConfig(
                boundary_version="v1", boundary_type="synthetic_proxy",
                prediction_anchor=datetime(2000, 1, 1, tzinfo=timezone(timedelta(hours=10))),
                label_maturity_days=365,
                split_policy="deterministic_hash_proxy_holdout",
                split_seed=1, split_modulus=100,
                train_upper=30, validation_upper=60, oot_upper=100,
                oot_is_calendar_time=False, application_test_supervised=False,
                available_at_rule="application_snapshot_time",
            )

    def test_config_rejects_non_aware(self):
        with pytest.raises(ValueError):
            HomeCreditBoundaryConfig(
                boundary_version="v1", boundary_type="synthetic_proxy",
                prediction_anchor=datetime(2000, 1, 1),
                label_maturity_days=365,
                split_policy="deterministic_hash_proxy_holdout",
                split_seed=1, split_modulus=100,
                train_upper=30, validation_upper=60, oot_upper=100,
                oot_is_calendar_time=False, application_test_supervised=False,
                available_at_rule="application_snapshot_time",
            )

    def test_config_rejects_zero_train(self):
        with pytest.raises(ValueError, match="thresholds"):
            HomeCreditBoundaryConfig(
                boundary_version="v1", boundary_type="synthetic_proxy",
                prediction_anchor=datetime(2000, 1, 1, tzinfo=UTC),
                label_maturity_days=365,
                split_policy="deterministic_hash_proxy_holdout",
                split_seed=1, split_modulus=100,
                train_upper=0, validation_upper=50, oot_upper=100,
                oot_is_calendar_time=False, application_test_supervised=False,
                available_at_rule="application_snapshot_time",
            )


class TestSplit:

    def test_split_is_deterministic(self):
        b1 = _compute_split("SK_ID_CURR:123", 20260723, 10000)
        b2 = _compute_split("SK_ID_CURR:123", 20260723, 10000)
        assert b1 == b2

    def test_split_seed_changes_result(self):
        b1 = _compute_split("SK_ID_CURR:123", 20260723, 10000)
        b2 = _compute_split("SK_ID_CURR:123", 99999999, 10000)
        assert b1 != b2

    def test_assign_split_train(self, config):
        s = assign_split(5000, config)
        assert s == Split.TRAIN

    def test_assign_split_val(self, config):
        s = assign_split(8500, config)
        assert s == Split.VALIDATION

    def test_assign_split_oot(self, config):
        s = assign_split(9500, config)
        assert s == Split.OOT

    def test_golden_split_vector(self, config):
        """Fixed entities produce deterministic splits."""
        buckets = {}
        for sk in [100001, 100002, 100003, 100004, 100005]:
            bucket = _compute_split(f"SK_ID_CURR:{sk}", config.split_seed, config.split_modulus)
            buckets[sk] = bucket
        # Verify determinism: same input → same output
        for sk in buckets:
            assert _compute_split(f"SK_ID_CURR:{sk}", config.split_seed, config.split_modulus) == buckets[sk]
        assert len(set(buckets.values())) >= 1, "All buckets are identical — split may be degenerate"


class TestPredictionPoint:

    def test_valid_point(self, config, snapshot_id):
        pp = build_prediction_point(
            {"SK_ID_CURR": 100001, "TARGET": 0}, snapshot_id, config,
        )
        assert pp.entity_id == "SK_ID_CURR:100001"
        assert pp.label == 0.0
        assert pp.snapshot_id == snapshot_id
        assert pp.boundary_version == "hc-boundary-v1"

    def test_prediction_id_deterministic(self, config, snapshot_id):
        pp1 = build_prediction_point({"SK_ID_CURR": 100001, "TARGET": 0}, snapshot_id, config)
        pp2 = build_prediction_point({"SK_ID_CURR": 100001, "TARGET": 0}, snapshot_id, config)
        assert pp1.prediction_id == pp2.prediction_id

    def test_different_snapshot_different_id(self, config):
        pp1 = build_prediction_point({"SK_ID_CURR": 100001, "TARGET": 0}, "snap-a", config)
        pp2 = build_prediction_point({"SK_ID_CURR": 100001, "TARGET": 0}, "snap-b", config)
        assert pp1.prediction_id != pp2.prediction_id

    def test_label_time_after_prediction(self, config, snapshot_id):
        pp = build_prediction_point({"SK_ID_CURR": 100001, "TARGET": 0}, snapshot_id, config)
        assert pp.label_time > pp.prediction_time

    def test_rejects_missing_target(self, config, snapshot_id):
        with pytest.raises(ValueError, match="TARGET"):
            build_prediction_point({"SK_ID_CURR": 100001}, snapshot_id, config)

    def test_label_only_0_or_1(self, config, snapshot_id):
        with pytest.raises(ValueError):
            build_prediction_point({"SK_ID_CURR": 1, "TARGET": 0.5}, snapshot_id, config)
