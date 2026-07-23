"""P1.1 — Home Credit Boundary tests."""

from __future__ import annotations

from datetime import datetime, timezone
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

    def test_config_rejects_invalid_thresholds(self):
        with pytest.raises(ValueError):
            HomeCreditBoundaryConfig(
                boundary_version="v1",
                prediction_anchor=datetime(2000, 1, 1, tzinfo=UTC),
                label_maturity_days=365,
                split_seed=1,
                split_modulus=100,
                train_upper=90,
                validation_upper=80,  # not increasing
                oot_upper=100,
            )

    def test_config_rejects_non_utc_anchor(self):
        with pytest.raises(ValueError):
            HomeCreditBoundaryConfig(
                boundary_version="v1",
                prediction_anchor=datetime(2000, 1, 1),
                label_maturity_days=365,
                split_seed=1,
                split_modulus=100,
                train_upper=50,
                validation_upper=75,
                oot_upper=100,
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

    def test_assign_split_train(self):
        s = assign_split(5000, HomeCreditBoundaryConfig(
            "v1", datetime(2000, 1, 1, tzinfo=UTC), 365, 1, 10000, 8000, 9000, 10000,
        ))
        assert s == Split.TRAIN

    def test_assign_split_val(self):
        s = assign_split(8500, HomeCreditBoundaryConfig(
            "v1", datetime(2000, 1, 1, tzinfo=UTC), 365, 1, 10000, 8000, 9000, 10000,
        ))
        assert s == Split.VALIDATION

    def test_assign_split_oot(self):
        s = assign_split(9500, HomeCreditBoundaryConfig(
            "v1", datetime(2000, 1, 1, tzinfo=UTC), 365, 1, 10000, 8000, 9000, 10000,
        ))
        assert s == Split.OOT


class TestPredictionPoint:

    def test_valid_point(self, config, snapshot_id):
        pp = build_prediction_point(
            {"SK_ID_CURR": 100001, "TARGET": 0}, snapshot_id, config,
        )
        assert pp.entity_id == "SK_ID_CURR:100001"
        assert pp.label == 0.0
        assert pp.snapshot_id == snapshot_id

    def test_prediction_id_deterministic(self, config, snapshot_id):
        pp1 = build_prediction_point(
            {"SK_ID_CURR": 100001, "TARGET": 0}, snapshot_id, config,
        )
        pp2 = build_prediction_point(
            {"SK_ID_CURR": 100001, "TARGET": 0}, snapshot_id, config,
        )
        assert pp1.prediction_id == pp2.prediction_id

    def test_different_snapshot_different_prediction_id(self, config):
        pp1 = build_prediction_point(
            {"SK_ID_CURR": 100001, "TARGET": 0}, "snap-a", config,
        )
        pp2 = build_prediction_point(
            {"SK_ID_CURR": 100001, "TARGET": 0}, "snap-b", config,
        )
        assert pp1.prediction_id != pp2.prediction_id

    def test_label_time_after_prediction_time(self, config, snapshot_id):
        pp = build_prediction_point(
            {"SK_ID_CURR": 100001, "TARGET": 0}, snapshot_id, config,
        )
        assert pp.label_time > pp.prediction_time

    def test_rejects_missing_target(self, config, snapshot_id):
        with pytest.raises(ValueError, match="TARGET"):
            build_prediction_point({"SK_ID_CURR": 100001}, snapshot_id, config)

    def test_rejects_missing_sk_id_curr(self, config, snapshot_id):
        with pytest.raises(ValueError, match="SK_ID_CURR"):
            build_prediction_point({"TARGET": 0}, snapshot_id, config)

    def test_label_only_0_or_1(self, config, snapshot_id):
        with pytest.raises(ValueError):
            build_prediction_point({"SK_ID_CURR": 1, "TARGET": 0.5}, snapshot_id, config)
