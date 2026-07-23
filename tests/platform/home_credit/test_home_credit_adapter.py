"""P1.1 — Home Credit Adapter closure tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from riskcloud.adapters.home_credit.adapter import HomeCreditAdapter
from riskcloud.adapters.home_credit.boundary import HomeCreditBoundaryConfig
from riskcloud.contracts.validation import ContractValidationError

UTC = timezone.utc
CONFIG_PATH = (
    Path(__file__).resolve().parents[3]
    / "case_studies"
    / "home_credit"
    / "configs"
    / "boundary_v1.yaml"
)
MANIFEST_SHA = "a" * 64
SNAPSHOT_ID = "snap-001"
INGESTED = datetime(2026, 7, 23, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def config():
    return HomeCreditBoundaryConfig.from_yaml(CONFIG_PATH)


@pytest.fixture
def adapter(config):
    return HomeCreditAdapter(SNAPSHOT_ID, MANIFEST_SHA, INGESTED, config)


class TestAdapterClosure:

    def test_validate_adapter_passes(self, adapter):
        errors = adapter.validate_adapter()
        assert errors == [], errors

    def test_dataset_id(self, adapter):
        assert adapter.dataset_id == "home_credit"

    def test_display_name(self, adapter):
        assert "Home Credit" in adapter.display_name

    def test_adapter_version_semver(self, adapter):
        assert adapter.adapter_version == "1.0.0"

    def test_prediction_time_column_is_proxy(self, adapter):
        col = adapter.prediction_time_column()
        assert col == "__proxy_application_time__"

    def test_label_column_is_target(self, adapter):
        assert adapter.label_column() == "TARGET"

    def test_label_time_column_is_proxy(self, adapter):
        assert adapter.label_time_column() == "__proxy_label_time__"

    def test_rejects_empty_snapshot_id(self, config):
        with pytest.raises(ContractValidationError):
            HomeCreditAdapter("", MANIFEST_SHA, INGESTED, config)

    def test_rejects_bad_manifest_sha(self, config):
        with pytest.raises(ContractValidationError):
            HomeCreditAdapter(SNAPSHOT_ID, "bad-sha", INGESTED, config)

    def test_rejects_naive_ingested_at(self, config):
        with pytest.raises(ContractValidationError):
            HomeCreditAdapter(SNAPSHOT_ID, MANIFEST_SHA, datetime(2026, 1, 1), config)

    def test_define_prediction_boundary(self, adapter):
        pp = adapter.define_prediction_boundary({"SK_ID_CURR": 100001, "TARGET": 0})
        assert pp.entity_id == "SK_ID_CURR:100001"
        assert pp.snapshot_id == SNAPSHOT_ID
        assert pp.boundary_version == "hc-boundary-v1"

    def test_all_abstract_members_implemented(self, adapter):
        for attr in ["dataset_id", "display_name", "adapter_version"]:
            getattr(adapter, attr)
        for method in ["define_prediction_boundary", "prediction_time_column",
                       "label_column", "label_time_column", "generate_events",
                       "build_feature_catalog", "semantic_group_mapping"]:
            getattr(adapter, method)
