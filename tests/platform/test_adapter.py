"""Adapter interface contract tests.

Every dataset adapter MUST implement all abstract methods.
These tests enforce the interface contract using a minimal
concrete adapter.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta
from typing import Any, Generator, Optional

from riskcloud.adapters.base import Adapter
from riskcloud.contracts.event import Event, EventType, EntityType
from riskcloud.contracts.feature_catalog import FeatureCatalogEntry, FeatureStage
from riskcloud.contracts.prediction_point import PredictionPoint, Split

UTC = timezone.utc
NOW = datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC)


# -----------------------------------------------------------------
# Minimal concrete adapter for testing
# -----------------------------------------------------------------

class _MinimalAdapter(Adapter):
    """A minimal adapter for testing the interface contract."""

    def __init__(self):
        pass

    @property
    def dataset_id(self) -> str:
        return "minimal_test"

    @property
    def display_name(self) -> str:
        return "Minimal Test Dataset"

    @property
    def adapter_version(self) -> str:
        return "0.1.0"

    def define_prediction_boundary(self, raw_record):
        return PredictionPoint(
            prediction_id=raw_record["id"],
            entity_id=raw_record["id"],
            prediction_time=datetime(2024, 1, 1, tzinfo=UTC),
            split=Split.TRAIN,
        )

    def prediction_time_column(self) -> str:
        return "application_date"

    def label_column(self) -> Optional[str]:
        return "target"

    def label_time_column(self) -> Optional[str]:
        return "outcome_date"

    def generate_events(self, raw_record, source_system=""):
        yield Event(
            dataset_id=self.dataset_id,
            event_id=Event.compute_event_id(
                self.dataset_id, EntityType.LOAN_APPLICATION,
                raw_record["id"], EventType.LOAN_APPLICATION, NOW,
            ),
            entity_type=EntityType.LOAN_APPLICATION,
            entity_id=raw_record["id"],
            customer_id=f"customer:{raw_record['id']}",
            event_type=EventType.LOAN_APPLICATION,
            event_time=NOW,
            available_at=NOW + timedelta(seconds=1),
            ingested_at=NOW + timedelta(seconds=2),
            source_system=source_system or self.dataset_id,
        )

    def build_feature_catalog(self):
        return [
            FeatureCatalogEntry(
                feature_id="f1",
                feature_name="Feature One",
                entity_type="application",
                feature_group="test",
                source_system="test",
                event_time_rule="application_date",
                availability_rule="application_date <= prediction_time",
                stage=FeatureStage.APPLICATION,
            ),
        ]

    def semantic_group_mapping(self):
        return {"f1": "test_group"}


# -----------------------------------------------------------------
# Tests
# -----------------------------------------------------------------

class TestAdapterInterface:

    @pytest.fixture
    def adapter(self):
        return _MinimalAdapter()

    def test_adapter_instantiates(self, adapter):
        assert adapter is not None
        assert isinstance(adapter, Adapter)

    def test_adapter_cannot_be_instantiated_directly(self):
        """Abstract base class cannot be instantiated."""
        with pytest.raises(TypeError):
            Adapter()  # type: ignore[abstract]

    def test_dataset_id(self, adapter):
        assert adapter.dataset_id == "minimal_test"

    def test_display_name(self, adapter):
        assert len(adapter.display_name) > 0

    def test_adapter_version(self, adapter):
        assert adapter.adapter_version == "0.1.0"

    def test_prediction_time_column(self, adapter):
        assert adapter.prediction_time_column() == "application_date"

    def test_label_column(self, adapter):
        assert adapter.label_column() == "target"

    def test_label_time_column(self, adapter):
        assert adapter.label_time_column() == "outcome_date"

    def test_define_prediction_boundary(self, adapter):
        record = {"id": "SK_001"}
        pp = adapter.define_prediction_boundary(record)
        assert isinstance(pp, PredictionPoint)
        assert pp.entity_id == "SK_001"
        assert pp.prediction_time.tzinfo == UTC

    def test_generate_events(self, adapter):
        record = {"id": "SK_001"}
        events = list(adapter.generate_events(record))
        assert len(events) == 1
        evt = events[0]
        assert isinstance(evt, Event)
        assert evt.dataset_id == "minimal_test"
        assert evt.entity_id == "SK_001"
        assert evt.customer_id == "customer:SK_001"
        assert evt.is_valid()

    def test_generate_events_all_valid(self, adapter):
        """Every generated event must pass its own validation."""
        record = {"id": "SK_001"}
        for evt in adapter.generate_events(record):
            assert evt.is_valid(), f"Invalid event: {evt.validate()}"

    def test_build_feature_catalog(self, adapter):
        catalog = adapter.build_feature_catalog()
        assert len(catalog) == 1
        assert isinstance(catalog[0], FeatureCatalogEntry)
        assert catalog[0].is_valid()

    def test_semantic_group_mapping(self, adapter):
        mapping = adapter.semantic_group_mapping()
        assert isinstance(mapping, dict)
        assert "f1" in mapping
        assert mapping["f1"] == "test_group"

    def test_validate_adapter_passes(self, adapter):
        errors = adapter.validate_adapter()
        assert errors == []

    def test_validate_adapter_catches_duplicate_feature_ids(self):
        """Adapter that returns duplicate feature_ids should produce errors."""

        class DuplicateAdapter(_MinimalAdapter):
            def build_feature_catalog(self):
                return [
                    FeatureCatalogEntry(
                        feature_id="dup",
                        feature_name="A",
                        entity_type="application",
                        feature_group="test",
                        source_system="test",
                        event_time_rule="a",
                        availability_rule="a <= b",
                        stage=FeatureStage.APPLICATION,
                    ),
                    FeatureCatalogEntry(
                        feature_id="dup",
                        feature_name="B",
                        entity_type="application",
                        feature_group="test",
                        source_system="test",
                        event_time_rule="b",
                        availability_rule="b <= c",
                        stage=FeatureStage.APPLICATION,
                    ),
                ]

        adapter = DuplicateAdapter()
        errors = adapter.validate_adapter()
        assert any("duplicate" in e for e in errors)

    def test_validate_adapter_catches_invalid_entries(self):
        """Adapter returning invalid catalog entries should get errors."""

        class BadCatalogAdapter(_MinimalAdapter):
            def build_feature_catalog(self):
                return [
                    FeatureCatalogEntry(
                        feature_id="",
                        feature_name="",
                        entity_type="",
                        feature_group="",
                        source_system="",
                        event_time_rule="",
                        availability_rule="",
                        stage=FeatureStage.APPLICATION,
                    ),
                ]

        adapter = BadCatalogAdapter()
        errors = adapter.validate_adapter()
        assert len(errors) >= 1


# -----------------------------------------------------------------
# Verify the adapter does NOT touch scientific core
# -----------------------------------------------------------------

class TestAdapterIsolation:

    def test_adapter_does_not_import_leakbench_core(self):
        """Adapter base must not depend on src/leakbench."""
        import riskcloud.adapters.base as base_mod
        import inspect, sys

        for name, _obj in inspect.getmembers(base_mod):
            if inspect.ismodule(_obj):
                mod_name = getattr(_obj, "__name__", "")
                assert not mod_name.startswith("leakbench"), (
                    f"Adapter module imports leakbench: {mod_name}"
                )

    def test_contracts_do_not_import_leakbench_core(self):
        """Contracts must not depend on src/leakbench."""
        import importlib, inspect

        contract_modules = [
            "riskcloud.contracts.event",
            "riskcloud.contracts.prediction_point",
            "riskcloud.contracts.feature_catalog",
            "riskcloud.contracts.document",
        ]
        for mod_name in contract_modules:
            mod = importlib.import_module(mod_name)
            for name, _obj in inspect.getmembers(mod):
                if inspect.ismodule(_obj):
                    full_name = getattr(_obj, "__name__", "")
                    assert not full_name.startswith("leakbench"), (
                        f"{mod_name} imports leakbench: {full_name}"
                    )
