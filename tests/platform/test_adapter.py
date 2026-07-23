"""Adapter interface contract tests with closure validation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from riskcloud.adapters.base import Adapter
from riskcloud.contracts.event import EntityType, Event, EventType, compute_event_id
from riskcloud.contracts.feature_catalog import (
    FeatureCatalogEntry,
)
from riskcloud.contracts.prediction_point import PredictionPoint

REPO_ROOT = Path(__file__).resolve().parents[2]

UTC = timezone.utc
NOW = datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC)


# -----------------------------------------------------------------
# Valid minimal adapter
# -----------------------------------------------------------------

class _ValidAdapter(Adapter):
    @property
    def dataset_id(self) -> str:
        return "minimal_test"

    @property
    def display_name(self) -> str:
        return "Minimal Test Dataset"

    @property
    def adapter_version(self) -> str:
        return "1.0.0"

    def define_prediction_boundary(self, raw_record):
        return PredictionPoint.parse({
            "prediction_id": raw_record["id"],
            "entity_id": raw_record["id"],
            "prediction_time": NOW.isoformat(),
            "split": "train",
            "snapshot_id": "snap-001",
            "boundary_version": "v1.0",
            "label": 1.0,
            "label_time": (NOW + timedelta(days=365)).isoformat(),
        })

    def prediction_time_column(self) -> str:
        return "application_date"

    def label_column(self) -> str | None:
        return "target"

    def label_time_column(self) -> str | None:
        return "outcome_date"

    def generate_events(self, raw_record, source_system=""):
        eid = compute_event_id(
            self.dataset_id, EntityType.LOAN_APPLICATION,
            raw_record["id"], EventType.LOAN_APPLICATION,
            NOW, source_record_id=f"src:{raw_record['id']}",
        )
        yield Event.parse({
            "dataset_id": self.dataset_id,
            "event_id": eid,
            "entity_type": "loan_application",
            "entity_id": raw_record["id"],
            "customer_id": f"customer:{raw_record['id']}",
            "event_type": "loan_application",
            "event_time": NOW.isoformat(),
            "available_at": (NOW + timedelta(seconds=1)).isoformat(),
            "ingested_at": (NOW + timedelta(seconds=2)).isoformat(),
            "source_system": source_system or self.dataset_id,
            "source_record_id": f"src:{raw_record['id']}",
        })

    def build_feature_catalog(self):
        return [
            FeatureCatalogEntry.parse({
                "feature_id": "f1",
                "feature_name": "Feature One",
                "entity_type": "application",
                "feature_group": "test",
                "source_system": "test",
                "event_time_rule": "application_date <= prediction_time",
                "availability_rule": "application_date <= prediction_time",
                "stage": "application",
                "owner": "test_team",
                "leakage_risk": "none",
                "semantic_group_id": "test_group",
                "lineage_expression": "SELECT * FROM x",
            }),
        ]

    def semantic_group_mapping(self):
        return {"f1": "test_group"}


# -----------------------------------------------------------------
# Tests
# -----------------------------------------------------------------

class TestAdapterInterface:

    @pytest.fixture
    def adapter(self):
        return _ValidAdapter()

    def test_adapter_instantiates(self, adapter):
        assert isinstance(adapter, Adapter)

    def test_adapter_cannot_be_instantiated_directly(self):
        with pytest.raises(TypeError):
            Adapter()  # type: ignore[abstract]

    def test_dataset_id(self, adapter):
        assert adapter.dataset_id == "minimal_test"

    def test_display_name(self, adapter):
        assert len(adapter.display_name) > 0

    def test_adapter_version(self, adapter):
        assert adapter.adapter_version == "1.0.0"

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
        assert pp.snapshot_id == "snap-001"

    def test_generate_events(self, adapter):
        record = {"id": "SK_001"}
        events = list(adapter.generate_events(record))
        assert len(events) == 1
        assert events[0].dataset_id == "minimal_test"
        assert events[0].customer_id == "customer:SK_001"

    def test_build_feature_catalog(self, adapter):
        catalog = adapter.build_feature_catalog()
        assert len(catalog) == 1
        assert catalog[0].is_publishable()

    def test_semantic_group_mapping(self, adapter):
        mapping = adapter.semantic_group_mapping()
        assert mapping == {"f1": "test_group"}

    # -- closure validation --

    def test_validate_adapter_passes(self, adapter):
        errors = adapter.validate_adapter()
        assert errors == []

    def test_validate_adapter_detects_empty_catalog(self):
        class EmptyCatalog(_ValidAdapter):
            def build_feature_catalog(self):
                return []

        errors = EmptyCatalog().validate_adapter()
        assert any("feature_catalog" in e.field_path for e in errors)

    def test_validate_adapter_detects_duplicate_feature_ids(self):
        class DuplicateAdapter(_ValidAdapter):
            def build_feature_catalog(self):
                base = _ValidAdapter().build_feature_catalog()[0]
                # Create a second entry with same feature_id
                dup = FeatureCatalogEntry.parse(base.to_dict())
                return [base, dup]

        errors = DuplicateAdapter().validate_adapter()
        assert any("duplicate" in str(e).lower() for e in errors)

    def test_validate_adapter_detects_missing_semantic_mapping(self):
        class MissingMapping(_ValidAdapter):
            def semantic_group_mapping(self):
                return {}  # f1 is not mapped

        errors = MissingMapping().validate_adapter()
        assert any("missing" in str(e).lower() for e in errors)

    def test_validate_adapter_detects_extra_semantic_keys(self):
        class ExtraMapping(_ValidAdapter):
            def semantic_group_mapping(self):
                return {"f1": "test_group", "ghost": "ghost_group"}

        errors = ExtraMapping().validate_adapter()
        assert any("not in catalog" in str(e).lower() for e in errors)

    def test_validate_adapter_detects_label_inconsistency(self):
        class BadLabel(_ValidAdapter):
            def label_column(self):
                return "target"
            def label_time_column(self):
                return None  # should error: label without label_time

        errors = BadLabel().validate_adapter()
        assert any("label_time" in e.field_path.lower() for e in errors)

    def test_validate_adapter_rejects_non_semver(self):
        class BadVersion(_ValidAdapter):
            @property
            def adapter_version(self):
                return "anything"

        errors = BadVersion().validate_adapter()
        assert any("adapter_version" in e.field_path for e in errors)

    def test_validate_adapter_checks_publishable(self):
        class DraftCatalog(_ValidAdapter):
            def build_feature_catalog(self):
                return [
                    FeatureCatalogEntry.parse({
                        "feature_id": "f1",
                        "feature_name": "F1",
                        "entity_type": "app",
                        "feature_group": "g",
                        "source_system": "s",
                        "event_time_rule": "x",
                        "availability_rule": "y",
                        "stage": "application",
                    }),
                ]

        errors = DraftCatalog().validate_adapter()
        assert len(errors) >= 1

    def test_validate_adapter_rejects_empty_prediction_column(self):
        class EmptyPredCol(_ValidAdapter):
            def prediction_time_column(self):
                return ""

        errors = EmptyPredCol().validate_adapter()
        assert any("prediction_time_column" in e.field_path for e in errors)

    def test_validate_adapter_rejects_empty_label_column(self):
        class EmptyLabel(_ValidAdapter):
            def label_column(self):
                return ""

        errors = EmptyLabel().validate_adapter()
        assert any("label_column" in e.field_path for e in errors)

    def test_validate_adapter_rejects_wrong_type_prediction_column(self):
        class WrongType(_ValidAdapter):
            def prediction_time_column(self):
                return 123

        errors = WrongType().validate_adapter()
        assert any("prediction_time_column" in e.field_path for e in errors)

    def test_validate_adapter_rejects_non_string_semantic_values(self):
        class BadMapping(_ValidAdapter):
            def semantic_group_mapping(self):
                return {"f1": 123}

        errors = BadMapping().validate_adapter()
        assert any("semantic_group_mapping" in e.field_path for e in errors)

    def test_validate_adapter_wrong_type_dataset_id(self):
        class BadId(_ValidAdapter):
            @property
            def dataset_id(self):
                return None

        errors = BadId().validate_adapter()
        assert any("dataset_id" in e.field_path for e in errors)

    def test_validate_adapter_wrong_type_version(self):
        class BadVer(_ValidAdapter):
            @property
            def adapter_version(self):
                return None

        errors = BadVer().validate_adapter()
        assert any("adapter_version" in e.field_path for e in errors)

    def test_validate_adapter_rejects_non_feature_catalog_element(self):
        class BadCatalog(_ValidAdapter):
            def build_feature_catalog(self):
                return [object()]

        errors = BadCatalog().validate_adapter()
        assert any("FeatureCatalogEntry" in str(e) for e in errors)

    def test_validate_adapter_mixed_mapping_key_types(self):
        class BadMapping(_ValidAdapter):
            def semantic_group_mapping(self):
                return {"f1": "g1", 1: "invalid"}

        errors = BadMapping().validate_adapter()
        assert any("semantic_group_mapping" in e.field_path for e in errors)

    def test_validate_adapter_empty_mapping_key(self):
        class BadMapping(_ValidAdapter):
            def semantic_group_mapping(self):
                return {"": "g1"}

        errors = BadMapping().validate_adapter()
        assert any("semantic_group_mapping" in e.field_path for e in errors)


# -----------------------------------------------------------------
# Import isolation
# -----------------------------------------------------------------

class TestImportIsolation:

    def test_contracts_dont_import_heavy_deps(self):
        """Contracts must not import pandas/numpy/sklearn/torch/tensorflow, verified via subprocess."""
        import subprocess
        import sys
        code = """
import sys
before = set(sys.modules.keys())
import riskcloud.contracts.event
import riskcloud.contracts.prediction_point
import riskcloud.contracts.feature_catalog
import riskcloud.contracts.document
after = set(sys.modules.keys())
new = after - before
FORBIDDEN = ("pandas", "numpy", "sklearn", "torch", "tensorflow", "requests", "boto3", "pyarrow", "pydantic")
violations = [m for m in new if any(m.startswith(p) for p in FORBIDDEN)]
if violations:
    print("VIOLATIONS:" + ",".join(violations))
    sys.exit(1)
print("CLEAN")
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, timeout=10,
            cwd=str(REPO_ROOT),
        )
        assert result.returncode == 0, f"Import isolation failed: {result.stderr}"
        assert "CLEAN" in result.stdout

    def test_adapter_dont_import_heavy_deps(self):
        """Adapter base must not import heavy frameworks, verified via subprocess."""
        import subprocess
        import sys
        code = """
import sys
before = set(sys.modules.keys())
import riskcloud.adapters.base
after = set(sys.modules.keys())
new = after - before
FORBIDDEN = ("pandas", "numpy", "sklearn", "torch", "tensorflow", "requests", "boto3", "pyarrow", "pydantic")
violations = [m for m in new if any(m.startswith(p) for p in FORBIDDEN)]
if violations:
    print("VIOLATIONS:" + ",".join(violations))
    sys.exit(1)
print("CLEAN")
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, timeout=10,
            cwd=str(REPO_ROOT),
        )
        assert result.returncode == 0, f"Adapter isolation failed: {result.stderr}"
        assert "CLEAN" in result.stdout
