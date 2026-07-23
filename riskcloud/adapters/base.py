"""Dataset Adapter Interface (Section 19 Phase 0) — with closure validation.

Each adapter provides:
  - field mapping from raw columns to contract fields
  - prediction boundary rules
  - feature catalog generation
  - event generation from raw records
  - validate_adapter() closure check (catalog ↔ semantic mapping consistency)
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Generator
from typing import Any

from riskcloud.contracts.event import Event
from riskcloud.contracts.feature_catalog import FeatureCatalogEntry
from riskcloud.contracts.prediction_point import PredictionPoint
from riskcloud.contracts.validation import FieldError

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


class Adapter(ABC):
    """Base class for all dataset adapters.

    Implementations live under riskcloud/adapters/<dataset_name>/.
    LeakBench scientific core MUST NOT be modified by adapter implementations.
    """

    # -- identity (abstract properties) ----------------------------------

    @property
    @abstractmethod
    def dataset_id(self) -> str:
        """Stable identifier (e.g. 'home_credit')."""

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name."""

    @property
    @abstractmethod
    def adapter_version(self) -> str:
        """Semantic version (e.g. '1.0.0')."""

    # -- prediction boundary (abstract methods) --------------------------

    @abstractmethod
    def define_prediction_boundary(self, raw_record: dict[str, Any]) -> PredictionPoint:
        """Extract a prediction point from a raw record."""

    @abstractmethod
    def prediction_time_column(self) -> str:
        """Column used to derive prediction_time."""

    @abstractmethod
    def label_column(self) -> str | None:
        """Target column name, or None."""

    @abstractmethod
    def label_time_column(self) -> str | None:
        """Column used to derive label_time (> prediction_time)."""

    # -- event generation (abstract method) ------------------------------

    @abstractmethod
    def generate_events(
        self, raw_record: dict[str, Any], source_system: str = ""
    ) -> Generator[Event, None, None]:
        """Yield platform events from one raw record."""

    # -- feature catalog (abstract method) -------------------------------

    @abstractmethod
    def build_feature_catalog(self) -> list[FeatureCatalogEntry]:
        """Return the complete feature catalog."""

    # -- semantic groups (abstract method) -------------------------------

    @abstractmethod
    def semantic_group_mapping(self) -> dict[str, str]:
        """Map feature_id → semantic_group_id for LeakBench governance."""

    # -- closure validation ----------------------------------------------

    def validate_adapter(self) -> list[FieldError]:
        """Run closure checks on the adapter configuration.

        Checks performed:
          1. Basic identity fields (dataset_id, display_name, version format)
          2. Catalog non-empty
          3. Catalog feature_ids match semantic_group_mapping keys
          4. Catalog semantic_group_id matches mapping value (if both set)
          5. Label column ↔ label_time column consistency
          6. Each publishable catalog entry passes the publishable check
        """
        errors: list[FieldError] = []

        # 1. Identity
        if not self.dataset_id.strip():
            errors.append(FieldError("dataset_id", "must be non-empty"))
        if not self.display_name.strip():
            errors.append(FieldError("display_name", "must be non-empty"))
        if not _SEMVER_RE.fullmatch(self.adapter_version):
            errors.append(FieldError("adapter_version", f"must be semver (X.Y.Z), got '{self.adapter_version}'"))

        # 1a. Column contracts — must be non-empty strings
        prediction_col = self.prediction_time_column()
        if not isinstance(prediction_col, str) or not prediction_col.strip():
            errors.append(FieldError("prediction_time_column", "must be a non-empty string"))

        label_col = self.label_column()
        label_time_col = self.label_time_column()
        for field_name, value in (("label_column", label_col), ("label_time_column", label_time_col)):
            if value is not None:
                if not isinstance(value, str) or not value.strip():
                    errors.append(FieldError(field_name, "must be None or a non-empty string"))

        # 1b. Guard mapping and catalog types
        mapping = self.semantic_group_mapping()
        if not isinstance(mapping, dict):
            errors.append(FieldError("semantic_group_mapping", f"expected dict, got {type(mapping).__name__}"))
            mapping = {}
        for k, v in mapping.items():
            if not isinstance(k, str) or not isinstance(v, str):
                errors.append(FieldError("semantic_group_mapping", "keys and values must be str"))

        catalog = self.build_feature_catalog()
        if not isinstance(catalog, list):
            errors.append(FieldError("feature_catalog", f"expected list, got {type(catalog).__name__}"))
            catalog = []
        # 2. Catalog non-empty
        if len(catalog) == 0:
            errors.append(FieldError("feature_catalog", "must contain at least one entry"))

        # Check for duplicate feature_ids
        feature_ids = [e.feature_id for e in catalog]
        if len(set(feature_ids)) != len(feature_ids):
            seen: dict[str, int] = {}
            for fid in feature_ids:
                seen[fid] = seen.get(fid, 0) + 1
            dupes = [fid for fid, count in seen.items() if count > 1]
            errors.append(FieldError("feature_catalog", f"duplicate feature_ids: {dupes}"))

        # 3. Catalog ↔ semantic mapping closure (mapping already loaded above)
        catalog_ids = set(feature_ids)
        mapping_ids = set(mapping.keys())

        only_in_catalog = catalog_ids - mapping_ids
        only_in_mapping = mapping_ids - catalog_ids

        if only_in_catalog:
            errors.append(FieldError(
                "semantic_group_mapping",
                f"missing mappings for feature_ids: {sorted(only_in_catalog)}",
            ))
        if only_in_mapping:
            errors.append(FieldError(
                "semantic_group_mapping",
                f"mapping contains feature_ids not in catalog: {sorted(only_in_mapping)}",
            ))

        # 4. Catalog semantic_group_id must match mapping value
        catalog_map = {e.feature_id: e.semantic_group_id for e in catalog}
        for fid, mapped_group in mapping.items():
            catalog_group = catalog_map.get(fid)
            if catalog_group is not None and catalog_group.strip():
                if catalog_group != mapped_group:
                    errors.append(FieldError(
                        f"feature_catalog.{fid}",
                        f"semantic_group_id '{catalog_group}' != mapping value '{mapped_group}'",
                    ))

        # 5. Label column ↔ label_time column consistency (variables from step 1a)
        has_label = label_col is not None
        has_label_time = label_time_col is not None
        if has_label and not has_label_time:
            errors.append(FieldError(
                "label_time_column",
                "must be set when label_column is set",
            ))
        if has_label_time and not has_label:
            errors.append(FieldError(
                "label_column",
                "must be set when label_time_column is set",
            ))

        # 6. Publishable check for every catalog entry
        for entry in catalog:
            pub_errors = entry.publishable_errors()
            for pe in pub_errors:
                errors.append(pe)

        return errors
