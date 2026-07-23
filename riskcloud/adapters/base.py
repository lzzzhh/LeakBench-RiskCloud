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

        This method MUST never raise — all errors are returned as FieldError.
        """
        errors: list[FieldError] = []

        _missing = object()

        def _safe_read(field: str, getter) -> object:
            try:
                return getter()
            except Exception as exc:
                errors.append(FieldError(field, f"failed to read: {type(exc).__name__}: {exc}"))
                return _missing

        def _check_nonempty_str(field: str, value: object) -> None:
            if value is _missing:
                return
            if not isinstance(value, str) or not value.strip():
                errors.append(FieldError(field, "must be a non-empty string"))

        # 1. Identity — getter exceptions become FieldError, never propagate
        dataset_id = _safe_read("dataset_id", lambda: self.dataset_id)
        display_name = _safe_read("display_name", lambda: self.display_name)
        adapter_version = _safe_read("adapter_version", lambda: self.adapter_version)

        _check_nonempty_str("dataset_id", dataset_id)
        _check_nonempty_str("display_name", display_name)
        if adapter_version is not _missing:
            if not isinstance(adapter_version, str) or not _SEMVER_RE.fullmatch(adapter_version):
                errors.append(FieldError("adapter_version", f"must be semver (X.Y.Z), got '{adapter_version}'"))

        # 1a. Column contracts — getter exceptions become FieldError
        pcol = _safe_read("prediction_time_column", self.prediction_time_column)
        _check_nonempty_str("prediction_time_column", pcol)

        label_col = _safe_read("label_column", self.label_column)
        label_time_col = _safe_read("label_time_column", self.label_time_column)

        for fname, val in (("label_column", label_col), ("label_time_column", label_time_col)):
            if val is _missing:
                continue  # error already recorded by _safe_read
            if val is None:
                continue  # None is the valid "unsupervised" signal
            if not isinstance(val, str) or not val.strip():
                errors.append(FieldError(fname, "must be None or a non-empty string"))

        # Convert sentinel to None for the consistency check below
        # (if getter failed, treat as absent)
        label_col_ok = label_col if label_col is not _missing else None
        label_time_col_ok = label_time_col if label_time_col is not _missing else None

        # 2. Build clean catalog (filter non-entries)
        raw_catalog: list = []
        try:
            raw_catalog_raw = self.build_feature_catalog()
            if not isinstance(raw_catalog_raw, list):
                errors.append(FieldError("feature_catalog", f"expected list, got {type(raw_catalog_raw).__name__}"))
            else:
                raw_catalog = raw_catalog_raw
        except Exception:
            errors.append(FieldError("feature_catalog", "failed to read catalog"))

        catalog: list[FeatureCatalogEntry] = []
        for i, entry in enumerate(raw_catalog):
            if not isinstance(entry, FeatureCatalogEntry):
                msg = f"expected FeatureCatalogEntry, got {type(entry).__name__}"
                errors.append(FieldError(f"feature_catalog[{i}]", msg))
                continue
            # Validate internal fields needed for closure ops
            fid = entry.feature_id
            if not isinstance(fid, str) or not fid.strip():
                errors.append(FieldError(
                    f"feature_catalog[{i}].feature_id",
                    f"must be a non-empty str, got {type(fid).__name__}",
                ))
                continue
            sgid = entry.semantic_group_id
            if sgid is not None and not isinstance(sgid, str):
                errors.append(FieldError(
                    f"feature_catalog[{i}].semantic_group_id",
                    f"must be None or str, got {type(sgid).__name__}",
                ))
                continue
            catalog.append(entry)

        if len(catalog) == 0:
            errors.append(FieldError("feature_catalog", "must contain at least one valid entry"))

        feature_ids = [e.feature_id for e in catalog]
        if len(set(feature_ids)) != len(feature_ids):
            seen: dict[str, int] = {}
            for fid in feature_ids:
                seen[fid] = seen.get(fid, 0) + 1
            dupes = [fid for fid, count in seen.items() if count > 1]
            errors.append(FieldError("feature_catalog", f"duplicate feature_ids: {dupes}"))

        # 3. Build clean mapping (only str→str pairs)
        raw_mapping: dict = {}
        try:
            raw_mapping = self.semantic_group_mapping()
            if not isinstance(raw_mapping, dict):
                errors.append(FieldError("semantic_group_mapping", f"expected dict, got {type(raw_mapping).__name__}"))
                raw_mapping = {}
        except Exception:
            errors.append(FieldError("semantic_group_mapping", "failed to read mapping"))

        clean_mapping: dict[str, str] = {}
        for k, v in raw_mapping.items():
            if not isinstance(k, str) or not k.strip():
                msg = f"key must be non-empty str, got {type(k).__name__}: {k}"
                errors.append(FieldError("semantic_group_mapping", msg))
                continue
            if not isinstance(v, str) or not v.strip():
                msg = f"value for '{k}' must be non-empty str, got {type(v).__name__}"
                errors.append(FieldError("semantic_group_mapping", msg))
                continue
            clean_mapping[k] = v

        # 4. Catalog ↔ clean mapping closure
        catalog_ids = set(feature_ids)
        mapping_ids = set(clean_mapping.keys())

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

        # 5. Catalog semantic_group_id must match mapping value
        catalog_map = {e.feature_id: e.semantic_group_id for e in catalog}
        for fid, mapped_group in clean_mapping.items():
            catalog_group = catalog_map.get(fid)
            if catalog_group is not None and catalog_group.strip():
                if catalog_group != mapped_group:
                    errors.append(FieldError(
                        f"feature_catalog.{fid}",
                        f"semantic_group_id '{catalog_group}' != mapping value '{mapped_group}'",
                    ))

        # 6. Label column ↔ label_time column consistency
        has_label = label_col_ok is not None
        has_label_time = label_time_col_ok is not None
        if has_label and not has_label_time:
            errors.append(FieldError("label_time_column", "must be set when label_column is set"))
        if has_label_time and not has_label:
            errors.append(FieldError("label_column", "must be set when label_time_column is set"))

        # 7. Publishable check for every valid catalog entry
        for entry in catalog:
            try:
                pub_errors = entry.publishable_errors()
                errors.extend(pub_errors)
            except Exception:
                errors.append(FieldError(entry.feature_id, "failed to check publishable"))

        return errors
