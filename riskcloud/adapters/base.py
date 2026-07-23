"""Dataset Adapter Interface (Section 19 Phase 0).

New datasets must implement this interface without modifying LeakBench Core.
Each adapter provides:
  - field mapping from raw columns to contract fields
  - prediction boundary rules
  - feature catalog generation
  - event generation from raw records
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generator, Optional

from riskcloud.contracts.event import Event, EventType, EntityType
from riskcloud.contracts.feature_catalog import FeatureCatalogEntry
from riskcloud.contracts.prediction_point import PredictionPoint


class Adapter(ABC):
    """Base class for all dataset adapters.

    Subclasses define how a specific dataset maps to the platform contracts.
    Implementations live under platform/adapters/<dataset_name>/.

    LeakBench scientific core MUST NOT be modified by adapter implementations.
    """

    # -- identity ------------------------------------------------------

    @property
    @abstractmethod
    def dataset_id(self) -> str:
        """Stable identifier for this dataset (e.g. 'home_credit')."""

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name (e.g. 'Home Credit Default Risk')."""

    @property
    @abstractmethod
    def adapter_version(self) -> str:
        """Semantic version of this adapter implementation."""

    # -- prediction boundary --------------------------------------------

    @abstractmethod
    def define_prediction_boundary(
        self, raw_record: dict[str, Any]
    ) -> PredictionPoint:
        """Extract a prediction point from a raw record.

        This is where the prediction_time boundary is established.
        Features whose available_at > prediction_time are temporally
        invalid for this record.
        """

    @abstractmethod
    def prediction_time_column(self) -> str:
        """Name of the column/field used to derive prediction_time."""

    @abstractmethod
    def label_column(self) -> Optional[str]:
        """Name of the target column, or None if unsupervised."""

    @abstractmethod
    def label_time_column(self) -> Optional[str]:
        """Column used to derive label_time (must be > prediction_time)."""

    # -- event generation -----------------------------------------------

    @abstractmethod
    def generate_events(
        self, raw_record: dict[str, Any], source_system: str = ""
    ) -> Generator[Event, None, None]:
        """Yield zero or more platform events from one raw record.

        A single raw record (e.g. a loan application row) may produce
        multiple events (e.g., application event + bureau snapshot event).
        The generator design avoids materializing all events at once.
        """

    # -- feature catalog ------------------------------------------------

    @abstractmethod
    def build_feature_catalog(self) -> list[FeatureCatalogEntry]:
        """Return the complete feature catalog for this dataset.

        Each entry specifies stage, availability_rule, entity_type,
        and leakage_risk. This catalog drives both Spark feature
        aggregation and Flink boundary checks.
        """

    # -- semantic groups ------------------------------------------------

    @abstractmethod
    def semantic_group_mapping(self) -> dict[str, str]:
        """Map feature_id -> semantic_group_id for LeakBench governance.

        Semantic groups are the units that LeakBench policies operate on.
        Example: 'bureau.*' -> 'credit_history'.
        """

    # -- helpers --------------------------------------------------------

    def validate_adapter(self) -> list[str]:
        """Run self-checks on the adapter configuration.

        Returns a list of errors (empty = valid).
        """
        errors: list[str] = []
        if not self.dataset_id.strip():
            errors.append("dataset_id is empty")
        if not self.display_name.strip():
            errors.append("display_name is empty")
        if not self.adapter_version.strip():
            errors.append("adapter_version is empty")

        catalog = self.build_feature_catalog()
        feature_ids = {e.feature_id for e in catalog}
        if len(feature_ids) != len(catalog):
            errors.append("build_feature_catalog: duplicate feature_ids found")

        for entry in catalog:
            entry_errors = entry.validate()
            for e in entry_errors:
                errors.append(f"feature '{entry.feature_id}': {e}")

        return errors
