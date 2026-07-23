"""Home Credit Dataset Adapter — P1.1.

Implements the abstract Adapter interface for the Home Credit Default Risk
dataset. Covers the first vertical slice: application_train, bureau, bureau_balance.
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta
from typing import Any

from riskcloud.adapters.base import Adapter
from riskcloud.adapters.home_credit.boundary import (
    HomeCreditBoundaryConfig,
    build_prediction_point,
)
from riskcloud.adapters.home_credit.feature_catalog import (
    get_features,
    get_semantic_group_mapping,
)
from riskcloud.adapters.home_credit.field_mapping import (
    APPLICATION_TABLE,
    BUREAU_BALANCE_TABLE,
    BUREAU_TABLE,
    SOURCE_TABLE_FIELD,
    application_id,
    customer_id,
    normalize_id,
)
from riskcloud.contracts.event import EntityType, Event, EventType, compute_event_id
from riskcloud.contracts.feature_catalog import FeatureCatalogEntry
from riskcloud.contracts.prediction_point import PredictionPoint
from riskcloud.contracts.validation import ContractValidationError, FieldError


def _validate_sha256(value: str, field_name: str) -> str:
    from riskcloud.contracts.validation import validate_sha256_hex

    return validate_sha256_hex(value, field_name)


class HomeCreditAdapter(Adapter):
    """Adapter for the Home Credit Default Risk dataset.

    Constructor enforces: snapshot_id, manifest SHA, UTC ingested_at,
    and valid boundary config.
    """

    def __init__(
        self,
        snapshot_id: str,
        source_manifest_sha256: str,
        ingested_at: datetime,
        boundary_config: HomeCreditBoundaryConfig,
    ):
        if not snapshot_id.strip():
            raise ContractValidationError([
                FieldError("snapshot_id", "must be non-empty"),
            ])
        _validate_sha256(source_manifest_sha256, "source_manifest_sha256")
        if ingested_at.tzinfo is None:
            raise ContractValidationError([
                FieldError("ingested_at", "must be timezone-aware"),
            ])

        self._snapshot_id = snapshot_id
        self._manifest_sha = source_manifest_sha256.lower()
        self._ingested_at = ingested_at
        self._boundary = boundary_config

    # -- identity ------------------------------------------------------

    @property
    def dataset_id(self) -> str:
        return "home_credit"

    @property
    def display_name(self) -> str:
        return "Home Credit Default Risk"

    @property
    def adapter_version(self) -> str:
        return "1.0.0"

    # -- prediction boundary --------------------------------------------

    def define_prediction_boundary(self, raw_record: dict[str, Any]) -> PredictionPoint:
        """Build a PredictionPoint from an application_train record."""
        return build_prediction_point(raw_record, self._snapshot_id, self._boundary)

    def prediction_time_column(self) -> str:
        return "__proxy_application_time__"

    def label_column(self) -> str | None:
        return "TARGET"

    def label_time_column(self) -> str | None:
        return "__proxy_label_time__"

    # -- event generation -----------------------------------------------

    def generate_events(
        self, raw_record: dict[str, Any], source_system: str = ""
    ) -> Generator[Event, None, None]:
        """Yield events based on __source_table__."""
        table = raw_record.get(SOURCE_TABLE_FIELD)
        if table is None:
            raise ContractValidationError([
                FieldError(SOURCE_TABLE_FIELD, "record must have __source_table__"),
            ])
        source = source_system or "home_credit_adapter"

        if table == APPLICATION_TABLE:
            yield from self._application_event(raw_record, source)
        elif table == BUREAU_TABLE:
            yield from self._bureau_event(raw_record, source)
        elif table == BUREAU_BALANCE_TABLE:
            yield self._bureau_balance_event(raw_record, source)
        else:
            raise ContractValidationError([
                FieldError(SOURCE_TABLE_FIELD, f"unknown table: {table}"),
            ])

    def _application_event(
        self, record: dict[str, Any], source: str
    ) -> Generator[Event, None, None]:
        sk = record.get("SK_ID_CURR")
        if sk is None:
            raise ContractValidationError([
                FieldError("SK_ID_CURR", "missing in application record"),
            ])
        eid = application_id(sk)
        cid = customer_id(sk)
        pt = self._boundary.prediction_anchor

        event_id = compute_event_id(
            self.dataset_id, EntityType.LOAN_APPLICATION, eid,
            EventType.LOAN_APPLICATION, pt,
            source_record_id=f"{APPLICATION_TABLE}:{normalize_id(sk)}",
            source_record_revision=self._manifest_sha,
        )
        yield Event(
            dataset_id=self.dataset_id,
            event_id=event_id,
            entity_type=EntityType.LOAN_APPLICATION,
            entity_id=eid,
            customer_id=cid,
            event_type=EventType.LOAN_APPLICATION,
            event_time=pt,
            available_at=pt,
            ingested_at=self._ingested_at,
            source_system=source,
            source_record_id=f"{APPLICATION_TABLE}:{normalize_id(sk)}",
            source_record_revision=self._manifest_sha,
            headers={
                "snapshot_id": self._snapshot_id,
                "adapter_version": self.adapter_version,
                "boundary_version": self._boundary.boundary_version,
                "source_table": APPLICATION_TABLE,
                "availability_semantics": "application_snapshot",
            },
        )

    def _bureau_event(
        self, record: dict[str, Any], source: str
    ) -> Generator[Event, None, None]:
        sk_curr = record.get("SK_ID_CURR")
        sk_bur = record.get("SK_ID_BUREAU")
        days_credit = record.get("DAYS_CREDIT")

        if sk_curr is None or sk_bur is None or days_credit is None:
            raise ContractValidationError([
                FieldError("bureau", "missing required bureau columns (SK_ID_CURR, SK_ID_BUREAU, DAYS_CREDIT)"),
            ])
        if not isinstance(days_credit, (int, float)):
            raise ContractValidationError([
                FieldError("DAYS_CREDIT", f"must be numeric, got {type(days_credit).__name__}"),
            ])
        days_credit = int(days_credit)
        if days_credit > 0:
            raise ContractValidationError([
                FieldError("DAYS_CREDIT", f"must be <= 0, got {days_credit}"),
            ])

        pt = self._boundary.prediction_anchor
        event_time = pt + timedelta(days=days_credit)

        eid = application_id(sk_curr)
        cid = customer_id(sk_curr)
        src_id = f"{BUREAU_TABLE}:{normalize_id(sk_bur)}"

        event_id = compute_event_id(
            self.dataset_id, EntityType.LOAN_APPLICATION, eid,
            EventType.BUREAU_SNAPSHOT, event_time,
            source_record_id=src_id,
            source_record_revision=self._manifest_sha,
        )
        yield Event(
            dataset_id=self.dataset_id,
            event_id=event_id,
            entity_type=EntityType.LOAN_APPLICATION,
            entity_id=eid,
            customer_id=cid,
            event_type=EventType.BUREAU_SNAPSHOT,
            event_time=event_time,
            available_at=pt,
            ingested_at=self._ingested_at,
            source_system=source,
            source_record_id=src_id,
            source_record_revision=self._manifest_sha,
            headers={
                "snapshot_id": self._snapshot_id,
                "adapter_version": self.adapter_version,
                "boundary_version": self._boundary.boundary_version,
                "source_table": BUREAU_TABLE,
                "availability_semantics": "application_snapshot",
            },
        )

    def _bureau_balance_event(
        self, record: dict[str, Any], source: str
    ) -> Event:
        sk_curr = record.get("SK_ID_CURR")
        sk_bur = record.get("SK_ID_BUREAU")
        months = record.get("MONTHS_BALANCE")
        status = record.get("STATUS")

        if sk_curr is None:
            raise ContractValidationError([
                FieldError("SK_ID_CURR", "bureau_balance must be enriched with SK_ID_CURR"),
            ])
        if sk_bur is None or months is None or status is None:
            raise ContractValidationError([
                FieldError("bureau_balance", "missing required columns (SK_ID_BUREAU, MONTHS_BALANCE, STATUS)"),
            ])
        if not isinstance(months, (int, float)):
            raise ContractValidationError([
                FieldError("MONTHS_BALANCE", f"must be numeric, got {type(months).__name__}"),
            ])
        months = int(months)
        if months > 0:
            raise ContractValidationError([
                FieldError("MONTHS_BALANCE", f"must be <= 0, got {months}"),
            ])
        if not isinstance(status, str) or not status.strip():
            raise ContractValidationError([
                FieldError("STATUS", "must be a non-empty string"),
            ])

        pt = self._boundary.prediction_anchor
        # Calendar-month shift: -N months from anchor
        event_time = pt.replace(
            year=pt.year + (pt.month - 1 + months) // 12,
            month=((pt.month - 1 + months) % 12) + 1,
        )

        eid = application_id(sk_curr)
        cid = customer_id(sk_curr)
        src_id = f"{BUREAU_BALANCE_TABLE}:{normalize_id(sk_bur)}:{months}"

        event_id = compute_event_id(
            self.dataset_id, EntityType.LOAN_APPLICATION, eid,
            EventType.BUREAU_SNAPSHOT, event_time,
            source_record_id=src_id,
            source_record_revision=self._manifest_sha,
        )
        return Event(
            dataset_id=self.dataset_id,
            event_id=event_id,
            entity_type=EntityType.LOAN_APPLICATION,
            entity_id=eid,
            customer_id=cid,
            event_type=EventType.BUREAU_SNAPSHOT,
            event_time=event_time,
            available_at=pt,
            ingested_at=self._ingested_at,
            source_system=source,
            source_record_id=src_id,
            source_record_revision=self._manifest_sha,
            headers={
                "snapshot_id": self._snapshot_id,
                "adapter_version": self.adapter_version,
                "boundary_version": self._boundary.boundary_version,
                "source_table": BUREAU_BALANCE_TABLE,
                "availability_semantics": "application_snapshot",
            },
        )

    # -- feature catalog ------------------------------------------------

    def build_feature_catalog(self) -> list[FeatureCatalogEntry]:
        return get_features()

    def semantic_group_mapping(self) -> dict[str, str]:
        return get_semantic_group_mapping()
