"""Home Credit Dataset Adapter — P1.1.

All events use Event.parse() and PredictionPoint.parse() (strict contract entries).
All temporal fields validated. Manifest SHA must match the actual manifest file.
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

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


def _compute_sha256_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _validate_strict_int(value: object, field: str) -> int:
    """Accept only int (not bool, not float)."""
    if isinstance(value, bool):
        raise ContractValidationError([FieldError(field, "must be int, not bool", value)])
    if not isinstance(value, int):
        raise ContractValidationError([FieldError(field, f"must be int, got {type(value).__name__}", value)])
    return value


def _calendar_month_shift(dt: datetime, months: int) -> datetime:
    """Shift a datetime by N calendar months. Clips day-of-month if needed."""
    if months == 0:
        return dt
    total_months = dt.year * 12 + (dt.month - 1) + months
    new_year = total_months // 12
    new_month = (total_months % 12) + 1
    import calendar
    max_day = calendar.monthrange(new_year, new_month)[1]
    new_day = min(dt.day, max_day)
    return dt.replace(year=new_year, month=new_month, day=new_day)


class HomeCreditAdapter(Adapter):

    def __init__(
        self,
        snapshot_id: str,
        manifest_path: Path,
        ingested_at: datetime,
        boundary_config: HomeCreditBoundaryConfig,
    ):
        # snapshot_id
        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            raise ContractValidationError([FieldError("snapshot_id", "must be non-empty string")])

        # manifest_path: must exist, be populated, compute SHA
        if not manifest_path.is_file():
            raise ContractValidationError([FieldError("manifest_path", f"not found: {manifest_path}")])
        self._manifest_sha = _compute_sha256_file(manifest_path)

        # Verify manifest is populated
        with open(manifest_path) as f:
            manifest_data = yaml.safe_load(f)
        for fspec in manifest_data.get("files", []):
            if fspec.get("required"):
                for field in ("sha256", "row_count", "columns"):
                    if fspec.get(field) is None:
                        raise ContractValidationError([
                            FieldError("manifest_path", f"{fspec['name']}.{field} is null — run --populate first"),
                        ])

        # ingested_at: UTC and >= prediction anchor
        if not isinstance(ingested_at, datetime):
            raise ContractValidationError([FieldError("ingested_at", "must be datetime")])
        if ingested_at.tzinfo is None:
            raise ContractValidationError([FieldError("ingested_at", "must be timezone-aware")])
        if ingested_at.utcoffset() != timedelta(0):
            raise ContractValidationError([FieldError("ingested_at", "must be UTC (offset 0)")])

        # boundary_config
        if not isinstance(boundary_config, HomeCreditBoundaryConfig):
            raise ContractValidationError([
                FieldError(
                    "boundary_config",
                    f"must be HomeCreditBoundaryConfig, got {type(boundary_config).__name__}",
                ),
            ])

        if ingested_at < boundary_config.prediction_anchor:
            raise ContractValidationError([
                FieldError("ingested_at", "must be >= prediction_anchor"),
            ])

        self._snapshot_id = snapshot_id
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
        table = raw_record.get(SOURCE_TABLE_FIELD)
        if table != APPLICATION_TABLE:
            raise ContractValidationError([
                FieldError(SOURCE_TABLE_FIELD, f"prediction boundary requires {APPLICATION_TABLE}, got {table!r}"),
            ])
        target = raw_record.get("TARGET")
        if target is None:
            raise ContractValidationError([
                FieldError("TARGET", "missing — prediction boundary requires labeled application_train records"),
            ])
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
        table = raw_record.get(SOURCE_TABLE_FIELD)
        if table is None:
            raise ContractValidationError([
                FieldError(SOURCE_TABLE_FIELD, "record must have __source_table__"),
            ])

        if table == APPLICATION_TABLE:
            yield self._application_event(raw_record)
        elif table == BUREAU_TABLE:
            yield self._bureau_event(raw_record)
        elif table == BUREAU_BALANCE_TABLE:
            yield self._bureau_balance_event(raw_record)
        else:
            raise ContractValidationError([
                FieldError(SOURCE_TABLE_FIELD, f"unknown table: {table}"),
            ])

    def _application_event(self, record: dict[str, Any]) -> Event:
        sk = record.get("SK_ID_CURR")
        if sk is None:
            raise ContractValidationError([FieldError("SK_ID_CURR", "missing")])
        eid = application_id(sk)
        pt = self._boundary.prediction_anchor

        event_id = compute_event_id(
            self.dataset_id, EntityType.LOAN_APPLICATION, eid,
            EventType.LOAN_APPLICATION, pt,
            source_record_id=f"{APPLICATION_TABLE}:{normalize_id(sk)}",
            source_record_revision=self._manifest_sha,
        )
        return Event.parse({
            "dataset_id": self.dataset_id,
            "event_id": event_id,
            "entity_type": "loan_application",
            "entity_id": eid,
            "customer_id": customer_id(sk),
            "event_type": "loan_application",
            "event_time": pt.isoformat(),
            "available_at": pt.isoformat(),
            "ingested_at": self._ingested_at.isoformat(),
            "source_system": "home_credit_adapter",
            "source_record_id": f"{APPLICATION_TABLE}:{normalize_id(sk)}",
            "source_record_revision": self._manifest_sha,
            "headers": {
                "snapshot_id": self._snapshot_id,
                "adapter_version": self.adapter_version,
                "boundary_version": self._boundary.boundary_version,
                "source_table": APPLICATION_TABLE,
                "availability_semantics": "application_snapshot",
            },
        })

    def _bureau_event(self, record: dict[str, Any]) -> Event:
        sk_curr = record.get("SK_ID_CURR")
        sk_bur = record.get("SK_ID_BUREAU")
        days_credit = record.get("DAYS_CREDIT")

        if sk_curr is None or sk_bur is None or days_credit is None:
            raise ContractValidationError([
                FieldError("bureau", "missing required columns (SK_ID_CURR, SK_ID_BUREAU, DAYS_CREDIT)"),
            ])
        days_credit = _validate_strict_int(days_credit, "DAYS_CREDIT")
        if days_credit > 0:
            raise ContractValidationError([FieldError("DAYS_CREDIT", f"must be <= 0, got {days_credit}")])

        pt = self._boundary.prediction_anchor
        event_time = pt + timedelta(days=days_credit)
        eid = application_id(sk_curr)
        src_id = f"{BUREAU_TABLE}:{normalize_id(sk_bur)}"

        event_id = compute_event_id(
            self.dataset_id, EntityType.LOAN_APPLICATION, eid,
            EventType.BUREAU_SNAPSHOT, event_time,
            source_record_id=src_id,
            source_record_revision=self._manifest_sha,
        )
        return Event.parse({
            "dataset_id": self.dataset_id,
            "event_id": event_id,
            "entity_type": "loan_application",
            "entity_id": eid,
            "customer_id": customer_id(sk_curr),
            "event_type": "bureau_snapshot",
            "event_time": event_time.isoformat(),
            "available_at": pt.isoformat(),
            "ingested_at": self._ingested_at.isoformat(),
            "source_system": "home_credit_adapter",
            "source_record_id": src_id,
            "source_record_revision": self._manifest_sha,
            "headers": {
                "snapshot_id": self._snapshot_id,
                "adapter_version": self.adapter_version,
                "boundary_version": self._boundary.boundary_version,
                "source_table": BUREAU_TABLE,
                "availability_semantics": "application_snapshot",
            },
        })

    def _bureau_balance_event(self, record: dict[str, Any]) -> Event:
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
        months = _validate_strict_int(months, "MONTHS_BALANCE")
        if months > 0:
            raise ContractValidationError([FieldError("MONTHS_BALANCE", f"must be <= 0, got {months}")])
        if not isinstance(status, str) or not status.strip():
            raise ContractValidationError([FieldError("STATUS", "must be non-empty string")])

        pt = self._boundary.prediction_anchor
        event_time = _calendar_month_shift(pt, months)
        eid = application_id(sk_curr)
        src_id = f"{BUREAU_BALANCE_TABLE}:{normalize_id(sk_bur)}:{months}"

        event_id = compute_event_id(
            self.dataset_id, EntityType.LOAN_APPLICATION, eid,
            EventType.BUREAU_SNAPSHOT, event_time,
            source_record_id=src_id,
            source_record_revision=self._manifest_sha,
        )
        return Event.parse({
            "dataset_id": self.dataset_id,
            "event_id": event_id,
            "entity_type": "loan_application",
            "entity_id": eid,
            "customer_id": customer_id(sk_curr),
            "event_type": "bureau_snapshot",
            "event_time": event_time.isoformat(),
            "available_at": pt.isoformat(),
            "ingested_at": self._ingested_at.isoformat(),
            "source_system": "home_credit_adapter",
            "source_record_id": src_id,
            "source_record_revision": self._manifest_sha,
            "headers": {
                "snapshot_id": self._snapshot_id,
                "adapter_version": self.adapter_version,
                "boundary_version": self._boundary.boundary_version,
                "source_table": BUREAU_BALANCE_TABLE,
                "availability_semantics": "application_snapshot",
            },
        })

    # -- feature catalog ------------------------------------------------

    def build_feature_catalog(self) -> list[FeatureCatalogEntry]:
        return get_features()

    def semantic_group_mapping(self) -> dict[str, str]:
        return get_semantic_group_mapping()
