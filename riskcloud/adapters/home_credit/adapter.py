"""Home Credit Dataset Adapter — P1.1.

All events use Event.parse() and PredictionPoint.parse().
Manifest must pass the full P1.0 validator contract.
"""

from __future__ import annotations

import hashlib
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
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
    APPLICATION_EVENT_COLUMNS,
    APPLICATION_TABLE,
    BUREAU_BALANCE_EVENT_COLUMNS,
    BUREAU_BALANCE_TABLE,
    BUREAU_EVENT_COLUMNS,
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

UTC = timezone.utc
_REQUIRED_FILES = {APPLICATION_TABLE, BUREAU_TABLE, BUREAU_BALANCE_TABLE}
_REQUIRED_COLUMNS = {
    APPLICATION_TABLE: APPLICATION_EVENT_COLUMNS,
    BUREAU_TABLE: BUREAU_EVENT_COLUMNS,
    BUREAU_BALANCE_TABLE: BUREAU_BALANCE_EVENT_COLUMNS,
}


def _validate_manifest(manifest_path: Path, data_dir: Path) -> str:
    """Validate manifest via P1.0 contract. Returns manifest SHA-256.

    Reuses the existing P1.0 validator with full checks:
    - Required files exist, metadata populated, SHA hex format
    - Row/column counts match actual files
    - Required columns (SK_ID_CURR, TARGET, etc.) present
    - Bool rejected as int for row_count/columns
    - No duplicate file names
    """
    if not isinstance(manifest_path, Path):
        raise ContractValidationError([
            FieldError("manifest_path", f"must be Path, got {type(manifest_path).__name__}"),
        ])
    if not manifest_path.is_file():
        raise ContractValidationError([FieldError("manifest_path", f"not found: {manifest_path}")])

    raw = manifest_path.read_bytes()
    manifest_sha = hashlib.sha256(raw).hexdigest()

    try:
        manifest = yaml.safe_load(raw)
    except Exception as exc:
        raise ContractValidationError([FieldError("manifest_path", f"invalid YAML: {exc}")]) from exc

    if not isinstance(manifest, dict):
        raise ContractValidationError([FieldError("manifest_path", "must be a YAML dict")])

    # Validate dataset
    ds = manifest.get("dataset")
    if ds != "home_credit":
        raise ContractValidationError([FieldError("manifest_path.dataset", f"must be home_credit, got {ds!r}")])

    files = manifest.get("files")
    if not isinstance(files, list) or len(files) == 0:
        raise ContractValidationError([FieldError("manifest_path", "files must be a non-empty list")])

    seen: set[str] = set()
    for i, fspec in enumerate(files):
        if not isinstance(fspec, dict):
            raise ContractValidationError([FieldError(f"manifest_path.files[{i}]", "must be a dict")])
        name = fspec.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ContractValidationError([FieldError(f"manifest_path.files[{i}]", "name must be non-empty str")])
        if name in seen:
            raise ContractValidationError([FieldError(f"manifest_path.files[{i}]", f"duplicate file name: {name}")])
        seen.add(name)

    for rf in _REQUIRED_FILES:
        if rf not in seen:
            raise ContractValidationError([FieldError("manifest_path", f"required file missing: {rf}")])

    for fspec in files:
        name = fspec.get("name", "")
        if name not in _REQUIRED_FILES:
            continue
        req = fspec.get("required")
        if req is not True:
            raise ContractValidationError([FieldError(f"manifest_path.{name}", "must have required: true")])
        for meta_field in ("sha256", "header_sha256", "row_count", "columns"):
            val = fspec.get(meta_field)
            if val is None:
                raise ContractValidationError([
                    FieldError(f"manifest_path.{name}", f"{meta_field} is null — run --populate first"),
                ])
        # SHA must be hex
        for sha_field in ("sha256", "header_sha256"):
            val = fspec.get(sha_field, "")
            if not isinstance(val, str) or len(val) != 64:
                raise ContractValidationError([
                    FieldError(f"manifest_path.{name}.{sha_field}", "must be 64 hex chars"),
                ])
            try:
                int(val, 16)
            except ValueError:
                raise ContractValidationError([
                    FieldError(f"manifest_path.{name}.{sha_field}", "must be valid hex"),
                ])
        # row_count and columns: must be int (not bool)
        for int_field in ("row_count", "columns"):
            val = fspec.get(int_field)
            if isinstance(val, bool):
                raise ContractValidationError([
                    FieldError(f"manifest_path.{name}.{int_field}", "must be int, not bool"),
                ])
            if not isinstance(val, int):
                raise ContractValidationError([
                    FieldError(f"manifest_path.{name}.{int_field}", f"must be int, got {type(val).__name__}"),
                ])
        rc = fspec["row_count"]
        if rc < 0:
            raise ContractValidationError([FieldError(f"manifest_path.{name}.row_count", "must be non-negative")])
        cols = fspec["columns"]
        if cols < 1:
            raise ContractValidationError([FieldError(f"manifest_path.{name}.columns", "must be positive")])
        min_cols = len(_REQUIRED_COLUMNS.get(name, set()))
        if cols < min_cols:
            raise ContractValidationError([
                FieldError(
                    f"manifest_path.{name}.columns",
                    f"requires >= {min_cols} columns for {_REQUIRED_COLUMNS[name]}",
                ),
            ])

    # --- Delegate file-level validation to P1.0 ---
    from case_studies.home_credit.scripts.validate_manifest import validate_manifest as p10_validate
    ok, errors = p10_validate(data_dir, manifest_path)
    if not ok:
        field_errors = [FieldError("manifest_path", e) for e in errors]
        raise ContractValidationError(field_errors)

    return manifest_sha


def _validate_strict_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ContractValidationError([FieldError(field, "must be int, not bool", value)])
    if not isinstance(value, int):
        raise ContractValidationError([FieldError(field, f"must be int, got {type(value).__name__}", value)])
    return value


def _calendar_month_shift(dt: datetime, months: int) -> datetime:
    if months == 0:
        return dt
    import calendar
    total = dt.year * 12 + (dt.month - 1) + months
    new_year, new_month = divmod(total, 12)
    new_month += 1
    max_day = calendar.monthrange(new_year, new_month)[1]
    return dt.replace(year=new_year, month=new_month, day=min(dt.day, max_day))


def _build_event_dict(
    dataset_id: str, entity_id: str, customer_id: str,
    event_type: str, event_time: datetime, available_at: datetime,
    ingested_at: datetime, source_system: str,
    source_record_id: str, source_record_revision: str,
    snapshot_id: str, adapter_version: str, boundary_version: str,
    source_table: str,
) -> dict[str, Any]:
    event_id = compute_event_id(
        dataset_id, EntityType.LOAN_APPLICATION, entity_id,
        EventType(event_type), event_time,
        source_record_id=source_record_id,
        source_record_revision=source_record_revision,
    )
    return {
        "dataset_id": dataset_id,
        "event_id": event_id,
        "entity_type": "loan_application",
        "entity_id": entity_id,
        "customer_id": customer_id,
        "event_type": event_type,
        "event_time": event_time.isoformat(),
        "available_at": available_at.isoformat(),
        "ingested_at": ingested_at.isoformat(),
        "source_system": source_system,
        "source_record_id": source_record_id,
        "source_record_revision": source_record_revision,
        "headers": {
            "snapshot_id": snapshot_id,
            "adapter_version": adapter_version,
            "boundary_version": boundary_version,
            "source_table": source_table,
            "availability_semantics": "application_snapshot",
        },
    }


class HomeCreditAdapter(Adapter):

    def __init__(
        self,
        snapshot_id: str,
        manifest_path: Path,
        data_dir: Path,
        ingested_at: datetime,
        boundary_config: HomeCreditBoundaryConfig,
    ):
        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            raise ContractValidationError([FieldError("snapshot_id", "must be non-empty string")])

        self._manifest_sha = _validate_manifest(manifest_path, data_dir)

        if not isinstance(ingested_at, datetime):
            raise ContractValidationError([FieldError("ingested_at", "must be datetime")])
        if ingested_at.tzinfo is None:
            raise ContractValidationError([FieldError("ingested_at", "must be timezone-aware")])
        if ingested_at.utcoffset() != timedelta(0):
            raise ContractValidationError([FieldError("ingested_at", "must be UTC (offset 0)")])

        if not isinstance(boundary_config, HomeCreditBoundaryConfig):
            raise ContractValidationError([
                FieldError(
                    "boundary_config",
                    f"must be HomeCreditBoundaryConfig, got {type(boundary_config).__name__}",
                ),
            ])
        if ingested_at < boundary_config.prediction_anchor:
            raise ContractValidationError([FieldError("ingested_at", "must be >= prediction_anchor")])

        self._snapshot_id = snapshot_id
        self._ingested_at = ingested_at
        self._boundary = boundary_config

    @property
    def dataset_id(self) -> str:
        return "home_credit"

    @property
    def display_name(self) -> str:
        return "Home Credit Default Risk"

    @property
    def adapter_version(self) -> str:
        return "1.0.0"

    def define_prediction_boundary(self, raw_record: dict[str, Any]) -> PredictionPoint:
        table = raw_record.get(SOURCE_TABLE_FIELD)
        if table != APPLICATION_TABLE:
            raise ContractValidationError([
                FieldError(SOURCE_TABLE_FIELD, f"prediction boundary requires {APPLICATION_TABLE}, got {table!r}"),
            ])
        try:
            return build_prediction_point(raw_record, self._snapshot_id, self._boundary)
        except ValueError as exc:
            raise ContractValidationError([FieldError("prediction_point", str(exc))]) from exc

    def prediction_time_column(self) -> str:
        return "__proxy_application_time__"

    def label_column(self) -> str | None:
        return "TARGET"

    def label_time_column(self) -> str | None:
        return "__proxy_label_time__"

    def generate_events(
        self, raw_record: dict[str, Any], source_system: str = ""
    ) -> Generator[Event, None, None]:
        table = raw_record.get(SOURCE_TABLE_FIELD)
        if table is None:
            raise ContractValidationError([FieldError(SOURCE_TABLE_FIELD, "record must have __source_table__")])
        source = source_system or "home_credit_adapter"

        try:
            if table == APPLICATION_TABLE:
                yield self._application_event(raw_record, source)
            elif table == BUREAU_TABLE:
                yield self._bureau_event(raw_record, source)
            elif table == BUREAU_BALANCE_TABLE:
                yield self._bureau_balance_event(raw_record, source)
            else:
                raise ContractValidationError([FieldError(SOURCE_TABLE_FIELD, f"unknown table: {table}")])
        except ContractValidationError:
            raise
        except ValueError as exc:
            raise ContractValidationError([FieldError(table, str(exc))]) from exc

    def _application_event(self, record: dict[str, Any], source: str) -> Event:
        for col in APPLICATION_EVENT_COLUMNS:
            if col not in record:
                raise ContractValidationError([FieldError(col, f"required column missing in {APPLICATION_TABLE}")])
        sk = record["SK_ID_CURR"]
        try:
            eid = application_id(sk)
        except ValueError as exc:
            raise ContractValidationError([FieldError("SK_ID_CURR", str(exc))]) from exc

        pt = self._boundary.prediction_anchor
        d = _build_event_dict(
            self.dataset_id, eid, customer_id(sk),
            "loan_application", pt, pt, self._ingested_at, source,
            f"{APPLICATION_TABLE}:{normalize_id(sk)}", self._manifest_sha,
            self._snapshot_id, self.adapter_version, self._boundary.boundary_version,
            APPLICATION_TABLE,
        )
        return Event.parse(d)

    def _bureau_event(self, record: dict[str, Any], source: str) -> Event:
        for col in BUREAU_EVENT_COLUMNS:
            if col not in record:
                raise ContractValidationError([FieldError(col, f"required column missing in {BUREAU_TABLE}")])
        sk_curr = record["SK_ID_CURR"]
        sk_bur = record["SK_ID_BUREAU"]
        days_credit = _validate_strict_int(record["DAYS_CREDIT"], "DAYS_CREDIT")
        if days_credit > 0:
            raise ContractValidationError([FieldError("DAYS_CREDIT", f"must be <= 0, got {days_credit}")])

        pt = self._boundary.prediction_anchor
        event_time = pt + timedelta(days=days_credit)
        try:
            eid = application_id(sk_curr)
        except ValueError as exc:
            raise ContractValidationError([FieldError("SK_ID_CURR", str(exc))]) from exc
        src_id = f"{BUREAU_TABLE}:{normalize_id(sk_bur)}"

        d = _build_event_dict(
            self.dataset_id, eid, customer_id(sk_curr),
            "bureau_snapshot", event_time, pt, self._ingested_at, source,
            src_id, self._manifest_sha,
            self._snapshot_id, self.adapter_version, self._boundary.boundary_version,
            BUREAU_TABLE,
        )
        return Event.parse(d)

    def _bureau_balance_event(self, record: dict[str, Any], source: str) -> Event:
        for col in BUREAU_BALANCE_EVENT_COLUMNS:
            if col not in record:
                raise ContractValidationError([FieldError(col, f"required column missing in {BUREAU_BALANCE_TABLE}")])
        sk_curr = record["SK_ID_CURR"]
        sk_bur = record["SK_ID_BUREAU"]
        months = _validate_strict_int(record["MONTHS_BALANCE"], "MONTHS_BALANCE")
        if months > 0:
            raise ContractValidationError([FieldError("MONTHS_BALANCE", f"must be <= 0, got {months}")])
        status = record["STATUS"]
        if not isinstance(status, str) or not status.strip():
            raise ContractValidationError([FieldError("STATUS", "must be non-empty string")])

        pt = self._boundary.prediction_anchor
        event_time = _calendar_month_shift(pt, months)
        try:
            eid = application_id(sk_curr)
        except ValueError as exc:
            raise ContractValidationError([FieldError("SK_ID_CURR", str(exc))]) from exc
        src_id = f"{BUREAU_BALANCE_TABLE}:{normalize_id(sk_bur)}:{months}"

        d = _build_event_dict(
            self.dataset_id, eid, customer_id(sk_curr),
            "bureau_snapshot", event_time, pt, self._ingested_at, source,
            src_id, self._manifest_sha,
            self._snapshot_id, self.adapter_version, self._boundary.boundary_version,
            BUREAU_BALANCE_TABLE,
        )
        return Event.parse(d)

    def build_feature_catalog(self) -> list[FeatureCatalogEntry]:
        return get_features()

    def semantic_group_mapping(self) -> dict[str, str]:
        return get_semantic_group_mapping()
