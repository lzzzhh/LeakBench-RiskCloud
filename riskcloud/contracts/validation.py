"""Contract validation — structured errors, strict parsing, runtime type checks.

All contracts MUST use the strict entry points (parse_*) in production paths.
The unchecked entry points (from_dict_unchecked) exist only for backward
compatibility and tests that deliberately construct invalid data.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Sequence


# -----------------------------------------------------------------
# Structured errors
# -----------------------------------------------------------------

class ContractValidationError(ValueError):
    """Raised when a contract object fails strict validation.

    All production parsing must use entry points that raise this exception.
    Never catch this silently; treat it as a data pipeline failure.
    """

    def __init__(self, errors: list["FieldError"], raw_message: str = ""):
        self.errors = errors
        detail = "\n".join(str(e) for e in errors) if errors else raw_message
        super().__init__(detail)
        self.detail = detail


@dataclass(frozen=True)
class FieldError:
    field_path: str         # e.g. "event_time" or "payload.sha256"
    message: str
    value: Any = field(default=None, repr=False)

    def __str__(self) -> str:
        return f"{self.field_path}: {self.message}"


# -----------------------------------------------------------------
# Type-safe coercions (fail on wrong type, unlike silent defaults)
# -----------------------------------------------------------------

def _check_type(value: Any, expected: type, field_path: str) -> list[FieldError]:
    """Require exact type match. None is always acceptable for Optional fields."""
    if value is None:
        return []
    if not isinstance(value, expected):
        return [FieldError(field_path, f"expected {expected.__name__}, got {type(value).__name__}", value)]
    return []


def _check_type_union(value: Any, types: tuple[type, ...], field_path: str) -> list[FieldError]:
    """Require value to be one of the allowed types."""
    if value is None:
        return []
    if not isinstance(value, types):
        names = " | ".join(t.__name__ for t in types)
        return [FieldError(field_path, f"expected {names}, got {type(value).__name__}", value)]
    return []


def coerce_str(value: Any, field_path: str) -> str:
    errors = _check_type(value, str, field_path)
    if errors:
        raise ContractValidationError(errors)
    return value  # type: ignore[return-value]


def coerce_str_nonempty(value: Any, field_path: str) -> str:
    """Coerce to non-empty string. Raises on None, wrong type, or empty/whitespace."""
    # First check type — coerce_str returns None if value is None
    s = coerce_str(value, field_path)
    if s is None:
        raise ContractValidationError([FieldError(field_path, "must be non-empty (got None)", value)])
    if not s.strip():
        raise ContractValidationError([FieldError(field_path, "must be non-empty", value)])
    return s


def coerce_int_opt(value: Any, field_path: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ContractValidationError([FieldError(field_path, "expected int, got bool", value)])
    errors = _check_type(value, int, field_path)
    if errors:
        raise ContractValidationError(errors)
    return value  # type: ignore[return-value]


def coerce_float_opt(value: Any, field_path: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ContractValidationError([FieldError(field_path, "expected float, got bool", value)])
    if not isinstance(value, (int, float)):
        raise ContractValidationError([FieldError(field_path, f"expected float, got {type(value).__name__}", value)])
    v = float(value)
    if math.isnan(v) or math.isinf(v):
        raise ContractValidationError([FieldError(field_path, "float must not be NaN or Inf", value)])
    return v


def coerce_bool_opt(value: Any, field_path: str) -> Optional[bool]:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ContractValidationError([FieldError(field_path, f"expected bool, got {type(value).__name__}", value)])
    return value


def coerce_datetime_utc(value: Any, field_path: str) -> datetime:
    """Parse a datetime string. Reject naive datetimes; fail on wrong type."""
    if isinstance(value, datetime):
        if value.utcoffset() is None:
            raise ContractValidationError([FieldError(field_path, "datetime must be timezone-aware", value)])
        return value
    if isinstance(value, str):
        return _parse_dt_strict(value, field_path)
    raise ContractValidationError([FieldError(field_path, f"expected ISO datetime string, got {type(value).__name__}", value)])


def _parse_dt_strict(s: str, field_path: str) -> datetime:
    s = s.strip()
    # Detect Z suffix
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except (ValueError, TypeError) as exc:
        raise ContractValidationError([FieldError(field_path, f"invalid ISO datetime: {exc}", s)]) from exc
    if dt.utcoffset() is None:
        raise ContractValidationError([FieldError(field_path, "datetime string must include timezone offset", s)])
    return dt


# -----------------------------------------------------------------
# Generic enum coercion
# -----------------------------------------------------------------

def coerce_enum(value: Any, enum_cls: type, field_path: str) -> Any:
    """Coerce a string into an enum member. Fail on unknown values."""
    if isinstance(value, enum_cls):
        return value
    if isinstance(value, str):
        try:
            return enum_cls(value)
        except ValueError as exc:
            raise ContractValidationError([FieldError(
                field_path,
                f"invalid value '{value}' for {enum_cls.__name__}; allowed: {[e.value for e in enum_cls]}",
                value,
            )]) from exc
    raise ContractValidationError([FieldError(field_path, f"expected str or {enum_cls.__name__}, got {type(value).__name__}", value)])


# -----------------------------------------------------------------
# Defensive copy helpers for deep immutability
# -----------------------------------------------------------------

def frozen_dict(d: dict) -> dict:
    """Return a shallow copy (for use in __post_init__)."""
    return dict(d)


def immutable_dict(d: dict) -> Any:
    """Return a read-only mapping proxy. Use in __post_init__ for deep immutability."""
    from types import MappingProxyType
    return MappingProxyType(dict(d))


def frozen_tuple(seq: Sequence) -> tuple:
    """Convert sequence to tuple (for frozen list-like fields)."""
    return tuple(seq)
