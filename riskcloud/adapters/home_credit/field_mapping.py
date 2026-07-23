"""Home Credit field and entity mapping.

All input records must carry __source_table__.
ID normalization rejects bool, float, empty, negative, non-digit.
"""

from __future__ import annotations

SOURCE_TABLE_FIELD = "__source_table__"

APPLICATION_TABLE = "application_train.csv"
BUREAU_TABLE = "bureau.csv"
BUREAU_BALANCE_TABLE = "bureau_balance.csv"

APPLICATION_EVENT_COLUMNS = {"SK_ID_CURR", "TARGET"}
BUREAU_EVENT_COLUMNS = {"SK_ID_CURR", "SK_ID_BUREAU", "DAYS_CREDIT"}
BUREAU_BALANCE_EVENT_COLUMNS = {"SK_ID_CURR", "SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"}


def normalize_id(value: object) -> str:
    """Normalize an entity ID to a canonical decimal string.

    Accepts: non-negative int, decimal-only str
    Rejects: bool, float, empty str, negative, non-digit characters
    """
    if isinstance(value, bool):
        raise ValueError(f"ID value must not be bool, got {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"ID must be non-negative, got {value}")
        return str(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("ID must not be empty")
        if not stripped.isdigit():
            raise ValueError(f"ID must contain only decimal digits, got {stripped!r}")
        # Strip leading zeros
        return stripped.lstrip("0") or "0"
    if isinstance(value, float):
        raise ValueError(f"ID must not be float, got {value!r}")
    raise ValueError(f"Unsupported ID type: {type(value).__name__}")


def customer_id(sk_id_curr: object) -> str:
    return f"customer:{normalize_id(sk_id_curr)}"


def application_id(sk_id_curr: object) -> str:
    return f"SK_ID_CURR:{normalize_id(sk_id_curr)}"


def bureau_id(sk_id_bureau: object) -> str:
    return f"SK_ID_BUREAU:{normalize_id(sk_id_bureau)}"
