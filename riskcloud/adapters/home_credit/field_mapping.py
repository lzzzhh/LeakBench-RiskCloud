"""Home Credit field and entity mapping.

Logical table names (used for __source_table__, event routing, source_record_id).
Physical file names (used for manifest validation).
"""

from __future__ import annotations

SOURCE_TABLE_FIELD = "__source_table__"

# Logical table names
APPLICATION_TABLE = "application_train"
BUREAU_TABLE = "bureau"
BUREAU_BALANCE_TABLE = "bureau_balance"

# Physical file names
APPLICATION_FILE = "application_train.csv"
BUREAU_FILE = "bureau.csv"
BUREAU_BALANCE_FILE = "bureau_balance.csv"

# Raw file required columns (P1.0 manifest contract)
RAW_REQUIRED_COLUMNS = {
    APPLICATION_FILE: {"SK_ID_CURR", "TARGET"},
    BUREAU_FILE: {"SK_ID_CURR", "SK_ID_BUREAU"},
    BUREAU_BALANCE_FILE: {"SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"},
}

# Event record required columns (after enrichment)
APPLICATION_EVENT_COLUMNS = {"SK_ID_CURR", "TARGET"}
BUREAU_EVENT_COLUMNS = {"SK_ID_CURR", "SK_ID_BUREAU", "DAYS_CREDIT"}
BUREAU_BALANCE_EVENT_COLUMNS = {
    "SK_ID_CURR",       # enriched from bureau
    "SK_ID_BUREAU",
    "MONTHS_BALANCE",
    "STATUS",
}


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
