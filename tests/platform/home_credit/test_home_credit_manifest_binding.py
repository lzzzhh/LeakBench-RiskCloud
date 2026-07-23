"""P1.1 — Field mapping and manifest binding tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from riskcloud.adapters.home_credit.field_mapping import (
    APPLICATION_FILE,
    APPLICATION_TABLE,
    BUREAU_BALANCE_FILE,
    BUREAU_BALANCE_TABLE,
    BUREAU_FILE,
    BUREAU_TABLE,
    SOURCE_TABLE_FIELD,
    application_id,
    bureau_id,
    customer_id,
    normalize_id,
)

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"


class TestFieldMapping:
    def test_normalize_id_int(self):
        assert normalize_id(100001) == "100001"

    def test_normalize_id_str(self):
        assert normalize_id("100001") == "100001"

    def test_normalize_id_strips_leading_zeros(self):
        assert normalize_id("00100001") == "100001"

    def test_normalize_id_rejects_bool(self):
        with pytest.raises(ValueError, match="bool"):
            normalize_id(True)

    def test_normalize_id_rejects_float(self):
        with pytest.raises(ValueError, match="float"):
            normalize_id(100001.0)

    def test_normalize_id_rejects_empty(self):
        with pytest.raises(ValueError):
            normalize_id("")

    def test_normalize_id_rejects_negative(self):
        with pytest.raises(ValueError):
            normalize_id(-1)

    def test_normalize_id_rejects_non_digit(self):
        with pytest.raises(ValueError):
            normalize_id("abc")

    def test_customer_id(self):
        assert customer_id(100001) == "customer:100001"

    def test_application_id(self):
        assert application_id(100001) == "SK_ID_CURR:100001"

    def test_bureau_id(self):
        assert bureau_id(500) == "SK_ID_BUREAU:500"

    def test_constants(self):
        assert APPLICATION_TABLE == "application_train"
        assert BUREAU_TABLE == "bureau"
        assert BUREAU_BALANCE_TABLE == "bureau_balance"
        assert APPLICATION_FILE == "application_train.csv"
        assert BUREAU_FILE == "bureau.csv"
        assert BUREAU_BALANCE_FILE == "bureau_balance.csv"
        assert SOURCE_TABLE_FIELD == "__source_table__"

    def test_raw_bureau_balance_columns_are_three(self):
        """Raw bureau_balance.csv has 3 columns (no SK_ID_CURR)."""
        from riskcloud.adapters.home_credit.field_mapping import RAW_REQUIRED_COLUMNS

        assert len(RAW_REQUIRED_COLUMNS["bureau_balance.csv"]) == 3
        assert "SK_ID_CURR" not in RAW_REQUIRED_COLUMNS["bureau_balance.csv"]

    def test_event_bureau_balance_columns_are_four(self):
        """Enriched bureau_balance event record requires 4 columns (with SK_ID_CURR)."""
        from riskcloud.adapters.home_credit.field_mapping import BUREAU_BALANCE_EVENT_COLUMNS

        assert len(BUREAU_BALANCE_EVENT_COLUMNS) == 4
        assert "SK_ID_CURR" in BUREAU_BALANCE_EVENT_COLUMNS

    def test_raw_fixture_bureau_balance_has_three_columns(self):
        """The raw fixture CSV must have 3 columns, not 4."""
        import csv

        fixture = FIXTURES / "bureau_balance.csv"
        with open(fixture, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert len(header) == 3, f"Expected 3 raw columns, got {len(header)}: {header}"
            assert "SK_ID_CURR" not in header, "Raw fixture must not contain enriched SK_ID_CURR"
