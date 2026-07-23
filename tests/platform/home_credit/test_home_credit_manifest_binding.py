"""P1.1 — Field mapping and manifest binding tests."""

from __future__ import annotations

import pytest

from riskcloud.adapters.home_credit.field_mapping import (
    APPLICATION_TABLE,
    BUREAU_BALANCE_TABLE,
    BUREAU_TABLE,
    SOURCE_TABLE_FIELD,
    application_id,
    bureau_id,
    customer_id,
    normalize_id,
)


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
        assert SOURCE_TABLE_FIELD == "__source_table__"
