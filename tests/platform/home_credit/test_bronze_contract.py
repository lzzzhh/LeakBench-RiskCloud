"""P1.2 — Bronze contract tests (pure Python, no Spark required)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from case_studies.home_credit.pipelines.bronze_ingestion import (
    BRONZE_META_COLUMNS,
    BRONZE_SCHEMA_VERSION,
    BronzeConfig,
    _compute_source_snapshot_id,
    _row_hash,
)


class TestBronzeConfig:

    def test_valid_config(self):
        config = BronzeConfig.from_yaml(
            Path(__file__).resolve().parents[3]
            / "case_studies" / "home_credit" / "configs" / "bronze_v1.yaml"
        )
        assert config.version == "hc-bronze-v1"
        assert config.write_mode == "overwrite_partitions"
        assert len(config.tables) == 3

    def test_rejects_missing_table(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.safe_dump({
                "bronze": {"version": "v1", "catalog": "r", "namespace": "b",
                           "partition_field": "f", "write_mode": "overwrite_partitions"},
                "tables": {
                    "application_train": {"file": "application_train.csv", "table": "t"},
                },
            }, f)
            path = Path(f.name)
        try:
            with pytest.raises(ValueError, match="missing"):
                BronzeConfig.from_yaml(path)
        finally:
            path.unlink()

    def test_rejects_duplicate_targets(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.safe_dump({
                "bronze": {"version": "v1", "catalog": "r", "namespace": "b",
                           "partition_field": "f", "write_mode": "overwrite_partitions"},
                "tables": {
                    "a": {"file": "application_train.csv", "table": "same.table"},
                    "b": {"file": "bureau.csv", "table": "same.table"},
                    "c": {"file": "bureau_balance.csv", "table": "ok.table"},
                },
            }, f)
            path = Path(f.name)
        try:
            with pytest.raises(ValueError, match="duplicate"):
                BronzeConfig.from_yaml(path)
        finally:
            path.unlink()


class TestSourceSnapshotId:

    def test_deterministic(self):
        id1 = _compute_source_snapshot_id("a" * 64, "hc-bronze-v1")
        id2 = _compute_source_snapshot_id("a" * 64, "hc-bronze-v1")
        assert id1 == id2

    def test_different_manifest_different_id(self):
        id1 = _compute_source_snapshot_id("a" * 64, "hc-bronze-v1")
        id2 = _compute_source_snapshot_id("b" * 64, "hc-bronze-v1")
        assert id1 != id2

    def test_different_version_different_id(self):
        id1 = _compute_source_snapshot_id("a" * 64, "hc-bronze-v1")
        id2 = _compute_source_snapshot_id("a" * 64, "hc-bronze-v2")
        assert id1 != id2


class TestRowHash:

    def test_deterministic(self):
        h1 = _row_hash(["100", "0", "C"], ["SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"])
        h2 = _row_hash(["100", "0", "C"], ["SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"])
        assert h1 == h2

    def test_null_not_omitted(self):
        """Null values must be included in the hash — not skipped."""
        h_with_null = _row_hash(["100", None, "C"], ["a", "b", "c"])
        h_with_empty = _row_hash(["100", "", "C"], ["a", "b", "c"])
        assert h_with_null != h_with_empty

    def test_column_order_matters(self):
        h1 = _row_hash(["1", "2"], ["a", "b"])
        h2 = _row_hash(["2", "1"], ["a", "b"])
        assert h1 != h2


class TestBronzeMetaColumns:

    def test_count(self):
        assert len(BRONZE_META_COLUMNS) == 7

    def test_all_present(self):
        expected = {
            "_source_file_name",
            "_source_file_sha256",
            "_source_header_sha256",
            "_source_manifest_sha256",
            "_source_snapshot_id",
            "_bronze_schema_version",
            "_raw_row_sha256",
        }
        assert set(BRONZE_META_COLUMNS) == expected

    def test_schema_version(self):
        assert BRONZE_SCHEMA_VERSION == 1
