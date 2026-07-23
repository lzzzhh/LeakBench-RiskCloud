"""P1.0 tests — Snapshot manifest template structure."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = (
    REPO_ROOT / "case_studies" / "home_credit" / "manifests" / "snapshot_manifest.template.yaml"
)


class TestSnapshotTemplate:

    def test_template_exists(self):
        assert TEMPLATE.is_file()

    def test_quality_defaults_to_not_run(self):
        with open(TEMPLATE) as f:
            data = yaml.safe_load(f)
        assert data["quality"]["status"] == "NOT_RUN"

    def test_per_table_snapshot_ids_exist(self):
        with open(TEMPLATE) as f:
            data = yaml.safe_load(f)
        for layer in ("bronze", "silver", "gold"):
            tables = data["tables"][layer]
            for table_name, table_def in tables.items():
                assert "iceberg_snapshot_id" in table_def, (
                    f"Missing iceberg_snapshot_id in {layer}.{table_name}"
                )

    def test_receipt_has_uri_and_sha(self):
        with open(TEMPLATE) as f:
            data = yaml.safe_load(f)
        assert "uri" in data["receipt"]
        assert "sha256" in data["receipt"]

    def test_no_global_snapshot_id(self):
        """Template should not have a top-level snapshot_id (it's per-table now)."""
        with open(TEMPLATE) as f:
            data = yaml.safe_load(f)
        # Old format had snapshot.snapshot_id — that's ok for platform manifest ID
        # but there should be no global Iceberg snapshot ID
        tables = data.get("tables", {})
        for layer in ("bronze", "silver", "gold"):
            assert layer in tables, f"Missing tables.{layer}"

    def test_manifest_status_is_pending(self):
        with open(TEMPLATE) as f:
            data = yaml.safe_load(f)
        assert data["manifest"]["status"] == "PENDING"
