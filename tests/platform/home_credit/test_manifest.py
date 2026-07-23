"""P1.0 tests — Data manifest validation."""

from __future__ import annotations

import tempfile
from pathlib import Path

from case_studies.home_credit.scripts.validate_manifest import (
    _validate_manifest_structure,
    populate_manifest,
    validate_manifest,
)


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(header + "\n")
        for r in rows:
            f.write(r + "\n")


class TestManifestStructure:

    def test_valid_structure_passes(self):
        manifest = {
            "files": [
                {"name": "application_train.csv", "required": True},
            ],
        }
        assert _validate_manifest_structure(manifest) == []

    def test_missing_files_key(self):
        assert len(_validate_manifest_structure({})) >= 1

    def test_files_not_a_list(self):
        assert len(_validate_manifest_structure({"files": "not-a-list"})) >= 1

    def test_file_entry_missing_name(self):
        manifest = {"files": [{"required": True}]}
        errors = _validate_manifest_structure(manifest)
        assert any("name" in e.lower() for e in errors)

    def test_file_entry_empty_name(self):
        manifest = {"files": [{"name": "", "required": True}]}
        errors = _validate_manifest_structure(manifest)
        assert any("name" in e.lower() for e in errors)

    def test_file_entry_not_a_dict(self):
        manifest = {"files": ["not-a-dict"]}
        errors = _validate_manifest_structure(manifest)
        assert len(errors) >= 1


class TestValidateManifestFailClosed:

    def test_required_file_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            # application_train.csv does NOT exist
            ok, errors = validate_manifest(data_dir)
            assert not ok
            assert any("MISSING" in e for e in errors)

    def test_required_metadata_null_fails(self):
        """When manifest has null SHA/rows/cols but files exist, validation fails."""
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            _write_csv(data_dir / "application_train.csv", "SK_ID_CURR,TARGET", ["1,0"])
            _write_csv(data_dir / "bureau.csv", "SK_ID_CURR,SK_ID_BUREAU", ["1,100"])
            _write_csv(data_dir / "bureau_balance.csv", "SK_ID_BUREAU,MONTHS_BALANCE", ["100,0"])
            ok, errors = validate_manifest(data_dir)
            # Not ok because either null metadata or SHA/header/col mismatch
            assert not ok
            error_text = "|".join(errors).lower()
            assert any(kw in error_text for kw in ["null", "metadata", "sha", "header", "col"])

    def test_row_count_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            _write_csv(data_dir / "application_train.csv", "SK_ID_CURR,TARGET", ["1,0"])
            _write_csv(data_dir / "bureau.csv", "SK_ID_BUREAU", ["100"])
            _write_csv(data_dir / "bureau_balance.csv", "SK_ID_BUREAU,MONTHS_BALANCE", ["100,0"])
            # Populate first
            ok = populate_manifest(data_dir)
            assert ok
            # Now add a row and validate
            _write_csv(data_dir / "bureau.csv", "SK_ID_BUREAU", ["100", "200"])
            ok, errors = validate_manifest(data_dir)
            assert not ok
            assert any("bureau" in e.lower() and "row" in e.lower() for e in errors)

    def test_column_count_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            _write_csv(data_dir / "application_train.csv", "SK_ID_CURR,TARGET", ["1,0"])
            _write_csv(data_dir / "bureau.csv", "SK_ID_BUREAU", ["100"])
            _write_csv(data_dir / "bureau_balance.csv", "SK_ID_BUREAU,MONTHS_BALANCE", ["100,0"])
            ok = populate_manifest(data_dir)
            assert ok
            # Change columns
            _write_csv(data_dir / "bureau.csv", "SK_ID_BUREAU,EXTRA_COL", ["100,0"])
            ok, errors = validate_manifest(data_dir)
            assert not ok
            assert any("bureau" in e.lower() and "col" in e.lower() for e in errors)

    def test_header_sha_drift_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            _write_csv(data_dir / "application_train.csv", "SK_ID_CURR,TARGET", ["1,0"])
            _write_csv(data_dir / "bureau.csv", "SK_ID_BUREAU", ["100"])
            _write_csv(data_dir / "bureau_balance.csv", "SK_ID_BUREAU,MONTHS_BALANCE", ["100,0"])
            ok = populate_manifest(data_dir)
            assert ok
            # Change header
            _write_csv(data_dir / "bureau.csv", "SK_ID_BUREAU_RENAMED", ["100"])
            ok, errors = validate_manifest(data_dir)
            assert not ok
            assert any("header" in e.lower() for e in errors)


class TestManifestPopulate:

    def test_populate_required_missing_fails(self):
        """Populate fails when a required file is missing."""
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            ok = populate_manifest(data_dir)
            assert not ok

    def test_populate_succeeds_and_validates(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            _write_csv(data_dir / "application_train.csv", "SK_ID_CURR,TARGET", ["1,0"])
            _write_csv(data_dir / "bureau.csv", "SK_ID_BUREAU", ["100"])
            _write_csv(data_dir / "bureau_balance.csv", "SK_ID_BUREAU,MONTHS_BALANCE", ["100,0"])
            ok = populate_manifest(data_dir)
            assert ok
            # Validate after populate
            ok2, errors = validate_manifest(data_dir)
            assert ok2, errors
