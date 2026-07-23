"""P1.0 tests — Data manifest validation (isolated, does not mutate production manifest)."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import yaml

from case_studies.home_credit.scripts.validate_manifest import (
    _validate_manifest_structure,
    populate_manifest,
    validate_manifest,
)

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "home_credit"
PRODUCTION_MANIFEST = (
    Path(__file__).resolve().parents[3] / "case_studies" / "home_credit" / "manifests" / "data_manifest.yaml"
)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _write_manifest(manifest_path: Path, files: list[dict]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {"files": files}
    with open(manifest_path, "w") as f:
        yaml.safe_dump(manifest, f)


class TestManifestStructure:
    def test_valid_passes(self):
        manifest = {
            "files": [
                {"name": "application_train.csv", "required": True},
                {"name": "bureau.csv", "required": True},
                {"name": "bureau_balance.csv", "required": True},
            ]
        }
        assert _validate_manifest_structure(manifest) == []

    def test_rejects_empty_files_list(self):
        errors = _validate_manifest_structure({"files": []})
        assert any("at least one" in e.lower() for e in errors)

    def test_rejects_missing_files_key(self):
        assert len(_validate_manifest_structure({})) >= 1

    def test_rejects_files_not_a_list(self):
        assert len(_validate_manifest_structure({"files": "x"})) >= 1

    def test_rejects_missing_name(self):
        errors = _validate_manifest_structure({"files": [{"required": True}]})
        assert any("name" in e.lower() for e in errors)

    def test_rejects_path_traversal(self):
        errors = _validate_manifest_structure(
            {
                "files": [
                    {"name": "../etc/passwd", "required": True},
                ]
            }
        )
        assert any("path" in e.lower() for e in errors)

    def test_rejects_duplicate_names(self):
        errors = _validate_manifest_structure(
            {
                "files": [
                    {"name": "a.csv", "required": True},
                    {"name": "a.csv", "required": True},
                ]
            }
        )
        assert any("duplicate" in e.lower() for e in errors)

    def test_rejects_required_not_bool(self):
        errors = _validate_manifest_structure(
            {
                "files": [
                    {"name": "application_train.csv", "required": "yes"},
                ]
            }
        )
        assert any("required" in e.lower() and "bool" in e.lower() for e in errors)

    def test_required_file_cannot_be_optional(self):
        """Required files must have required=true."""
        errors = _validate_manifest_structure(
            {
                "files": [
                    {"name": "application_train.csv", "required": False},
                    {"name": "bureau.csv", "required": False},
                    {"name": "bureau_balance.csv", "required": False},
                ]
            }
        )
        assert any("required" in e.lower() and "true" in e.lower() for e in errors)

    def test_required_files_must_all_exist(self):
        errors = _validate_manifest_structure(
            {
                "files": [
                    {"name": "application_train.csv", "required": True},
                ]
            }
        )
        assert any("bureau" in e.lower() for e in errors)


class TestValidateManifestFailClosed:
    def test_required_file_missing(self):
        """All required files in manifest, but one file physically missing."""
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            # Create application_train and bureau, but NOT bureau_balance
            (data_dir / "application_train.csv").write_text((FIXTURES / "application_train.csv").read_text())
            (data_dir / "bureau.csv").write_text((FIXTURES / "bureau.csv").read_text())
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            ok, errors = validate_manifest(data_dir, manifest_path)
            assert not ok
            error_text = "|".join(errors).lower()
            assert any(kw in error_text for kw in ["missing", "not found"])

    def test_metadata_null_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            # Copy fixture CSVs
            for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
                (data_dir / f).write_text((FIXTURES / f).read_text())
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            ok, errors = validate_manifest(data_dir, manifest_path)
            assert not ok
            error_text = "|".join(errors).lower()
            assert any(kw in error_text for kw in ["null", "metadata"])

    def test_row_count_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
                (data_dir / f).write_text((FIXTURES / f).read_text())
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            assert populate_manifest(data_dir, manifest_path)
            # Add a row, re-validate
            (data_dir / "bureau.csv").write_text("SK_ID_CURR,SK_ID_BUREAU\n1,100\n2,200\n")
            ok, errors = validate_manifest(data_dir, manifest_path)
            assert not ok
            assert any("bureau" in e.lower() and "row" in e.lower() for e in errors)

    def test_header_sha_drift_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
                (data_dir / f).write_text((FIXTURES / f).read_text())
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            assert populate_manifest(data_dir, manifest_path)
            (data_dir / "bureau.csv").write_text("SK_ID_BUREAU_RENAMED\n100\n")
            ok, errors = validate_manifest(data_dir, manifest_path)
            assert not ok
            assert any("header" in e.lower() for e in errors)

    def test_bureau_requires_sk_id_curr(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            (data_dir / "application_train.csv").write_text((FIXTURES / "application_train.csv").read_text())
            (data_dir / "bureau_balance.csv").write_text((FIXTURES / "bureau_balance.csv").read_text())
            # bureau missing SK_ID_CURR
            (data_dir / "bureau.csv").write_text("SK_ID_BUREAU\n100\n")
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            # populate will compute SHA/rows/cols but then validation fails → returns False
            ok = populate_manifest(data_dir, manifest_path)
            assert not ok, "populate should fail when required column is missing"

    def test_bureau_balance_requires_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            (data_dir / "application_train.csv").write_text((FIXTURES / "application_train.csv").read_text())
            (data_dir / "bureau.csv").write_text((FIXTURES / "bureau.csv").read_text())
            # bureau_balance missing STATUS
            (data_dir / "bureau_balance.csv").write_text("SK_ID_BUREAU,MONTHS_BALANCE\n100,0\n")
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            ok = populate_manifest(data_dir, manifest_path)
            assert not ok, "populate should fail when required column (STATUS) is missing"


class TestManifestPopulate:
    def test_populate_required_missing_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                ],
            )
            ok = populate_manifest(data_dir, manifest_path)
            assert not ok

    def test_populate_non_dict_manifest_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            manifest_path = Path(tmp) / "manifest.yaml"
            manifest_path.write_text("[]")  # list, not dict
            ok = populate_manifest(data_dir, manifest_path)
            assert not ok

    def test_populate_succeeds_and_validates(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
                (data_dir / f).write_text((FIXTURES / f).read_text())
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            assert populate_manifest(data_dir, manifest_path)
            ok, errors = validate_manifest(data_dir, manifest_path)
            assert ok, errors

    def test_failed_populate_preserves_manifest(self):
        """If populate fails, the manifest file must remain unchanged."""
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
                (data_dir / f).write_text((FIXTURES / f).read_text())
            # bureau_balance missing STATUS column
            with open(data_dir / "bureau_balance.csv", "w") as f:
                f.write("SK_ID_BUREAU,MONTHS_BALANCE\n100,0\n")
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            before = _sha256_file(manifest_path)
            ok = populate_manifest(data_dir, manifest_path)
            assert not ok  # Should fail due to missing STATUS column
            after = _sha256_file(manifest_path)
            assert after == before, "Failed populate mutated the manifest!"

    def test_default_manifest_is_internally_consistent(self):
        """The checked-in manifest must have null metadata (template state).
        If someone populates with real data, the columns must satisfy the
        required columns contract."""
        import yaml

        data = yaml.safe_load(PRODUCTION_MANIFEST.read_text())
        specs = {f["name"]: f for f in data["files"]}

        from case_studies.home_credit.scripts.validate_manifest import (
            REQUIRED_COLUMNS,
            REQUIRED_FILES,
        )

        for rf in REQUIRED_FILES:
            spec = specs.get(rf)
            assert spec is not None, f"Required file {rf} missing from manifest"
            assert spec.get("required") is True, f"{rf} must have required: true"

            # If metadata is populated, it must be internally consistent
            cols = spec.get("columns")
            if cols is not None:
                min_cols = len(REQUIRED_COLUMNS[rf])
                assert cols >= min_cols, (
                    f"{rf}: columns={cols} but requires {min_cols} columns ({REQUIRED_COLUMNS[rf]})"
                )


class TestProductionManifestNotMutated:
    def test_unit_tests_do_not_mutate_production_manifest(self):
        """Running tests must not modify the production manifest."""
        before = _sha256_file(PRODUCTION_MANIFEST)
        # Run a round-trip on a temp manifest
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            data_dir.mkdir()
            for f in ["application_train.csv", "bureau.csv", "bureau_balance.csv"]:
                (data_dir / f).write_text((FIXTURES / f).read_text())
            manifest_path = Path(tmp) / "manifest.yaml"
            _write_manifest(
                manifest_path,
                [
                    {"name": "application_train.csv", "required": True},
                    {"name": "bureau.csv", "required": True},
                    {"name": "bureau_balance.csv", "required": True},
                ],
            )
            validate_manifest(data_dir, manifest_path)
        after = _sha256_file(PRODUCTION_MANIFEST)
        assert after == before, "Production manifest was mutated by tests!"
