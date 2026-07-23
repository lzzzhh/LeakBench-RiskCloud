#!/usr/bin/env python
"""P1.0 — Validate the Home Credit data manifest (fail-closed).

Usage:
    python case_studies/home_credit/scripts/validate_manifest.py --data-dir /path/to/csvs
    python case_studies/home_credit/scripts/validate_manifest.py --data-dir /path/to/csvs --populate
    python case_studies/home_credit/scripts/validate_manifest.py \\
        --data-dir /path/to/csvs --manifest /tmp/my_manifest.yaml

Exit 0: all checks pass.
Exit 1: validation failure.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANIFEST = REPO_ROOT / "case_studies" / "home_credit" / "manifests" / "data_manifest.yaml"

SHA256_LEN = 64

# Required files for the first vertical slice
REQUIRED_FILES = {"application_train.csv", "bureau.csv", "bureau_balance.csv"}

# Columns each required file MUST contain
REQUIRED_COLUMNS = {
    "application_train.csv": {"SK_ID_CURR", "TARGET"},
    "bureau.csv": {"SK_ID_CURR", "SK_ID_BUREAU"},
    "bureau_balance.csv": {"SK_ID_BUREAU", "MONTHS_BALANCE", "STATUS"},
}

# Required metadata fields that must be non-null per file
REQUIRED_META = ("row_count", "columns", "sha256", "header_sha256")


# -----------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_header(path: Path) -> str:
    with open(path, "rb") as f:
        return _sha256(f.readline())


def _count_rows(path: Path) -> int:
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def _count_columns(path: Path) -> int:
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        return len(header) if header else 0


def _column_names(path: Path) -> list[str]:
    with open(path, newline="") as f:
        reader = csv.reader(f)
        return next(reader, [])


# -----------------------------------------------------------------
# Manifest structure validation
# -----------------------------------------------------------------

def _validate_manifest_structure(manifest: dict) -> list[str]:
    """Validate manifest shape. Returns list of error messages."""
    errors: list[str] = []

    files = manifest.get("files")
    if not isinstance(files, list):
        errors.append("manifest.files must be a list")
        return errors

    if len(files) == 0:
        errors.append("manifest.files must contain at least one entry")

    seen_names: set[str] = set()
    for i, fspec in enumerate(files):
        prefix = f"manifest.files[{i}]"
        if not isinstance(fspec, dict):
            errors.append(f"{prefix} must be a dict, got {type(fspec).__name__}")
            continue

        name = fspec.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append(f"{prefix}.name must be a non-empty string")
            continue

        # Reject path traversal
        if "/" in name or "\\" in name:
            errors.append(f"{prefix}.name must be a plain filename, not a path")

        if name in seen_names:
            errors.append(f"duplicate file name: {name}")
        seen_names.add(name)

        required = fspec.get("required")
        if not isinstance(required, bool):
            errors.append(f"{prefix}.required must be a boolean")

        # Required files must have required=true
        if name in REQUIRED_FILES and required is not True:
            errors.append(f"{prefix} is a required file but required != true")

    # Every required file must appear
    for req_file in REQUIRED_FILES:
        if req_file not in seen_names:
            errors.append(f"required file missing from manifest: {req_file}")

    return errors


# -----------------------------------------------------------------
# Main validation
# -----------------------------------------------------------------

def validate_manifest(
    data_dir: Path,
    manifest_path: Path = DEFAULT_MANIFEST,
) -> tuple[bool, list[str]]:
    """Validate manifest against actual files. Returns (ok, errors)."""
    errors: list[str] = []

    if not manifest_path.exists():
        return False, [f"Manifest not found: {manifest_path}"]

    if not data_dir.exists() or not data_dir.is_dir():
        return False, [f"Data directory not found: {data_dir}"]

    try:
        with open(manifest_path) as f:
            manifest = yaml.safe_load(f)
    except Exception as exc:
        return False, [f"Failed to parse manifest: {exc}"]

    if not isinstance(manifest, dict):
        return False, ["Manifest must be a YAML dict"]

    errors.extend(_validate_manifest_structure(manifest))
    if errors:
        return False, errors

    for fspec in manifest.get("files", []):
        name = fspec.get("name", "")
        if not name:
            continue
        required = fspec.get("required", False)
        fpath = data_dir / name

        if not fpath.exists():
            if required:
                errors.append(f"[MISSING] Required file not found: {name}")
            continue

        # Required metadata must be non-null for in-scope files
        if required:
            for field in REQUIRED_META:
                val = fspec.get(field)
                if val is None:
                    errors.append(f"[METADATA] {name}: {field} is null — run --populate first")
                elif field in ("sha256", "header_sha256"):
                    if not isinstance(val, str) or len(val) != SHA256_LEN:
                        errors.append(f"[SHA] {name}: {field} must be {SHA256_LEN} hex chars")
                elif field == "row_count":
                    if not isinstance(val, int) or val < 0:
                        errors.append(f"[ROWS] {name}: row_count must be a non-negative integer")
                elif field == "columns":
                    if not isinstance(val, int) or val < 1:
                        errors.append(f"[COLS] {name}: columns must be a positive integer")

        # Compare actual vs expected
        expected_sha = fspec.get("sha256")
        if expected_sha is not None and isinstance(expected_sha, str) and len(expected_sha) == SHA256_LEN:
            actual = _sha256_file(fpath)
            if actual != expected_sha:
                errors.append(f"[SHA] {name}: mismatch")

        expected_header = fspec.get("header_sha256")
        if expected_header is not None and isinstance(expected_header, str) and len(expected_header) == SHA256_LEN:
            actual = _sha256_header(fpath)
            if actual != expected_header:
                errors.append(f"[HEADER] {name}: header line changed (schema drift)")

        expected_rows = fspec.get("row_count")
        if isinstance(expected_rows, int) and expected_rows >= 0:
            actual = _count_rows(fpath)
            if actual != expected_rows:
                errors.append(f"[ROWS] {name}: expected {expected_rows}, got {actual}")

        expected_cols = fspec.get("columns")
        if isinstance(expected_cols, int) and expected_cols >= 1:
            actual = _count_columns(fpath)
            if actual != expected_cols:
                errors.append(f"[COLS] {name}: expected {expected_cols} cols, got {actual}")

        # Required columns
        if name in REQUIRED_COLUMNS and fpath.exists():
            actual_cols = set(_column_names(fpath))
            missing = REQUIRED_COLUMNS[name] - actual_cols
            if missing:
                errors.append(f"[COLUMNS] {name}: missing required columns: {sorted(missing)}")

    return len(errors) == 0, errors


# -----------------------------------------------------------------
# Populate
# -----------------------------------------------------------------

def populate_manifest(
    data_dir: Path,
    manifest_path: Path = DEFAULT_MANIFEST,
) -> bool:
    """Populate SHA-256, row counts, column counts. Returns True on success."""
    if not data_dir.exists():
        print(f"ERROR: Data directory does not exist: {data_dir}")
        return False

    if not manifest_path.exists():
        print(f"ERROR: Manifest not found: {manifest_path}")
        return False

    try:
        with open(manifest_path) as f:
            manifest = yaml.safe_load(f)
    except Exception as exc:
        print(f"ERROR: Failed to load manifest: {exc}")
        return False

    if not isinstance(manifest, dict):
        print("ERROR: Manifest must be a YAML dict")
        return False

    structure_errors = _validate_manifest_structure(manifest)
    if structure_errors:
        for e in structure_errors:
            print(f"  ERROR: {e}")
        return False

    had_required_missing = False

    for fspec in manifest.get("files", []):
        name = fspec.get("name", "")
        if not name:
            continue
        required = fspec.get("required", False)
        fpath = data_dir / name

        if not fpath.exists():
            if required:
                print(f"  ERROR: Required file missing: {name}")
                had_required_missing = True
            else:
                print(f"  [SKIP] Optional file not found: {name}")
            continue

        fspec["sha256"] = _sha256_file(fpath)
        fspec["header_sha256"] = _sha256_header(fpath)
        fspec["row_count"] = _count_rows(fpath)
        fspec["columns"] = _count_columns(fpath)

        sha_short = fspec["sha256"][:16]
        print(f"  [OK] {name}: {fspec['row_count']} rows, {fspec['columns']} cols, sha={sha_short}...")

    if had_required_missing:
        print("\nERROR: One or more required files are missing. Manifest not saved.")
        return False

    with open(manifest_path, "w") as f:
        yaml.safe_dump(manifest, f, default_flow_style=False, sort_keys=False)

    print(f"\nManifest updated: {manifest_path}")
    return True


# -----------------------------------------------------------------
# CLI
# -----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate Home Credit data manifest")
    parser.add_argument("--data-dir", required=True, help="Path to directory containing Home Credit CSVs")
    parser.add_argument("--manifest", default=None, help="Path to manifest YAML (default: repo manifest)")
    parser.add_argument("--populate", action="store_true",
                        help="Populate SHA-256 and row counts, then validate")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    manifest_path = Path(args.manifest) if args.manifest else DEFAULT_MANIFEST

    if args.populate:
        if not populate_manifest(data_dir, manifest_path):
            sys.exit(1)
        ok, errors = validate_manifest(data_dir, manifest_path)
        if not ok:
            for e in errors:
                print(f"  FAIL: {e}")
            sys.exit(1)
        print("Manifest populated and validated successfully.")
        sys.exit(0)

    ok, errors = validate_manifest(data_dir, manifest_path)
    if errors:
        for e in errors:
            print(f"  FAIL: {e}")
        print(f"\n{len(errors)} validation error(s)")
        sys.exit(1)

    print("All required files validated successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
