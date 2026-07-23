#!/usr/bin/env python
"""P1.0 — Validate the Home Credit data manifest (fail-closed).

Checks:
  1. Manifest structure is valid (files list, required fields present)
  2. Required files exist on disk
  3. SHA-256 matches (both full file and header line)
  4. Row counts match
  5. Column counts match
  6. Essential columns exist (SK_ID_CURR, TARGET for application_train)

Usage:
    python case_studies/home_credit/scripts/validate_manifest.py --data-dir /path/to/csvs
    python case_studies/home_credit/scripts/validate_manifest.py --data-dir /path/to/csvs --populate

Exit 0: all required files valid (or successfully populated).
Exit 1: validation failure or populate error.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "case_studies" / "home_credit" / "manifests" / "data_manifest.yaml"

REQUIRED_APP_COLUMNS = {"SK_ID_CURR", "TARGET"}
SHA256_LEN = 64


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
    """SHA-256 of the first line (header) as raw bytes."""
    with open(path, "rb") as f:
        return _sha256(f.readline())


def _count_rows(path: Path) -> int:
    """Count data rows (excluding header). Uses csv.reader for robustness."""
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return 0
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
# Manifest validation
# -----------------------------------------------------------------

def _validate_manifest_structure(manifest: dict) -> list[str]:
    """Ensure the manifest has the expected top-level shape."""
    errors: list[str] = []
    files = manifest.get("files")
    if not isinstance(files, list):
        errors.append("manifest.files must be a list")
        return errors

    for i, fspec in enumerate(files):
        if not isinstance(fspec, dict):
            errors.append(f"manifest.files[{i}] must be a dict, got {type(fspec).__name__}")
            continue
        if not isinstance(fspec.get("name"), str) or not fspec["name"].strip():
            errors.append(f"manifest.files[{i}].name must be a non-empty string")

    return errors


def validate_manifest(data_dir: Path) -> tuple[bool, list[str]]:
    """Validate manifest against actual files. Returns (ok, errors)."""
    errors: list[str] = []

    if not MANIFEST_PATH.exists():
        return False, [f"Manifest not found: {MANIFEST_PATH}"]

    if not data_dir.exists() or not data_dir.is_dir():
        return False, [f"Data directory not found: {data_dir}"]

    try:
        with open(MANIFEST_PATH) as f:
            manifest = yaml.safe_load(f)
    except Exception as exc:
        return False, [f"Failed to parse manifest: {exc}"]

    if not isinstance(manifest, dict):
        return False, ["Manifest must be a YAML dict"]

    errors.extend(_validate_manifest_structure(manifest))
    if errors:
        return False, errors

    for fspec in manifest.get("files", []):
        name = fspec["name"]
        required = fspec.get("required", False)
        fpath = data_dir / name

        # Required files must exist
        if not fpath.exists():
            if required:
                errors.append(f"[MISSING] Required file not found: {name}")
            continue

        # Required metadata must be non-null
        if required:
            for field, label in [
                ("row_count", "row_count"), ("columns", "columns"),
                ("sha256", "sha256"), ("header_sha256", "header_sha256"),
            ]:
                val = fspec.get(field)
                if val is None:
                    errors.append(f"[METADATA] {name}: {label} is null — run --populate first")

        expected_sha = fspec.get("sha256")
        if expected_sha is not None:
            if not isinstance(expected_sha, str) or len(expected_sha) != SHA256_LEN:
                errors.append(f"[SHA] {name}: sha256 must be {SHA256_LEN} hex chars")
            else:
                actual = _sha256_file(fpath)
                if actual != expected_sha:
                    errors.append(f"[SHA] {name}: mismatch")

        expected_header = fspec.get("header_sha256")
        if expected_header is not None:
            if not isinstance(expected_header, str) or len(expected_header) != SHA256_LEN:
                errors.append(f"[HEADER] {name}: header_sha256 must be {SHA256_LEN} hex chars")
            else:
                actual = _sha256_header(fpath)
                if actual != expected_header:
                    errors.append(f"[HEADER] {name}: header line changed (schema drift)")

        expected_rows = fspec.get("row_count")
        if expected_rows is not None:
            if not isinstance(expected_rows, int) or expected_rows < 0:
                errors.append(f"[ROWS] {name}: row_count must be a non-negative integer")
            else:
                actual = _count_rows(fpath)
                if actual != expected_rows:
                    errors.append(f"[ROWS] {name}: expected {expected_rows}, got {actual}")

        expected_cols = fspec.get("columns")
        if expected_cols is not None:
            if not isinstance(expected_cols, int) or expected_cols < 1:
                errors.append(f"[COLS] {name}: columns must be a positive integer")
            else:
                actual = _count_columns(fpath)
                if actual != expected_cols:
                    errors.append(f"[COLS] {name}: expected {expected_cols} cols, got {actual}")

        # Essential columns for application_train
        if name == "application_train.csv" and fpath.exists():
            cols = set(_column_names(fpath))
            missing = REQUIRED_APP_COLUMNS - cols
            if missing:
                errors.append(f"[COLUMNS] {name}: missing required columns: {missing}")

    return len(errors) == 0, errors


# -----------------------------------------------------------------
# Populate
# -----------------------------------------------------------------

def populate_manifest(data_dir: Path) -> bool:
    """Populate SHA-256, row counts, and column counts. Returns True on success."""
    if not data_dir.exists():
        print(f"ERROR: Data directory does not exist: {data_dir}")
        return False

    try:
        with open(MANIFEST_PATH) as f:
            manifest = yaml.safe_load(f)
    except Exception as exc:
        print(f"ERROR: Failed to load manifest: {exc}")
        return False

    structure_errors = _validate_manifest_structure(manifest)
    if structure_errors:
        for e in structure_errors:
            print(f"  ERROR: {e}")
        return False

    had_required_missing = False

    for fspec in manifest.get("files", []):
        name = fspec["name"]
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

    with open(MANIFEST_PATH, "w") as f:
        yaml.safe_dump(manifest, f, default_flow_style=False, sort_keys=False)

    print(f"\nManifest updated: {MANIFEST_PATH}")
    return True


# -----------------------------------------------------------------
# CLI
# -----------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate Home Credit data manifest")
    parser.add_argument("--data-dir", required=True, help="Path to directory containing Home Credit CSVs")
    parser.add_argument(
        "--populate", action="store_true",
        help="Populate SHA-256 and row counts in the manifest, then validate",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    if args.populate:
        if not populate_manifest(data_dir):
            sys.exit(1)
        # After populating, validate
        ok, errors = validate_manifest(data_dir)
        if not ok:
            for e in errors:
                print(f"  FAIL: {e}")
            sys.exit(1)
        print("Manifest populated and validated successfully.")
        sys.exit(0)

    ok, errors = validate_manifest(data_dir)
    if errors:
        for e in errors:
            print(f"  FAIL: {e}")
        print(f"\n{len(errors)} validation error(s)")
        sys.exit(1)

    print("All required files validated successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
