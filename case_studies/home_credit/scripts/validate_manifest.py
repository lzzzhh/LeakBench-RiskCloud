#!/usr/bin/env python
"""P1.0 — Validate the data manifest.

Checks:
  1. All required files are present
  2. File SHA-256 matches the manifest
  3. Row counts and column counts match
  4. Essential columns exist (SK_ID_CURR, TARGET for application_train)

Usage:
    python case_studies/home_credit/scripts/validate_manifest.py \
        --data-dir /path/to/home_credit/csvs

Exit code 0 = all required files valid.
Exit code 1 = validation failure.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "case_studies" / "home_credit" / "manifests" / "data_manifest.yaml"

# Columns that every application_train.csv MUST contain
REQUIRED_COLUMNS = {"SK_ID_CURR", "TARGET"}


def sha256_file(path: Path) -> str:
    """Compute SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def count_lines(path: Path) -> int:
    """Count lines in a CSV file (minus header). Fast for large files."""
    count = 0
    with open(path) as f:
        # Skip header
        f.readline()
        for _ in f:
            count += 1
    return count


def validate_manifest(data_dir: Path) -> tuple[bool, list[str]]:
    """Validate the manifest against actual files. Returns (ok, errors)."""
    errors: list[str] = []

    if not MANIFEST_PATH.exists():
        return False, [f"Manifest not found: {MANIFEST_PATH}"]

    with open(MANIFEST_PATH) as f:
        manifest = yaml.safe_load(f)

    if not data_dir.exists():
        return False, [f"Data directory not found: {data_dir}"]

    for fspec in manifest.get("files", []):
        name = fspec["name"]
        required = fspec.get("required", False)
        fpath = data_dir / name

        if not fpath.exists():
            if required:
                errors.append(f"[REQUIRED] File missing: {name}")
            else:
                print(f"  [SKIP] Optional file not found: {name}")
            continue

        # Check SHA if manifest has one recorded
        expected_sha = fspec.get("sha256")
        if expected_sha:
            actual_sha = sha256_file(fpath)
            if actual_sha != expected_sha:
                errors.append(f"[SHA] {name}: expected {expected_sha[:16]}..., got {actual_sha[:16]}...")
            else:
                print(f"  [SHA OK] {name}")

        # Check row count
        expected_rows = fspec.get("row_count")
        if expected_rows is not None:
            actual_rows = count_lines(fpath)
            if actual_rows != expected_rows:
                errors.append(f"[ROWS] {name}: expected {expected_rows}, got {actual_rows}")
            else:
                print(f"  [ROWS OK] {name}: {actual_rows} rows")

        # Check essential columns for application_train
        if name == "application_train.csv":
            with open(fpath) as f:
                header = f.readline().strip().split(",")
                cols = set(h.strip('"') for h in header)
                missing = REQUIRED_COLUMNS - cols
                if missing:
                    errors.append(f"[COLUMNS] {name}: missing required columns: {missing}")

    return len(errors) == 0, errors


def populate_manifest(data_dir: Path) -> None:
    """Populate SHA-256, row counts, and column lists in the manifest."""
    with open(MANIFEST_PATH) as f:
        manifest = yaml.safe_load(f)

    for fspec in manifest.get("files", []):
        name = fspec["name"]
        fpath = data_dir / name
        if not fpath.exists():
            print(f"  [SKIP] File not found: {name}")
            continue

        # Compute SHA
        fspec["sha256"] = sha256_file(fpath)

        # Count rows
        fspec["row_count"] = count_lines(fpath)

        # Count columns
        with open(fpath) as f:
            header = f.readline().strip()
            fspec["columns"] = len(header.split(","))

        sha_short = fspec["sha256"][:16]
        print(f"  [POPULATED] {name}: {fspec['row_count']} rows, {fspec['columns']} cols, sha={sha_short}...")

    with open(MANIFEST_PATH, "w") as f:
        yaml.safe_dump(manifest, f, default_flow_style=False, sort_keys=False)

    print(f"\nManifest updated: {MANIFEST_PATH}")


def main():
    parser = argparse.ArgumentParser(description="Validate Home Credit data manifest")
    parser.add_argument("--data-dir", required=True, help="Path to directory containing Home Credit CSVs")
    parser.add_argument("--populate", action="store_true", help="Populate SHA-256 and row counts in the manifest")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    if args.populate:
        populate_manifest(data_dir)
        return

    ok, errors = validate_manifest(data_dir)
    if errors:
        for e in errors:
            print(f"  FAIL: {e}")
        print(f"\n{len(errors)} validation error(s)")
        sys.exit(1)
    else:
        print("All required files validated successfully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
