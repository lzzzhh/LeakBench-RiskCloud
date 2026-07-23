#!/usr/bin/env python
"""P1.2 — Run Bronze ingestion CLI.

Usage:
    python -m case_studies.home_credit.scripts.run_bronze \
        --data-dir ~/data/home_credit \
        --manifest case_studies/home_credit/manifests/data_manifest.yaml \
        --warehouse /tmp/riskcloud_warehouse \
        --run-id p12-local-001
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
UTC = timezone.utc


def _get_git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=REPO_ROOT,
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""


def main():
    parser = argparse.ArgumentParser(description="Home Credit Bronze Ingestion")
    parser.add_argument("--data-dir", required=True, help="Path to CSV directory")
    parser.add_argument("--manifest", required=True, help="Path to data_manifest.yaml")
    parser.add_argument("--warehouse", required=True, help="Iceberg warehouse path")
    parser.add_argument("--run-id", required=True, help="Unique run identifier")
    parser.add_argument(
        "--receipt-root",
        default=None,
        help="Receipt output dir (default: case_studies/home_credit/manifests/runs/<run_id>)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    manifest_path = Path(args.manifest)
    warehouse = args.warehouse
    run_id = args.run_id
    git_commit = _get_git_commit()

    # Config
    config_path = REPO_ROOT / "case_studies" / "home_credit" / "configs" / "bronze_v1.yaml"
    from case_studies.home_credit.pipelines.bronze_ingestion import BronzeConfig, ingest_bronze

    config = BronzeConfig.from_yaml(config_path)

    # Receipt dir
    if args.receipt_root:
        receipt_dir = Path(args.receipt_root) / run_id
    else:
        receipt_dir = REPO_ROOT / "case_studies" / "home_credit" / "manifests" / "runs" / run_id

    print(f"Bronze ingestion: {run_id}")
    print(f"  manifest: {manifest_path}")
    print("  manifest SHA: computing...")

    try:
        receipt = ingest_bronze(
            config=config,
            data_dir=data_dir,
            manifest_path=manifest_path,
            receipt_dir=receipt_dir,
            run_id=run_id,
            git_commit=git_commit,
            warehouse=warehouse,
        )
        print(f"  manifest SHA: {receipt['input']['manifest_sha256']}")
        print(f"  source_snapshot_id: {receipt['input']['source_snapshot_id']}")
        for tbl_name, tres in receipt["tables"].items():
            print(
                f"  {tbl_name}: {tres['source_row_count']} source → "
                f"{tres['bronze_row_count']} Bronze rows, "
                f"snapshot={tres['iceberg_snapshot_id']}, "
                f"fingerprint={tres['content_multiset_sha256'][:16]}..."
            )
        print(f"  receipt: {receipt_dir / 'bronze_receipt.yaml'}")
        print("  status: PASS")
        sys.exit(0)
    except Exception as exc:
        print(f"  FAIL: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
