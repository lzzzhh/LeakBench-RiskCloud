#!/bin/bash
# Verify RiskCloud deployment on VM — exits non-zero on any failure
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
fi

REPO_DIR="${RISKCLOUD_REPO_DIR:-${HOME}/LeakBench-RiskCloud}"
cd "$REPO_DIR"
failed=0

echo "=== Verifying RiskCloud Deployment ==="
docker --version

required_files=(
    "data/artifacts/demo/bronze/bronze_receipt.yaml"
    "data/artifacts/demo/silver/silver_receipt.yaml"
    "data/artifacts/demo/prediction_points/prediction_points_receipt.yaml"
    "data/artifacts/demo/features/features_receipt.yaml"
    "data/artifacts/demo/woe/woe_rules.yaml"
    "data/artifacts/deployment/latest.log"
)

for f in "${required_files[@]}"; do
    if [[ -s "$f" ]]; then
        echo "  [OK] $f"
    else
        echo "  [MISSING OR EMPTY] $f" >&2
        failed=1
    fi
done

if [[ -d data/warehouse ]] && find data/warehouse -type f -print -quit | grep -q .; then
    echo "  [OK] warehouse contains files"
else
    echo "  [MISSING OR EMPTY] warehouse" >&2
    failed=1
fi

LATEST_LOG="data/artifacts/deployment/latest.log"
grep -q "Prediction Points: 30" "$LATEST_LOG" || { echo "  [FAIL] Prediction Points" >&2; failed=1; }
grep -q "Feature Values: 600" "$LATEST_LOG" || { echo "  [FAIL] Feature Values" >&2; failed=1; }
grep -q "Feature IDs: 20" "$LATEST_LOG" || { echo "  [FAIL] Feature IDs" >&2; failed=1; }
grep -Eq "WOE Rules: [1-9][0-9]*" "$LATEST_LOG" || { echo "  [FAIL] WOE Rules" >&2; failed=1; }

if [[ "$failed" -ne 0 ]]; then
    echo "Cloud deployment verification FAILED" >&2
    exit 1
fi

echo "Cloud deployment verification PASSED"
