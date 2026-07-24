#!/bin/bash
# Verify RiskCloud deployment on VM
set -euo pipefail

REPO_DIR="${HOME}/LeakBench-RiskCloud"
cd "$REPO_DIR"

echo "=== Verifying RiskCloud Deployment ==="

# Check Docker
docker --version

# Check artifacts
for f in \
    data/artifacts/demo/bronze/bronze_receipt.yaml \
    data/artifacts/demo/silver/silver_receipt.yaml \
    data/artifacts/demo/prediction_points/prediction_points_receipt.yaml \
    data/artifacts/demo/features/features_receipt.yaml \
    data/artifacts/demo/woe/woe_rules.yaml; do
    if [ -f "$f" ]; then
        echo "  [OK] $f"
    else
        echo "  [MISSING] $f"
    fi
done

# Check warehouse
if [ -d "data/warehouse" ]; then
    echo "  [OK] warehouse exists"
else
    echo "  [MISSING] warehouse"
fi

echo "=== Verification complete ==="
