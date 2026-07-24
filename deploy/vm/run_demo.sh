#!/bin/bash
# Run the RiskCloud MVP demo on VM
set -euo pipefail

REPO_DIR="${HOME}/LeakBench-RiskCloud"
cd "$REPO_DIR"

echo "=== Running RiskCloud Demo ==="
docker compose run --rm riskcloud-demo 2>&1 | tee /tmp/riskcloud-demo-$(date +%Y%m%d-%H%M%S).log

echo "=== Demo complete ==="
echo "Logs: /tmp/riskcloud-demo-*.log"
echo "Warehouse: data/warehouse"
echo "Artifacts: data/artifacts"
