#!/bin/bash
# Run the RiskCloud MVP demo on VM
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
fi

REPO_DIR="${RISKCLOUD_REPO_DIR:-${HOME}/LeakBench-RiskCloud}"
cd "$REPO_DIR"

LOG_DIR="${REPO_DIR}/data/artifacts/deployment"
mkdir -p "$LOG_DIR"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="${LOG_DIR}/cloud-demo-${TIMESTAMP}.log"

echo "=== Running RiskCloud Demo ==="
docker compose run --rm riskcloud-demo 2>&1 | tee "$LOG_FILE"
ln -sfn "$(basename "$LOG_FILE")" "${LOG_DIR}/latest.log"

echo "=== Demo complete ==="
echo "Log: $LOG_FILE"
echo "Warehouse: data/warehouse"
echo "Artifacts: data/artifacts"
