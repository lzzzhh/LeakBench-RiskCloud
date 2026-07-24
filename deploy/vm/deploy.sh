#!/bin/bash
# Deploy RiskCloud on VM — clone repo, build Docker image
set -euo pipefail

REPO_URL="${RISKCLOUD_REPO_URL:-https://github.com/lzzzhh/LeakBench-RiskCloud.git}"
REPO_DIR="${HOME}/LeakBench-RiskCloud"

echo "=== RiskCloud Deploy ==="

if [ -d "$REPO_DIR" ]; then
    echo "Updating existing repo..."
    cd "$REPO_DIR"
    git pull
else
    echo "Cloning repo..."
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
fi

echo "Building Docker image..."
docker compose build

echo "=== Deploy complete ==="
echo "Image built: $(docker images | grep riskcloud-demo | head -1)"
