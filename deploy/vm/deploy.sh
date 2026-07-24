#!/bin/bash
# Deploy RiskCloud on VM — clone repo, build Docker image
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
fi

REPO_URL="${RISKCLOUD_REPO_URL:-https://github.com/lzzzhh/LeakBench-RiskCloud.git}"
REPO_REF="${RISKCLOUD_REF:-main}"
REPO_DIR="${RISKCLOUD_REPO_DIR:-${HOME}/LeakBench-RiskCloud}"

echo "=== RiskCloud Deploy ==="

if ! docker info >/dev/null 2>&1; then
    echo "Docker is not available to the current user." >&2
    echo "Exit SSH and reconnect after bootstrap.sh." >&2
    exit 1
fi

if [ -d "$REPO_DIR" ]; then
    echo "Updating existing repo..."
    cd "$REPO_DIR"
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "Repository has uncommitted changes: $REPO_DIR" >&2
        exit 1
    fi
    git fetch origin "$REPO_REF"
    git checkout "$REPO_REF"
    git pull --ff-only origin "$REPO_REF"
else
    echo "Cloning repo..."
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
    git checkout "$REPO_REF"
fi

echo "Building Docker image..."
docker compose build
docker compose images

echo "=== Deploy complete ==="
