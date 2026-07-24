#!/bin/bash
# Bootstrap a fresh Ubuntu VM for RiskCloud deployment
set -euo pipefail

echo "=== RiskCloud VM Bootstrap ==="

# Install Docker
if ! command -v docker &>/dev/null; then
    echo "Installing Docker..."
    sudo apt-get update -y
    sudo apt-get install -y docker.io docker-compose-v2
    sudo systemctl enable docker
    sudo systemctl start docker
    sudo usermod -aG docker "$USER"
fi

# Install git if needed
sudo apt-get install -y git curl

# Verify
docker --version
docker compose version
git --version

echo ""
echo "=== Bootstrap complete ==="
echo "Docker group membership was updated."
echo "Exit this SSH session and reconnect before running deploy.sh."
