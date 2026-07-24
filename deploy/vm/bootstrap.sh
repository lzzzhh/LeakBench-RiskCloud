#!/bin/bash
# Bootstrap a fresh Ubuntu VM for RiskCloud deployment
set -euo pipefail

echo "=== RiskCloud VM Bootstrap ==="

# Update system
sudo apt-get update -y
sudo apt-get upgrade -y

# Install Docker
if ! command -v docker &>/dev/null; then
    echo "Installing Docker..."
    sudo apt-get install -y docker.io docker-compose-v2
    sudo systemctl enable docker
    sudo systemctl start docker
    sudo usermod -aG docker "$USER"
    echo "Docker installed. You may need to re-login for group changes."
fi

# Install git if needed
sudo apt-get install -y git curl

echo "=== Bootstrap complete ==="
echo "Next: run deploy.sh to clone and build"
