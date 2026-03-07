#!/bin/bash
# SessionStart hook: ensure Temporal CLI is installed and dev server is running.
# This script is idempotent — safe to run multiple times.
set -euo pipefail

# Ensure ~/go/bin is on PATH
export PATH="$HOME/go/bin:$PATH"

# Install Temporal CLI if not already present
if ! command -v temporal &> /dev/null; then
  echo "Installing Temporal CLI..."
  mkdir -p "$HOME/go/bin"
  TEMPORAL_VERSION=$(curl -sL "https://api.github.com/repos/temporalio/cli/releases/latest" | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'].lstrip('v'))")
  curl -sL "https://github.com/temporalio/cli/releases/download/v${TEMPORAL_VERSION}/temporal_cli_${TEMPORAL_VERSION}_linux_amd64.tar.gz" | tar -xz -C "$HOME/go/bin/" temporal
  echo "Temporal CLI installed: $(temporal --version)"
else
  echo "Temporal CLI already installed: $(temporal --version)"
fi

# Check if Temporal dev server is already listening on port 7233 (gRPC, not HTTP)
check_port() {
  (echo > /dev/tcp/localhost/7233) 2>/dev/null
}

if check_port; then
  echo "Temporal dev server already running on port 7233."
  exit 0
fi

echo "Starting Temporal dev server..."
nohup temporal server start-dev --port 7233 --headless > /tmp/temporal-dev-server.log 2>&1 &

# Wait for the server to be ready (up to 30s)
for i in $(seq 1 30); do
  if check_port; then
    echo "Temporal dev server is ready."
    exit 0
  fi
  sleep 1
done

echo "Warning: Temporal dev server did not become ready within 30s. Check /tmp/temporal-dev-server.log"
exit 1
