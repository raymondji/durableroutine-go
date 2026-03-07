#!/bin/bash
# Setup script for Claude Code web environment.
# Installs the Temporal CLI so integration tests can run against a local server.
# Configure this file as the "Setup script" in your Claude Code web environment settings.

set -euo pipefail

# Ensure ~/go/bin is on PATH
if ! echo "$PATH" | grep -q "$HOME/go/bin"; then
  export PATH="$HOME/go/bin:$PATH"
  if ! grep -q 'export PATH="$HOME/go/bin:$PATH"' "$HOME/.bashrc" 2>/dev/null; then
    echo 'export PATH="$HOME/go/bin:$PATH"' >> "$HOME/.bashrc"
  fi
fi

# Install Temporal CLI if not already present
if command -v temporal &> /dev/null; then
  echo "Temporal CLI already installed: $(temporal --version)"
else
  echo "Installing Temporal CLI..."
  # Fetch the latest version tag from GitHub (temporal.download is blocked in some environments)
  TEMPORAL_VERSION=$(curl -sL "https://api.github.com/repos/temporalio/cli/releases/latest" | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'].lstrip('v'))")
  curl -sL "https://github.com/temporalio/cli/releases/download/v${TEMPORAL_VERSION}/temporal_cli_${TEMPORAL_VERSION}_linux_amd64.tar.gz" -o /tmp/temporal.tar.gz
  tar -xzf /tmp/temporal.tar.gz -C /tmp/
  mv /tmp/temporal "$HOME/go/bin/temporal"
  chmod +x "$HOME/go/bin/temporal"
  rm -f /tmp/temporal.tar.gz
  echo "Temporal CLI installed: $(temporal --version)"
fi
