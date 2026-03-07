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
  echo "Temporal CLI already installed: $(temporal version)"
else
  echo "Installing Temporal CLI..."
  go install github.com/temporalio/cli/cmd/temporal@latest
  echo "Temporal CLI installed: $(temporal version)"
fi
