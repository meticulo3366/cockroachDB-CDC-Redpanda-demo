#!/usr/bin/env bash
# Tear down the CHANGEFEED demo. Pass --keep-data to preserve volumes.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" = "--keep-data" ]; then
  echo "==> Stopping stack (keeping volumes)..."
  docker compose down
else
  echo "==> Stopping stack and removing volumes..."
  docker compose down -v
fi
echo "==> Done."
