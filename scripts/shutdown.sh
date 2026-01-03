#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

step() {
  echo "[shutdown] $1"
}

step "Stopping legacy Dash containers (if any)"
cd "$ROOT_DIR"
docker compose down || true

step "Stopping Superset containers (if any)"
docker compose -f docker-compose.superset.yml down || true
