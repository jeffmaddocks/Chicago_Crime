#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

step() {
  echo "[startup] $1"
}

step "Checking Docker availability"
if ! command -v docker >/dev/null 2>&1; then
  echo "[startup] ERROR: docker not found in PATH"
  exit 1
fi

step "Building Docker images"
cd "$ROOT_DIR"
docker compose build

step "Running data ingest container"
docker compose run --rm chicago_crime_ingest

step "Starting Dash app (Docker)"
step "Tip: Use DASH_PORT in .env to override the default port if needed."
docker compose up -d chicago_crime_app

step "App container status"
docker compose ps chicago_crime_app
