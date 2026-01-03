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

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  step "Creating .env from .env.example"
  if [[ -f "$ROOT_DIR/.env.example" ]]; then
    cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
  else
    echo "[startup] ERROR: .env.example not found"
    exit 1
  fi
fi

step "Building Docker images"
cd "$ROOT_DIR"
docker compose build

step "Running data ingest container"
ingest_env=()
if find "$ROOT_DIR/data/lake" -name "*.parquet" -print -quit >/dev/null 2>&1; then
  step "Existing lake data found; ingesting only new records (no backfill)"
  ingest_env=(-e BACKFILL_DAYS=0)
fi
docker compose run --rm "${ingest_env[@]}" chicago_crime_ingest

step "Starting Dash app (Docker)"
step "Tip: Use DASH_PORT in .env to override the default port if needed."
docker compose up -d chicago_crime_app

step "App container status"
docker compose ps chicago_crime_app

step "App logs (last 50 lines)"
docker compose logs --tail=50 chicago_crime_app

dash_host="$(grep -m1 '^DASH_HOST=' "$ROOT_DIR/.env" 2>/dev/null | cut -d= -f2- || true)"
dash_port="$(grep -m1 '^DASH_PORT=' "$ROOT_DIR/.env" 2>/dev/null | cut -d= -f2- || true)"
dash_host="${dash_host:-0.0.0.0}"
dash_port="${dash_port:-8050}"
if [[ "$dash_host" == "0.0.0.0" ]]; then
  dash_host="localhost"
fi
step "App available at http://$dash_host:$dash_port/"
