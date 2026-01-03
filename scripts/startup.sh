#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

step() {
  echo "[startup] $1"
}

cd "$ROOT_DIR"

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  step "Creating .env from .env.example"
  if [[ -f "$ROOT_DIR/.env.example" ]]; then
    cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
  else
    echo "[startup] ERROR: .env.example not found"
    exit 1
  fi
fi

if [[ ! -x "$ROOT_DIR/.venv/bin/python" ]]; then
  echo "[startup] ERROR: .venv not found. Run: make install"
  exit 1
fi

step "Running ingest via venv"
make ingest

step "Refreshing dimensions via venv"
make dims

step "Building DuckDB bridge via venv"
make duckdb

step "Starting Superset (Docker)"
if command -v docker >/dev/null 2>&1; then
  make superset-up
  step "Building Superset DuckDB bridge (container paths)"
  make duckdb-superset
else
  echo "[startup] WARNING: docker not found in PATH; skipping Superset startup"
fi

step "Next steps:"
echo "  Superset: make superset-up (http://localhost:8088)"
echo "  Superset URL: http://localhost:8088"
echo "  Legacy Dash: make app OR docker compose up chicago_crime_app"
