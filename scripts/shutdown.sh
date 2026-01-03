#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

step() {
  echo "[shutdown] $1"
}

step "Stopping Docker containers"
cd "$ROOT_DIR"
docker compose down || true
