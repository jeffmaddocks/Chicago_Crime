#!/usr/bin/env bash
set -euo pipefail

SUP_BIN="/app/.venv/bin/superset"
PY_BIN="/app/.venv/bin/python"

db_uri="${SQLALCHEMY_DATABASE_URI:-postgresql+psycopg2://superset:superset@superset_db:5432/superset}"
redis_host="${REDIS_HOST:-superset_redis}"
redis_port="${REDIS_PORT:-6379}"

read -r db_host db_port <<<"$("$PY_BIN" - "$db_uri" <<'PY'
import sys
from urllib.parse import urlparse

uri = sys.argv[1] if len(sys.argv) > 1 else ""
parsed = urlparse(uri)
host = parsed.hostname or "superset_db"
port = parsed.port or 5432
print(host, port)
PY
)"

wait_for() {
  local host="$1"
  local port="$2"
  local name="$3"
  for _ in $(seq 1 60); do
    if "$PY_BIN" - <<'PY' "$host" "$port"
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
sock = socket.socket()
sock.settimeout(1.0)
try:
    sock.connect((host, port))
    sys.exit(0)
except Exception:
    sys.exit(1)
finally:
    sock.close()
PY
    then
      echo "[superset-init] ${name} is ready"
      return 0
    fi
    echo "[superset-init] Waiting for ${name}..."
    sleep 1
  done
  echo "[superset-init] ERROR: ${name} not ready"
  return 1
}

wait_for "$db_host" "$db_port" "Postgres"
wait_for "$redis_host" "$redis_port" "Redis"

if [[ -x "$SUP_BIN" ]]; then
  "$SUP_BIN" db upgrade
else
  "$PY_BIN" -m superset db upgrade
fi

if [[ -x "$SUP_BIN" ]]; then
  "$SUP_BIN" fab create-admin \
    --username "${ADMIN_USERNAME:-admin}" \
    --firstname "${ADMIN_FIRSTNAME:-Superset}" \
    --lastname "${ADMIN_LASTNAME:-Admin}" \
    --email "${ADMIN_EMAIL:-admin@example.com}" \
    --password "${ADMIN_PASSWORD:-admin}" || true
else
  "$PY_BIN" -m superset fab create-admin \
  --username "${ADMIN_USERNAME:-admin}" \
  --firstname "${ADMIN_FIRSTNAME:-Superset}" \
  --lastname "${ADMIN_LASTNAME:-Admin}" \
  --email "${ADMIN_EMAIL:-admin@example.com}" \
  --password "${ADMIN_PASSWORD:-admin}" || true
fi

if [[ -x "$SUP_BIN" ]]; then
  "$SUP_BIN" init
else
  "$PY_BIN" -m superset init
fi
