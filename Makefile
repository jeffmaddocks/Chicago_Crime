PYTHON ?= python3
VENV ?= .venv
BIN = $(VENV)/bin

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(BIN)/pip install -e ".[dev]"

ingest:
	$(BIN)/python -m chicago_crime.ingest.ingest_crimes --once

dims:
	$(BIN)/python scripts/ingest_dims.py

duckdb:
	$(BIN)/python -m chicago_crime.ingest.build_duckdb --rebuild

app:
	$(BIN)/python scripts/run_app.py

test:
	$(BIN)/pytest

docker-build:
	docker compose build

docker-up:
	docker compose up

docker-down:
	docker compose down

superset-up:
	docker compose -f docker-compose.superset.yml up -d --build

superset-down:
	docker compose -f docker-compose.superset.yml down

superset-logs:
	docker compose -f docker-compose.superset.yml logs -f superset
