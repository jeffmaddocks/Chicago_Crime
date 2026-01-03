PYTHON ?= python3
VENV ?= .venv
BIN = $(VENV)/bin

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(BIN)/pip install -e ".[dev]"

ingest:
	$(BIN)/python -m chicago_crime.ingest.ingest_crimes --once

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
