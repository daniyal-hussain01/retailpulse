.PHONY: help setup install ingest transform dbt-run dbt-docs quality serve \
        test test-unit test-integration lint security clean docker-up docker-down

PYTHON  := python3.11
VENV    := .venv
PIP     := $(VENV)/bin/pip
PY      := $(VENV)/bin/python
PYTEST  := $(VENV)/bin/pytest
RUFF    := $(VENV)/bin/ruff
MYPY    := $(VENV)/bin/mypy
BANDIT  := $(VENV)/bin/bandit
DBT     := $(VENV)/bin/dbt

help:
	@echo ""
	@echo "RetailPulse — available commands:"
	@echo "  make setup          Create venv + install all deps"
	@echo "  make docker-up      Start full Docker stack"
	@echo "  make docker-down    Stop Docker stack"
	@echo "  make ingest         Produce sample CDC events to Kafka"
	@echo "  make transform      Run Bronze→Silver PySpark job (local mode)"
	@echo "  make dbt-run        Run dbt Silver→Gold models"
	@echo "  make dbt-docs       Serve dbt docs on :8888"
	@echo "  make quality        Run Great Expectations suite"
	@echo "  make serve          Start FastAPI serving layer"
	@echo "  make test           Run full test suite"
	@echo "  make test-unit      Run unit tests only (no Docker needed)"
	@echo "  make lint           Run ruff + mypy"
	@echo "  make security       Run bandit + pip-audit"
	@echo "  make clean          Remove venv + generated artifacts"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────

setup: $(VENV)/bin/activate

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip wheel
	$(PIP) install -e ".[dev]"
	@echo "✓ Virtual environment ready. Activate with: source $(VENV)/bin/activate"

install: setup

# ── Docker ────────────────────────────────────────────────────────────────────

docker-up:
	docker compose up -d --wait
	@echo "✓ Stack up. Airflow: http://localhost:8081 | Kafka UI: http://localhost:8080 | API: http://localhost:8000/docs"

docker-down:
	docker compose down -v

# ── Pipeline stages ───────────────────────────────────────────────────────────

ingest: setup
	$(PY) -m ingestion.producer --events 1000 --topic orders.cdc

transform: setup
	$(PY) -m spark.jobs.bronze_to_silver --date $$(date +%Y-%m-%d) --local

dbt-run: setup
	cd dbt && $(DBT) deps && $(DBT) run --profiles-dir ../.dbt

dbt-test: setup
	cd dbt && $(DBT) test --profiles-dir ../.dbt

dbt-docs: setup
	cd dbt && $(DBT) docs generate --profiles-dir ../.dbt && $(DBT) docs serve --port 8888 --profiles-dir ../.dbt

quality: setup
	$(PY) -m data_quality.run_suite --suite orders_silver

serve: setup
	$(VENV)/bin/uvicorn ml.serving.app:app --reload --port 8000

# ── Quality gates ─────────────────────────────────────────────────────────────

test: setup
	$(PYTEST) tests/ airflow/tests/ spark/tests/ --cov=. --cov-report=term-missing --cov-report=html

test-unit: setup
	$(PYTEST) tests/unit/ spark/tests/unit/ airflow/tests/unit/ -v

test-integration: setup
	$(PYTEST) tests/integration/ -v --timeout=120

lint: setup
	$(RUFF) check . --fix
	$(MYPY) ingestion/ spark/ ml/ --ignore-missing-imports

security: setup
	$(BANDIT) -r ingestion/ spark/ ml/ airflow/ -ll
	$(VENV)/bin/pip-audit --desc

# ── Utilities ─────────────────────────────────────────────────────────────────

seed-db:
	docker compose exec postgres psql -U retailpulse -d retailpulse -f /docker-entrypoint-initdb.d/seed.sql

clean:
	rm -rf $(VENV) .coverage htmlcov/ .mypy_cache/ .ruff_cache/ \
	       dbt/target/ dbt/dbt_packages/ \
	       data/bronze/ data/silver/ data/gold/ \
	       mlruns/ __pycache__/ .pytest_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Cleaned"
