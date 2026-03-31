# RetailPulse — End-to-End Data Pipeline

A production-grade data lakehouse portfolio project demonstrating:
- CDC ingestion (Postgres → Kafka → S3/local)
- Medallion architecture (Bronze / Silver / Gold)
- PySpark transformations with Delta Lake
- dbt dimensional models (Star Schema)
- Airflow orchestration
- Great Expectations data quality
- FastAPI serving layer
- Full test suite + CI/CD

## Stack

| Layer | Tool | Version |
|---|---|---|
| Language | Python | 3.11 |
| Stream ingestion | Apache Kafka | 3.7 |
| CDC | Debezium | 2.6 |
| Processing | PySpark + Delta Lake | 3.5 / 3.0 |
| Orchestration | Apache Airflow | 2.9 |
| Transformation | dbt-core + dbt-duckdb | 1.8 |
| Warehouse (local) | DuckDB | 0.10 |
| Data quality | Great Expectations | 0.18 |
| Serving API | FastAPI | 0.111 |
| Testing | pytest | 8.x |
| Containers | Docker + Docker Compose | 26.x |

## Quick Start (5 minutes)

### Prerequisites
- Docker Desktop 26+ and Docker Compose v2
- Python 3.11 (for local dev outside Docker)
- VS Code with recommended extensions (see `.vscode/extensions.json`)
- 8 GB RAM minimum for Docker

### 1. Clone and configure
```bash
git clone <repo-url> retailpulse
cd retailpulse
cp .env.example .env          # review and adjust if needed
```

### 2. Start the full stack
```bash
docker compose up -d
```
Services started:
- Postgres :5432 (source OLTP)
- Kafka + Zookeeper :9092
- Kafka UI :8080
- Airflow Webserver :8081
- FastAPI serving layer :8000
- DuckDB warehouse (file-based, auto-created)

### 3. Seed sample data
```bash
docker compose exec postgres psql -U retailpulse -d retailpulse -f /docker-entrypoint-initdb.d/seed.sql
```

### 4. Run the pipeline manually
```bash
# Trigger the full ingestion → transform → load DAG
docker compose exec airflow-webserver airflow dags trigger retailpulse_full_pipeline

# OR run each stage locally (Python venv)
make setup          # creates venv, installs deps
make ingest         # simulates CDC events into Kafka
make transform      # runs PySpark Bronze→Silver job
make dbt-run        # runs dbt Silver→Gold models
make quality        # runs Great Expectations suite
make serve          # starts FastAPI locally
```

### 5. Validate
```bash
make test           # runs full pytest suite
make security       # runs bandit + pip-audit
make lint           # ruff + mypy
```

Open dashboards:
- Airflow UI: http://localhost:8081 (admin / admin)
- Kafka UI: http://localhost:8080
- API docs: http://localhost:8000/docs
- dbt docs: `make dbt-docs` → http://localhost:8888

## Project Structure
```
retailpulse/
├── ingestion/           # Kafka producer, Debezium configs
├── airflow/dags/        # Airflow DAG definitions
├── spark/jobs/          # PySpark transformation jobs
├── dbt/                 # dbt project (models, tests, snapshots)
├── data_quality/        # Great Expectations suites
├── ml/                  # Feature store + model training stubs
├── scripts/             # Utility scripts (seed, backfill, etc.)
├── config/              # Environment configs
├── tests/               # Integration tests
├── .github/workflows/   # CI/CD pipelines
├── docker-compose.yml
├── Makefile
└── pyproject.toml
```

## Security Checklist
- [ ] No secrets in code — all via `.env` (git-ignored)
- [ ] PII columns SHA-256 hashed in Silver layer
- [ ] GDPR deletion DAG implemented
- [ ] `bandit` security scan passes (0 high severity)
- [ ] `pip-audit` vulnerability scan passes
- [ ] Kafka SASL/PLAINTEXT auth in production config
- [ ] DuckDB file permissions restricted to owner

## Running Tests
```bash
make test                    # all tests
make test-unit               # unit tests only (fast, no Docker)
make test-integration        # requires Docker stack running
pytest tests/ -v --cov=.    # with coverage report
```

## CI/CD
Every pull request runs:
1. `ruff` lint + `mypy` type check
2. `pytest` unit tests
3. `dbt compile` + `dbt test` on modified models
4. `bandit` security scan
5. `pip-audit` dependency vulnerability scan
