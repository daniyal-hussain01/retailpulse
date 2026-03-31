# Changelog

All notable changes to RetailPulse are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org).

---

## [Unreleased]

### Planned
- Snowflake production target for dbt (currently DuckDB-only)
- Flink streaming job for sub-5-minute latency
- Reverse ETL: Census connector to push churn segments to Salesforce
- Grafana dashboard for pipeline observability metrics

---

## [0.1.0] — 2024-04-25

### Added

**Ingestion layer**
- Debezium Postgres connector config for CDC on `orders`, `order_items`, `users`, `inventory`
- Kafka Connect S3 sink (Parquet/Snappy, time-partitioned)
- Airbyte connection spec for Google Ads API
- Kafka producer simulator for local development (`ingestion/producer.py`)

**Processing layer**
- Bronze → Silver PySpark job with PII hashing, deduplication, validation
- Clickstream sessionisation job (30-minute idle timeout)
- Pandas-mode for local development (no JVM required)

**Transformation layer (dbt)**
- `stg_orders` staging view over Silver parquet
- `int_orders_enriched` intermediate model with derived business fields
- `fact_orders` Gold fact table (incremental, partitioned by order_date)
- `agg_daily_revenue` daily KPI aggregate
- `agg_user_cohorts` monthly acquisition cohort retention model
- `snap_users` SCD Type 2 snapshot
- Custom singular test: `assert_no_negative_revenue`
- Macros: `safe_divide`, `cents_to_dollars`, `assert_not_negative`

**Data quality**
- Great Expectations-style suite: 7 checks on Silver orders
- Volume anomaly detection (30% drop threshold)
- PII column detection in Gold

**Orchestration**
- `retailpulse_full_pipeline` Airflow DAG (daily, 02:00 UTC)
- `gdpr_deletion` Airflow DAG (manually triggered, parallel erasure)

**ML layer**
- XGBoost churn propensity model with MLflow tracking
- Prophet demand forecast (per-SKU, nightly)
- Feast feature store definitions (user RFM features)
- FastAPI serving layer: `/health`, `/metrics/daily-revenue`, `/orders/{id}`, `/predict/churn-risk`

**Observability**
- `scripts/monitoring.py`: 5 automated health checks (freshness, row counts, volume anomaly, PII scan, API liveness)
- `scripts/backfill.py`: date-range reprocessing utility

**Infrastructure**
- Docker Compose stack: Postgres, Kafka (KRaft), Kafka UI, Airflow, FastAPI
- Terraform stubs for S3 buckets, Kafka, Snowflake warehouse
- VS Code workspace with launch configs, tasks, recommended extensions

**Developer experience**
- `Makefile` with 15 targets
- `pre-commit` config: ruff, gitleaks, bandit, yamllint, sqlfluff
- `conftest.py` with shared fixtures (tmp_data_dir, seeded_duckdb, sample records)
- `scripts/validate.py`: 126-check standalone validator (zero pip dependencies)

**Documentation**
- `README.md` with 5-minute quick start
- `docs/data_dictionary.md`: all tables, columns, PII register
- `docs/runbook.md`: incidents, backfill, GDPR, scaling checklist
- `docs/adr/ADR-001`: Technology stack rationale
- `docs/adr/ADR-002`: PII handling and GDPR compliance strategy
- `CONTRIBUTING.md`: branch strategy, code standards, PR process

**Tests (126 total)**
- Unit: hash_pii, parse_cdc_record, validate, sessionisation (32 tests)
- Unit: FastAPI endpoints including injection prevention (25 tests)
- Unit: data quality suite (9 tests)
- Unit: monitoring checks (9 tests)
- Unit: GDPR deletion (11 tests)
- Unit: Airflow DAG integrity (8 tests)
- Integration: full Bronze → Silver → Gold → API pipeline (15 tests)

### Security
- PII hashed at Bronze→Silver boundary with SHA-256 + salt
- GDPR deletion with append-only audit trail
- UUID regex validation on all API path parameters
- Parameterised DuckDB queries throughout
- `bandit` scan: 0 HIGH severity findings
- No secrets in tracked files (gitleaks + pre-commit)

---

## [0.0.1] — 2024-04-01

### Added
- Initial project scaffold
- Postgres schema and seed data
- Basic Kafka producer
