# ADR-001: Technology stack selection for RetailPulse

**Status:** Accepted  
**Date:** 2024-04-25  
**Deciders:** Data Engineering Team  

---

## Context

RetailPulse needs a data pipeline that can:
1. Ingest real-time transactional changes (CDC) and batch external data
2. Scale from ~80 GB/day to ~500 GB/day without architectural rework
3. Support both SQL analysts and Python data scientists
4. Be deployable locally (for development) and to cloud (AWS/GCP for production)
5. Demonstrate modern best practices for a portfolio project

## Decision: Medallion Architecture on Delta Lake + DuckDB for local dev

### Storage format: Delta Lake (Parquet + transaction log)

**Chosen over:** Apache Iceberg, Hudi, raw Parquet

**Rationale:**
- ACID transactions on object storage (S3/GCS) enable safe concurrent writes
- Time-travel via `VERSION AS OF` is essential for debugging production incidents
- Z-ordering on `(order_date, user_id_hash)` reduces scan cost ~4× on the most common query pattern
- DuckDB can read Delta Lake natively via `delta_scan()` for local development
- Databricks integration is seamless in cloud deployment

**Trade-offs:**
- Slightly higher write overhead vs plain Parquet (~5%)
- Vacuuming must be scheduled or storage costs grow unboundedly

### Transformation: dbt-core with dbt-duckdb for local dev

**Chosen over:** SQLMesh, Dataform, hand-rolled Python

**Rationale:**
- dbt is the de facto standard in modern data stacks — critical for portfolio visibility
- `ref()` function auto-generates column-level lineage exposed in dbt Docs
- Built-in `unique`, `not_null`, `accepted_values` tests mean data contracts are enforced at the model layer, not just in GE
- dbt-duckdb allows running the exact same SQL locally without a Snowflake account, making CI fast and free
- `dbt snapshot` implements SCD Type 2 with three lines of config

**Trade-offs:**
- dbt is SQL-only; complex Python transformations (sessionisation, ML feature engineering) live in PySpark/Pandas
- Model dependencies must be explicit (`ref()`) — you cannot `SELECT *` from a raw table in a mart

### Local warehouse: DuckDB (replacing Snowflake in dev)

**Chosen over:** SQLite, Postgres, local Snowflake trial

**Rationale:**
- Columnar OLAP engine — queries that take 30s on Postgres finish in 300ms on DuckDB for analytics workloads
- Reads Parquet natively (`read_parquet('s3://...')` or local) — no ETL step to load into the warehouse for development
- Zero infra: a single `.duckdb` file, checked into nothing, works offline
- The dbt-duckdb adapter means the same dbt models run against DuckDB locally and Snowflake in production

**Trade-offs:**
- Single-writer concurrency model — not suitable for production serving with multiple API workers (use connection pooling or switch to Snowflake)
- File-based: no access control at the table level (mitigated by running the API as a read-only DuckDB connection)

### Orchestration: Apache Airflow (Astronomer Cloud in production)

**Chosen over:** Prefect, Dagster, Luigi

**Rationale:**
- Airflow is the dominant orchestrator at FAANG and large tech companies — portfolio visibility
- The DAG-as-code pattern (Python files) is version-controlled and testable
- Astronomer provides a managed deployment that eliminates scheduler maintenance
- `TaskFlow API` (Python decorators) makes simple pipelines readable

**Trade-offs:**
- Heavier than Prefect for simple use cases — Airflow's metadata DB is an additional service
- DAG parsing lag can be 10–30s; not suitable for sub-minute scheduling (use Kafka Streams or Flink for those)

### Stream processing: Spark Structured Streaming (not Flink)

**Chosen over:** Apache Flink, Kafka Streams

**Rationale:**
- Unified API: the same PySpark code handles both batch backfills and streaming — reduces maintenance burden
- Databricks supports Spark Structured Streaming natively with Delta Live Tables
- For the current latency requirement (<15 min), micro-batch at 5-minute intervals is sufficient — Flink's millisecond latency is not needed yet

**Trade-offs:**
- Flink would be the correct choice if latency SLA dropped to <30 seconds
- Spark's driver is a SPOF; Flink has better fault isolation

---

## Consequences

- **Positive:** Full pipeline runnable locally on a laptop with `make docker-up && make dbt-run`
- **Positive:** Same dbt models, same SQL, same tests in dev and prod — no environment drift
- **Positive:** Delta Lake time-travel provides free audit log for regulatory compliance
- **Negative:** Local stack requires ~8 GB RAM for Docker (Kafka + Airflow + Postgres)
- **Negative:** Snowflake account required for production deployment (not free)

---

## Review date

Revisit if: daily data volume exceeds 500 GB, latency SLA drops below 5 minutes, or team size exceeds 10 engineers (at which point Databricks Unity Catalog becomes compelling over the current Glue/DuckDB setup).
