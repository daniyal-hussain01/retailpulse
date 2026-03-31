# RetailPulse Data Dictionary

**Last updated:** 2024-04-25  
**Maintainer:** Data Engineering  
**Source of truth:** dbt models + `dbt/models/marts/schema.yml`

---

## Layers

| Layer | Location | Format | Retention |
|---|---|---|---|
| Bronze | `s3://retailpulse-{env}/bronze/` (or `./data/bronze/` locally) | Avro / JSONL | 365 days |
| Silver | `s3://retailpulse-{env}/silver/` (or `./data/silver/`) | Parquet (Snappy) | 90 days |
| Gold | Snowflake `RETAILPULSE.MARTS.*` (or `./data/gold/retailpulse.duckdb`) | Columnar (Delta/DuckDB) | Indefinite |

---

## Gold tables

### `fact_orders`

One row per order. Primary source of truth for revenue reporting.

| Column | Type | Description | PII? |
|---|---|---|---|
| `order_id` | VARCHAR | UUID primary key from source Postgres | No |
| `user_id_hash` | VARCHAR(64) | SHA-256(PII_SALT \| user_id). Never the raw user_id. | Pseudonymous |
| `warehouse_id` | VARCHAR | UUID of fulfilling warehouse | No |
| `order_status` | VARCHAR | One of: placed, processing, shipped, delivered, refunded, cancelled | No |
| `is_delivered` | BOOLEAN | True when order_status = 'delivered' | No |
| `is_refunded_or_cancelled` | BOOLEAN | True when status is refunded or cancelled | No |
| `currency` | CHAR(3) | ISO 4217 currency code (e.g. USD) | No |
| `subtotal_cents` | BIGINT | Pre-discount order value in cents | No |
| `discount_cents` | BIGINT | Total discount applied in cents | No |
| `shipping_cents` | BIGINT | Shipping charge in cents | No |
| `total_cents` | BIGINT | subtotal + shipping, before discount | No |
| `net_revenue_cents` | BIGINT | total_cents − discount_cents. Used for revenue KPIs | No |
| `order_date` | TIMESTAMP | Truncated to day; used for partitioning | No |
| `order_year` | INTEGER | Calendar year of order | No |
| `order_month` | INTEGER | Calendar month (1–12) | No |
| `order_day_of_week` | INTEGER | Day of week (0=Sunday) | No |
| `cdc_op` | VARCHAR | Debezium operation: c=create, u=update | No |
| `created_at` | TIMESTAMP | Source row creation timestamp | No |
| `updated_at` | TIMESTAMP | Source row last update timestamp | No |
| `dbt_loaded_at` | TIMESTAMP | When this row was written by dbt | No |

**Primary key:** `order_id`  
**Partition key:** `order_date`  
**Notes:** Rows with `user_id_hash = 'ERASED'` have been processed via GDPR deletion. Aggregate values are preserved; user is no longer identifiable.

---

### `agg_daily_revenue`

Pre-aggregated daily revenue summary. Powers executive dashboards.
**One row per calendar day.**

| Column | Type | Description |
|---|---|---|
| `order_date` | DATE | The calendar day (primary key) |
| `order_year` | INTEGER | Calendar year |
| `order_month` | INTEGER | Calendar month (1–12) |
| `order_count` | BIGINT | Total orders placed on this day |
| `unique_buyers` | BIGINT | Count of distinct `user_id_hash` values |
| `gross_revenue_cents` | BIGINT | Sum of `total_cents` |
| `net_revenue_cents` | BIGINT | Sum of `net_revenue_cents` (after discounts) |
| `total_discount_cents` | BIGINT | Sum of `discount_cents` |
| `total_shipping_cents` | BIGINT | Sum of `shipping_cents` |
| `avg_order_value_cents` | BIGINT | Mean `total_cents` per order |
| `refunded_order_count` | BIGINT | Orders with is_refunded_or_cancelled = true |
| `refund_rate_pct` | DOUBLE | refunded_order_count / order_count × 100 |
| `delivered_order_count` | BIGINT | Orders with is_delivered = true |
| `dbt_loaded_at` | TIMESTAMP | When this row was written by dbt |

---

## Silver tables

### `orders` (`./data/silver/orders/orders_YYYY-MM-DD.parquet`)

Cleaned and deduplicated order events. All PII masked.

| Column | Type | Notes |
|---|---|---|
| `order_id` | VARCHAR | UUID |
| `user_id_hash` | VARCHAR(64) | SHA-256. See hashing note. |
| `warehouse_id` | VARCHAR | UUID |
| `status` | VARCHAR | Source status string |
| `currency` | VARCHAR | ISO 4217 |
| `subtotal_cents` | INTEGER | |
| `discount_cents` | INTEGER | |
| `shipping_cents` | INTEGER | |
| `total_cents` | INTEGER | |
| `cdc_op` | VARCHAR | c / u |
| `cdc_ts_ms` | BIGINT | Epoch milliseconds from Debezium |
| `created_at` | VARCHAR | ISO 8601 timestamp |
| `updated_at` | VARCHAR | ISO 8601 timestamp |

**Note on `user_id_hash`:** Computed as `SHA-256(PII_SALT + "|" + user_id)`. The salt is stored in AWS Secrets Manager (prod) or `.env` (local). Rotating the salt invalidates all existing hashes — see the runbook for the rotation procedure.

---

## Kafka topics

| Topic | Producer | Consumer | Format | Retention |
|---|---|---|---|---|
| `retailpulse.orders` | Debezium | Kafka Connect S3 Sink | JSON (Debezium envelope) | 24h |
| `retailpulse.order_items` | Debezium | Kafka Connect S3 Sink | JSON | 24h |
| `retailpulse.users` | Debezium | Kafka Connect S3 Sink | JSON | 24h |
| `retailpulse.inventory` | Debezium | Kafka Connect S3 Sink | JSON | 24h |
| `retailpulse.dlq` | All connectors (errors) | Monitoring | JSON | 7 days |

---

## Metrics glossary

| Metric | Definition | Table | Formula |
|---|---|---|---|
| GMV | Gross Merchandise Value | `agg_daily_revenue` | `SUM(gross_revenue_cents) / 100` |
| Net Revenue | Revenue after discounts | `agg_daily_revenue` | `SUM(net_revenue_cents) / 100` |
| AOV | Average Order Value | `agg_daily_revenue` | `avg_order_value_cents / 100` |
| Refund Rate | % orders refunded | `agg_daily_revenue` | `refund_rate_pct` |
| Unique Buyers | DAU (order-based) | `agg_daily_revenue` | `unique_buyers` |
| Churn Score | Propensity to not reorder | ML serving layer | RFM heuristic / XGBoost model |

---

## PII register

| Field | Source Table | Classification | Handling |
|---|---|---|---|
| `email` | `users` | PII | Never enters Silver or Gold. Hashed at ingest. |
| `user_id` (raw) | `users`, `orders` | Pseudonymous | SHA-256 hashed to `user_id_hash` in Bronze→Silver job |
| `ip_address` | `events` | PII | Dropped in Silver; never enters Gold |
| `first_name`, `last_name` | `users` | PII | Never ingested past Bronze |

GDPR Article 17 deletion: see `airflow/dags/gdpr_deletion.py` and `docs/runbook.md`.
