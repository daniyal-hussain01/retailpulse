"""
conftest.py — shared pytest fixtures for the entire test suite.

Placed at the repo root so fixtures are available to all test directories:
  tests/unit/
  tests/integration/
  spark/tests/unit/
  airflow/tests/unit/
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest


# ── Environment setup ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def set_test_env(monkeypatch):
    """
    Set safe test environment variables for every test.
    Prevents tests from accidentally reading production values from a real .env.
    """
    monkeypatch.setenv("PII_SALT",          "test_pii_salt_do_not_use_in_prod")
    monkeypatch.setenv("API_SECRET_KEY",    "test_api_key")
    monkeypatch.setenv("AIRFLOW_FERNET_KEY", "test_fernet_key_placeholder_not_real_=")
    monkeypatch.setenv("LOG_LEVEL",         "warning")  # suppress noisy logs during tests


# ── Temporary filesystem fixtures ─────────────────────────────────────────────

@pytest.fixture
def tmp_data_dir(tmp_path: Path, monkeypatch) -> Path:
    """
    Provides a temporary data directory tree and patches all env-var paths to it.
    Tests can write Bronze/Silver/Gold data without touching the real data/ directory.
    """
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold   = tmp_path / "gold"
    for d in [bronze, silver, gold]:
        d.mkdir(parents=True)

    monkeypatch.setenv("BRONZE_PATH", str(bronze))
    monkeypatch.setenv("SILVER_PATH", str(silver))
    monkeypatch.setenv("GOLD_PATH",   str(gold))
    monkeypatch.setenv("DUCKDB_PATH", str(gold / "test.duckdb"))

    return tmp_path


@pytest.fixture
def sample_bronze_record() -> dict:
    """A minimal valid Debezium CDC envelope for an order create."""
    return {
        "schema": "retailpulse",
        "table": "orders",
        "op": "c",
        "ts_ms": 1714000000000,
        "after": {
            "order_id":        "test-order-0001-0001-0001-000000000001",
            "user_id":         "user-pii-value-0001",
            "warehouse_id":    "wh-0001-0001-0001-000000000001",
            "status":          "placed",
            "currency":        "USD",
            "subtotal_cents":  4500,
            "discount_cents":  0,
            "shipping_cents":  499,
            "total_cents":     4999,
            "created_at":      "2024-04-25T10:00:00+00:00",
            "updated_at":      "2024-04-25T10:00:00+00:00",
        },
        "source": {"db": "retailpulse", "schema": "public", "table": "orders"},
    }


@pytest.fixture
def sample_silver_record() -> dict:
    """A minimal valid Silver-layer record (PII already hashed)."""
    return {
        "order_id":        "test-order-0001-0001-0001-000000000001",
        "user_id_hash":    "a" * 64,  # placeholder SHA-256
        "warehouse_id":    "wh-0001-0001-0001-000000000001",
        "status":          "placed",
        "currency":        "USD",
        "subtotal_cents":  4500,
        "discount_cents":  0,
        "shipping_cents":  499,
        "total_cents":     4999,
        "cdc_op":          "c",
        "cdc_ts_ms":       1714000000000,
        "created_at":      "2024-04-25 10:00:00",
        "updated_at":      "2024-04-25 10:00:00",
    }


# ── In-memory DuckDB fixture ──────────────────────────────────────────────────

@pytest.fixture
def empty_duckdb():
    """In-memory DuckDB connection with Gold schema but no data."""
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE fact_orders (
            order_id              VARCHAR PRIMARY KEY,
            user_id_hash          VARCHAR,
            warehouse_id          VARCHAR,
            order_status          VARCHAR,
            is_delivered          BOOLEAN,
            is_refunded_or_cancelled BOOLEAN,
            currency              VARCHAR,
            subtotal_cents        BIGINT,
            discount_cents        BIGINT,
            shipping_cents        BIGINT,
            total_cents           BIGINT,
            net_revenue_cents     BIGINT,
            order_date            TIMESTAMP,
            order_year            INTEGER,
            order_month           INTEGER,
            order_day_of_week     INTEGER,
            cdc_op                VARCHAR,
            created_at            TIMESTAMP,
            updated_at            TIMESTAMP,
            dbt_loaded_at         TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE agg_daily_revenue (
            order_date            TIMESTAMP PRIMARY KEY,
            order_year            INTEGER,
            order_month           INTEGER,
            order_count           BIGINT,
            unique_buyers         BIGINT,
            gross_revenue_cents   BIGINT,
            net_revenue_cents     BIGINT,
            total_discount_cents  BIGINT,
            total_shipping_cents  BIGINT,
            avg_order_value_cents BIGINT,
            refunded_order_count  BIGINT,
            refund_rate_pct       DOUBLE,
            delivered_order_count BIGINT,
            dbt_loaded_at         TIMESTAMP
        )
    """)
    yield conn
    conn.close()


@pytest.fixture
def seeded_duckdb(empty_duckdb):
    """DuckDB connection with 10 sample Gold records across 2 days."""
    import uuid
    conn = empty_duckdb

    for i in range(10):
        day = "2024-04-25" if i < 6 else "2024-04-26"
        conn.execute(
            """
            INSERT INTO fact_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                "a" * 64,
                "wh-001",
                "delivered",
                True,
                False,
                "USD",
                4500, 0, 499, 4999, 4999,
                f"{day} 10:00:00",
                2024,
                4,
                3,
                "c",
                f"{day} 10:00:00",
                f"{day} 10:00:00",
                f"{day} 12:00:00",
            ],
        )

    # Populate agg table
    conn.execute("""
        INSERT INTO agg_daily_revenue
        SELECT
            order_date::DATE, order_year, order_month,
            COUNT(*), COUNT(DISTINCT user_id_hash),
            SUM(total_cents), SUM(net_revenue_cents),
            SUM(discount_cents), SUM(shipping_cents),
            AVG(total_cents)::BIGINT,
            0, 0.0, COUNT(*),
            CURRENT_TIMESTAMP
        FROM fact_orders
        GROUP BY 1, 2, 3
    """)

    return conn


# ── Markers ───────────────────────────────────────────────────────────────────

def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow (>5s)")
    config.addinivalue_line("markers", "integration: requires Docker stack")
    config.addinivalue_line("markers", "spark: requires Spark JVM")
