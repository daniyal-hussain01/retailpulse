"""
tests/integration/test_pipeline_e2e.py

End-to-end integration test of the local pipeline:
  1. Write synthetic Bronze events
  2. Run Bronze → Silver transform
  3. Run data quality suite
  4. Load Silver into DuckDB via dbt-equivalent SQL
  5. Query Gold via FastAPI

Requires: pandas, duckdb, fastapi, httpx (all in pyproject.toml)
Does NOT require: Docker, Kafka, Spark JVM, Airflow

Run: pytest tests/integration/ -v
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def pipeline_workspace():
    """
    Create a temporary workspace, run the full local pipeline,
    and yield paths + DuckDB connection for assertions.
    Cleans up after the module finishes.
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="rp_integration_"))
    bronze_path = tmpdir / "bronze"
    silver_path = tmpdir / "silver"
    gold_path   = tmpdir / "gold"
    duckdb_path = gold_path / "retailpulse.duckdb"

    for p in [bronze_path, silver_path, gold_path]:
        p.mkdir(parents=True)

    # ── Set env vars for this test ─────────────────────────────────────────────
    os.environ["BRONZE_PATH"] = str(bronze_path)
    os.environ["SILVER_PATH"] = str(silver_path)
    os.environ["GOLD_PATH"]   = str(gold_path)
    os.environ["DUCKDB_PATH"] = str(duckdb_path)
    os.environ["PII_SALT"]    = "integration_test_salt"

    # ── Stage 1: Write Bronze events ──────────────────────────────────────────
    import uuid
    from datetime import datetime, timezone

    run_date = "2024-04-25"
    bronze_dir = bronze_path / "orders" / run_date
    bronze_dir.mkdir(parents=True)

    statuses = ["placed", "shipped", "delivered", "delivered", "placed"]
    records = []
    for i in range(25):
        qty = (i % 3) + 1
        price = [999, 2999, 4999, 8999][i % 4]
        records.append({
            "op": "c",
            "ts_ms": 1714000000000 + i * 1000,
            "after": {
                "order_id": str(uuid.uuid4()),
                "user_id": f"user-{i % 5:04d}",
                "warehouse_id": f"wh-{(i % 3) + 1:04d}",
                "status": statuses[i % 5],
                "currency": "USD",
                "subtotal_cents": qty * price,
                "discount_cents": 0,
                "shipping_cents": 499,
                "total_cents": qty * price + 499,
                "created_at": datetime(2024, 4, 25, 10, i % 60, 0, tzinfo=timezone.utc).isoformat(),
                "updated_at": datetime(2024, 4, 25, 10, i % 60, 0, tzinfo=timezone.utc).isoformat(),
            },
            "source": {"db": "retailpulse", "table": "orders"},
        })

    # Include 2 delete ops (should be filtered out)
    records.append({"op": "d", "ts_ms": 9999, "after": None, "before": {"order_id": "xxx"}})
    # Include 1 record with invalid status (should be filtered as invalid)
    records.append({
        "op": "c", "ts_ms": 1714000099000,
        "after": {
            "order_id": str(uuid.uuid4()), "user_id": "bad-user",
            "warehouse_id": "wh-0001", "status": "INVALID_STATUS",
            "currency": "USD", "subtotal_cents": 100, "discount_cents": 0,
            "shipping_cents": 0, "total_cents": 100,
            "created_at": "2024-04-25T10:00:00+00:00",
            "updated_at": "2024-04-25T10:00:00+00:00",
        },
    })

    with open(bronze_dir / "batch.jsonl", "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    # ── Stage 2: Bronze → Silver ──────────────────────────────────────────────
    import spark.jobs.bronze_to_silver as b2s_mod
    b2s_mod.BRONZE_PATH = bronze_path
    b2s_mod.SILVER_PATH = silver_path
    b2s_mod.PII_SALT    = "integration_test_salt"

    metrics = b2s_mod.run_local_transform(run_date)

    # ── Stage 3: Silver → Gold (inline dbt-equivalent SQL) ───────────────────
    silver_parquet = str(silver_path / "orders" / f"orders_{run_date}.parquet")
    conn = duckdb.connect(str(duckdb_path))

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS fact_orders AS
        SELECT
            order_id,
            user_id_hash,
            warehouse_id,
            status                                      AS order_status,
            status = 'delivered'                        AS is_delivered,
            status IN ('refunded','cancelled')          AS is_refunded_or_cancelled,
            currency,
            CAST(subtotal_cents AS BIGINT)              AS subtotal_cents,
            CAST(discount_cents AS BIGINT)              AS discount_cents,
            CAST(shipping_cents AS BIGINT)              AS shipping_cents,
            CAST(total_cents AS BIGINT)                 AS total_cents,
            CAST(total_cents - discount_cents AS BIGINT) AS net_revenue_cents,
            CAST(created_at AS TIMESTAMP)               AS order_date,
            YEAR(CAST(created_at AS TIMESTAMP))         AS order_year,
            MONTH(CAST(created_at AS TIMESTAMP))        AS order_month,
            DAYOFWEEK(CAST(created_at AS TIMESTAMP))    AS order_day_of_week,
            cdc_op,
            CAST(created_at AS TIMESTAMP)               AS created_at,
            CAST(updated_at AS TIMESTAMP)               AS updated_at,
            CURRENT_TIMESTAMP                           AS dbt_loaded_at
        FROM read_parquet('{silver_parquet}')
        WHERE order_id IS NOT NULL
          AND user_id_hash IS NOT NULL
          AND total_cents > 0
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS agg_daily_revenue AS
        SELECT
            order_date::DATE                            AS order_date,
            order_year,
            order_month,
            COUNT(*)                                    AS order_count,
            COUNT(DISTINCT user_id_hash)                AS unique_buyers,
            SUM(total_cents)                            AS gross_revenue_cents,
            SUM(net_revenue_cents)                      AS net_revenue_cents,
            SUM(discount_cents)                         AS total_discount_cents,
            SUM(shipping_cents)                         AS total_shipping_cents,
            AVG(total_cents)::BIGINT                    AS avg_order_value_cents,
            SUM(CASE WHEN is_refunded_or_cancelled THEN 1 ELSE 0 END) AS refunded_order_count,
            ROUND(100.0 * SUM(CASE WHEN is_refunded_or_cancelled THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*), 0), 2)             AS refund_rate_pct,
            SUM(CASE WHEN is_delivered THEN 1 ELSE 0 END) AS delivered_order_count,
            CURRENT_TIMESTAMP                           AS dbt_loaded_at
        FROM fact_orders
        GROUP BY 1, 2, 3
    """)

    yield {
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "gold_path":   gold_path,
        "duckdb_path": duckdb_path,
        "duckdb_conn": conn,
        "transform_metrics": metrics,
        "run_date": run_date,
        "n_input_records": 25,  # valid creates only
    }

    conn.close()
    shutil.rmtree(tmpdir, ignore_errors=True)


# ── Stage 1 → 2 assertions ────────────────────────────────────────────────────

class TestBronzeToSilverIntegration:
    def test_silver_parquet_exists(self, pipeline_workspace):
        silver_file = (
            pipeline_workspace["silver_path"]
            / "orders"
            / f"orders_{pipeline_workspace['run_date']}.parquet"
        )
        assert silver_file.exists(), f"Silver parquet not found at {silver_file}"

    def test_correct_row_count(self, pipeline_workspace):
        m = pipeline_workspace["transform_metrics"]
        # 25 valid creates processed; 1 delete filtered out; 1 invalid status filtered
        assert m["valid"] == 25, f"Expected 25 valid rows, got {m['valid']}"

    def test_invalid_records_counted(self, pipeline_workspace):
        m = pipeline_workspace["transform_metrics"]
        assert m["invalid"] >= 1

    def test_no_raw_user_id_in_silver(self, pipeline_workspace):
        silver_file = (
            pipeline_workspace["silver_path"]
            / "orders"
            / f"orders_{pipeline_workspace['run_date']}.parquet"
        )
        df = pd.read_parquet(silver_file)
        assert "user_id" not in df.columns, "Raw user_id must not be present in Silver"

    def test_user_id_hash_is_sha256(self, pipeline_workspace):
        silver_file = (
            pipeline_workspace["silver_path"]
            / "orders"
            / f"orders_{pipeline_workspace['run_date']}.parquet"
        )
        df = pd.read_parquet(silver_file)
        assert "user_id_hash" in df.columns
        # All non-null hashes should be 64-char hex
        invalid_hashes = df["user_id_hash"].dropna().apply(
            lambda h: len(h) != 64 or not all(c in "0123456789abcdef" for c in h)
        )
        assert not invalid_hashes.any(), "Non-SHA256 hashes found in Silver"

    def test_all_statuses_valid(self, pipeline_workspace):
        silver_file = (
            pipeline_workspace["silver_path"]
            / "orders"
            / f"orders_{pipeline_workspace['run_date']}.parquet"
        )
        df = pd.read_parquet(silver_file)
        valid = {"placed", "processing", "shipped", "delivered", "refunded", "cancelled"}
        assert df["status"].isin(valid).all(), "Invalid status values found in Silver"


# ── Data quality suite assertions ─────────────────────────────────────────────

class TestDataQualitySuite:
    def test_ge_suite_passes_on_clean_silver(self, pipeline_workspace, monkeypatch):
        import data_quality.run_suite as suite_mod
        monkeypatch.setattr(suite_mod, "SILVER_PATH", pipeline_workspace["silver_path"])

        result = suite_mod.run_orders_suite()
        assert result["success"], f"GE suite failed: {result['failures']}"

    def test_ge_suite_reports_correct_row_count(self, pipeline_workspace, monkeypatch):
        import data_quality.run_suite as suite_mod
        monkeypatch.setattr(suite_mod, "SILVER_PATH", pipeline_workspace["silver_path"])

        result = suite_mod.run_orders_suite()
        assert result["stats"]["rows"] == 25


# ── Gold layer (DuckDB) assertions ────────────────────────────────────────────

class TestGoldLayer:
    def test_fact_orders_populated(self, pipeline_workspace):
        conn = pipeline_workspace["duckdb_conn"]
        count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
        assert count == 25

    def test_no_null_order_ids_in_gold(self, pipeline_workspace):
        conn = pipeline_workspace["duckdb_conn"]
        nulls = conn.execute("SELECT COUNT(*) FROM fact_orders WHERE order_id IS NULL").fetchone()[0]
        assert nulls == 0

    def test_no_negative_revenue_in_gold(self, pipeline_workspace):
        conn = pipeline_workspace["duckdb_conn"]
        neg = conn.execute(
            "SELECT COUNT(*) FROM fact_orders WHERE net_revenue_cents < 0"
        ).fetchone()[0]
        assert neg == 0

    def test_agg_daily_revenue_has_row(self, pipeline_workspace):
        conn = pipeline_workspace["duckdb_conn"]
        count = conn.execute("SELECT COUNT(*) FROM agg_daily_revenue").fetchone()[0]
        assert count >= 1

    def test_agg_gross_revenue_equals_sum_of_facts(self, pipeline_workspace):
        conn = pipeline_workspace["duckdb_conn"]
        fact_sum = conn.execute("SELECT SUM(total_cents) FROM fact_orders").fetchone()[0]
        agg_sum  = conn.execute("SELECT SUM(gross_revenue_cents) FROM agg_daily_revenue").fetchone()[0]
        assert fact_sum == agg_sum, "Aggregate revenue doesn't match sum of facts"

    def test_unique_buyers_not_exceeds_total_orders(self, pipeline_workspace):
        conn = pipeline_workspace["duckdb_conn"]
        row = conn.execute(
            "SELECT order_count, unique_buyers FROM agg_daily_revenue LIMIT 1"
        ).fetchone()
        if row:
            assert row[1] <= row[0], "unique_buyers cannot exceed order_count"


# ── FastAPI serving layer assertions ─────────────────────────────────────────

class TestApiServingLayer:
    @pytest.fixture
    def api_client(self, pipeline_workspace):
        from ml.serving.app import app, get_db
        conn = pipeline_workspace["duckdb_conn"]
        app.dependency_overrides[get_db] = lambda: conn
        yield TestClient(app)
        app.dependency_overrides.clear()

    def test_health_returns_correct_counts(self, api_client, pipeline_workspace):
        resp = api_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["table_counts"]["fact_orders"] == 25

    def test_daily_revenue_returns_data(self, api_client):
        resp = api_client.get("/metrics/daily-revenue")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["order_count"] >= 1

    def test_revenue_amounts_are_positive(self, api_client):
        data = api_client.get("/metrics/daily-revenue").json()
        for row in data:
            assert row["gross_revenue_cents"] > 0
            assert row["net_revenue_cents"] > 0
            assert row["avg_order_value_cents"] > 0

    def test_order_lookup_returns_valid_order(self, pipeline_workspace, api_client):
        conn = pipeline_workspace["duckdb_conn"]
        order_id = conn.execute("SELECT order_id FROM fact_orders LIMIT 1").fetchone()[0]
        resp = api_client.get(f"/orders/{order_id}")
        assert resp.status_code == 200
        assert resp.json()["order_id"] == order_id

    def test_churn_prediction_consistent_with_data(self, api_client):
        # A user who hasn't ordered in 120 days with 1 order should be high risk
        resp = api_client.post("/predict/churn-risk", json={
            "user_id_hash": "a" * 64,
            "days_since_last_order": 120,
            "total_orders": 1,
            "avg_order_value_cents": 999,
        })
        assert resp.status_code == 200
        assert resp.json()["risk_tier"] == "high"
