"""
tests/unit/test_api.py
Unit tests for the FastAPI serving layer.
Uses TestClient — no running server or Docker required.
"""
from __future__ import annotations

import duckdb
import pytest
from fastapi.testclient import TestClient

from ml.serving.app import app, _calculate_churn_score, get_db, settings


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def in_memory_db():
    """Provide an in-memory DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE fact_orders (
            order_id             VARCHAR PRIMARY KEY,
            user_id_hash         VARCHAR,
            warehouse_id         VARCHAR,
            order_status         VARCHAR,
            is_delivered         BOOLEAN,
            is_refunded_or_cancelled BOOLEAN,
            currency             VARCHAR,
            subtotal_cents       BIGINT,
            discount_cents       BIGINT,
            shipping_cents       BIGINT,
            total_cents          BIGINT,
            net_revenue_cents    BIGINT,
            order_date           TIMESTAMP,
            order_year           INTEGER,
            order_month          INTEGER,
            order_day_of_week    INTEGER,
            cdc_op               VARCHAR,
            created_at           TIMESTAMP,
            updated_at           TIMESTAMP,
            dbt_loaded_at        TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE agg_daily_revenue (
            order_date           TIMESTAMP PRIMARY KEY,
            order_year           INTEGER,
            order_month          INTEGER,
            order_count          BIGINT,
            unique_buyers        BIGINT,
            gross_revenue_cents  BIGINT,
            net_revenue_cents    BIGINT,
            total_discount_cents BIGINT,
            total_shipping_cents BIGINT,
            avg_order_value_cents BIGINT,
            refunded_order_count BIGINT,
            refund_rate_pct      DOUBLE,
            delivered_order_count BIGINT,
            dbt_loaded_at        TIMESTAMP
        )
    """)

    conn.execute("""
        INSERT INTO fact_orders VALUES (
            'test-order-0001-0001-0001-000000000001',
            'abc123hash',
            'wh-001',
            'delivered',
            true,
            false,
            'USD',
            4500, 0, 499, 4999, 4999,
            '2024-04-25', 2024, 4, 3,
            'c',
            '2024-04-25 10:00:00',
            '2024-04-25 10:00:00',
            '2024-04-25 12:00:00'
        )
    """)
    conn.execute("""
        INSERT INTO agg_daily_revenue VALUES (
            '2024-04-25', 2024, 4,
            10, 8,
            50000, 48000, 2000, 4990, 5000,
            1, 10.0, 8,
            '2024-04-25 12:00:00'
        )
    """)

    return conn


@pytest.fixture
def client(in_memory_db):
    """TestClient with DB dependency overridden to use in-memory DuckDB."""
    app.dependency_overrides[get_db] = lambda: in_memory_db
    yield TestClient(app)
    app.dependency_overrides.clear()


# ── /health ────────────────────────────────────────────────────────────────────

class TestHealthEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/health").json()
        assert data["status"] == "ok"
        assert "table_counts" in data
        assert "fact_orders" in data["table_counts"]
        assert "agg_daily_revenue" in data["table_counts"]

    def test_table_counts_are_non_negative(self, client):
        data = client.get("/health").json()
        for v in data["table_counts"].values():
            assert v >= 0


# ── /metrics/daily-revenue ─────────────────────────────────────────────────────

class TestDailyRevenueEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/metrics/daily-revenue")
        assert resp.status_code == 200

    def test_returns_list(self, client):
        data = client.get("/metrics/daily-revenue").json()
        assert isinstance(data, list)

    def test_row_has_required_fields(self, client):
        data = client.get("/metrics/daily-revenue").json()
        if data:
            row = data[0]
            required = [
                "order_date", "order_count", "unique_buyers",
                "gross_revenue_cents", "net_revenue_cents",
                "avg_order_value_cents", "refund_rate_pct",
            ]
            for field in required:
                assert field in row, f"Missing field: {field}"

    def test_limit_parameter_respected(self, client):
        resp = client.get("/metrics/daily-revenue?limit=1")
        data = resp.json()
        assert len(data) <= 1

    def test_invalid_limit_returns_400(self, client):
        resp = client.get("/metrics/daily-revenue?limit=0")
        assert resp.status_code == 400

    def test_limit_over_365_returns_400(self, client):
        resp = client.get("/metrics/daily-revenue?limit=400")
        assert resp.status_code == 400


# ── /orders/{order_id} ────────────────────────────────────────────────────────

class TestOrderEndpoint:
    VALID_ORDER_ID = "test-order-0001-0001-0001-000000000001"

    def test_returns_200_for_known_order(self, client):
        resp = client.get(f"/orders/{self.VALID_ORDER_ID}")
        assert resp.status_code == 200

    def test_response_contains_correct_order_id(self, client):
        data = client.get(f"/orders/{self.VALID_ORDER_ID}").json()
        assert data["order_id"] == self.VALID_ORDER_ID

    def test_raw_user_id_not_in_response(self, client):
        data = client.get(f"/orders/{self.VALID_ORDER_ID}").json()
        # user_id_hash should be present, not raw user_id
        assert "user_id_hash" in data
        assert "user_id" not in data or data.get("user_id") is None

    def test_returns_404_for_unknown_order(self, client):
        resp = client.get("/orders/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404

    def test_returns_400_for_invalid_uuid_format(self, client):
        resp = client.get("/orders/not-a-valid-uuid!!!")
        assert resp.status_code == 400

    def test_returns_400_for_sql_injection_attempt(self, client):
        resp = client.get("/orders/'; DROP TABLE fact_orders; --")
        assert resp.status_code == 400


# ── /predict/churn-risk ───────────────────────────────────────────────────────

class TestChurnEndpoint:
    def test_returns_200(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "a" * 64,
            "days_since_last_order": 45,
            "total_orders": 3,
            "avg_order_value_cents": 5000,
        })
        assert resp.status_code == 200

    def test_response_shape(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "a" * 64,
            "days_since_last_order": 45,
            "total_orders": 3,
            "avg_order_value_cents": 5000,
        })
        data = resp.json()
        assert "churn_probability" in data
        assert "risk_tier" in data
        assert "recommendation" in data

    def test_probability_in_valid_range(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "b" * 64,
            "days_since_last_order": 10,
            "total_orders": 20,
            "avg_order_value_cents": 10000,
        })
        prob = resp.json()["churn_probability"]
        assert 0.0 <= prob <= 1.0

    def test_high_recency_gives_high_risk(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "c" * 64,
            "days_since_last_order": 365,
            "total_orders": 1,
            "avg_order_value_cents": 500,
        })
        assert resp.json()["risk_tier"] == "high"

    def test_recent_active_customer_low_risk(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "d" * 64,
            "days_since_last_order": 5,
            "total_orders": 30,
            "avg_order_value_cents": 15000,
        })
        assert resp.json()["risk_tier"] == "low"

    def test_negative_days_rejected(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "e" * 64,
            "days_since_last_order": -1,
            "total_orders": 3,
            "avg_order_value_cents": 5000,
        })
        assert resp.status_code == 422  # Pydantic validation error


# ── _calculate_churn_score (pure function) ────────────────────────────────────

class TestChurnScoreFunction:
    def test_long_inactive_scores_high(self):
        score = _calculate_churn_score(
            days_since_last_order=180,
            total_orders=1,
            avg_order_value_cents=500,
        )
        assert score >= 0.7

    def test_very_active_customer_scores_low(self):
        score = _calculate_churn_score(
            days_since_last_order=3,
            total_orders=50,
            avg_order_value_cents=20000,
        )
        assert score <= 0.35

    def test_score_always_in_unit_interval(self):
        test_cases = [
            (0, 0, 0),
            (365, 0, 0),
            (365, 100, 100000),
            (0, 100, 100000),
        ]
        for days, orders, aov in test_cases:
            score = _calculate_churn_score(days, orders, aov)
            assert 0.0 <= score <= 1.0, f"Score out of range for inputs ({days}, {orders}, {aov})"
