"""
tests/unit/test_security.py
Security-specific tests: input validation, PII handling, injection prevention.
These run as part of the standard unit test suite.
"""
from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient

from ml.serving.app import app, get_db, hash_pii
from spark.jobs.bronze_to_silver import hash_pii, parse_cdc_record


# ── PII / hashing guarantees ───────────────────────────────────────────────────

class TestPiiProtection:
    KNOWN_EMAILS = [
        "alice@example.com",
        "bob+tag@company.co.uk",
        "carol.d@subdomain.org",
    ]

    def test_original_email_not_recoverable(self):
        for email in self.KNOWN_EMAILS:
            hashed = hash_pii(email, salt="test")
            assert email not in hashed
            assert "@" not in hashed

    def test_hash_is_hex_only(self):
        hashed = hash_pii("user@example.com", salt="s")
        assert re.fullmatch(r"[0-9a-f]{64}", hashed), f"Not a valid hex SHA-256: {hashed}"

    def test_different_users_never_share_hash(self):
        hashes = [hash_pii(f"user{i}@example.com", salt="s") for i in range(100)]
        assert len(set(hashes)) == 100, "Hash collision detected!"

    def test_cdc_parse_removes_raw_user_id(self):
        raw = {
            "op": "c",
            "ts_ms": 1000,
            "after": {
                "order_id": "ord-001",
                "user_id": "SENSITIVE_USER_ID",
                "warehouse_id": "wh-001",
                "status": "placed",
                "currency": "USD",
                "subtotal_cents": 100,
                "discount_cents": 0,
                "shipping_cents": 0,
                "total_cents": 100,
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
            },
        }
        result = parse_cdc_record(raw)
        assert result is not None
        assert "user_id" not in result, "Raw user_id must be stripped"
        assert "SENSITIVE_USER_ID" not in str(result), "Raw PII value must not appear in output"


# ── API input validation / injection prevention ───────────────────────────────

@pytest.fixture
def client():
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE fact_orders (order_id VARCHAR, user_id_hash VARCHAR, warehouse_id VARCHAR, order_status VARCHAR, is_delivered BOOLEAN, is_refunded_or_cancelled BOOLEAN, currency VARCHAR, subtotal_cents BIGINT, discount_cents BIGINT, shipping_cents BIGINT, total_cents BIGINT, net_revenue_cents BIGINT, order_date TIMESTAMP, order_year INTEGER, order_month INTEGER, order_day_of_week INTEGER, cdc_op VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, dbt_loaded_at TIMESTAMP)")
    conn.execute("CREATE TABLE agg_daily_revenue (order_date TIMESTAMP, order_year INTEGER, order_month INTEGER, order_count BIGINT, unique_buyers BIGINT, gross_revenue_cents BIGINT, net_revenue_cents BIGINT, total_discount_cents BIGINT, total_shipping_cents BIGINT, avg_order_value_cents BIGINT, refunded_order_count BIGINT, refund_rate_pct DOUBLE, delivered_order_count BIGINT, dbt_loaded_at TIMESTAMP)")
    app.dependency_overrides[get_db] = lambda: conn
    yield TestClient(app)
    app.dependency_overrides.clear()


class TestSqlInjectionPrevention:
    SQL_INJECTION_PAYLOADS = [
        "' OR '1'='1",
        "'; DROP TABLE fact_orders; --",
        "1 UNION SELECT * FROM fact_orders--",
        "../../../etc/passwd",
        "<script>alert('xss')</script>",
        "' AND 1=1 --",
        "%27%20OR%20%271%27%3D%271",
    ]

    def test_order_endpoint_rejects_injection_payloads(self, client):
        for payload in self.SQL_INJECTION_PAYLOADS:
            resp = client.get(f"/orders/{payload}")
            assert resp.status_code in (400, 404, 422), (
                f"Payload '{payload}' was not rejected (status={resp.status_code})"
            )

    def test_order_id_must_match_uuid_pattern(self, client):
        """Only valid UUID format accepted — blocks all non-UUID injection attempts."""
        valid_uuid = "12345678-1234-1234-1234-123456789abc"
        resp = client.get(f"/orders/{valid_uuid}")
        # 404 is fine — it passed validation and just wasn't found
        assert resp.status_code in (200, 404)


class TestChurnEndpointValidation:
    def test_rejects_negative_days(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "a" * 64,
            "days_since_last_order": -1,
            "total_orders": 5,
            "avg_order_value_cents": 2000,
        })
        assert resp.status_code == 422

    def test_rejects_extremely_large_days(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "a" * 64,
            "days_since_last_order": 99999,
            "total_orders": 5,
            "avg_order_value_cents": 2000,
        })
        assert resp.status_code == 422

    def test_rejects_negative_order_count(self, client):
        resp = client.post("/predict/churn-risk", json={
            "user_id_hash": "a" * 64,
            "days_since_last_order": 30,
            "total_orders": -1,
            "avg_order_value_cents": 2000,
        })
        assert resp.status_code == 422

    def test_rejects_missing_required_fields(self, client):
        resp = client.post("/predict/churn-risk", json={"user_id_hash": "a" * 64})
        assert resp.status_code == 422


class TestApiKeyAuth:
    def test_dev_mode_allows_requests_without_key(self, client):
        """In dev mode (require_api_key=False) requests succeed without a key."""
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_invalid_key_blocked_when_auth_required(self, monkeypatch, client):
        from ml.serving import app as app_module
        monkeypatch.setattr(app_module.settings, "require_api_key", True)
        resp = client.get("/health", headers={"X-API-Key": "wrong_key"})
        assert resp.status_code == 403
