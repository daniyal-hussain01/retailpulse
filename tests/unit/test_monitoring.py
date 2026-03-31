"""
tests/unit/test_monitoring.py
Unit tests for the pipeline monitoring checks.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest


class TestSilverFreshnessCheck:
    def test_passes_when_file_is_fresh(self, tmp_path):
        from scripts.monitoring import check_silver_freshness

        orders_dir = tmp_path / "orders"
        orders_dir.mkdir()
        (orders_dir / "orders_2024-04-25.parquet").write_bytes(b"fake")

        result = check_silver_freshness(tmp_path, max_age_minutes=60)
        assert result["status"] == "pass"

    def test_fails_when_no_files(self, tmp_path):
        from scripts.monitoring import check_silver_freshness

        result = check_silver_freshness(tmp_path, max_age_minutes=30)
        assert result["status"] == "fail"
        assert "No Silver" in result["message"]

    def test_check_name_is_correct(self, tmp_path):
        from scripts.monitoring import check_silver_freshness

        result = check_silver_freshness(tmp_path)
        assert result["check"] == "silver_freshness"

    def test_detail_contains_age(self, tmp_path):
        from scripts.monitoring import check_silver_freshness

        orders_dir = tmp_path / "orders"
        orders_dir.mkdir()
        (orders_dir / "recent.parquet").write_bytes(b"x")

        result = check_silver_freshness(tmp_path, max_age_minutes=60)
        assert "age_minutes" in result["detail"]
        assert result["detail"]["age_minutes"] >= 0


class TestVolumeAnomalyCheck:
    def test_skips_when_duckdb_missing(self, tmp_path):
        from scripts.monitoring import check_volume_anomaly

        result = check_volume_anomaly(str(tmp_path / "missing.duckdb"))
        assert result["status"] == "skip"

    def test_check_name_is_correct(self, tmp_path):
        from scripts.monitoring import check_volume_anomaly

        result = check_volume_anomaly(str(tmp_path / "missing.duckdb"))
        assert result["check"] == "volume_anomaly"


class TestPiiNotInGoldCheck:
    def test_passes_when_no_pii_columns(self, seeded_duckdb, tmp_path):
        import duckdb
        from scripts.monitoring import check_pii_not_in_gold

        db_path = str(tmp_path / "test.duckdb")
        # Write the seeded db to a temp file
        seeded_duckdb.execute(f"ATTACH '{db_path}' AS export_db")
        seeded_duckdb.execute("CREATE TABLE export_db.fact_orders AS SELECT * FROM fact_orders")
        seeded_duckdb.execute("DETACH export_db")

        result = check_pii_not_in_gold(db_path)
        assert result["status"] == "pass"

    def test_fails_when_pii_column_present(self, tmp_path):
        import duckdb
        from scripts.monitoring import check_pii_not_in_gold

        db_path = str(tmp_path / "pii_test.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE bad_table (order_id VARCHAR, email VARCHAR)")
        conn.close()

        result = check_pii_not_in_gold(db_path)
        assert result["status"] == "fail"
        assert "email" in result["message"]

    def test_skips_when_duckdb_missing(self, tmp_path):
        from scripts.monitoring import check_pii_not_in_gold

        result = check_pii_not_in_gold(str(tmp_path / "missing.duckdb"))
        assert result["status"] == "skip"


class TestApiLivenessCheck:
    def test_fails_when_api_unreachable(self):
        from scripts.monitoring import check_api_liveness

        result = check_api_liveness("http://localhost:19999")  # nothing listening here
        assert result["status"] == "fail"
        assert "unreachable" in result["message"].lower()

    def test_check_name_is_correct(self):
        from scripts.monitoring import check_api_liveness

        result = check_api_liveness("http://localhost:19999")
        assert result["check"] == "api_liveness"
