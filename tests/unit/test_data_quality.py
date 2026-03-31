"""
tests/unit/test_data_quality.py
Tests for the Great Expectations-style data quality suite.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def silver_dir(tmp_path, monkeypatch):
    """Set up a temp Silver directory and patch the module path."""
    import data_quality.run_suite as module

    silver_path = tmp_path / "silver"
    monkeypatch.setattr(module, "SILVER_PATH", silver_path)
    return silver_path


def _write_silver_parquet(silver_dir: Path, records: list[dict]) -> None:
    orders_dir = silver_dir / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)
    df.to_parquet(orders_dir / "orders_test.parquet", index=False)


def _valid_record(override: dict = {}) -> dict:
    base = {
        "order_id": "ord-0001-0001-0001-0001-000000000001",
        "user_id_hash": "a" * 64,
        "warehouse_id": "wh-001",
        "status": "placed",
        "currency": "USD",
        "subtotal_cents": 4999,
        "discount_cents": 0,
        "shipping_cents": 499,
        "total_cents": 5498,
        "cdc_op": "c",
        "cdc_ts_ms": 1714000000000,
        "created_at": "2024-04-25 10:00:00",
        "updated_at": "2024-04-25 10:00:00",
    }
    return {**base, **override}


class TestOrdersSilverSuite:
    def test_passes_on_valid_data(self, silver_dir):
        records = [_valid_record({"order_id": f"ord-{i:04d}" + "x" * 28}) for i in range(10)]
        _write_silver_parquet(silver_dir, records)

        from data_quality.run_suite import run_orders_suite
        result = run_orders_suite()
        assert result["success"], f"Expected pass but got failures: {result['failures']}"

    def test_fails_on_duplicate_order_ids(self, silver_dir):
        records = [_valid_record({"order_id": "same-order-id"})] * 2
        _write_silver_parquet(silver_dir, records)

        from data_quality.run_suite import run_orders_suite
        result = run_orders_suite()
        assert not result["success"]
        assert any("duplicate" in f.lower() for f in result["failures"])

    def test_fails_if_raw_user_id_present(self, silver_dir):
        record = _valid_record()
        record["user_id"] = "raw-pii-value"  # should never be here
        _write_silver_parquet(silver_dir, [record])

        from data_quality.run_suite import run_orders_suite
        result = run_orders_suite()
        assert not result["success"]
        assert any("user_id" in f for f in result["failures"])

    def test_fails_on_invalid_status(self, silver_dir):
        records = [_valid_record({"status": "INVALID_STATUS"})]
        _write_silver_parquet(silver_dir, records)

        from data_quality.run_suite import run_orders_suite
        result = run_orders_suite()
        assert not result["success"]

    def test_fails_on_nonpositive_total_cents(self, silver_dir):
        records = [_valid_record({"total_cents": -100})]
        _write_silver_parquet(silver_dir, records)

        from data_quality.run_suite import run_orders_suite
        result = run_orders_suite()
        assert not result["success"]

    def test_passes_when_no_files_present(self, silver_dir):
        """Empty directory should not fail — just skip."""
        from data_quality.run_suite import run_orders_suite
        result = run_orders_suite()
        assert result["success"]
        assert result["stats"]["files_checked"] == 0

    def test_stats_row_count_is_accurate(self, silver_dir):
        records = [_valid_record({"order_id": f"ord-stat-{i:04d}" + "x" * 24}) for i in range(7)]
        _write_silver_parquet(silver_dir, records)

        from data_quality.run_suite import run_orders_suite
        result = run_orders_suite()
        assert result["stats"]["rows"] == 7

    def test_user_id_hash_must_be_64_chars(self, silver_dir):
        records = [_valid_record({"user_id_hash": "tooshort"})]
        _write_silver_parquet(silver_dir, records)

        from data_quality.run_suite import run_orders_suite
        result = run_orders_suite()
        assert not result["success"]
        assert any("64" in f for f in result["failures"])
