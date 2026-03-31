"""
spark/tests/unit/test_bronze_to_silver.py
Unit tests for the Bronze → Silver transformation logic.
No Spark JVM required — tests pure Python functions only.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from spark.jobs.bronze_to_silver import (
    hash_pii,
    parse_cdc_record,
    run_local_transform,
    validate_silver_record,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_cdc_create() -> dict:
    return {
        "schema": "retailpulse",
        "table": "orders",
        "op": "c",
        "ts_ms": 1714000000000,
        "after": {
            "order_id": "abc123de-0001-0001-0001-000000000001",
            "user_id": "user-uuid-1234",
            "warehouse_id": "wh-uuid-0001",
            "status": "placed",
            "currency": "USD",
            "subtotal_cents": 4999,
            "discount_cents": 0,
            "shipping_cents": 499,
            "total_cents": 5498,
            "created_at": "2024-04-25T10:00:00+00:00",
            "updated_at": "2024-04-25T10:00:00+00:00",
        },
        "source": {"db": "retailpulse", "table": "orders"},
    }


@pytest.fixture
def valid_cdc_update(valid_cdc_create) -> dict:
    event = dict(valid_cdc_create)
    event["op"] = "u"
    event["before"] = valid_cdc_create["after"]
    after = dict(valid_cdc_create["after"])
    after["status"] = "shipped"
    event["after"] = after
    return event


@pytest.fixture
def cdc_delete() -> dict:
    return {
        "op": "d",
        "ts_ms": 1714000001000,
        "before": {"order_id": "abc123de-0001-0001-0001-000000000001"},
        "after": None,
    }


# ── hash_pii ───────────────────────────────────────────────────────────────────

class TestHashPii:
    def test_returns_64_char_hex(self):
        result = hash_pii("user@example.com", salt="test_salt")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_same_input_same_output(self):
        h1 = hash_pii("user@example.com", salt="test_salt")
        h2 = hash_pii("user@example.com", salt="test_salt")
        assert h1 == h2

    def test_different_salt_different_hash(self):
        h1 = hash_pii("user@example.com", salt="salt_a")
        h2 = hash_pii("user@example.com", salt="salt_b")
        assert h1 != h2

    def test_different_value_different_hash(self):
        h1 = hash_pii("alice@example.com", salt="salt")
        h2 = hash_pii("bob@example.com", salt="salt")
        assert h1 != h2

    def test_none_returns_none(self):
        assert hash_pii(None) is None

    def test_does_not_store_original_value(self):
        original = "sensitive@example.com"
        hashed = hash_pii(original, salt="salt")
        assert original not in hashed


# ── parse_cdc_record ───────────────────────────────────────────────────────────

class TestParseCdcRecord:
    def test_create_op_extracts_after(self, valid_cdc_create):
        result = parse_cdc_record(valid_cdc_create)
        assert result is not None
        assert result["order_id"] == "abc123de-0001-0001-0001-000000000001"

    def test_update_op_extracts_after(self, valid_cdc_update):
        result = parse_cdc_record(valid_cdc_update)
        assert result is not None
        assert result["order_id"] == valid_cdc_update["after"]["order_id"]

    def test_delete_op_returns_none(self, cdc_delete):
        result = parse_cdc_record(cdc_delete)
        assert result is None

    def test_user_id_is_hashed_not_plain(self, valid_cdc_create):
        result = parse_cdc_record(valid_cdc_create)
        assert result is not None
        assert "user_id" not in result, "Raw user_id must not appear in Silver"
        assert "user_id_hash" in result
        assert result["user_id_hash"] != "user-uuid-1234"

    def test_cdc_metadata_preserved(self, valid_cdc_create):
        result = parse_cdc_record(valid_cdc_create)
        assert result is not None
        assert result["cdc_op"] == "c"
        assert result["cdc_ts_ms"] == 1714000000000

    def test_missing_after_returns_none(self):
        result = parse_cdc_record({"op": "c", "after": None, "ts_ms": 0})
        assert result is None

    def test_empty_record_returns_none(self):
        result = parse_cdc_record({})
        assert result is None


# ── validate_silver_record ─────────────────────────────────────────────────────

class TestValidateSilverRecord:
    def test_valid_record_returns_no_errors(self):
        record = {
            "order_id": "abc123de-0001-0001-0001-000000000001",
            "user_id_hash": "a" * 64,
            "total_cents": 4999,
            "status": "placed",
        }
        errors = validate_silver_record(record)
        assert errors == []

    def test_null_order_id_is_error(self):
        record = {
            "order_id": None,
            "user_id_hash": "a" * 64,
            "total_cents": 4999,
            "status": "placed",
        }
        errors = validate_silver_record(record)
        assert any("order_id" in e for e in errors)

    def test_null_user_id_hash_is_error(self):
        record = {
            "order_id": "abc",
            "user_id_hash": None,
            "total_cents": 4999,
            "status": "placed",
        }
        errors = validate_silver_record(record)
        assert any("user_id_hash" in e for e in errors)

    def test_negative_total_is_error(self):
        record = {
            "order_id": "abc",
            "user_id_hash": "a" * 64,
            "total_cents": -1,
            "status": "placed",
        }
        errors = validate_silver_record(record)
        assert any("total_cents" in e for e in errors)

    def test_invalid_status_is_error(self):
        record = {
            "order_id": "abc",
            "user_id_hash": "a" * 64,
            "total_cents": 100,
            "status": "INVALID_STATUS",
        }
        errors = validate_silver_record(record)
        assert any("status" in e for e in errors)

    @pytest.mark.parametrize(
        "status",
        ["placed", "processing", "shipped", "delivered", "refunded", "cancelled"],
    )
    def test_all_valid_statuses_pass(self, status):
        record = {
            "order_id": "abc",
            "user_id_hash": "a" * 64,
            "total_cents": 100,
            "status": status,
        }
        assert validate_silver_record(record) == []


# ── run_local_transform (integration-lite) ────────────────────────────────────

class TestRunLocalTransform:
    def test_creates_silver_output(self, tmp_path, monkeypatch):
        """End-to-end: write bronze JSONL → transform → check silver parquet exists."""
        import spark.jobs.bronze_to_silver as module

        run_date = "2024-04-25"
        bronze_dir = tmp_path / "bronze" / "orders" / run_date
        silver_dir = tmp_path / "silver" / "orders"
        bronze_dir.mkdir(parents=True)

        # Write 5 valid CDC records
        records = []
        for i in range(5):
            records.append({
                "schema": "retailpulse",
                "op": "c",
                "ts_ms": 1714000000000 + i,
                "after": {
                    "order_id": f"order-{i:04d}-0001-0001-0001-00000000000{i}",
                    "user_id": f"user-{i}",
                    "warehouse_id": "wh-0001",
                    "status": "placed",
                    "currency": "USD",
                    "subtotal_cents": 4999,
                    "discount_cents": 0,
                    "shipping_cents": 499,
                    "total_cents": 5498,
                    "created_at": "2024-04-25T10:00:00+00:00",
                    "updated_at": "2024-04-25T10:00:00+00:00",
                },
                "source": {},
            })

        jsonl_file = bronze_dir / "batch_001.jsonl"
        with open(jsonl_file, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        monkeypatch.setattr(module, "BRONZE_PATH", tmp_path / "bronze")
        monkeypatch.setattr(module, "SILVER_PATH", tmp_path / "silver")

        metrics = run_local_transform(run_date)

        assert metrics["processed"] == 5
        assert metrics["valid"] == 5
        assert metrics["invalid"] == 0
        assert (silver_dir / f"orders_{run_date}.parquet").exists()

    def test_skips_invalid_records(self, tmp_path, monkeypatch):
        import spark.jobs.bronze_to_silver as module

        run_date = "2024-04-26"
        bronze_dir = tmp_path / "bronze" / "orders" / run_date
        bronze_dir.mkdir(parents=True)

        bad_records = [
            '{"op": "c", "ts_ms": 1, "after": {"order_id": null, "user_id": "u1", "total_cents": 100, "status": "placed"}}',
            '{"op": "c", "ts_ms": 2, "after": {"order_id": "ord-001", "user_id": "u1", "total_cents": -1, "status": "placed"}}',
            "NOT VALID JSON {{{{",
        ]
        with open(bronze_dir / "bad.jsonl", "w") as f:
            f.write("\n".join(bad_records))

        monkeypatch.setattr(module, "BRONZE_PATH", tmp_path / "bronze")
        monkeypatch.setattr(module, "SILVER_PATH", tmp_path / "silver")

        metrics = run_local_transform(run_date)
        assert metrics["invalid"] >= 1

    def test_deduplicates_same_order_id(self, tmp_path, monkeypatch):
        import spark.jobs.bronze_to_silver as module

        run_date = "2024-04-27"
        bronze_dir = tmp_path / "bronze" / "orders" / run_date
        bronze_dir.mkdir(parents=True)

        # Same order_id, two CDC events (create then update)
        order_id = "dup-order-0001-0001-0001-000000000001"
        events = []
        for ts, status in [(1000, "placed"), (2000, "shipped")]:
            events.append({
                "op": "u" if status == "shipped" else "c",
                "ts_ms": ts,
                "after": {
                    "order_id": order_id,
                    "user_id": "u1",
                    "warehouse_id": "wh1",
                    "status": status,
                    "currency": "USD",
                    "subtotal_cents": 1000,
                    "discount_cents": 0,
                    "shipping_cents": 0,
                    "total_cents": 1000,
                    "created_at": "2024-04-27T10:00:00+00:00",
                    "updated_at": "2024-04-27T10:00:00+00:00",
                },
                "source": {},
            })

        with open(bronze_dir / "dup.jsonl", "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

        monkeypatch.setattr(module, "BRONZE_PATH", tmp_path / "bronze")
        monkeypatch.setattr(module, "SILVER_PATH", tmp_path / "silver")

        metrics = run_local_transform(run_date)
        assert metrics["valid"] == 1
        assert metrics["deduplicated"] == 1
