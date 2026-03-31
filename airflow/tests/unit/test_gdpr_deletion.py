"""
airflow/tests/unit/test_gdpr_deletion.py
Unit tests for the GDPR deletion DAG logic.
Tests the hashing, erasure, and audit trail functions in isolation.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest


# ── Import the module under test ──────────────────────────────────────────────

@pytest.fixture(autouse=True)
def patch_env(monkeypatch):
    monkeypatch.setenv("PII_SALT", "test_gdpr_salt")
    monkeypatch.setenv("SILVER_PATH", "/tmp/rp_gdpr_test_silver")
    monkeypatch.setenv("GOLD_PATH",   "/tmp/rp_gdpr_test_gold")
    monkeypatch.setenv("DUCKDB_PATH", "/tmp/rp_gdpr_test_gold/retailpulse.duckdb")


# ── _hash_user_id ─────────────────────────────────────────────────────────────

class TestHashUserId:
    def test_produces_64_char_hex(self):
        from airflow.dags.gdpr_deletion import _hash_user_id
        h = _hash_user_id("user@example.com")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic_with_same_salt(self, monkeypatch):
        monkeypatch.setenv("PII_SALT", "stable_salt")
        from airflow.dags.gdpr_deletion import _hash_user_id
        assert _hash_user_id("alice@example.com") == _hash_user_id("alice@example.com")

    def test_different_users_different_hashes(self, monkeypatch):
        monkeypatch.setenv("PII_SALT", "stable_salt")
        from airflow.dags.gdpr_deletion import _hash_user_id
        assert _hash_user_id("alice@example.com") != _hash_user_id("bob@example.com")

    def test_missing_pii_salt_raises(self, monkeypatch):
        monkeypatch.delenv("PII_SALT", raising=False)
        with pytest.raises(RuntimeError, match="PII_SALT"):
            from importlib import reload
            import airflow.dags.gdpr_deletion as mod
            reload(mod)
            mod._hash_user_id("test@example.com")

    def test_raw_value_not_in_hash_output(self):
        from airflow.dags.gdpr_deletion import _hash_user_id
        raw = "very_sensitive@pii.com"
        h = _hash_user_id(raw)
        assert raw not in h
        assert "@" not in h
        assert "pii" not in h


# ── erase_from_silver ─────────────────────────────────────────────────────────

class TestEraseFromSilver:
    def test_replaces_matching_hash_with_erased(self, tmp_path, monkeypatch):
        import pandas as pd
        from airflow.dags.gdpr_deletion import _hash_user_id

        monkeypatch.setenv("SILVER_PATH", str(tmp_path / "silver"))
        monkeypatch.setenv("PII_SALT", "test_gdpr_salt")

        # Setup: write Silver parquet with 5 records, 2 belonging to target user
        target_user_id = "alice@example.com"
        target_hash = _hash_user_id(target_user_id)

        orders_dir = tmp_path / "silver" / "orders"
        orders_dir.mkdir(parents=True)

        records = [
            {"order_id": f"o{i}", "user_id_hash": target_hash if i < 2 else "b" * 64,
             "total_cents": 100, "status": "placed"}
            for i in range(5)
        ]
        pd.DataFrame(records).to_parquet(orders_dir / "test.parquet", index=False)

        # Mock Airflow context
        class MockTI:
            def xcom_pull(self, key, task_ids):
                return target_hash
            def xcom_push(self, key, value): pass

        context = {"ti": MockTI()}

        import importlib, sys
        # Patch SILVER_PATH at module level
        import airflow.dags.gdpr_deletion as mod
        original = mod.os.environ.get("SILVER_PATH")
        mod.os.environ["SILVER_PATH"] = str(tmp_path / "silver")

        result = mod.erase_from_silver(**context)

        # Verify
        df = pd.read_parquet(orders_dir / "test.parquet")
        erased = df[df["user_id_hash"] == "ERASED"]
        intact = df[df["user_id_hash"] != "ERASED"]

        assert len(erased) == 2, f"Expected 2 erased rows, got {len(erased)}"
        assert len(intact) == 3, f"Expected 3 intact rows, got {len(intact)}"
        assert result["affected_rows"] == 2

    def test_no_effect_when_hash_not_present(self, tmp_path, monkeypatch):
        import pandas as pd

        monkeypatch.setenv("SILVER_PATH", str(tmp_path / "silver"))

        orders_dir = tmp_path / "silver" / "orders"
        orders_dir.mkdir(parents=True)
        records = [{"order_id": "o1", "user_id_hash": "a" * 64, "total_cents": 100, "status": "placed"}]
        pd.DataFrame(records).to_parquet(orders_dir / "test.parquet", index=False)

        class MockTI:
            def xcom_pull(self, key, task_ids): return "z" * 64  # not present
            def xcom_push(self, key, value): pass

        import airflow.dags.gdpr_deletion as mod
        mod.os.environ["SILVER_PATH"] = str(tmp_path / "silver")
        result = mod.erase_from_silver(**{"ti": MockTI()})

        assert result["affected_rows"] == 0


# ── write_audit_log ───────────────────────────────────────────────────────────

class TestAuditLog:
    def test_audit_record_written(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GOLD_PATH", str(tmp_path / "gold"))

        class MockTI:
            def xcom_pull(self, key=None, task_ids=None):
                if key == "user_id_hash": return "a" * 64
                if key == "request_id":   return "REQ-TEST-001"
                return {}
            def xcom_push(self, key, value): pass

        import airflow.dags.gdpr_deletion as mod
        mod.os.environ["GOLD_PATH"] = str(tmp_path / "gold")

        context = {
            "ti": MockTI(),
            "run_id": "manual__2024-04-25T10:00:00",
        }
        mod.write_audit_log(**context)

        audit_file = tmp_path / "gold" / "audit" / "erasure_audit.jsonl"
        assert audit_file.exists()

        record = json.loads(audit_file.read_text().strip())
        assert record["event"] == "gdpr_erasure"
        assert record["request_id"] == "REQ-TEST-001"
        assert "completed_at" in record
        # Full hash must NOT appear in audit log — only prefix
        assert "a" * 64 not in record.get("user_id_hash_prefix", "")
        assert "..." in record.get("user_id_hash_prefix", "")

    def test_audit_is_append_only(self, tmp_path, monkeypatch):
        """Multiple erasure requests → multiple lines in the same file."""
        monkeypatch.setenv("GOLD_PATH", str(tmp_path / "gold"))

        import airflow.dags.gdpr_deletion as mod
        mod.os.environ["GOLD_PATH"] = str(tmp_path / "gold")

        class MockTI:
            def __init__(self, rid):
                self._rid = rid
            def xcom_pull(self, key=None, task_ids=None):
                if key == "user_id_hash": return "b" * 64
                if key == "request_id":   return self._rid
                return {}
            def xcom_push(self, key, value): pass

        for i in range(3):
            mod.write_audit_log(**{"ti": MockTI(f"REQ-{i}"), "run_id": f"run-{i}"})

        audit_file = tmp_path / "gold" / "audit" / "erasure_audit.jsonl"
        lines = audit_file.read_text().strip().splitlines()
        assert len(lines) == 3, f"Expected 3 audit lines, got {len(lines)}"
