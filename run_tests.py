"""
run_tests.py  —  Run all unit tests with zero pip installs
Uses Python's built-in unittest only.

Usage:
    python run_tests.py             # run all tests
    python run_tests.py -v          # verbose output
    python run_tests.py transform   # run only transform tests
    python run_tests.py api         # run only API tests
    python run_tests.py security    # run only security tests
    python run_tests.py quality     # run only data quality tests
    python run_tests.py session     # run only sessionisation tests
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Make project root importable
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("PII_SALT",       "test_pii_salt_do_not_use_in_prod")
os.environ.setdefault("API_SECRET_KEY", "test_api_key")
os.environ.setdefault("REQUIRE_API_KEY","false")
os.environ.setdefault("BRONZE_PATH",    str(ROOT / "data" / "bronze"))
os.environ.setdefault("SILVER_PATH",    str(ROOT / "data" / "silver"))
os.environ.setdefault("GOLD_PATH",      str(ROOT / "data" / "gold"))


# ═══════════════════════════════════════════════════════════════════════════════
# TRANSFORM TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestHashPii(unittest.TestCase):

    def test_returns_64_char_hex(self):
        from spark.jobs.bronze_to_silver import hash_pii
        h = hash_pii("user@example.com", salt="test")
        self.assertEqual(len(h), 64)
        self.assertTrue(re.fullmatch(r"[0-9a-f]{64}", h))

    def test_deterministic(self):
        from spark.jobs.bronze_to_silver import hash_pii
        self.assertEqual(hash_pii("x", "s"), hash_pii("x", "s"))

    def test_different_salt_different_hash(self):
        from spark.jobs.bronze_to_silver import hash_pii
        self.assertNotEqual(hash_pii("x", "a"), hash_pii("x", "b"))

    def test_none_returns_none(self):
        from spark.jobs.bronze_to_silver import hash_pii
        self.assertIsNone(hash_pii(None))

    def test_original_not_in_output(self):
        from spark.jobs.bronze_to_silver import hash_pii
        h = hash_pii("sensitive@example.com", "s")
        self.assertNotIn("sensitive", h)
        self.assertNotIn("@", h)

    def test_no_collision_200_samples(self):
        from spark.jobs.bronze_to_silver import hash_pii
        hashes = {hash_pii(f"u{i}@x.com", "s") for i in range(200)}
        self.assertEqual(len(hashes), 200)


class TestParseCdcRecord(unittest.TestCase):

    def _record(self, op="c", override=None):
        after = {
            "order_id": "ord-0001",
            "user_id":  "SENSITIVE_USER_ID",
            "status":   "placed",
            "total_cents": 4999,
            "currency": "USD",
            "subtotal_cents": 4500,
            "discount_cents": 0,
            "shipping_cents": 499,
            "warehouse_id": "wh-001",
            "created_at": "2024-04-25T10:00:00+00:00",
            "updated_at": "2024-04-25T10:00:00+00:00",
        }
        if override:
            after.update(override)
        return {"op": op, "ts_ms": 1714000000000, "after": after, "source": {}}

    def test_create_extracts_order_id(self):
        from spark.jobs.bronze_to_silver import parse_cdc_record
        r = parse_cdc_record(self._record())
        self.assertIsNotNone(r)
        self.assertEqual(r["order_id"], "ord-0001")

    def test_user_id_stripped(self):
        from spark.jobs.bronze_to_silver import parse_cdc_record
        r = parse_cdc_record(self._record())
        self.assertNotIn("user_id", r)

    def test_pii_value_not_in_output(self):
        from spark.jobs.bronze_to_silver import parse_cdc_record
        r = parse_cdc_record(self._record())
        self.assertNotIn("SENSITIVE_USER_ID", str(r))

    def test_user_id_hash_is_sha256(self):
        from spark.jobs.bronze_to_silver import parse_cdc_record
        r = parse_cdc_record(self._record())
        self.assertIn("user_id_hash", r)
        self.assertEqual(len(r["user_id_hash"]), 64)

    def test_delete_returns_none(self):
        from spark.jobs.bronze_to_silver import parse_cdc_record
        self.assertIsNone(parse_cdc_record({"op": "d", "after": None, "ts_ms": 0}))

    def test_missing_after_returns_none(self):
        from spark.jobs.bronze_to_silver import parse_cdc_record
        self.assertIsNone(parse_cdc_record({"op": "c", "after": None}))

    def test_empty_returns_none(self):
        from spark.jobs.bronze_to_silver import parse_cdc_record
        self.assertIsNone(parse_cdc_record({}))


class TestValidateSilverRecord(unittest.TestCase):

    def _valid(self, **kwargs):
        r = {"order_id": "o1", "user_id_hash": "a"*64,
             "total_cents": 100, "status": "placed"}
        r.update(kwargs)
        return r

    def test_valid_record_no_errors(self):
        from spark.jobs.bronze_to_silver import validate_silver_record
        self.assertEqual(validate_silver_record(self._valid()), [])

    def test_null_order_id_is_error(self):
        from spark.jobs.bronze_to_silver import validate_silver_record
        self.assertTrue(len(validate_silver_record(self._valid(order_id=None))) > 0)

    def test_null_hash_is_error(self):
        from spark.jobs.bronze_to_silver import validate_silver_record
        self.assertTrue(len(validate_silver_record(self._valid(user_id_hash=None))) > 0)

    def test_negative_total_is_error(self):
        from spark.jobs.bronze_to_silver import validate_silver_record
        self.assertTrue(len(validate_silver_record(self._valid(total_cents=-1))) > 0)

    def test_invalid_status_is_error(self):
        from spark.jobs.bronze_to_silver import validate_silver_record
        self.assertTrue(len(validate_silver_record(self._valid(status="BOGUS"))) > 0)

    def test_all_valid_statuses_pass(self):
        from spark.jobs.bronze_to_silver import validate_silver_record
        for s in ["placed","processing","shipped","delivered","refunded","cancelled"]:
            self.assertEqual(validate_silver_record(self._valid(status=s)), [],
                             msg=f"Status '{s}' should be valid")


# ═══════════════════════════════════════════════════════════════════════════════
# SESSIONISATION TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestSessioniseEvents(unittest.TestCase):

    def _ts(self, offset=0):
        base = datetime(2024, 4, 25, 10, 0, 0, tzinfo=timezone.utc)
        return (base + timedelta(minutes=offset)).isoformat()

    def _evt(self, sid, offset, etype="page_view"):
        return {"event_id": f"e{sid}{offset}", "session_id": sid,
                "user_id": None, "event_type": etype,
                "page_url": f"/p/{offset}", "occurred_at": self._ts(offset)}

    def test_empty_input(self):
        from spark.jobs.sessionize_clicks import sessionise_events
        self.assertEqual(sessionise_events([]), [])

    def test_single_event_one_session(self):
        from spark.jobs.sessionize_clicks import sessionise_events
        self.assertEqual(len(sessionise_events([self._evt("s1", 0)])), 1)

    def test_5_events_same_session(self):
        from spark.jobs.sessionize_clicks import sessionise_events
        evts = [self._evt("s1", i) for i in range(5)]
        sessions = sessionise_events(evts)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]["event_count"], 5)

    def test_two_session_ids_two_sessions(self):
        from spark.jobs.sessionize_clicks import sessionise_events
        evts = [self._evt("s1",0), self._evt("s1",1), self._evt("s2",2), self._evt("s2",3)]
        self.assertEqual(len(sessionise_events(evts)), 2)

    def test_timeout_splits_session(self):
        from spark.jobs.sessionize_clicks import sessionise_events, SESSION_TIMEOUT_MINUTES
        evts = [self._evt("s1", 0), self._evt("s1", SESSION_TIMEOUT_MINUTES + 5)]
        self.assertEqual(len(sessionise_events(evts)), 2)

    def test_within_window_one_session(self):
        from spark.jobs.sessionize_clicks import sessionise_events, SESSION_TIMEOUT_MINUTES
        evts = [self._evt("s1", 0), self._evt("s1", SESSION_TIMEOUT_MINUTES - 1)]
        self.assertEqual(len(sessionise_events(evts)), 1)

    def test_duration_10_min(self):
        from spark.jobs.sessionize_clicks import sessionise_events
        evts = [self._evt("s1", 0), self._evt("s1", 10)]
        self.assertEqual(sessionise_events(evts)[0]["duration_seconds"], 600)

    def test_purchase_sets_converted(self):
        from spark.jobs.sessionize_clicks import sessionise_events
        evts = [self._evt("s1", 0), self._evt("s1", 1, "purchase")]
        self.assertTrue(sessionise_events(evts)[0]["converted"])

    def test_no_purchase_not_converted(self):
        from spark.jobs.sessionize_clicks import sessionise_events
        evts = [self._evt("s1", 0, "page_view"), self._evt("s1", 1, "product_view")]
        self.assertFalse(sessionise_events(evts)[0]["converted"])

    def test_event_type_counts(self):
        from spark.jobs.sessionize_clicks import sessionise_events
        types = ["page_view","product_view","product_view","add_to_cart","search","purchase"]
        evts  = [self._evt("s1", i, t) for i, t in enumerate(types)]
        s = sessionise_events(evts)[0]
        self.assertEqual(s["page_views"],          1)
        self.assertEqual(s["product_views"],       2)
        self.assertEqual(s["add_to_cart_count"],   1)
        self.assertEqual(s["search_count"],        1)


# ═══════════════════════════════════════════════════════════════════════════════
# API / FLASK TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestFlaskAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from app import app, init_db
        init_db()
        cls.client = app.test_client()
        app.config["TESTING"] = True

    def test_health_200(self):
        r = self.client.get("/api/health")
        self.assertEqual(r.status_code, 200)

    def test_health_status_ok(self):
        r = self.client.get("/api/health")
        data = json.loads(r.data)
        self.assertEqual(data["status"], "ok")

    def test_health_has_table_counts(self):
        r = self.client.get("/api/health")
        data = json.loads(r.data)
        self.assertIn("table_counts", data)
        self.assertIn("fact_orders", data["table_counts"])

    def test_daily_revenue_200(self):
        r = self.client.get("/api/metrics/daily-revenue")
        self.assertEqual(r.status_code, 200)

    def test_daily_revenue_returns_list(self):
        r = self.client.get("/api/metrics/daily-revenue")
        data = json.loads(r.data)
        self.assertIsInstance(data, list)

    def test_daily_revenue_limit(self):
        r = self.client.get("/api/metrics/daily-revenue?limit=7")
        data = json.loads(r.data)
        self.assertLessEqual(len(data), 7)

    def test_daily_revenue_invalid_limit(self):
        r = self.client.get("/api/metrics/daily-revenue?limit=0")
        self.assertEqual(r.status_code, 400)

    def test_summary_200(self):
        r = self.client.get("/api/metrics/summary?days=30")
        self.assertEqual(r.status_code, 200)

    def test_summary_has_required_fields(self):
        r = self.client.get("/api/metrics/summary")
        data = json.loads(r.data)
        for f in ["total_orders","gross_revenue_cents","net_revenue_cents","avg_order_value_cents"]:
            self.assertIn(f, data, f"Missing field: {f}")

    def test_orders_list_200(self):
        r = self.client.get("/api/orders")
        self.assertEqual(r.status_code, 200)

    def test_orders_list_is_list(self):
        data = json.loads(self.client.get("/api/orders").data)
        self.assertIsInstance(data, list)

    def test_orders_no_raw_user_id(self):
        data = json.loads(self.client.get("/api/orders").data)
        if data:
            self.assertNotIn("user_id", data[0])

    def test_order_404_for_unknown(self):
        r = self.client.get("/api/orders/00000000-0000-0000-0000-000000000000")
        self.assertEqual(r.status_code, 404)

    def test_inventory_200(self):
        r = self.client.get("/api/metrics/inventory")
        self.assertEqual(r.status_code, 200)

    def test_inventory_has_sku(self):
        data = json.loads(self.client.get("/api/metrics/inventory").data)
        if data:
            self.assertIn("sku", data[0])

    def test_pipeline_status_200(self):
        r = self.client.get("/api/pipeline/status")
        self.assertEqual(r.status_code, 200)

    def test_pipeline_history_200(self):
        r = self.client.get("/api/pipeline/history")
        self.assertEqual(r.status_code, 200)

    def test_channel_200(self):
        r = self.client.get("/api/metrics/revenue-by-channel")
        self.assertEqual(r.status_code, 200)


# ═══════════════════════════════════════════════════════════════════════════════
# CHURN / ML TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestChurnEndpoint(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from app import app, init_db
        init_db()
        cls.client = app.test_client()
        app.config["TESTING"] = True

    def _post(self, payload):
        return self.client.post("/api/customers/churn-risk",
                                data=json.dumps(payload),
                                content_type="application/json")

    def test_valid_request_200(self):
        r = self._post({"user_id_hash":"a"*64,"days_since_last_order":45,
                        "total_orders":3,"avg_order_value_cents":5000})
        self.assertEqual(r.status_code, 200)

    def test_response_has_required_fields(self):
        r = self._post({"user_id_hash":"a"*64,"days_since_last_order":45,
                        "total_orders":3,"avg_order_value_cents":5000})
        data = json.loads(r.data)
        for f in ["churn_probability","risk_tier","recommendation"]:
            self.assertIn(f, data)

    def test_probability_in_unit_interval(self):
        r = self._post({"user_id_hash":"b"*64,"days_since_last_order":10,
                        "total_orders":20,"avg_order_value_cents":10000})
        p = json.loads(r.data)["churn_probability"]
        self.assertGreaterEqual(p, 0.0)
        self.assertLessEqual(p, 1.0)

    def test_long_inactive_high_risk(self):
        r = self._post({"user_id_hash":"c"*64,"days_since_last_order":365,
                        "total_orders":1,"avg_order_value_cents":500})
        self.assertEqual(json.loads(r.data)["risk_tier"], "high")

    def test_very_active_low_risk(self):
        r = self._post({"user_id_hash":"d"*64,"days_since_last_order":5,
                        "total_orders":30,"avg_order_value_cents":15000})
        self.assertEqual(json.loads(r.data)["risk_tier"], "low")

    def test_negative_days_rejected(self):
        r = self._post({"user_id_hash":"e"*64,"days_since_last_order":-1,
                        "total_orders":5,"avg_order_value_cents":2000})
        self.assertEqual(r.status_code, 422)

    def test_huge_days_rejected(self):
        r = self._post({"user_id_hash":"f"*64,"days_since_last_order":99999,
                        "total_orders":5,"avg_order_value_cents":2000})
        self.assertEqual(r.status_code, 422)


# ═══════════════════════════════════════════════════════════════════════════════
# SECURITY TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestSecurity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from app import app, init_db
        init_db()
        cls.client = app.test_client()
        app.config["TESTING"] = True

    def test_sql_injection_in_order_id_rejected(self):
        payloads = [
            "' OR '1'='1",
            "'; DROP TABLE fact_orders; --",
            "1 UNION SELECT * FROM fact_orders--",
            "../../../etc/passwd",
            "<script>alert(1)</script>",
            "admin'--",
        ]
        for p in payloads:
            r = self.client.get(f"/api/orders/{p}")
            self.assertIn(r.status_code, [400, 404],
                          msg=f"Payload not rejected: {p!r} (got {r.status_code})")

    def test_valid_uuid_passes_format_check(self):
        r = self.client.get("/api/orders/12345678-1234-1234-1234-123456789abc")
        self.assertIn(r.status_code, [200, 404])  # 404 = not found, which is fine

    def test_pii_not_in_api_responses(self):
        data = json.loads(self.client.get("/api/orders").data)
        for row in data:
            self.assertNotIn("user_id", row,
                             "Raw user_id must not appear in API responses")

    def test_hash_is_sha256(self):
        from spark.jobs.bronze_to_silver import hash_pii
        h = hash_pii("test@example.com", "salt")
        self.assertTrue(re.fullmatch(r"[0-9a-f]{64}", h),
                        f"Hash is not valid SHA-256: {h}")

    def test_different_users_different_hashes(self):
        from spark.jobs.bronze_to_silver import hash_pii
        hashes = [hash_pii(f"user{i}@ex.com", "s") for i in range(100)]
        self.assertEqual(len(set(hashes)), 100, "Hash collision detected")


# ═══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestDataQuality(unittest.TestCase):

    def setUp(self):
        import tempfile, os
        self.tmpdir = tempfile.mkdtemp()
        os.makedirs(f"{self.tmpdir}/orders", exist_ok=True)
        os.environ["SILVER_PATH"] = self.tmpdir

    def tearDown(self):
        import shutil, os
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        os.environ.pop("SILVER_PATH", None)

    def _write(self, records):
        import csv
        if not records:
            return
        fpath = f"{self.tmpdir}/orders/test.csv"
        with open(fpath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
            writer.writeheader()
            writer.writerows(records)

    def _valid_rec(self, **kw):
        r = {"order_id": "o1", "user_id_hash": "a"*64,
             "status": "placed", "currency": "USD",
             "subtotal_cents": 100, "discount_cents": 0,
             "shipping_cents": 0, "total_cents": 100,
             "cdc_op": "c", "cdc_ts_ms": 1000,
             "created_at": "2024-01-01", "updated_at": "2024-01-01"}
        r.update(kw)
        return r

    def _run(self):
        import importlib
        import data_quality.run_suite as m
        importlib.reload(m)  # re-read SILVER_PATH env var
        return m.run_orders_suite()

    def test_clean_data_passes(self):
        self._write([self._valid_rec(order_id=f"o{i}"+"x"*30) for i in range(5)])
        result = self._run()
        self.assertTrue(result["success"], result["failures"])

    def test_duplicate_order_id_fails(self):
        self._write([self._valid_rec(), self._valid_rec()])
        result = self._run()
        self.assertFalse(result["success"])

    def test_raw_user_id_column_fails(self):
        rec = self._valid_rec()
        rec["user_id"] = "raw_pii"
        self._write([rec])
        result = self._run()
        self.assertFalse(result["success"])

    def test_invalid_status_fails(self):
        self._write([self._valid_rec(status="BOGUS")])
        result = self._run()
        self.assertFalse(result["success"])

    def test_empty_dir_passes(self):
        result = self._run()
        self.assertTrue(result["success"])
        self.assertEqual(result["stats"]["files_checked"], 0)


# ═══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# REAL-TIME PIPELINE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestStreamEngine(unittest.TestCase):

    def test_event_generator_hashes_user_id(self):
        from realtime.stream_engine import EventGenerator
        gen = EventGenerator(eps=1)
        e = gen._make_event()
        self.assertNotIn("user_id", e, "Raw user_id must never appear in events")
        self.assertIn("user_id_hash", e)
        self.assertEqual(len(e["user_id_hash"]), 64)

    def test_event_total_cents_positive(self):
        from realtime.stream_engine import EventGenerator
        gen = EventGenerator(eps=1)
        for _ in range(20):
            e = gen._make_event()
            self.assertGreater(e["total_cents"], 0)

    def test_event_has_required_fields(self):
        from realtime.stream_engine import EventGenerator
        gen = EventGenerator(eps=1)
        e = gen._make_event()
        for f in ["order_id","user_id_hash","status","channel","total_cents","sku"]:
            self.assertIn(f, e, f"Missing field: {f}")

    def test_stream_processor_validates_good_event(self):
        from realtime.stream_engine import StreamProcessor, EventGenerator
        proc = StreamProcessor(0)
        e = EventGenerator(eps=1)._make_event()
        errors = proc._validate(e)
        self.assertEqual(errors, [])

    def test_stream_processor_rejects_null_order_id(self):
        from realtime.stream_engine import StreamProcessor, EventGenerator
        proc = StreamProcessor(0)
        e = EventGenerator(eps=1)._make_event()
        e["order_id"] = None
        self.assertTrue(len(proc._validate(e)) > 0)

    def test_stream_processor_enriches_value_tier(self):
        from realtime.stream_engine import StreamProcessor, EventGenerator
        proc = StreamProcessor(0)
        e = EventGenerator(eps=1)._make_event()
        enriched = proc._enrich(dict(e))
        self.assertIn(enriched["value_tier"], ("low", "mid", "high"))

    def test_churn_scorer_high_risk(self):
        import time as t
        from realtime.stream_engine import ChurnScorer
        scorer = ChurnScorer()
        user = {
            "user_id_hash":"h","order_count":1,"total_spend":500,
            "last_seen": t.monotonic()-95*86400,
            "first_seen":t.monotonic()-95*86400,"channels":[],"refunds":0
        }
        score = scorer._rfm_score(user)
        self.assertGreaterEqual(score, 0.7, f"Expected high risk, got {score}")

    def test_churn_scorer_low_risk(self):
        import time as t
        from realtime.stream_engine import ChurnScorer
        scorer = ChurnScorer()
        user = {
            "user_id_hash":"h","order_count":20,"total_spend":500000,
            "last_seen": t.monotonic()-2*86400,
            "first_seen":t.monotonic()-90*86400,"channels":[],"refunds":0
        }
        score = scorer._rfm_score(user)
        self.assertLessEqual(score, 0.35, f"Expected low risk, got {score}")

    def test_churn_score_always_in_unit_interval(self):
        import time as t
        from realtime.stream_engine import ChurnScorer
        scorer = ChurnScorer()
        test_cases = [(0,0,0),(365*86400,0,0),(2*86400,30,500000),(100*86400,1,200)]
        for days_s, orders, spend in test_cases:
            user = {"user_id_hash":"h","order_count":orders,"total_spend":spend,
                    "last_seen":t.monotonic()-days_s,"first_seen":t.monotonic()-365*86400,
                    "channels":[],"refunds":0}
            s = scorer._rfm_score(user)
            self.assertGreaterEqual(s, 0.0, f"Score below 0 for {days_s},{orders},{spend}")
            self.assertLessEqual(s, 1.0,   f"Score above 1 for {days_s},{orders},{spend}")

    def test_pipeline_state_thread_safe_increment(self):
        import threading
        from realtime.stream_engine import PipelineState
        ps = PipelineState()
        def increment_many():
            for _ in range(1000):
                ps.increment("counter")
        threads = [threading.Thread(target=increment_many) for _ in range(5)]
        for th in threads: th.start()
        for th in threads: th.join()
        self.assertEqual(ps.metrics["counter"], 5000)

    def test_pipeline_runs_and_processes_events(self):
        import time
        from realtime.stream_engine import (
            start_pipeline, stop_pipeline,
            get_live_orders, get_pipeline_metrics, state
        )
        # Reset state
        state._running.clear()
        state._threads.clear()
        state.live_orders.clear()
        state.metrics["events_generated"] = 0
        state.metrics["events_processed"] = 0

        start_pipeline()
        time.sleep(2.5)
        m = get_pipeline_metrics()
        stop_pipeline()

        self.assertGreater(m["events_generated"], 0, "No events generated")
        self.assertGreater(m["events_processed"], 0, "No events processed")
        self.assertLess(m["pipeline_latency_ms"], 500, "Latency too high")

    def test_sse_subscribe_unsubscribe(self):
        from realtime.stream_engine import PipelineState
        ps = PipelineState()
        received = []
        def cb(payload): received.append(payload)
        ps.subscribe_sse(cb)
        ps.broadcast("test", {"value": 42})
        ps.unsubscribe_sse(cb)
        ps.broadcast("test", {"value": 99})
        self.assertEqual(len(received), 1)
        self.assertIn("42", received[0])





# ═══════════════════════════════════════════════════════════════════════════════
# BACKPRESSURE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestBackpressure(unittest.TestCase):

    def _make_state(self):
        from realtime.stream_engine import PipelineState
        ps = PipelineState()
        ps._running.set()
        return ps

    def test_green_level_when_queues_empty(self):
        from realtime.backpressure import BackpressureController, PressureLevel
        ps = self._make_state()
        ctrl = BackpressureController(ps)
        snap = ctrl._assess()
        self.assertEqual(snap.level, PressureLevel.GREEN)
        self.assertEqual(snap.rate_multiplier, 1.0)

    def test_rate_multiplier_in_unit_interval(self):
        from realtime.backpressure import BackpressureController
        ps = self._make_state()
        ctrl = BackpressureController(ps)
        snap = ctrl._assess()
        self.assertGreaterEqual(snap.rate_multiplier, 0.0)
        self.assertLessEqual(snap.rate_multiplier, 1.0)

    def test_get_status_returns_dict(self):
        from realtime.backpressure import BackpressureController
        ps = self._make_state()
        ctrl = BackpressureController(ps)
        status = ctrl.get_status()
        for key in ["level","rate_multiplier","raw_queue_pct","latency_ms"]:
            self.assertIn(key, status, f"Missing key: {key}")

    def test_history_initially_empty(self):
        from realtime.backpressure import BackpressureController
        ps = self._make_state()
        ctrl = BackpressureController(ps)
        self.assertEqual(ctrl.get_history(), [])


# ═══════════════════════════════════════════════════════════════════════════════
# ALERTING TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestAlerting(unittest.TestCase):

    def setUp(self):
        import tempfile, os
        self.tmpdir = tempfile.mkdtemp()
        os.environ["ALERT_LOG_FILE"] = str(
            __import__("pathlib").Path(self.tmpdir) / "alerts.jsonl"
        )
        os.environ["ALERT_COOLDOWN_SECONDS"] = "0"  # no cooldown in tests

    def tearDown(self):
        import shutil, os
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        os.environ.pop("ALERT_LOG_FILE", None)
        os.environ.pop("ALERT_COOLDOWN_SECONDS", None)

    def _make_dispatcher(self):
        from realtime.alerting import AlertDispatcher
        return AlertDispatcher()

    def test_alert_writes_to_log_file(self):
        import json
        from pathlib import Path
        d = self._make_dispatcher()
        d.force_alert("test", "Test Alert", "This is a test body", {})
        log_path = Path(os.environ["ALERT_LOG_FILE"])
        self.assertTrue(log_path.exists())
        record = json.loads(log_path.read_text().strip())
        self.assertEqual(record["alert_type"], "test")
        self.assertEqual(record["title"], "Test Alert")

    def test_cooldown_suppresses_duplicate(self):
        import os
        os.environ["ALERT_COOLDOWN_SECONDS"] = "9999"
        d = self._make_dispatcher()
        sent1 = d.maybe_alert("churn","user1","Alert 1","Body",severity="warning")
        sent2 = d.maybe_alert("churn","user1","Alert 2","Body",severity="warning")
        self.assertTrue(sent1)
        self.assertFalse(sent2)
        self.assertEqual(d._suppressed, 1)

    def test_different_entities_not_suppressed(self):
        import os
        os.environ["ALERT_COOLDOWN_SECONDS"] = "9999"
        d = self._make_dispatcher()
        sent1 = d.maybe_alert("churn","user1","Alert","Body",severity="warning")
        sent2 = d.maybe_alert("churn","user2","Alert","Body",severity="warning")
        self.assertTrue(sent1)
        self.assertTrue(sent2)

    def test_get_stats_shape(self):
        d = self._make_dispatcher()
        stats = d.get_stats()
        for key in ["alerts_sent","alerts_suppressed","log_file"]:
            self.assertIn(key, stats)

    def test_alert_count_increments(self):
        d = self._make_dispatcher()
        d.force_alert("t","T","B",{})
        d.force_alert("t","T","B",{})
        self.assertEqual(d._sent_count, 2)


SUITES = {
    "transform": [TestHashPii, TestParseCdcRecord, TestValidateSilverRecord],
    "session":   [TestSessioniseEvents],
    "api":       [TestFlaskAPI],
    "churn":     [TestChurnEndpoint],
    "security":  [TestSecurity],
    "quality":   [TestDataQuality],
    "realtime":  [TestStreamEngine],
    "backpressure": [TestBackpressure],
    "alerting":     [TestAlerting],
}

def main():
    filter_arg = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "-v" else None
    verbose    = "-v" in sys.argv

    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()

    if filter_arg and filter_arg in SUITES:
        classes = SUITES[filter_arg]
        print(f"\nRunning '{filter_arg}' tests only\n")
    elif filter_arg:
        print(f"Unknown suite '{filter_arg}'. Available: {list(SUITES.keys())}")
        sys.exit(1)
    else:
        classes = [cls for group in SUITES.values() for cls in group]

    for cls in classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2 if verbose else 1)
    result = runner.run(suite)

    total   = result.testsRun
    passed  = total - len(result.failures) - len(result.errors)
    failed  = len(result.failures) + len(result.errors)

    print(f"\n{'='*52}")
    print(f"  ✓ {passed} passed   ✗ {failed} failed   ({total} total)")
    print(f"{'='*52}\n")

    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
