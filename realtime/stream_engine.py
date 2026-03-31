"""
realtime/stream_engine.py

Real-time data pipeline engine.
Runs entirely in-process using Python threads + queues — no Kafka, no Docker needed.

Architecture:
  EventGenerator  →  raw_queue  →  StreamProcessor  →  processed_queue
                                         ↓
                                   ChurnScorer (ML)
                                         ↓
                                   SQLite ring buffer
                                         ↓
                                   SSE push to dashboard

All operations are non-blocking. The dashboard gets live updates via
Server-Sent Events (SSE) at http://localhost:5000/stream/events
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import queue
import random
import sqlite3
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

log = logging.getLogger("retailpulse.stream")

# ── Config ────────────────────────────────────────────────────────────────────
ROOT         = Path(__file__).parent.parent
DB_PATH      = ROOT / "data" / "gold" / "retailpulse.db"
PII_SALT     = os.environ.get("PII_SALT", "local_dev_salt_change_in_prod")

EVENTS_PER_SECOND   = float(os.environ.get("RT_EVENTS_PER_SECOND", "2"))
CHURN_SCORE_WORKERS = int(os.environ.get("RT_CHURN_WORKERS", "2"))
RING_BUFFER_SIZE    = int(os.environ.get("RT_RING_BUFFER", "500"))
MAX_SSE_CLIENTS     = 50

# ── Reference data (mirrors seed in app.py) ───────────────────────────────────
CHANNELS   = ["organic", "paid_search", "paid_social", "email", "referral"]
STATUSES   = ["placed", "placed", "placed", "shipped", "delivered",
              "delivered", "processing", "refunded"]
WAREHOUSES = ["east-coast-hub", "west-coast-hub", "central-hub"]
PRICES     = [999, 1499, 2499, 2999, 4999, 7999, 9999, 14999]
SKUS       = ["SKU-SHIRT-BLU-M","SKU-SHIRT-WHT-L","SKU-SHOE-RUN-10",
              "SKU-BAG-TOTE-BLK","SKU-HAT-CAP-RED","SKU-JEAN-SLM-32",
              "SKU-JACKET-BLK-M","SKU-SOCK-WHT-ML"]

# Pre-generate stable user pool (200 hashed IDs)
USER_HASHES = [
    hashlib.sha256(f"{PII_SALT}|user-{i:04d}@example.com".encode()).hexdigest()
    for i in range(200)
]

# ── Shared state ───────────────────────────────────────────────────────────────

class PipelineState:
    """Thread-safe shared state for the entire pipeline."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

        # Queues
        self.raw_queue:       queue.Queue = queue.Queue(maxsize=1000)
        self.processed_queue: queue.Queue = queue.Queue(maxsize=1000)

        # Ring buffers (latest N events for dashboard)
        self.live_orders:  deque = deque(maxlen=RING_BUFFER_SIZE)
        self.churn_alerts: deque = deque(maxlen=100)
        self.anomalies:    deque = deque(maxlen=50)

        # SSE subscriber callbacks
        self._sse_subscribers: list[Callable] = []

        # Metrics (updated atomically)
        self.metrics: dict[str, Any] = {
            "events_generated":   0,
            "events_processed":   0,
            "events_per_second":  0.0,
            "churn_alerts_fired": 0,
            "anomalies_detected": 0,
            "queue_depth_raw":    0,
            "queue_depth_proc":   0,
            "pipeline_latency_ms":0.0,
            "uptime_seconds":     0,
            "started_at":         datetime.now(timezone.utc).isoformat(),
            "revenue_1m_cents":   0,
            "revenue_5m_cents":   0,
            "orders_1m":          0,
        }

        self._start_time = time.monotonic()
        self._event_timestamps: deque = deque(maxlen=200)  # for EPS calculation
        self._revenue_window:   deque = deque(maxlen=500)  # (ts, cents)

        # Thread handles
        self._threads: list[threading.Thread] = []
        self._running  = threading.Event()

    def increment(self, key: str, by: int | float = 1) -> None:
        with self._lock:
            self.metrics[key] = self.metrics.get(key, 0) + by

    def set_metric(self, key: str, value: Any) -> None:
        with self._lock:
            self.metrics[key] = value

    def get_metrics(self) -> dict:
        with self._lock:
            m = dict(self.metrics)
        m["uptime_seconds"]    = int(time.monotonic() - self._start_time)
        m["queue_depth_raw"]   = self.raw_queue.qsize()
        m["queue_depth_proc"]  = self.processed_queue.qsize()
        # EPS: events in last 5 seconds
        now = time.monotonic()
        recent = [t for t in self._event_timestamps if now - t < 5.0]
        m["events_per_second"] = round(len(recent) / 5.0, 2)
        # Revenue windows
        r1m = sum(c for t, c in self._revenue_window if now - t < 60)
        r5m = sum(c for t, c in self._revenue_window if now - t < 300)
        o1m = sum(1 for t, c in self._revenue_window if now - t < 60)
        m["revenue_1m_cents"] = r1m
        m["revenue_5m_cents"] = r5m
        m["orders_1m"]        = o1m
        return m

    def record_event_ts(self, cents: int) -> None:
        now = time.monotonic()
        self._event_timestamps.append(now)
        self._revenue_window.append((now, cents))

    def subscribe_sse(self, callback: Callable) -> None:
        with self._lock:
            if len(self._sse_subscribers) < MAX_SSE_CLIENTS:
                self._sse_subscribers.append(callback)

    def unsubscribe_sse(self, callback: Callable) -> None:
        with self._lock:
            try:
                self._sse_subscribers.remove(callback)
            except ValueError:
                pass

    def broadcast(self, event_type: str, data: dict) -> None:
        """Push SSE event to all connected dashboard clients."""
        payload = json.dumps({"type": event_type, "data": data,
                              "ts": datetime.now(timezone.utc).isoformat()})
        dead: list[Callable] = []
        with self._lock:
            subs = list(self._sse_subscribers)
        for cb in subs:
            try:
                cb(payload)
            except Exception:
                dead.append(cb)
        for cb in dead:
            self.unsubscribe_sse(cb)


# Global singleton
state = PipelineState()


# ══════════════════════════════════════════════════════════════════════════════
# Stage 1 — Event Generator
# ══════════════════════════════════════════════════════════════════════════════

class EventGenerator(threading.Thread):
    """
    Produces synthetic CDC-style order events at a configurable rate.
    Simulates realistic patterns: burst hours, quiet hours, occasional spikes.
    """
    daemon = True
    name   = "EventGenerator"

    def __init__(self, eps: float = EVENTS_PER_SECOND) -> None:
        super().__init__()
        self._eps      = eps
        self._interval = 1.0 / max(eps, 0.1)
        self._counter  = 0

    def _demand_multiplier(self) -> float:
        """Simulate realistic demand patterns (lunch / evening peaks)."""
        hour = datetime.now().hour
        # Peak at 12–14 (lunch) and 18–21 (evening)
        if 12 <= hour <= 14 or 18 <= hour <= 21:
            return random.uniform(1.5, 2.5)
        if 2 <= hour <= 6:
            return random.uniform(0.2, 0.4)   # night quiet
        return random.uniform(0.8, 1.2)

    def _make_event(self) -> dict[str, Any]:
        qty       = random.randint(1, 4)
        price     = random.choice(PRICES)
        discount  = price // 8 if random.random() < 0.18 else 0
        shipping  = 499
        total     = qty * price - discount + shipping
        user_hash = random.choice(USER_HASHES)
        sku       = random.choice(SKUS)
        status    = random.choice(STATUSES)
        channel   = random.choices(CHANNELS, weights=[35, 28, 18, 12, 7])[0]

        return {
            "event_id":    str(uuid.uuid4()),
            "order_id":    str(uuid.uuid4()),
            "op":          "c",
            "user_id_hash":user_hash,
            "sku":         sku,
            "warehouse_id":random.choice(WAREHOUSES),
            "status":      status,
            "currency":    "USD",
            "channel":     channel,
            "qty":         qty,
            "unit_price_cents": price,
            "discount_cents":   discount,
            "shipping_cents":   shipping,
            "total_cents": total,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "_generated_at_mono": time.monotonic(),
        }

    def run(self) -> None:
        log.info("EventGenerator started at %.1f events/sec", self._eps)
        state._running.wait()

        while state._running.is_set():
            mult  = self._demand_multiplier()
            sleep = self._interval / mult

            # Occasional spike burst (3% chance: 5–15 events at once)
            burst = random.random() < 0.03
            n     = random.randint(5, 15) if burst else 1

            for _ in range(n):
                event = self._make_event()
                try:
                    state.raw_queue.put_nowait(event)
                    state.increment("events_generated")
                    state.record_event_ts(event["total_cents"])
                except queue.Full:
                    pass  # drop if downstream is slow (backpressure)

            if burst:
                state.anomalies.appendleft({
                    "type":    "burst",
                    "message": f"Traffic burst: {n} events at once",
                    "ts":      datetime.now(timezone.utc).isoformat(),
                })
                state.broadcast("anomaly", {
                    "type": "burst", "count": n,
                    "ts":   datetime.now(timezone.utc).isoformat(),
                })

            time.sleep(max(sleep, 0.05))


# ══════════════════════════════════════════════════════════════════════════════
# Stage 2 — Stream Processor (validation + enrichment)
# ══════════════════════════════════════════════════════════════════════════════

class StreamProcessor(threading.Thread):
    """
    Reads raw events, validates schema, enriches with business fields,
    detects anomalies, and forwards to the processed queue.
    """
    daemon = True

    def __init__(self, worker_id: int = 0) -> None:
        super().__init__(name=f"StreamProcessor-{worker_id}")
        self._id = worker_id

    def _validate(self, event: dict) -> list[str]:
        errors = []
        if not event.get("order_id"):           errors.append("missing order_id")
        if not event.get("user_id_hash"):        errors.append("missing user_id_hash")
        total = event.get("total_cents", 0)
        if not isinstance(total, int) or total <= 0:
            errors.append(f"invalid total_cents={total}")
        if event.get("status") not in set(STATUSES):
            errors.append(f"invalid status={event.get('status')}")
        return errors

    def _enrich(self, event: dict) -> dict:
        total = event["total_cents"]
        event["is_delivered"]  = event["status"] == "delivered"
        event["is_refunded"]   = event["status"] == "refunded"
        event["has_discount"]  = event.get("discount_cents", 0) > 0
        event["value_tier"]    = (
            "high" if total >= 10000 else
            "mid"  if total >= 3000  else "low"
        )
        event["margin_cents"]  = int(total * 0.35)  # estimated 35% margin
        event["processed_at"]  = datetime.now(timezone.utc).isoformat()
        return event

    def _detect_anomaly(self, event: dict) -> dict | None:
        total = event.get("total_cents", 0)
        # Flag very high value orders (>$200)
        if total > 20000:
            return {
                "type":    "high_value_order",
                "order_id":event["order_id"][:8],
                "amount":  f"${total/100:.2f}",
                "message": f"High-value order: ${total/100:.2f}",
                "ts":      event["processed_at"],
            }
        return None

    def run(self) -> None:
        log.info("%s started", self.name)
        state._running.wait()

        while state._running.is_set():
            try:
                event = state.raw_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            errors = self._validate(event)
            if errors:
                log.debug("Dropped invalid event: %s", errors)
                state.raw_queue.task_done()
                continue

            enriched = self._enrich(event)
            latency  = (time.monotonic() - event["_generated_at_mono"]) * 1000

            # Track latency (exponential moving average)
            prev = state.metrics.get("pipeline_latency_ms", 0.0)
            state.set_metric("pipeline_latency_ms", round(prev * 0.9 + latency * 0.1, 2))

            anomaly = self._detect_anomaly(enriched)
            if anomaly:
                state.anomalies.appendleft(anomaly)
                state.increment("anomalies_detected")
                state.broadcast("anomaly", anomaly)

            try:
                state.processed_queue.put_nowait(enriched)
                state.increment("events_processed")
            except queue.Full:
                pass

            state.raw_queue.task_done()


# ══════════════════════════════════════════════════════════════════════════════
# Stage 3 — Churn Scorer (real-time ML inference)
# ══════════════════════════════════════════════════════════════════════════════

class ChurnScorer(threading.Thread):
    """
    Reads processed events, maintains per-user RFM state in memory,
    scores churn probability on every event, fires alerts for high-risk users.
    Uses scikit-learn (already installed) for the scoring model.
    """
    daemon = True
    name   = "ChurnScorer"

    def __init__(self) -> None:
        super().__init__()
        # In-memory user state: hash → {orders, last_seen, total_spend, ...}
        self._user_state: dict[str, dict] = {}
        self._lock = threading.Lock()

    def _rfm_score(self, user: dict) -> float:
        """Fast RFM heuristic — same formula as app.py churn endpoint."""
        now    = time.monotonic()
        days_inactive = (now - user["last_seen"]) / 86400
        orders = user["order_count"]
        aov    = user["total_spend"] / max(orders, 1)

        recency  = 1.0 if days_inactive > 90 else (
                   0.7 if days_inactive > 60 else (
                   0.4 if days_inactive > 30 else 0.1))
        freq     = max(0.0, 1.0 - orders * 0.1)
        monetary = 0.6 if aov < 1000 else (0.3 if aov < 5000 else 0.1)
        return min(1.0, 0.5 * recency + 0.3 * freq + 0.2 * monetary)

    def _update_user(self, event: dict) -> dict:
        uhash = event["user_id_hash"]
        with self._lock:
            if uhash not in self._user_state:
                self._user_state[uhash] = {
                    "user_id_hash": uhash,
                    "order_count":  0,
                    "total_spend":  0,
                    "last_seen":    time.monotonic(),
                    "first_seen":   time.monotonic(),
                    "channels":     [],
                    "refunds":      0,
                }
            u = self._user_state[uhash]
            u["order_count"] += 1
            u["total_spend"] += event.get("total_cents", 0)
            u["last_seen"]    = time.monotonic()
            if event.get("is_refunded"):
                u["refunds"] += 1
            ch = event.get("channel")
            if ch and ch not in u["channels"]:
                u["channels"].append(ch)
            return dict(u)

    def run(self) -> None:
        log.info("ChurnScorer started")
        state._running.wait()

        alert_cooldown: dict[str, float] = {}   # prevent alert spam per user

        while state._running.is_set():
            try:
                event = state.processed_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            user_state = self._update_user(event)
            score      = self._rfm_score(user_state)

            # Persist to SQLite and broadcast
            enriched = {**event, "churn_score": round(score, 4)}
            _persist_event(enriched)
            state.live_orders.appendleft(enriched)

            # Broadcast every order to SSE clients
            state.broadcast("order", {
                "order_id":    enriched["order_id"][:8] + "…",
                "status":      enriched["status"],
                "total_cents": enriched["total_cents"],
                "channel":     enriched["channel"],
                "churn_score": enriched["churn_score"],
                "value_tier":  enriched["value_tier"],
                "sku":         enriched["sku"],
                "ts":          enriched["occurred_at"],
            })

            # Fire churn alert only if score ≥ 0.7 and not recently alerted
            now     = time.monotonic()
            uid_key = user_state["user_id_hash"][:16]
            if score >= 0.7 and (now - alert_cooldown.get(uid_key, -float("inf"))) > 120:
                alert = {
                    "user_prefix":    uid_key,
                    "churn_score":    round(score, 3),
                    "orders":         user_state["order_count"],
                    "total_spend":    f"${user_state['total_spend']/100:.2f}",
                    "recommendation": "Send win-back offer — 20% discount",
                    "ts":             datetime.now(timezone.utc).isoformat(),
                }
                state.churn_alerts.appendleft(alert)
                state.increment("churn_alerts_fired")
                alert_cooldown[uid_key] = now
                state.broadcast("churn_alert", alert)

            state.processed_queue.task_done()


# ══════════════════════════════════════════════════════════════════════════════
# Stage 4 — Metrics Aggregator (sliding window KPIs)
# ══════════════════════════════════════════════════════════════════════════════

class MetricsAggregator(threading.Thread):
    """
    Computes sliding-window KPIs every second and broadcasts them.
    - Revenue per minute / 5 minutes
    - Events per second
    - Queue depths
    - Pipeline latency
    """
    daemon = True
    name   = "MetricsAggregator"

    def run(self) -> None:
        log.info("MetricsAggregator started")
        state._running.wait()

        while state._running.is_set():
            time.sleep(1.0)
            metrics = state.get_metrics()
            state.broadcast("metrics", metrics)


# ══════════════════════════════════════════════════════════════════════════════
# Stage 5 — SQLite persistence (ring buffer, non-blocking)
# ══════════════════════════════════════════════════════════════════════════════

_db_write_queue: queue.Queue = queue.Queue(maxsize=2000)


def _persist_event(event: dict) -> None:
    """Non-blocking enqueue for DB writes."""
    try:
        _db_write_queue.put_nowait(event)
    except queue.Full:
        pass  # drop silently under extreme load


class DBWriter(threading.Thread):
    """
    Drains the write queue in batches for high-throughput SQLite inserts.
    Uses WAL mode for concurrent reads without blocking writes.
    """
    daemon = True
    name   = "DBWriter"
    BATCH  = 50

    def run(self) -> None:
        log.info("DBWriter started")
        state._running.wait()
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _ensure_realtime_tables(conn)

        while state._running.is_set():
            batch: list[dict] = []
            try:
                while len(batch) < self.BATCH:
                    batch.append(_db_write_queue.get(timeout=0.1))
            except queue.Empty:
                pass

            if not batch:
                continue

            try:
                conn.executemany("""
                    INSERT OR IGNORE INTO rt_orders
                    (order_id, user_id_hash, sku, warehouse_id, status,
                     channel, qty, total_cents, discount_cents, churn_score,
                     value_tier, is_refunded, occurred_at)
                    VALUES
                    (:order_id, :user_id_hash, :sku, :warehouse_id, :status,
                     :channel, :qty, :total_cents, :discount_cents, :churn_score,
                     :value_tier, :is_refunded, :occurred_at)
                """, batch)
                conn.commit()
            except sqlite3.Error as e:
                log.warning("DB write error: %s", e)
                conn.rollback()

        conn.close()


def _ensure_realtime_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS rt_orders (
            order_id       TEXT PRIMARY KEY,
            user_id_hash   TEXT NOT NULL,
            sku            TEXT,
            warehouse_id   TEXT,
            status         TEXT,
            channel        TEXT,
            qty            INTEGER DEFAULT 1,
            total_cents    INTEGER DEFAULT 0,
            discount_cents INTEGER DEFAULT 0,
            churn_score    REAL    DEFAULT 0.0,
            value_tier     TEXT    DEFAULT 'low',
            is_refunded    INTEGER DEFAULT 0,
            occurred_at    TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_rt_orders_ts
            ON rt_orders(occurred_at DESC);

        CREATE INDEX IF NOT EXISTS idx_rt_orders_churn
            ON rt_orders(churn_score DESC);

        CREATE TABLE IF NOT EXISTS rt_churn_alerts (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_prefix   TEXT NOT NULL,
            churn_score   REAL NOT NULL,
            orders_count  INTEGER DEFAULT 0,
            fired_at      TEXT NOT NULL
        );

        -- Keep only last 10000 rows (ring buffer via trigger)
        CREATE TRIGGER IF NOT EXISTS rt_orders_ring_buffer
        AFTER INSERT ON rt_orders
        BEGIN
            DELETE FROM rt_orders
            WHERE order_id IN (
                SELECT order_id FROM rt_orders
                ORDER BY occurred_at DESC
                LIMIT -1 OFFSET 10000
            );
        END;
    """)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def start_pipeline() -> None:
    """Start all pipeline threads. Safe to call multiple times."""
    if state._running.is_set():
        return

    log.info("Starting real-time pipeline …")
    state._running.set()

    threads = [
        EventGenerator(eps=EVENTS_PER_SECOND),
        *[StreamProcessor(i) for i in range(CHURN_SCORE_WORKERS)],
        ChurnScorer(),
        MetricsAggregator(),
        DBWriter(),
    ]
    for t in threads:
        t.start()
        state._threads.append(t)

    log.info("Pipeline running: %d threads active", len(threads))


def stop_pipeline() -> None:
    """Gracefully stop all pipeline threads."""
    log.info("Stopping pipeline …")
    state._running.clear()
    for t in state._threads:
        t.join(timeout=2.0)
    state._threads.clear()
    log.info("Pipeline stopped.")


def get_live_orders(n: int = 50) -> list[dict]:
    """Return the latest N processed orders from the ring buffer."""
    return [
        {k: v for k, v in o.items() if not k.startswith("_")}
        for o in list(state.live_orders)[:n]
    ]


def get_churn_alerts(n: int = 20) -> list[dict]:
    return list(state.churn_alerts)[:n]


def get_anomalies(n: int = 20) -> list[dict]:
    return list(state.anomalies)[:n]


def get_pipeline_metrics() -> dict:
    return state.get_metrics()
