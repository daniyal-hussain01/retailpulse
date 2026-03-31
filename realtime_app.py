"""
realtime_app.py  —  RetailPulse Real-Time Pipeline Server

Zero extra pip installs. Uses only: Flask, sqlite3, threading, queue (all built-in or already installed).

HOW TO RUN:
    python realtime_app.py

Opens:
    http://localhost:5000              → Real-time analytics dashboard
    http://localhost:5000/stream/events → Server-Sent Events (SSE) stream
    http://localhost:5000/api/rt/orders → Latest live orders (JSON)
    http://localhost:5000/api/rt/metrics → Pipeline KPIs (JSON)
    http://localhost:5000/api/rt/churn-alerts → High-risk users (JSON)
    http://localhost:5000/api/rt/anomalies → Detected anomalies (JSON)
    http://localhost:5000/api/health    → System health check (JSON)

    All original batch endpoints still work:
    http://localhost:5000/api/metrics/daily-revenue
    http://localhost:5000/api/metrics/summary
    http://localhost:5000/api/orders
    etc.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import random
import re
import sqlite3
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_from_directory

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("retailpulse")

# ── Config ────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent
DB_PATH     = ROOT / "data" / "gold" / "retailpulse.db"
PII_SALT    = os.environ.get("PII_SALT", "local_dev_salt_change_in_prod")
API_KEY     = os.environ.get("API_SECRET_KEY", "")
REQUIRE_KEY = os.environ.get("REQUIRE_API_KEY", "false").lower() == "true"

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

app = Flask(__name__, static_folder=str(ROOT / "dashboard"))


# ══════════════════════════════════════════════════════════════════════════════
# Database — same as app.py plus real-time tables
# ══════════════════════════════════════════════════════════════════════════════

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id          TEXT PRIMARY KEY,
            user_id_hash      TEXT NOT NULL,
            warehouse_id      TEXT,
            order_status      TEXT NOT NULL DEFAULT 'placed',
            is_delivered      INTEGER NOT NULL DEFAULT 0,
            is_refunded       INTEGER NOT NULL DEFAULT 0,
            currency          TEXT NOT NULL DEFAULT 'USD',
            subtotal_cents    INTEGER NOT NULL DEFAULT 0,
            discount_cents    INTEGER NOT NULL DEFAULT 0,
            shipping_cents    INTEGER NOT NULL DEFAULT 0,
            total_cents       INTEGER NOT NULL DEFAULT 0,
            net_revenue_cents INTEGER NOT NULL DEFAULT 0,
            order_date        TEXT NOT NULL,
            order_year        INTEGER,
            order_month       INTEGER,
            channel           TEXT DEFAULT 'organic',
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS agg_daily_revenue (
            order_date            TEXT PRIMARY KEY,
            order_count           INTEGER NOT NULL DEFAULT 0,
            unique_buyers         INTEGER NOT NULL DEFAULT 0,
            gross_revenue_cents   INTEGER NOT NULL DEFAULT 0,
            net_revenue_cents     INTEGER NOT NULL DEFAULT 0,
            discount_cents        INTEGER NOT NULL DEFAULT 0,
            shipping_cents        INTEGER NOT NULL DEFAULT 0,
            avg_order_value_cents INTEGER NOT NULL DEFAULT 0,
            refunded_order_count  INTEGER NOT NULL DEFAULT 0,
            refund_rate_pct       REAL    NOT NULL DEFAULT 0.0,
            delivered_order_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS inventory (
            sku             TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            category        TEXT NOT NULL,
            warehouse       TEXT NOT NULL,
            qty_on_hand     INTEGER NOT NULL DEFAULT 0,
            reorder_point   INTEGER NOT NULL DEFAULT 20,
            sell_through_7d REAL    NOT NULL DEFAULT 0.0,
            days_remaining  INTEGER NOT NULL DEFAULT 99,
            status          TEXT    NOT NULL DEFAULT 'healthy'
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id     TEXT PRIMARY KEY,
            run_date   TEXT NOT NULL,
            task_name  TEXT NOT NULL,
            status     TEXT NOT NULL,
            duration_s INTEGER NOT NULL DEFAULT 0,
            records    INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS silver_quality (
            check_date   TEXT NOT NULL,
            check_name   TEXT NOT NULL,
            passed       INTEGER NOT NULL DEFAULT 1,
            rows_checked INTEGER NOT NULL DEFAULT 0,
            failed_rows  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (check_date, check_name)
        );
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

    row = conn.execute("SELECT count(*) n FROM fact_orders").fetchone()
    if row["n"] == 0:
        log.info("Seeding 90 days of demo data …")
        _seed(conn)
        log.info("Seed complete.")
    conn.close()


def _seed(conn: sqlite3.Connection) -> None:
    channels   = ["organic","paid_search","paid_social","email","referral"]
    statuses   = ["placed","processing","shipped","delivered","delivered",
                  "delivered","refunded","cancelled"]
    warehouses = ["east-coast-hub","west-coast-hub","central-hub"]
    prices     = [999,1499,2999,4999,8999,14999,499,3999]

    user_hashes = [
        hashlib.sha256(f"{PII_SALT}|user-{i:04d}@example.com".encode()).hexdigest()
        for i in range(200)
    ]
    orders: list = []
    agg: dict    = {}

    for offset in range(89, -1, -1):
        d = date.today() - timedelta(days=offset)
        ds = d.isoformat()
        cnt = random.randint(80, 220)
        totals: dict = {"oc":0,"buyers":set(),"gross":0,"net":0,
                        "disc":0,"ship":0,"ref":0,"del":0}
        for _ in range(cnt):
            qty = random.randint(1,3); price = random.choice(prices)
            sub = qty*price; disc = sub//10 if random.random()<0.15 else 0
            ship = 499; total = sub-disc+ship; net = total-disc
            status = random.choice(statuses)
            uhash  = random.choice(user_hashes)
            ch     = random.choices(channels,weights=[35,28,18,12,7])[0]
            h,m    = random.randint(8,22),random.randint(0,59)
            ts     = f"{ds}T{h:02d}:{m:02d}:00"
            orders.append((str(uuid.uuid4()),uhash,
                           random.choice(warehouses),status,
                           1 if status=="delivered" else 0,
                           1 if status in ("refunded","cancelled") else 0,
                           "USD",sub,disc,ship,total,net,
                           ds,d.year,d.month,ch,ts,ts))
            totals["oc"]+=1; totals["buyers"].add(uhash)
            totals["gross"]+=total; totals["net"]+=net
            totals["disc"]+=disc; totals["ship"]+=ship
            if status in("refunded","cancelled"): totals["ref"]+=1
            if status=="delivered":               totals["del"]+=1
        oc=totals["oc"]; ref=totals["ref"]
        agg[ds]=(ds,oc,len(totals["buyers"]),
                 totals["gross"],totals["net"],totals["disc"],totals["ship"],
                 totals["gross"]//oc if oc else 0,
                 ref,round(ref/oc*100,2) if oc else 0.0,totals["del"])

    conn.executemany(
        "INSERT OR REPLACE INTO fact_orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        orders)
    conn.executemany(
        "INSERT OR REPLACE INTO agg_daily_revenue VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        list(agg.values()))

    skus=[
        ("SKU-SHIRT-BLU-M","Blue Oxford Shirt M","Apparel","east-coast-hub",12,20,72.4,5,"critical"),
        ("SKU-SHIRT-WHT-L","White Oxford Shirt L","Apparel","west-coast-hub",45,20,55.1,18,"warning"),
        ("SKU-SHOE-RUN-10","Running Shoe Size 10","Footwear","central-hub",28,15,63.2,12,"warning"),
        ("SKU-BAG-TOTE-BLK","Black Tote Bag","Accessories","east-coast-hub",90,10,48.7,40,"healthy"),
        ("SKU-HAT-CAP-RED","Red Baseball Cap","Accessories","west-coast-hub",130,8,35.2,65,"healthy"),
        ("SKU-JEAN-SLM-32","Slim Jeans W32","Apparel","east-coast-hub",8,25,88.1,3,"critical"),
        ("SKU-JACKET-BLK-M","Black Bomber Jacket M","Outerwear","central-hub",5,20,91.3,2,"critical"),
        ("SKU-SOCK-WHT-ML","White Sport Socks 3pk","Apparel","west-coast-hub",200,8,22.1,99,"healthy"),
    ]
    conn.executemany("INSERT OR REPLACE INTO inventory VALUES(?,?,?,?,?,?,?,?,?)",skus)

    tasks=["produce_events","bronze_to_silver","dbt_run","dbt_test","data_quality","done"]
    durs=[45,180,90,30,25,2]
    for offset in range(6,-1,-1):
        d=date.today()-timedelta(days=offset); ds=d.isoformat()
        for i,(t,dur) in enumerate(zip(tasks,durs)):
            conn.execute(
                "INSERT OR REPLACE INTO pipeline_runs VALUES (?,?,?,?,?,?,?)",
                (str(uuid.uuid4()),ds,t,
                 "failed" if random.random()<0.04 else "success",
                 dur+random.randint(-5,20),random.randint(40000,80000),
                 f"{ds}T02:{i*3:02d}:00"))

    checks=["order_id_unique","user_id_hash_not_null","status_valid",
            "total_cents_positive","pii_not_in_gold","volume_anomaly"]
    for offset in range(6,-1,-1):
        d=date.today()-timedelta(days=offset); ds=d.isoformat()
        for chk in checks:
            fails=random.randint(0,2) if random.random()<0.06 else 0
            conn.execute("INSERT OR REPLACE INTO silver_quality VALUES (?,?,?,?,?)",
                         (ds,chk,0 if fails>0 else 1,
                          random.randint(45000,80000),fails))
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# Real-time pipeline (import stream engine)
# ══════════════════════════════════════════════════════════════════════════════

from realtime.stream_engine import (
    start_pipeline, stop_pipeline,
    get_live_orders, get_churn_alerts, get_anomalies,
    get_pipeline_metrics, state as pipeline_state,
)


# ══════════════════════════════════════════════════════════════════════════════
# Auth + helpers
# ══════════════════════════════════════════════════════════════════════════════

def check_auth():
    if not REQUIRE_KEY: return None
    if request.headers.get("X-API-Key","") != API_KEY:
        return jsonify({"error":"Invalid or missing API key"}), 403
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SSE — Server-Sent Events endpoint
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/stream/events")
def sse_stream():
    """
    Real-time SSE endpoint. The dashboard connects here and receives
    live events: orders, churn_alerts, anomalies, metrics.

    Message format:
        data: {"type": "order"|"churn_alert"|"anomaly"|"metrics", "data": {...}, "ts": "..."}
    """
    def event_generator():
        q: queue.Queue = queue.Queue(maxsize=200)

        def push(payload: str) -> None:
            try:
                q.put_nowait(payload)
            except queue.Full:
                pass

        pipeline_state.subscribe_sse(push)
        try:
            # Send initial snapshot so dashboard shows data immediately
            snapshot = {
                "type": "snapshot",
                "data": {
                    "live_orders":   get_live_orders(20),
                    "churn_alerts":  get_churn_alerts(10),
                    "anomalies":     get_anomalies(10),
                    "metrics":       get_pipeline_metrics(),
                },
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            yield f"data: {json.dumps(snapshot)}\n\n"

            while True:
                try:
                    payload = q.get(timeout=15.0)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    # Heartbeat to keep connection alive
                    yield f"data: {json.dumps({'type':'heartbeat'})}\n\n"
        except GeneratorExit:
            pass
        finally:
            pipeline_state.unsubscribe_sse(push)

    return Response(
        event_generator(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# Real-time REST endpoints
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/rt/orders")
def rt_orders():
    n = min(int(request.args.get("limit", 50)), 200)
    return jsonify(get_live_orders(n))


@app.route("/api/rt/churn-alerts")
def rt_churn_alerts():
    return jsonify(get_churn_alerts())


@app.route("/api/rt/anomalies")
def rt_anomalies():
    return jsonify(get_anomalies())


@app.route("/api/rt/metrics")
def rt_metrics():
    return jsonify(get_pipeline_metrics())


@app.route("/api/rt/stats")
def rt_stats():
    """Aggregated stats from the rt_orders table (last N minutes)."""
    minutes = int(request.args.get("minutes", 5))
    since   = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    since_s = since.isoformat()

    conn = get_db()
    row  = conn.execute("""
        SELECT count(*)               total_orders,
               sum(total_cents)       revenue_cents,
               avg(total_cents)       avg_order_cents,
               avg(churn_score)       avg_churn_score,
               sum(is_refunded)       refunds,
               count(distinct channel) channels_active
        FROM   rt_orders
        WHERE  occurred_at >= ?
    """, (since_s,)).fetchone()
    by_channel = conn.execute("""
        SELECT channel, count(*) cnt, sum(total_cents) rev
        FROM   rt_orders
        WHERE  occurred_at >= ?
        GROUP  BY channel ORDER BY cnt DESC
    """, (since_s,)).fetchall()
    by_tier = conn.execute("""
        SELECT value_tier, count(*) cnt
        FROM   rt_orders WHERE occurred_at >= ?
        GROUP  BY value_tier
    """, (since_s,)).fetchall()
    conn.close()

    return jsonify({
        "window_minutes": minutes,
        "summary":        dict(row),
        "by_channel":     [dict(r) for r in by_channel],
        "by_tier":        [dict(r) for r in by_tier],
    })


@app.route("/api/rt/churn-score", methods=["POST", "OPTIONS"])
def rt_churn_score():
    """On-demand churn score for any user."""
    if request.method == "OPTIONS": return "", 200
    data = request.get_json(silent=True) or {}
    try:
        days   = int(data.get("days_since_last_order", 0))
        orders = int(data.get("total_orders", 0))
        aov    = int(data.get("avg_order_value_cents", 0))
        uhash  = str(data.get("user_id_hash",""))
    except (TypeError, ValueError):
        return jsonify({"error":"Invalid input"}), 422

    if not (0 <= days <= 3650):
        return jsonify({"error":"days_since_last_order must be 0–3650"}), 422

    r = 1.0 if days>90 else(0.7 if days>60 else(0.4 if days>30 else 0.1))
    f = max(0.0, 1.0 - orders*0.1)
    m = 0.6 if aov<1000 else(0.3 if aov<5000 else 0.1)
    score = min(1.0, 0.5*r + 0.3*f + 0.2*m)

    tier = "high" if score>=0.7 else("medium" if score>=0.4 else "low")
    rec  = {
        "high":   "Send win-back offer — 20% discount immediately.",
        "medium": "Enrol in 3-touch re-engagement email sequence.",
        "low":    "No action needed — review at next 30-day cycle.",
    }[tier]

    return jsonify({
        "user_id_hash":      uhash,
        "churn_probability": round(score,4),
        "risk_tier":         tier,
        "recommendation":    rec,
        "scored_at":         datetime.now(timezone.utc).isoformat(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# Original batch endpoints (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def dashboard():
    rt_dash = ROOT / "dashboard" / "realtime.html"
    std_dash = ROOT / "dashboard" / "index.html"
    target = rt_dash if rt_dash.exists() else std_dash
    if target.exists():
        return send_from_directory(str(ROOT/"dashboard"), target.name)
    return "<h2>Dashboard not found</h2>", 404


@app.route("/classic")
def classic_dashboard():
    return send_from_directory(str(ROOT/"dashboard"), "index.html")


@app.route("/api/health")
def health():
    conn = get_db(); counts = {}
    for t in ("fact_orders","agg_daily_revenue","inventory","pipeline_runs","rt_orders"):
        try:    counts[t] = conn.execute(f"SELECT count(*) n FROM {t}").fetchone()["n"]
        except: counts[t] = -1
    conn.close()
    pm = get_pipeline_metrics()
    return jsonify({
        "status":           "ok",
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "database":         str(DB_PATH),
        "table_counts":     counts,
        "pipeline_running": pipeline_state._running.is_set(),
        "pipeline_metrics": {
            "events_per_second":  pm["events_per_second"],
            "events_processed":   pm["events_processed"],
            "churn_alerts_fired": pm["churn_alerts_fired"],
            "uptime_seconds":     pm["uptime_seconds"],
        },
    })


@app.route("/api/metrics/daily-revenue")
def daily_revenue():
    limit = min(int(request.args.get("limit",30)),365)
    if limit < 1: return jsonify({"error":"limit must be >= 1"}),400
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM agg_daily_revenue ORDER BY order_date DESC LIMIT ?",
        (limit,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/metrics/summary")
def summary():
    days  = int(request.args.get("days",30))
    since = (date.today()-timedelta(days=days)).isoformat()
    conn  = get_db()
    row   = conn.execute("""
        SELECT sum(order_count)           total_orders,
               sum(gross_revenue_cents)   gross_revenue_cents,
               sum(net_revenue_cents)     net_revenue_cents,
               sum(discount_cents)        discount_cents,
               avg(avg_order_value_cents) avg_order_value_cents,
               avg(refund_rate_pct)       avg_refund_rate_pct,
               sum(delivered_order_count) delivered_count
        FROM agg_daily_revenue WHERE order_date >= ?
    """, (since,)).fetchone()
    conn.close()
    return jsonify(dict(row))


@app.route("/api/metrics/revenue-by-channel")
def by_channel():
    days  = int(request.args.get("days",30))
    since = (date.today()-timedelta(days=days)).isoformat()
    conn  = get_db()
    rows  = conn.execute("""
        SELECT channel, count(*) order_count,
               sum(total_cents) revenue_cents,
               avg(total_cents) avg_order_value_cents
        FROM fact_orders WHERE order_date >= ?
        GROUP BY channel ORDER BY revenue_cents DESC
    """, (since,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/orders")
def list_orders():
    limit  = min(int(request.args.get("limit",20)),100)
    status = request.args.get("status")
    conn   = get_db()
    if status:
        rows = conn.execute(
            "SELECT order_id,user_id_hash,warehouse_id,order_status,"
            "total_cents,currency,channel,order_date,created_at "
            "FROM fact_orders WHERE order_status=? "
            "ORDER BY created_at DESC LIMIT ?", (status,limit)).fetchall()
    else:
        rows = conn.execute(
            "SELECT order_id,user_id_hash,warehouse_id,order_status,"
            "total_cents,currency,channel,order_date,created_at "
            "FROM fact_orders ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/orders/<order_id>")
def get_order(order_id: str):
    if not re.match(r"^[0-9a-f\-]{32,36}$", order_id.lower()):
        return jsonify({"error":"Invalid order_id format"}),400
    conn = get_db()
    row  = conn.execute(
        "SELECT * FROM fact_orders WHERE order_id=?", (order_id,)).fetchone()
    conn.close()
    if not row: return jsonify({"error":"Order not found"}),404
    return jsonify(dict(row))


@app.route("/api/metrics/inventory")
def inventory():
    sf   = request.args.get("status")
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM inventory WHERE status=? ORDER BY days_remaining" if sf
        else "SELECT * FROM inventory ORDER BY days_remaining",
        (sf,) if sf else ()).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/pipeline/status")
def pipeline_status():
    conn = get_db()
    runs = conn.execute("""
        SELECT task_name, status, duration_s, records, started_at
        FROM pipeline_runs
        WHERE run_date=(SELECT max(run_date) FROM pipeline_runs)
        ORDER BY started_at
    """).fetchall()
    qual = conn.execute("""
        SELECT check_name, passed, rows_checked, failed_rows
        FROM silver_quality
        WHERE check_date=(SELECT max(check_date) FROM silver_quality)
    """).fetchall()
    last = conn.execute(
        "SELECT max(run_date) last_run FROM pipeline_runs").fetchone()
    conn.close()
    return jsonify({
        "last_run_date":  last["last_run"],
        "tasks":          [dict(r) for r in runs],
        "quality_checks": [dict(r) for r in qual],
    })


@app.route("/api/pipeline/history")
def pipeline_history():
    conn = get_db()
    rows = conn.execute("""
        SELECT run_date,
               sum(case when status='success' then 1 else 0 end) success_count,
               sum(case when status='failed'  then 1 else 0 end) failed_count,
               sum(records) total_records
        FROM pipeline_runs
        GROUP BY run_date ORDER BY run_date DESC LIMIT 30
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/customers/churn-risk", methods=["POST","OPTIONS"])
def churn_risk():
    if request.method == "OPTIONS": return "", 200
    return rt_churn_score()


# ── CORS + errors ─────────────────────────────────────────────────────────────
@app.after_request
def cors(r: Response) -> Response:
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type, X-API-Key"
    r.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return r

@app.errorhandler(404)
def e404(e): return jsonify({"error":"Not found"}), 404

@app.errorhandler(500)
def e500(e): return jsonify({"error":"Internal server error"}), 500


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("Initialising database …")
    init_db()

    log.info("Starting real-time pipeline …")
    start_pipeline()

    port = int(os.environ.get("PORT", 5000))
    log.info("=" * 60)
    log.info("  RetailPulse Real-Time Pipeline is LIVE!")
    log.info("  Dashboard    →  http://localhost:%d", port)
    log.info("  Classic dash →  http://localhost:%d/classic", port)
    log.info("  Live events  →  http://localhost:%d/stream/events", port)
    log.info("  API health   →  http://localhost:%d/api/health", port)
    log.info("  RT metrics   →  http://localhost:%d/api/rt/metrics", port)
    log.info("=" * 60)

    try:
        app.run(host="0.0.0.0", port=port, debug=False,
                use_reloader=False, threaded=True)
    finally:
        stop_pipeline()
