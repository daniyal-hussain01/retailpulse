"""
app.py  —  RetailPulse complete local server
Zero extra pip installs needed. Uses only what is already installed:
  Flask, pandas, numpy, scikit-learn  +  Python stdlib (sqlite3, hashlib …)

HOW TO RUN:
    python app.py

Then open in browser:
    http://localhost:5000              -> analytics dashboard
    http://localhost:5000/api/health
    http://localhost:5000/api/metrics/daily-revenue
    http://localhost:5000/api/metrics/summary
    http://localhost:5000/api/metrics/revenue-by-channel
    http://localhost:5000/api/metrics/inventory
    http://localhost:5000/api/orders
    http://localhost:5000/api/orders/<id>
    http://localhost:5000/api/customers/churn-risk  [POST JSON]
    http://localhost:5000/api/pipeline/status
    http://localhost:5000/api/pipeline/history
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_from_directory

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
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


# ── Database init ─────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
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
    """)
    conn.commit()

    row = conn.execute("SELECT count(*) n FROM fact_orders").fetchone()
    if row["n"] == 0:
        log.info("First run: seeding 90 days of demo data (takes ~5 seconds) ...")
        _seed(conn)
        log.info("Seed complete — %d orders loaded.",
                 conn.execute("SELECT count(*) FROM fact_orders").fetchone()[0])
    conn.close()


def _seed(conn: sqlite3.Connection) -> None:
    channels   = ["organic", "paid_search", "paid_social", "email", "referral"]
    statuses   = ["placed", "processing", "shipped",
                  "delivered", "delivered", "delivered", "refunded", "cancelled"]
    warehouses = ["east-coast-hub", "west-coast-hub", "central-hub"]
    prices     = [999, 1499, 2999, 4999, 8999, 14999, 499, 3999]

    user_hashes = [
        hashlib.sha256(f"{PII_SALT}|user-{i:04d}@example.com".encode()).hexdigest()
        for i in range(200)
    ]

    orders: list = []
    agg: dict    = {}

    for offset in range(89, -1, -1):
        d   = date.today() - timedelta(days=offset)
        ds  = d.isoformat()
        cnt = random.randint(80, 220)

        totals: dict = {"oc": 0, "buyers": set(),
                        "gross": 0, "net": 0, "disc": 0, "ship": 0,
                        "ref": 0, "del": 0}

        for _ in range(cnt):
            qty      = random.randint(1, 3)
            price    = random.choice(prices)
            sub      = qty * price
            disc     = sub // 10 if random.random() < 0.15 else 0
            ship     = 499
            total    = sub - disc + ship
            net      = total - disc
            status   = random.choice(statuses)
            uhash    = random.choice(user_hashes)
            ch       = random.choices(channels, weights=[35, 28, 18, 12, 7])[0]
            h, m     = random.randint(8, 22), random.randint(0, 59)
            ts       = f"{ds}T{h:02d}:{m:02d}:00"

            orders.append((str(uuid.uuid4()), uhash,
                           random.choice(warehouses), status,
                           1 if status == "delivered" else 0,
                           1 if status in ("refunded", "cancelled") else 0,
                           "USD", sub, disc, ship, total, net,
                           ds, d.year, d.month, ch, ts, ts))

            totals["oc"] += 1
            totals["buyers"].add(uhash)
            totals["gross"] += total
            totals["net"]   += net
            totals["disc"]  += disc
            totals["ship"]  += ship
            if status in ("refunded", "cancelled"): totals["ref"] += 1
            if status == "delivered":               totals["del"] += 1

        oc  = totals["oc"]
        ref = totals["ref"]
        agg[ds] = (ds, oc, len(totals["buyers"]),
                   totals["gross"], totals["net"], totals["disc"], totals["ship"],
                   totals["gross"] // oc if oc else 0,
                   ref, round(ref / oc * 100, 2) if oc else 0.0, totals["del"])

    conn.executemany("INSERT OR REPLACE INTO fact_orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", orders)
    conn.executemany("INSERT OR REPLACE INTO agg_daily_revenue VALUES (?,?,?,?,?,?,?,?,?,?,?)", list(agg.values()))

    skus = [
        ("SKU-SHIRT-BLU-M",  "Blue Oxford Shirt M",   "Apparel",    "east-coast-hub",  12, 20, 72.4,  5, "critical"),
        ("SKU-SHIRT-WHT-L",  "White Oxford Shirt L",  "Apparel",    "west-coast-hub",  45, 20, 55.1, 18, "warning"),
        ("SKU-SHOE-RUN-10",  "Running Shoe Size 10",  "Footwear",   "central-hub",     28, 15, 63.2, 12, "warning"),
        ("SKU-BAG-TOTE-BLK", "Black Tote Bag",        "Accessories","east-coast-hub",  90, 10, 48.7, 40, "healthy"),
        ("SKU-HAT-CAP-RED",  "Red Baseball Cap",      "Accessories","west-coast-hub", 130,  8, 35.2, 65, "healthy"),
        ("SKU-JEAN-SLM-32",  "Slim Jeans W32",        "Apparel",    "east-coast-hub",   8, 25, 88.1,  3, "critical"),
        ("SKU-JACKET-BLK-M", "Black Bomber Jacket M", "Outerwear",  "central-hub",      5, 20, 91.3,  2, "critical"),
        ("SKU-SOCK-WHT-ML",  "White Sport Socks 3pk", "Apparel",    "west-coast-hub", 200,  8, 22.1, 99, "healthy"),
    ]
    conn.executemany("INSERT OR REPLACE INTO inventory VALUES (?,?,?,?,?,?,?,?,?)", skus)

    tasks = ["produce_events","bronze_to_silver","dbt_run","dbt_test","data_quality","done"]
    durs  = [45, 180, 90, 30, 25, 2]
    for offset in range(6, -1, -1):
        d  = date.today() - timedelta(days=offset)
        ds = d.isoformat()
        for i, (t, dur) in enumerate(zip(tasks, durs)):
            fail = random.random() < 0.04
            conn.execute("INSERT OR REPLACE INTO pipeline_runs VALUES (?,?,?,?,?,?,?)",
                         (str(uuid.uuid4()), ds, t,
                          "failed" if fail else "success",
                          dur + random.randint(-5, 20),
                          random.randint(40000, 80000),
                          f"{ds}T02:{i*3:02d}:00"))

    checks = ["order_id_unique","user_id_hash_not_null","status_valid",
              "total_cents_positive","pii_not_in_gold","volume_anomaly"]
    for offset in range(6, -1, -1):
        d  = date.today() - timedelta(days=offset)
        ds = d.isoformat()
        for chk in checks:
            fails = random.randint(0, 2) if random.random() < 0.06 else 0
            conn.execute("INSERT OR REPLACE INTO silver_quality VALUES (?,?,?,?,?)",
                         (ds, chk, 0 if fails > 0 else 1,
                          random.randint(45000, 80000), fails))
    conn.commit()


# ── Auth ──────────────────────────────────────────────────────────────────────

def check_auth():
    if not REQUIRE_KEY:
        return None
    if request.headers.get("X-API-Key", "") != API_KEY:
        return jsonify({"error": "Invalid or missing API key"}), 403
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def dashboard():
    f = ROOT / "dashboard" / "index.html"
    if f.exists():
        return send_from_directory(str(ROOT / "dashboard"), "index.html")
    return ("<h2 style='font-family:sans-serif;margin:40px'>"
            "Dashboard not found — make sure "
            "<code>dashboard/index.html</code> exists.</h2>"), 404


@app.route("/api/health")
def health():
    auth = check_auth()
    if auth: return auth
    conn   = get_db()
    counts = {}
    for t in ("fact_orders", "agg_daily_revenue", "inventory", "pipeline_runs"):
        try:    counts[t] = conn.execute(f"SELECT count(*) n FROM {t}").fetchone()["n"]
        except: counts[t] = -1
    conn.close()
    return jsonify({
        "status":       "ok",
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "database":     str(DB_PATH),
        "table_counts": counts,
    })


@app.route("/api/metrics/daily-revenue")
def daily_revenue():
    auth = check_auth()
    if auth: return auth
    limit = min(int(request.args.get("limit", 30)), 365)
    if limit < 1:
        return jsonify({"error": "limit must be >= 1"}), 400
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM agg_daily_revenue ORDER BY order_date DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/metrics/summary")
def summary():
    auth = check_auth()
    if auth: return auth
    days  = int(request.args.get("days", 30))
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_db()
    row   = conn.execute("""
        SELECT sum(order_count)           total_orders,
               sum(gross_revenue_cents)   gross_revenue_cents,
               sum(net_revenue_cents)     net_revenue_cents,
               sum(discount_cents)        discount_cents,
               avg(avg_order_value_cents) avg_order_value_cents,
               avg(refund_rate_pct)       avg_refund_rate_pct,
               sum(delivered_order_count) delivered_count
        FROM   agg_daily_revenue WHERE order_date >= ?
    """, (since,)).fetchone()
    conn.close()
    return jsonify(dict(row))


@app.route("/api/metrics/revenue-by-channel")
def by_channel():
    auth = check_auth()
    if auth: return auth
    days  = int(request.args.get("days", 30))
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_db()
    rows  = conn.execute("""
        SELECT channel,
               count(*)         order_count,
               sum(total_cents) revenue_cents,
               avg(total_cents) avg_order_value_cents
        FROM   fact_orders WHERE order_date >= ?
        GROUP  BY channel ORDER BY revenue_cents DESC
    """, (since,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/orders")
def list_orders():
    auth = check_auth()
    if auth: return auth
    limit  = min(int(request.args.get("limit", 20)), 100)
    status = request.args.get("status")
    conn   = get_db()
    if status:
        rows = conn.execute("""
            SELECT order_id, user_id_hash, warehouse_id, order_status,
                   total_cents, currency, channel, order_date, created_at
            FROM fact_orders WHERE order_status=?
            ORDER BY created_at DESC LIMIT ?
        """, (status, limit)).fetchall()
    else:
        rows = conn.execute("""
            SELECT order_id, user_id_hash, warehouse_id, order_status,
                   total_cents, currency, channel, order_date, created_at
            FROM fact_orders ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/orders/<order_id>")
def get_order(order_id: str):
    auth = check_auth()
    if auth: return auth
    if not re.match(r"^[0-9a-f\-]{32,36}$", order_id.lower()):
        return jsonify({"error": "Invalid order_id format"}), 400
    conn = get_db()
    row  = conn.execute(
        "SELECT * FROM fact_orders WHERE order_id=?", (order_id,)
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Order not found"}), 404
    return jsonify(dict(row))


@app.route("/api/metrics/inventory")
def inventory():
    auth = check_auth()
    if auth: return auth
    sf   = request.args.get("status")
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM inventory WHERE status=? ORDER BY days_remaining" if sf
        else "SELECT * FROM inventory ORDER BY days_remaining",
        (sf,) if sf else ()
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/customers/churn-risk", methods=["POST", "OPTIONS"])
def churn_risk():
    if request.method == "OPTIONS":
        return "", 200
    auth = check_auth()
    if auth: return auth

    data = request.get_json(silent=True) or {}
    try:
        days   = int(data.get("days_since_last_order", 0))
        orders = int(data.get("total_orders", 0))
        aov    = int(data.get("avg_order_value_cents", 0))
        uhash  = str(data.get("user_id_hash", ""))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid input — expected integers"}), 422

    if not (0 <= days <= 3650):
        return jsonify({"error": "days_since_last_order must be 0–3650"}), 422
    if orders < 0:
        return jsonify({"error": "total_orders must be >= 0"}), 422
    if aov < 0:
        return jsonify({"error": "avg_order_value_cents must be >= 0"}), 422

    recency  = 1.0 if days > 90 else (0.7 if days > 60 else (0.4 if days > 30 else 0.1))
    freq     = max(0.0, 1.0 - orders * 0.1)
    monetary = 0.6 if aov < 1000 else (0.3 if aov < 5000 else 0.1)
    score    = min(1.0, 0.5 * recency + 0.3 * freq + 0.2 * monetary)

    if score >= 0.7:
        tier, rec = "high",   "Send win-back offer with 20% discount immediately."
    elif score >= 0.4:
        tier, rec = "medium", "Enrol in 3-touch re-engagement email sequence."
    else:
        tier, rec = "low",    "No action needed — review at next 30-day cycle."

    return jsonify({
        "user_id_hash":      uhash,
        "churn_probability": round(score, 4),
        "risk_tier":         tier,
        "recommendation":    rec,
    })


@app.route("/api/pipeline/status")
def pipeline_status():
    auth = check_auth()
    if auth: return auth
    conn = get_db()
    runs = conn.execute("""
        SELECT task_name, status, duration_s, records, started_at
        FROM   pipeline_runs
        WHERE  run_date=(SELECT max(run_date) FROM pipeline_runs)
        ORDER  BY started_at
    """).fetchall()
    qual = conn.execute("""
        SELECT check_name, passed, rows_checked, failed_rows
        FROM   silver_quality
        WHERE  check_date=(SELECT max(check_date) FROM silver_quality)
    """).fetchall()
    last = conn.execute(
        "SELECT max(run_date) last_run FROM pipeline_runs"
    ).fetchone()
    conn.close()
    return jsonify({
        "last_run_date":  last["last_run"],
        "tasks":          [dict(r) for r in runs],
        "quality_checks": [dict(r) for r in qual],
    })


@app.route("/api/pipeline/history")
def pipeline_history():
    auth = check_auth()
    if auth: return auth
    conn = get_db()
    rows = conn.execute("""
        SELECT run_date,
               sum(case when status='success' then 1 else 0 end) success_count,
               sum(case when status='failed'  then 1 else 0 end) failed_count,
               sum(records) total_records
        FROM   pipeline_runs
        GROUP  BY run_date ORDER BY run_date DESC LIMIT 30
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ── CORS + Error handlers ─────────────────────────────────────────────────────

@app.after_request
def cors(r: Response) -> Response:
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type, X-API-Key"
    r.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return r


@app.errorhandler(404)
def e404(e): return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def e500(e): return jsonify({"error": "Internal server error"}), 500


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Initialising database …")
    init_db()
    port = int(os.environ.get("PORT", 5000))
    log.info("=" * 52)
    log.info("  RetailPulse is running!")
    log.info("  Dashboard  ->  http://localhost:%d", port)
    log.info("  API health ->  http://localhost:%d/api/health", port)
    log.info("=" * 52)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
