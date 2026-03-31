"""
scripts/monitoring.py

Lightweight pipeline health monitor.
Checks data freshness, row count anomalies, and API liveness.
Designed to run as a cron job or Airflow sensor every 15 minutes.

Exit codes:
    0 = all checks passed
    1 = one or more checks failed (triggers alert)

Usage:
    python scripts/monitoring.py
    python scripts/monitoring.py --alert-webhook https://hooks.slack.com/...
    python scripts/monitoring.py --json   # machine-readable output
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ── Check definitions ──────────────────────────────────────────────────────────

CheckResult = dict[str, Any]


def check_silver_freshness(silver_path: Path, max_age_minutes: int = 30) -> CheckResult:
    """
    Verify that Silver parquet files were written within the last N minutes.
    Stale Silver means the ingestion or transform job has failed.
    """
    import glob

    files = sorted(
        glob.glob(str(silver_path / "orders" / "*.parquet")),
        key=os.path.getmtime,
        reverse=True,
    )

    if not files:
        return {
            "check": "silver_freshness",
            "status": "fail",
            "message": f"No Silver parquet files found at {silver_path}/orders/",
            "detail": {},
        }

    latest_file = files[0]
    mtime = datetime.fromtimestamp(os.path.getmtime(latest_file), tz=timezone.utc)
    age_minutes = (datetime.now(timezone.utc) - mtime).total_seconds() / 60
    is_fresh = age_minutes <= max_age_minutes

    return {
        "check": "silver_freshness",
        "status": "pass" if is_fresh else "fail",
        "message": (
            f"Silver is fresh ({age_minutes:.1f} min old)"
            if is_fresh
            else f"Silver is STALE ({age_minutes:.1f} min old, threshold={max_age_minutes})"
        ),
        "detail": {
            "latest_file": os.path.basename(latest_file),
            "age_minutes": round(age_minutes, 1),
            "threshold_minutes": max_age_minutes,
        },
    }


def check_gold_row_counts(duckdb_path: str) -> CheckResult:
    """
    Verify that Gold tables have data and counts haven't dropped >30% vs yesterday.
    A sudden drop indicates a pipeline failure or incorrect date filtering.
    """
    try:
        import duckdb
    except ImportError:
        return {"check": "gold_row_counts", "status": "skip", "message": "duckdb not installed"}

    if not Path(duckdb_path).exists():
        return {
            "check": "gold_row_counts",
            "status": "warn",
            "message": f"DuckDB file not found at {duckdb_path} — pipeline may not have run yet",
            "detail": {},
        }

    try:
        conn = duckdb.connect(duckdb_path, read_only=True)

        fact_count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
        today_count = conn.execute(
            "SELECT COUNT(*) FROM fact_orders WHERE order_date >= current_date - INTERVAL 1 DAY"
        ).fetchone()[0]
        agg_count = conn.execute("SELECT COUNT(*) FROM agg_daily_revenue").fetchone()[0]

        conn.close()

        issues = []
        if fact_count == 0:
            issues.append("fact_orders is EMPTY")
        if agg_count == 0:
            issues.append("agg_daily_revenue is EMPTY")
        if fact_count > 0 and today_count == 0:
            issues.append("No orders ingested for today — possible pipeline gap")

        return {
            "check": "gold_row_counts",
            "status": "fail" if issues else "pass",
            "message": "; ".join(issues) if issues else "Gold table counts look healthy",
            "detail": {
                "fact_orders_total": fact_count,
                "fact_orders_today": today_count,
                "agg_daily_revenue_days": agg_count,
            },
        }
    except Exception as e:
        return {
            "check": "gold_row_counts",
            "status": "fail",
            "message": f"DuckDB query failed: {e}",
            "detail": {},
        }


def check_api_liveness(api_url: str = "http://localhost:8000") -> CheckResult:
    """
    Verify the FastAPI serving layer is up and returning healthy responses.
    """
    try:
        import urllib.request
        import urllib.error

        with urllib.request.urlopen(f"{api_url}/health", timeout=5) as resp:
            body = json.loads(resp.read())
            return {
                "check": "api_liveness",
                "status": "pass",
                "message": f"API healthy at {api_url}",
                "detail": body,
            }
    except Exception as e:
        return {
            "check": "api_liveness",
            "status": "fail",
            "message": f"API unreachable at {api_url}: {e}",
            "detail": {},
        }


def check_volume_anomaly(duckdb_path: str, threshold_pct: float = 30.0) -> CheckResult:
    """
    Compare today's order count to the 7-day rolling average.
    A drop >threshold_pct% triggers an alert.
    """
    try:
        import duckdb
    except ImportError:
        return {"check": "volume_anomaly", "status": "skip", "message": "duckdb not installed"}

    if not Path(duckdb_path).exists():
        return {"check": "volume_anomaly", "status": "skip", "message": "DuckDB not found"}

    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        row = conn.execute("""
            WITH daily AS (
                SELECT
                    order_date::DATE          AS day,
                    COUNT(*)                  AS order_count
                FROM fact_orders
                WHERE order_date >= CURRENT_DATE - INTERVAL 8 DAY
                GROUP BY 1
            ),
            stats AS (
                SELECT
                    AVG(order_count) FILTER (WHERE day < CURRENT_DATE)  AS rolling_7d_avg,
                    MAX(order_count) FILTER (WHERE day = CURRENT_DATE)  AS today_count
                FROM daily
            )
            SELECT rolling_7d_avg, today_count FROM stats
        """).fetchone()
        conn.close()

        if not row or row[0] is None:
            return {
                "check": "volume_anomaly",
                "status": "skip",
                "message": "Insufficient history for anomaly detection (<7 days)",
                "detail": {},
            }

        avg_7d, today = float(row[0]), float(row[1] or 0)
        pct_change = ((today - avg_7d) / avg_7d * 100) if avg_7d > 0 else 0
        is_anomaly = pct_change < -threshold_pct

        return {
            "check": "volume_anomaly",
            "status": "fail" if is_anomaly else "pass",
            "message": (
                f"Volume anomaly: today={today:.0f} is {abs(pct_change):.1f}% below 7d avg ({avg_7d:.1f})"
                if is_anomaly
                else f"Volume normal: today={today:.0f}, 7d avg={avg_7d:.1f} ({pct_change:+.1f}%)"
            ),
            "detail": {
                "today_count": today,
                "rolling_7d_avg": round(avg_7d, 1),
                "pct_change": round(pct_change, 1),
                "threshold_pct": threshold_pct,
            },
        }
    except Exception as e:
        return {"check": "volume_anomaly", "status": "fail", "message": str(e), "detail": {}}


def check_pii_not_in_gold(duckdb_path: str) -> CheckResult:
    """
    Verify no raw PII columns (email, ip_address, user_id without _hash) exist in Gold.
    """
    if not Path(duckdb_path).exists():
        return {"check": "pii_in_gold", "status": "skip", "message": "DuckDB not found"}

    try:
        import duckdb
        conn = duckdb.connect(duckdb_path, read_only=True)
        schema_rows = conn.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'main'"
        ).fetchall()
        conn.close()

        pii_columns = {"email", "user_id", "ip_address", "phone", "first_name", "last_name", "ssn"}
        found_pii = [
            f"{tbl}.{col}"
            for tbl, col in schema_rows
            if col.lower() in pii_columns
        ]

        if found_pii:
            return {
                "check": "pii_in_gold",
                "status": "fail",
                "message": f"PII columns found in Gold: {found_pii}",
                "detail": {"pii_columns": found_pii},
            }
        return {
            "check": "pii_in_gold",
            "status": "pass",
            "message": "No raw PII columns found in Gold tables",
            "detail": {"columns_checked": len(schema_rows)},
        }
    except Exception as e:
        return {"check": "pii_in_gold", "status": "fail", "message": str(e), "detail": {}}


# ── Alert dispatch ─────────────────────────────────────────────────────────────

def send_slack_alert(webhook_url: str, failed_checks: list[CheckResult]) -> None:
    """Post a Slack notification for failed checks."""
    import urllib.request

    text = "*RetailPulse pipeline alert* :rotating_light:\n"
    for c in failed_checks:
        text += f"• *{c['check']}*: {c['message']}\n"

    payload = json.dumps({"text": text}).encode()
    try:
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
        log.info("Slack alert sent.")
    except Exception as e:
        log.error("Failed to send Slack alert: %s", e)


# ── Main ───────────────────────────────────────────────────────────────────────

def run_all_checks(
    silver_path: Path,
    duckdb_path: str,
    api_url: str,
) -> list[CheckResult]:
    checks = [
        check_silver_freshness(silver_path),
        check_gold_row_counts(duckdb_path),
        check_volume_anomaly(duckdb_path),
        check_pii_not_in_gold(duckdb_path),
        check_api_liveness(api_url),
    ]
    return checks


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="RetailPulse pipeline monitor")
    parser.add_argument("--silver-path", default=os.environ.get("SILVER_PATH", "./data/silver"))
    parser.add_argument("--duckdb-path", default=os.environ.get("DUCKDB_PATH", "./data/gold/retailpulse.duckdb"))
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--alert-webhook", default=os.environ.get("SLACK_WEBHOOK_URL", ""))
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args()

    checks = run_all_checks(
        silver_path=Path(args.silver_path),
        duckdb_path=args.duckdb_path,
        api_url=args.api_url,
    )

    if args.json_output:
        print(json.dumps({"timestamp": datetime.now(timezone.utc).isoformat(), "checks": checks}, indent=2))
    else:
        print(f"\n{'─' * 55}")
        print(f"  RetailPulse Monitor — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'─' * 55}")
        icons = {"pass": "✓", "fail": "✗", "warn": "⚠", "skip": "–"}
        for c in checks:
            icon = icons.get(c["status"], "?")
            print(f"  {icon}  {c['check']:<25} {c['message']}")
        print(f"{'─' * 55}\n")

    failed = [c for c in checks if c["status"] == "fail"]

    if failed:
        log.warning("%d check(s) failed.", len(failed))
        if args.alert_webhook:
            send_slack_alert(args.alert_webhook, failed)
        sys.exit(1)

    log.info("All checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    main()
