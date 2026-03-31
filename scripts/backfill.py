#!/usr/bin/env python3
"""
scripts/backfill.py

Reprocess Bronze → Silver for a date range.
Useful after pipeline outages, schema migrations, or salt rotations.

Usage:
    # Reprocess last 7 days
    python scripts/backfill.py --start 2024-04-18 --end 2024-04-25

    # Reprocess a single date
    python scripts/backfill.py --start 2024-04-25 --end 2024-04-25

    # Dry run (show what would be processed without writing)
    python scripts/backfill.py --start 2024-04-18 --end 2024-04-25 --dry-run

    # Include clickstream sessionisation
    python scripts/backfill.py --start 2024-04-18 --end 2024-04-25 --include-sessions
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from typing import Generator

log = logging.getLogger(__name__)


def date_range(start: date, end: date) -> Generator[date, None, None]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def backfill_orders(
    start: date,
    end: date,
    dry_run: bool = False,
) -> dict[str, int]:
    from spark.jobs.bronze_to_silver import run_local_transform

    total_processed = 0
    total_valid     = 0
    total_failed    = 0
    failed_dates    = []

    dates = list(date_range(start, end))
    log.info("Backfilling %d dates: %s → %s", len(dates), start, end)

    for i, d in enumerate(dates, 1):
        ds = d.isoformat()
        log.info("[%d/%d] Processing %s ...", i, len(dates), ds)

        if dry_run:
            log.info("  DRY RUN — would process %s", ds)
            continue

        try:
            metrics = run_local_transform(run_date=ds)
            total_processed += metrics.get("processed", 0)
            total_valid     += metrics.get("valid", 0)
            log.info("  ✓ %s: %d valid records", ds, metrics.get("valid", 0))
        except Exception as e:
            log.error("  ✗ %s failed: %s", ds, e)
            total_failed += 1
            failed_dates.append(ds)

    return {
        "dates_attempted": len(dates),
        "dates_failed":    total_failed,
        "total_processed": total_processed,
        "total_valid":     total_valid,
        "failed_dates":    failed_dates,
    }


def backfill_sessions(
    start: date,
    end: date,
    dry_run: bool = False,
) -> dict[str, int]:
    from spark.jobs.sessionize_clicks import run_local as run_sessions

    total_events   = 0
    total_sessions = 0
    failed_dates   = []

    for d in date_range(start, end):
        ds = d.isoformat()
        if dry_run:
            log.info("  DRY RUN — would sessionise %s", ds)
            continue
        try:
            m = run_sessions(ds)
            total_events   += m.get("events", 0)
            total_sessions += m.get("sessions", 0)
            log.info("  ✓ %s: %d sessions from %d events", ds, m.get("sessions", 0), m.get("events", 0))
        except Exception as e:
            log.error("  ✗ %s sessions failed: %s", ds, e)
            failed_dates.append(ds)

    return {"total_events": total_events, "total_sessions": total_sessions, "failed_dates": failed_dates}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="RetailPulse backfill utility")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end",   required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run without writing")
    parser.add_argument("--include-sessions", action="store_true", help="Also reprocess clickstream sessions")
    args = parser.parse_args()

    try:
        start = date.fromisoformat(args.start)
        end   = date.fromisoformat(args.end)
    except ValueError as e:
        log.error("Invalid date format: %s", e)
        sys.exit(1)

    if end < start:
        log.error("--end must be >= --start")
        sys.exit(1)

    n_days = (end - start).days + 1
    if n_days > 90 and not args.dry_run:
        log.warning("Backfilling %d days — this may take a while. Use --dry-run first.", n_days)

    log.info("=== Orders backfill ===")
    order_results = backfill_orders(start, end, dry_run=args.dry_run)

    if args.include_sessions:
        log.info("=== Clickstream sessions backfill ===")
        session_results = backfill_sessions(start, end, dry_run=args.dry_run)
    else:
        session_results = {}

    print("\n" + "=" * 50)
    print(f"  Backfill complete: {start} → {end}")
    print(f"  Dates attempted:   {order_results['dates_attempted']}")
    print(f"  Dates failed:      {order_results['dates_failed']}")
    print(f"  Records processed: {order_results['total_processed']}")
    print(f"  Records valid:     {order_results['total_valid']}")
    if session_results:
        print(f"  Sessions built:    {session_results.get('total_sessions', 0)}")
    if order_results["failed_dates"]:
        print(f"  Failed dates: {order_results['failed_dates']}")
    print("=" * 50)

    if order_results["dates_failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
