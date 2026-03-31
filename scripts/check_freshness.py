#!/usr/bin/env python3
"""
scripts/check_freshness.py

Quick CLI data freshness check. Referenced in the operations runbook.
Exits 0 if all tables are fresh, 1 if any are stale.

Usage:
    python scripts/check_freshness.py
    python scripts/check_freshness.py --max-age 60  # allow up to 60 minutes
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from scripts.monitoring import check_silver_freshness, check_gold_row_counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age", type=int, default=30, help="Max age in minutes")
    parser.add_argument("--silver-path", default=os.environ.get("SILVER_PATH", "./data/silver"))
    parser.add_argument("--duckdb-path", default=os.environ.get("DUCKDB_PATH", "./data/gold/retailpulse.duckdb"))
    args = parser.parse_args()

    results = [
        check_silver_freshness(Path(args.silver_path), max_age_minutes=args.max_age),
        check_gold_row_counts(args.duckdb_path),
    ]

    failed = False
    for r in results:
        icon = "✓" if r["status"] == "pass" else ("–" if r["status"] == "skip" else "✗")
        print(f"  {icon}  {r['check']}: {r['message']}")
        if r["status"] == "fail":
            failed = True

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
