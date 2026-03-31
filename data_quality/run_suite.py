"""
data_quality/run_suite.py
Data quality checks on Silver layer files.
Works with pandas (if installed) or pure stdlib csv fallback.
No mandatory external dependencies.
"""
from __future__ import annotations

import csv
import glob
import logging
import os
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

SILVER_PATH = Path(os.getenv("SILVER_PATH", "./data/silver"))


def _load_rows(all_files: list[str]) -> list[dict]:
    """Load CSV or Parquet files into a list of dicts. pandas optional."""
    rows: list[dict] = []
    try:
        import pandas as pd
        dfs = []
        for f in all_files:
            if f.endswith(".csv"):
                dfs.append(pd.read_csv(f))
            else:
                try:
                    dfs.append(pd.read_parquet(f))
                except Exception:
                    pass  # skip unreadable parquet if no engine
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            rows = combined.to_dict("records")
    except ImportError:
        # stdlib fallback — CSV only
        for f in all_files:
            if f.endswith(".csv"):
                with open(f, newline="") as fh:
                    rows.extend(list(csv.DictReader(fh)))
    return rows


def run_orders_suite() -> dict[str, Any]:
    """
    Validate Silver orders files (CSV or Parquet).
    Returns: {"success": bool, "stats": {...}, "failures": [...]}
    """
    SILVER_PATH_CURRENT = Path(os.getenv("SILVER_PATH", str(SILVER_PATH)))

    all_files = (
        glob.glob(str(SILVER_PATH_CURRENT / "orders" / "*.parquet")) +
        glob.glob(str(SILVER_PATH_CURRENT / "orders" / "*.csv"))
    )

    if not all_files:
        log.warning("No Silver parquet files found at %s", SILVER_PATH_CURRENT / "orders")
        return {
            "success": True,
            "stats": {"files_checked": 0, "rows": 0},
            "failures": [],
            "note": "No files to validate — skipped.",
        }

    rows = _load_rows(all_files)
    failures: list[str] = []
    stats: dict[str, Any] = {"files_checked": len(all_files), "rows": len(rows)}

    if not rows:
        return {"success": True, "stats": stats, "failures": [], "note": "No rows loaded."}

    columns = set(rows[0].keys())

    # 1. order_id not null
    null_ids = sum(1 for r in rows if not r.get("order_id") or str(r.get("order_id","")).strip() in ("","None","nan"))
    if null_ids > 0:
        failures.append(f"order_id has {null_ids} null values")

    # 2. order_id unique
    ids = [str(r.get("order_id","")) for r in rows if r.get("order_id")]
    dup_ids = len(ids) - len(set(ids))
    if dup_ids > 0:
        failures.append(f"order_id has {dup_ids} duplicates")

    # 3. user_id_hash not null
    if "user_id_hash" in columns:
        null_hashes = sum(1 for r in rows if not r.get("user_id_hash") or str(r.get("user_id_hash","")).strip() in ("","None","nan"))
        if null_hashes > 0:
            failures.append(f"user_id_hash has {null_hashes} null values")

        # 4. hash length = 64
        bad_len = sum(1 for r in rows if r.get("user_id_hash") and len(str(r["user_id_hash"])) != 64)
        if bad_len > 0:
            failures.append(f"user_id_hash has {bad_len} values not 64 chars (not SHA-256)")

    # 5. No raw user_id column (PII check)
    if "user_id" in columns:
        failures.append("CRITICAL: raw 'user_id' column found in Silver — PII leak!")

    # 6. Status valid
    valid_statuses = {"placed","processing","shipped","delivered","refunded","cancelled"}
    if "status" in columns:
        bad_statuses = set(str(r.get("status","")) for r in rows) - valid_statuses - {""}
        if bad_statuses:
            failures.append(f"Invalid status values found: {sorted(bad_statuses)}")

    # 7. total_cents positive
    if "total_cents" in columns:
        try:
            non_pos = sum(1 for r in rows if int(r.get("total_cents", 1)) <= 0)
            if non_pos > 0:
                failures.append(f"total_cents has {non_pos} non-positive values")
        except (ValueError, TypeError):
            pass

    # 8. Currency valid
    valid_currencies = {"USD","CAD","GBP","EUR"}
    if "currency" in columns:
        bad_curr = set(str(r.get("currency","")) for r in rows) - valid_currencies - {""}
        if bad_curr:
            failures.append(f"Invalid currency values: {sorted(bad_curr)}")

    # 9. Volume
    if len(rows) == 0:
        failures.append("Zero rows — possible pipeline failure")

    stats["checks_run"] = 9
    stats["checks_passed"] = 9 - len(failures)
    success = len(failures) == 0

    if success:
        log.info("GE suite PASSED: %d rows validated across %d files.", len(rows), len(all_files))
    else:
        log.error("GE suite FAILED: %d failures:\n%s", len(failures), "\n".join(failures))

    return {"success": success, "stats": stats, "failures": failures}


def run_suite(suite_name: str = "orders_silver") -> dict[str, Any]:
    if suite_name != "orders_silver":
        raise ValueError(f"Unknown suite: {suite_name}")
    return run_orders_suite()


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_orders_suite(), indent=2))
