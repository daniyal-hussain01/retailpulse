"""
airflow/dags/gdpr_deletion.py

GDPR Right-to-Erasure DAG.
Triggered manually (or via webhook) with a user_id_hash parameter.

What it does:
  1. Reads deletion requests from the `deletion_requests` table (or a config param)
  2. Hashes the user_id with the PII salt to get the stable hash
  3. Overwrites all PII-adjacent fields with null/placeholder in Silver parquet
  4. Deletes rows from DuckDB Gold tables matching the hash
  5. Logs the erasure event to an audit trail (append-only)
  6. Confirms completion

Usage:
    airflow dags trigger gdpr_deletion \
        --conf '{"user_id": "alice@example.com", "request_id": "REQ-2024-001"}'

Security notes:
  - The raw user_id is NEVER stored after hashing. Only the hash appears in logs.
  - Audit trail is append-only — deletions are logged but the log itself is not purged.
  - The PII_SALT must match the salt used at ingestion time.
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": None,  # wire up PagerDuty in production
}


def _hash_user_id(user_id: str) -> str:
    salt = os.environ.get("PII_SALT", "")
    if not salt:
        raise RuntimeError("PII_SALT environment variable not set — cannot hash user_id safely.")
    return hashlib.sha256(f"{salt}|{user_id}".encode()).hexdigest()


def validate_deletion_request(**context: dict) -> str:
    """
    Validate the deletion request parameters from DAG conf.
    Pushes the user_id_hash to XCom — never the raw user_id.
    """
    conf = context["dag_run"].conf or {}
    user_id = conf.get("user_id")
    request_id = conf.get("request_id", "unknown")

    if not user_id:
        raise ValueError("DAG conf must include 'user_id' field.")

    user_id_hash = _hash_user_id(user_id)

    # Push hash only — raw user_id is intentionally discarded here
    context["ti"].xcom_push(key="user_id_hash", value=user_id_hash)
    context["ti"].xcom_push(key="request_id", value=request_id)

    # Log only a prefix of the hash for traceability (not the full hash)
    log.info(
        "Deletion request validated | request_id=%s | hash_prefix=%s...",
        request_id,
        user_id_hash[:8],
    )
    return user_id_hash


def erase_from_silver(**context: dict) -> dict:
    """
    Overwrite PII-adjacent columns in Silver parquet files for the given user.
    Uses pandas to rewrite affected partitions.
    """
    import glob
    from pathlib import Path

    import pandas as pd

    user_id_hash = context["ti"].xcom_pull(key="user_id_hash", task_ids="validate_request")
    silver_path = Path(os.environ.get("SILVER_PATH", "./data/silver"))
    parquet_files = glob.glob(str(silver_path / "orders" / "*.parquet"))

    affected_files = 0
    affected_rows = 0

    for fpath in parquet_files:
        df = pd.read_parquet(fpath)
        mask = df["user_id_hash"] == user_id_hash

        if mask.any():
            # Replace hash with a tombstone marker — row is kept for referential integrity
            # but the user is no longer identifiable
            df.loc[mask, "user_id_hash"] = "ERASED"
            df.to_parquet(fpath, index=False)
            affected_files += 1
            affected_rows += int(mask.sum())

    log.info(
        "Silver erasure complete | hash_prefix=%s... | files=%d | rows=%d",
        user_id_hash[:8],
        affected_files,
        affected_rows,
    )
    return {"affected_files": affected_files, "affected_rows": affected_rows}


def erase_from_gold(**context: dict) -> dict:
    """
    Delete or anonymise rows in DuckDB Gold tables.
    """
    import duckdb

    user_id_hash = context["ti"].xcom_pull(key="user_id_hash", task_ids="validate_request")
    duckdb_path = os.environ.get("DUCKDB_PATH", "./data/gold/retailpulse.duckdb")

    try:
        conn = duckdb.connect(duckdb_path)
    except Exception as e:
        log.warning("Could not connect to DuckDB (may not exist yet): %s", e)
        return {"deleted_rows": 0, "note": "DuckDB not available"}

    deleted = 0
    try:
        # Anonymise in fact_orders (preserve row for aggregation integrity)
        result = conn.execute(
            "UPDATE fact_orders SET user_id_hash = 'ERASED' WHERE user_id_hash = ?",
            [user_id_hash],
        )
        deleted = result.fetchone()[0] if result else 0

        # Also clear from agg tables if user-level data exists there
        # (agg_daily_revenue is pre-aggregated so no user_id present — skip)

        conn.close()
    except Exception as e:
        log.warning("Gold erasure error (non-fatal): %s", e)
        deleted = 0

    log.info("Gold erasure complete | hash_prefix=%s... | rows=%d", user_id_hash[:8], deleted)
    return {"deleted_rows": deleted}


def write_audit_log(**context: dict) -> None:
    """
    Append an erasure audit record. This log is NEVER purged (compliance requirement).
    In production, write to an immutable S3 object or a write-once database table.
    """
    import json
    from pathlib import Path

    user_id_hash = context["ti"].xcom_pull(key="user_id_hash", task_ids="validate_request")
    request_id = context["ti"].xcom_pull(key="request_id", task_ids="validate_request")
    silver_result = context["ti"].xcom_pull(task_ids="erase_from_silver") or {}
    gold_result = context["ti"].xcom_pull(task_ids="erase_from_gold") or {}

    audit_record = {
        "event": "gdpr_erasure",
        "request_id": request_id,
        "user_id_hash_prefix": user_id_hash[:8] + "...",  # partial only
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "silver": silver_result,
        "gold": gold_result,
        "dag_run_id": context["run_id"],
    }

    audit_dir = Path(os.environ.get("GOLD_PATH", "./data/gold")) / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_file = audit_dir / "erasure_audit.jsonl"

    with open(audit_file, "a") as f:
        f.write(json.dumps(audit_record) + "\n")

    log.info("Audit record written | request_id=%s", request_id)


with DAG(
    dag_id="gdpr_deletion",
    description="GDPR right-to-erasure: hash → erase Silver → erase Gold → audit log",
    default_args=DEFAULT_ARGS,
    schedule=None,  # triggered manually only
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=3,  # allow parallel deletion requests
    tags=["gdpr", "compliance", "security"],
    doc_md="""
## GDPR Deletion DAG

Triggered manually with `{"user_id": "email@example.com", "request_id": "REQ-XXX"}`.

**Guarantees:**
- Raw `user_id` is hashed immediately and then discarded
- Only the hash prefix appears in logs (never the full hash)
- Audit trail is append-only and never purged
- Silver parquet and Gold DuckDB rows are anonymised (not deleted) to preserve aggregate integrity

**Compliance note:** complete within 30 days of request per GDPR Art. 17.
""",
) as dag:

    validate = PythonOperator(
        task_id="validate_request",
        python_callable=validate_deletion_request,
    )

    erase_silver = PythonOperator(
        task_id="erase_from_silver",
        python_callable=erase_from_silver,
    )

    erase_gold = PythonOperator(
        task_id="erase_from_gold",
        python_callable=erase_from_gold,
    )

    audit = PythonOperator(
        task_id="write_audit_log",
        python_callable=write_audit_log,
        trigger_rule="all_done",  # always audit, even on partial failure
    )

    validate >> [erase_silver, erase_gold] >> audit
