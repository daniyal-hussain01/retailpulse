"""
spark/jobs/bronze_to_silver.py

Reads raw CDC JSON from Bronze zone (local parquet files or S3),
applies cleaning / dedup / PII masking, and writes to Silver
as Delta Lake tables (or local parquet in unit-test mode).

Run locally:
    python -m spark.jobs.bronze_to_silver --date 2024-04-25 --local
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

# load .env manually (no python-dotenv needed)
import pathlib as _pl
_env = _pl.Path(__file__).parent.parent.parent / ".env"
if _env.exists():
    for _line in _env.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            import os as _os; _os.environ.setdefault(_k.strip(), _v.strip())

log = logging.getLogger(__name__)

BRONZE_PATH = Path(os.getenv("BRONZE_PATH", "./data/bronze"))
SILVER_PATH = Path(os.getenv("SILVER_PATH", "./data/silver"))
PII_SALT = os.getenv("PII_SALT", "default_salt_change_me")


# ── Pure-Python transformation functions (testable without Spark) ─────────────

def hash_pii(value: str | None, salt: str = PII_SALT) -> str | None:
    """SHA-256 hash a PII string with a project-level salt."""
    if value is None:
        return None
    return hashlib.sha256(f"{salt}|{value}".encode()).hexdigest()


def parse_cdc_record(raw: dict[str, Any]) -> dict[str, Any] | None:
    """
    Extract the payload from a Debezium CDC envelope.
    Returns None for delete operations (op='d').
    """
    op = raw.get("op", "c")
    if op == "d":
        return None

    after = raw.get("after")
    if not after:
        return None

    return {
        "order_id": after.get("order_id"),
        "user_id_hash": hash_pii(after.get("user_id")),
        "warehouse_id": after.get("warehouse_id"),
        "status": after.get("status"),
        "currency": after.get("currency", "USD"),
        "subtotal_cents": after.get("subtotal_cents", 0),
        "discount_cents": after.get("discount_cents", 0),
        "shipping_cents": after.get("shipping_cents", 0),
        "total_cents": after.get("total_cents", 0),
        "created_at": after.get("created_at"),
        "updated_at": after.get("updated_at"),
        "cdc_op": op,
        "cdc_ts_ms": raw.get("ts_ms"),
        "_bronze_source": raw.get("source", {}),
    }


def validate_silver_record(record: dict[str, Any]) -> list[str]:
    """
    Returns a list of validation errors. Empty list = record is valid.
    These mirror the Great Expectations suite checks.
    """
    errors: list[str] = []

    if not record.get("order_id"):
        errors.append("order_id is null")

    if not record.get("user_id_hash"):
        errors.append("user_id_hash is null")

    total = record.get("total_cents", 0)
    if not isinstance(total, int) or total < 0:
        errors.append(f"total_cents invalid: {total}")

    status = record.get("status")
    valid_statuses = {"placed", "processing", "shipped", "delivered", "refunded", "cancelled"}
    if status not in valid_statuses:
        errors.append(f"invalid status: {status}")

    return errors


# ── Pandas-based local transform (no Spark JVM required for MVP demo) ─────────

def run_local_transform(run_date: str) -> dict[str, int]:
    """
    Transform Bronze → Silver using Pandas (local/unit-test mode).
    Returns metrics: {processed, valid, invalid, deduplicated}.
    """
    import pandas as pd

    bronze_dir = BRONZE_PATH / "orders" / run_date
    silver_dir = SILVER_PATH / "orders"
    silver_dir.mkdir(parents=True, exist_ok=True)

    if not bronze_dir.exists():
        log.warning("No bronze data at %s — generating synthetic data for demo.", bronze_dir)
        bronze_dir.mkdir(parents=True, exist_ok=True)
        _write_synthetic_bronze(bronze_dir, n=200)

    records = []
    for fpath in bronze_dir.glob("*.jsonl"):
        with open(fpath) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        raw = json.loads(line)
                        parsed = parse_cdc_record(raw)
                        if parsed:
                            records.append(parsed)
                    except (json.JSONDecodeError, KeyError) as e:
                        log.warning("Skipping malformed record: %s", e)

    if not records:
        log.info("No records to process.")
        return {"processed": 0, "valid": 0, "invalid": 0, "deduplicated": 0}

    df = pd.DataFrame(records)
    processed = len(df)

    # Validate
    invalid_mask = df.apply(
        lambda row: len(validate_silver_record(row.to_dict())) > 0, axis=1
    )
    valid_df = df[~invalid_mask].copy()
    invalid_count = invalid_mask.sum()

    # Deduplicate: keep latest CDC event per order_id
    before_dedup = len(valid_df)
    valid_df = (
        valid_df.sort_values("cdc_ts_ms", ascending=False)
        .drop_duplicates(subset=["order_id"])
        .reset_index(drop=True)
    )
    dedup_count = before_dedup - len(valid_df)

    # Write Silver
    out_path = silver_dir / f"orders_{run_date}.parquet"
    valid_df.to_parquet(out_path, index=False)
    log.info(
        "Silver written: %s | processed=%d valid=%d invalid=%d deduplicated=%d",
        out_path,
        processed,
        len(valid_df),
        invalid_count,
        dedup_count,
    )

    return {
        "processed": processed,
        "valid": len(valid_df),
        "invalid": int(invalid_count),
        "deduplicated": dedup_count,
    }


def _write_synthetic_bronze(output_dir: Path, n: int = 200) -> None:
    """Write synthetic CDC events for local demo (no Kafka required)."""
    import random

    user_ids = [f"c1b2c3d4-000{i}-000{i}-000{i}-00000000000{i}" for i in range(1, 6)]
    warehouse_ids = [f"a1b2c3d4-000{i}-000{i}-000{i}-00000000000{i}" for i in range(1, 4)]
    statuses = ["placed", "processing", "shipped", "delivered"]
    prices = [999, 1499, 2999, 4999, 8999]

    output_file = output_dir / "synthetic.jsonl"
    with open(output_file, "w") as f:
        for _ in range(n):
            import uuid
            from datetime import datetime, timezone

            qty = random.randint(1, 3)
            price = random.choice(prices)
            user_id = random.choice(user_ids)
            record = {
                "schema": "retailpulse",
                "table": "orders",
                "op": random.choice(["c", "c", "c", "u"]),  # mostly creates
                "ts_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                "after": {
                    "order_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "warehouse_id": random.choice(warehouse_ids),
                    "status": random.choice(statuses),
                    "currency": "USD",
                    "subtotal_cents": qty * price,
                    "discount_cents": 0,
                    "shipping_cents": 499,
                    "total_cents": qty * price + 499,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                "source": {"db": "retailpulse", "table": "orders"},
            }
            f.write(json.dumps(record) + "\n")

    log.info("Wrote %d synthetic records to %s", n, output_file)


# ── Spark entrypoint (used in Docker / production) ────────────────────────────

def run_spark_transform(run_date: str) -> None:  # pragma: no cover
    """
    Full Spark + Delta Lake job.
    Requires JAVA_HOME and pyspark installed.
    Not executed in unit tests (no JVM).
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError as e:
        raise RuntimeError("PySpark not available. Use --local for local mode.") from e

    spark = (
        SparkSession.builder.appName("bronze_to_silver_orders")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .getOrCreate()
    )

    bronze_path = str(BRONZE_PATH / "orders" / run_date)
    silver_path = str(SILVER_PATH / "orders")

    df_raw = spark.read.json(bronze_path)

    df_silver = (
        df_raw.filter(F.col("op").isin(["c", "u"]))
        .select("after.*", "ts_ms", "op")
        .withColumn(
            "user_id_hash",
            F.sha2(F.concat_ws("|", F.lit(PII_SALT), F.col("user_id")), 256),
        )
        .drop("user_id")
        .dropDuplicates(["order_id"])
        .filter(F.col("total_cents") > 0)
        .filter(
            F.col("status").isin(
                ["placed", "processing", "shipped", "delivered", "refunded", "cancelled"]
            )
        )
        .withColumnRenamed("op", "cdc_op")
        .withColumnRenamed("ts_ms", "cdc_ts_ms")
    )

    df_silver.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).partitionBy("status").save(silver_path)

    count = df_silver.count()
    log.info("Wrote %d records to Silver Delta table at %s", count, silver_path)
    spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Bronze → Silver transformation")
    parser.add_argument(
        "--date", default=date.today().isoformat(), help="Processing date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--local", action="store_true", help="Use Pandas local mode (no Spark JVM)"
    )
    args = parser.parse_args()

    if args.local:
        metrics = run_local_transform(args.date)
        print(f"\nTransform complete: {metrics}")
    else:
        run_spark_transform(args.date)


if __name__ == "__main__":
    main()
