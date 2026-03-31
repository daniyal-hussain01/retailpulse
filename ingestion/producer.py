"""
ingestion/producer.py
Simulates Postgres CDC events published to Kafka.
In production this is replaced by Debezium connector.
Usage:
    python -m ingestion.producer --events 500 --topic orders.cdc
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer  # <-- FIX: was missing, caused NameError

# load .env manually (no python-dotenv needed)
import pathlib as _pl
_env = _pl.Path(__file__).parent.parent / ".env"
if _env.exists():
    for _line in _env.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            import os as _os; _os.environ.setdefault(_k.strip(), _v.strip())

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Sample reference data (mirrors seed.sql)
SAMPLE_USER_IDS = [
    "c1b2c3d4-0001-0001-0001-000000000001",
    "c1b2c3d4-0002-0002-0002-000000000002",
    "c1b2c3d4-0003-0003-0003-000000000003",
    "c1b2c3d4-0004-0004-0004-000000000004",
    "c1b2c3d4-0005-0005-0005-000000000005",
]
SAMPLE_PRODUCT_IDS = [
    "b1b2c3d4-0001-0001-0001-000000000001",
    "b1b2c3d4-0002-0002-0002-000000000002",
    "b1b2c3d4-0003-0003-0003-000000000003",
    "b1b2c3d4-0004-0004-0004-000000000004",
    "b1b2c3d4-0005-0005-0005-000000000005",
]
WAREHOUSE_IDS = [
    "a1b2c3d4-0001-0001-0001-000000000001",
    "a1b2c3d4-0002-0002-0002-000000000002",
    "a1b2c3d4-0003-0003-0003-000000000003",
]
STATUSES = ["placed", "processing", "shipped", "delivered"]


def make_order_event(op: str = "c") -> dict[str, Any]:
    """Build a Debezium-style CDC envelope for an order row."""
    import random

    order_id = str(uuid.uuid4())
    user_id = random.choice(SAMPLE_USER_IDS)
    product_id = random.choice(SAMPLE_PRODUCT_IDS)
    qty = random.randint(1, 4)
    unit_price = random.choice([999, 1499, 2999, 4999, 8999, 14999])
    total = qty * unit_price

    row = {
        "order_id": order_id,
        "user_id": user_id,
        "warehouse_id": random.choice(WAREHOUSE_IDS),
        "status": random.choice(STATUSES),
        "currency": "USD",
        "subtotal_cents": total,
        "discount_cents": 0,
        "shipping_cents": 499,
        "total_cents": total + 499,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "schema": "retailpulse",
        "table": "orders",
        "op": op,  # c=create, u=update, d=delete
        "ts_ms": int(time.time() * 1000),
        "before": None if op == "c" else row,
        "after": row if op != "d" else None,
        "source": {
            "db": "retailpulse",
            "schema": "public",
            "table": "orders",
            "connector": "postgresql",
        },
    }


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        log.error("Message delivery failed: %s", err)
    else:
        log.debug(
            "Delivered to %s [%d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def produce(topic: str, n_events: int, delay_ms: int = 50) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    log.info("Producing %d events to topic '%s' on %s", n_events, topic, BOOTSTRAP_SERVERS)

    for i in range(n_events):
        event = make_order_event()
        producer.produce(
            topic=topic,
            key=event["after"]["order_id"].encode(),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )

        # Poll to serve delivery callbacks
        producer.poll(0)

        if (i + 1) % 100 == 0:
            log.info("Produced %d / %d events", i + 1, n_events)
            producer.flush()

        if delay_ms:
            time.sleep(delay_ms / 1000)

    producer.flush()
    log.info("Done. All %d events flushed.", n_events)


def main() -> None:
    parser = argparse.ArgumentParser(description="RetailPulse Kafka producer (CDC simulator)")
    parser.add_argument("--events", type=int, default=500, help="Number of events to produce")
    parser.add_argument("--topic", default="orders.cdc", help="Kafka topic name")
    parser.add_argument("--delay-ms", type=int, default=50, help="Delay between events (ms)")
    args = parser.parse_args()

    produce(args.topic, args.events, args.delay_ms)


if __name__ == "__main__":
    main()
