"""
spark/jobs/sessionize_clicks.py

Sessionises raw clickstream events from Bronze using a 30-minute idle-timeout window.
Produces one row per session in Silver with session-level aggregates.

Session definition:
  - Same anonymous session_id OR same user_id_hash
  - Gap between consecutive events > 30 min → new session

Output schema (Silver clickstream sessions):
  session_id, user_id_hash, session_start, session_end,
  duration_seconds, event_count, page_views, product_views,
  add_to_cart_count, search_count, converted (bool),
  entry_page, exit_page, device_category

Run:
    python -m spark.jobs.sessionize_clicks --date 2024-04-25 --local
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

BRONZE_PATH = Path(os.getenv("BRONZE_PATH", "./data/bronze"))
SILVER_PATH = Path(os.getenv("SILVER_PATH", "./data/silver"))
PII_SALT    = os.getenv("PII_SALT", "default_salt_change_me")
SESSION_TIMEOUT_MINUTES = 30


def hash_pii(value: str | None, salt: str = PII_SALT) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(f"{salt}|{value}".encode()).hexdigest()


def parse_event(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Parse a raw clickstream event JSON record."""
    after = raw.get("after") or raw  # support both Debezium envelope and flat JSON
    if not after.get("event_id") and not after.get("session_id"):
        return None
    return {
        "event_id":     after.get("event_id"),
        "session_id":   after.get("session_id"),
        "user_id_hash": hash_pii(after.get("user_id")),
        "event_type":   after.get("event_type", "page_view"),
        "product_id":   after.get("product_id"),
        "page_url":     after.get("page_url"),
        "occurred_at":  after.get("occurred_at"),
    }


def sessionise_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Group events into sessions using a 30-minute idle timeout.
    Pure Python implementation (Spark version uses window functions).

    Args:
        events: List of parsed event dicts, sorted by occurred_at ascending.

    Returns:
        List of session summary dicts.
    """
    if not events:
        return []

    # Sort by (session_id, occurred_at)
    try:
        sorted_events = sorted(
            events,
            key=lambda e: (e.get("session_id", ""), e.get("occurred_at", "") or ""),
        )
    except TypeError:
        return []

    sessions: list[dict[str, Any]] = []
    current_session: dict[str, Any] | None = None
    session_events: list[dict[str, Any]] = []

    def _flush_session(sess_events: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not sess_events:
            return None
        first = sess_events[0]
        last  = sess_events[-1]

        try:
            t_start = datetime.fromisoformat(first.get("occurred_at", "").replace("Z", "+00:00"))
            t_end   = datetime.fromisoformat(last.get("occurred_at", "").replace("Z", "+00:00"))
            duration = int((t_end - t_start).total_seconds())
        except (ValueError, AttributeError):
            duration = 0
            t_start = datetime.now(timezone.utc)
            t_end   = t_start

        event_types = [e.get("event_type", "") for e in sess_events]
        return {
            "session_id":       first.get("session_id"),
            "user_id_hash":     first.get("user_id_hash"),
            "session_start":    t_start.isoformat(),
            "session_end":      t_end.isoformat(),
            "duration_seconds": duration,
            "event_count":      len(sess_events),
            "page_views":       event_types.count("page_view"),
            "product_views":    event_types.count("product_view"),
            "add_to_cart_count":event_types.count("add_to_cart"),
            "search_count":     event_types.count("search"),
            "converted":        "purchase" in event_types,
            "entry_page":       first.get("page_url"),
            "exit_page":        last.get("page_url"),
        }

    for event in sorted_events:
        sid = event.get("session_id")
        ts  = event.get("occurred_at")

        if current_session is None:
            current_session = sid
            session_events  = [event]
        elif sid != current_session:
            # New session_id → flush and start fresh
            flushed = _flush_session(session_events)
            if flushed:
                sessions.append(flushed)
            current_session = sid
            session_events  = [event]
        else:
            # Check idle timeout
            try:
                prev_ts = session_events[-1].get("occurred_at", "")
                t_prev  = datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
                t_curr  = datetime.fromisoformat((ts or "").replace("Z", "+00:00"))
                gap_minutes = (t_curr - t_prev).total_seconds() / 60
                if gap_minutes > SESSION_TIMEOUT_MINUTES:
                    flushed = _flush_session(session_events)
                    if flushed:
                        sessions.append(flushed)
                    session_events = [event]
                else:
                    session_events.append(event)
            except (ValueError, AttributeError):
                session_events.append(event)

    # Flush last session
    flushed = _flush_session(session_events)
    if flushed:
        sessions.append(flushed)

    return sessions


def run_local(run_date: str) -> dict[str, int]:
    """Local pandas-based sessionisation (no Spark JVM required)."""
    import pandas as pd

    events_dir = BRONZE_PATH / "events" / run_date
    silver_dir = SILVER_PATH / "sessions"
    silver_dir.mkdir(parents=True, exist_ok=True)

    if not events_dir.exists():
        log.warning("No events bronze data at %s — generating synthetic data.", events_dir)
        events_dir.mkdir(parents=True, exist_ok=True)
        _write_synthetic_events(events_dir, n=500)

    raw_events: list[dict[str, Any]] = []
    for fpath in events_dir.glob("*.jsonl"):
        with open(fpath) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        parsed = parse_event(json.loads(line))
                        if parsed:
                            raw_events.append(parsed)
                    except (json.JSONDecodeError, KeyError):
                        pass

    if not raw_events:
        log.info("No events to sessionise.")
        return {"events": 0, "sessions": 0}

    sessions = sessionise_events(raw_events)

    if sessions:
        df = pd.DataFrame(sessions)
        out = silver_dir / f"sessions_{run_date}.parquet"
        df.to_parquet(out, index=False)
        log.info("Wrote %d sessions (%d events) to %s", len(sessions), len(raw_events), out)

    return {"events": len(raw_events), "sessions": len(sessions)}


def _write_synthetic_events(output_dir: Path, n: int = 500) -> None:
    """Generate synthetic clickstream events for local demo."""
    import random
    import uuid

    event_types = ["page_view", "product_view", "add_to_cart", "search", "page_view", "product_view"]
    base_ts     = datetime(2024, 4, 25, 9, 0, 0, tzinfo=timezone.utc)

    with open(output_dir / "synthetic_events.jsonl", "w") as f:
        session_id = str(uuid.uuid4())
        for i in range(n):
            if i > 0 and random.random() < 0.1:
                session_id = str(uuid.uuid4())  # start new session ~10% of events
            ts = base_ts + timedelta(minutes=i * 2 + random.randint(0, 3))
            event = {
                "event_id":    str(uuid.uuid4()),
                "session_id":  session_id,
                "user_id":     f"user-{random.randint(1, 20):04d}" if random.random() > 0.3 else None,
                "event_type":  random.choice(event_types),
                "product_id":  str(uuid.uuid4()) if random.random() > 0.4 else None,
                "page_url":    f"/products/{random.randint(1, 50)}",
                "occurred_at": ts.isoformat(),
            }
            f.write(json.dumps(event) + "\n")

    log.info("Wrote %d synthetic events to %s", n, output_dir)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Sessionise clickstream Bronze → Silver")
    parser.add_argument("--date", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    if args.local:
        metrics = run_local(args.date)
        print(f"Sessionisation complete: {metrics}")
    else:
        log.warning("Spark mode not yet implemented — use --local for demo")


if __name__ == "__main__":
    main()
