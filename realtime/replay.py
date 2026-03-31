"""
realtime/replay.py

Historical event replay engine.
Re-plays orders from the fact_orders SQLite table into the real-time
pipeline at a configurable speed multiplier.

Use cases:
  - Demo mode: replay a busy day at 10× speed
  - Backtest: replay 90 days to calibrate the churn model
  - Load test: replay at 100× to stress-test the pipeline

Usage:
    from realtime.replay import EventReplayer
    replayer = EventReplayer(db_path, pipeline_state, speed_multiplier=5.0)
    replayer.start()
    replayer.stop()
"""
from __future__ import annotations

import logging
import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("retailpulse.replay")


class EventReplayer(threading.Thread):
    """
    Reads historical orders from SQLite and injects them into the
    raw_queue as if they were arriving live — preserving the original
    inter-event timing but compressed by speed_multiplier.

    Example: speed_multiplier=10 means 1 hour of history plays in 6 minutes.
    """
    daemon = True
    name   = "EventReplayer"

    def __init__(
        self,
        db_path:          Path,
        pipeline_state:   Any,
        speed_multiplier: float = 5.0,
        date_from:        str | None = None,
        date_to:          str | None = None,
        loop:             bool = False,
    ) -> None:
        super().__init__()
        self._db_path    = db_path
        self._state      = pipeline_state
        self._speed      = max(0.1, speed_multiplier)
        self._date_from  = date_from
        self._date_to    = date_to
        self._loop       = loop
        self._stop_event = threading.Event()

        # Stats
        self.replayed_count = 0
        self.skipped_count  = 0
        self.started_at:    float | None = None

    def _load_events(self) -> list[dict]:
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row

        sql = "SELECT * FROM fact_orders"
        params: list = []
        if self._date_from:
            sql += " WHERE order_date >= ?"
            params.append(self._date_from)
        if self._date_to:
            sql += (" AND" if params else " WHERE") + " order_date <= ?"
            params.append(self._date_to)
        sql += " ORDER BY created_at ASC"

        rows = conn.execute(sql, params).fetchall()
        conn.close()
        log.info("Replay: loaded %d historical events", len(rows))
        return [dict(r) for r in rows]

    def _row_to_event(self, row: dict) -> dict:
        """Convert a fact_orders row into a pipeline-compatible event."""
        return {
            "event_id":      f"replay-{row.get('order_id','')}",
            "order_id":      row.get("order_id", ""),
            "op":            "c",
            "user_id_hash":  row.get("user_id_hash", ""),
            "sku":           "REPLAY",
            "warehouse_id":  row.get("warehouse_id", ""),
            "status":        row.get("order_status", "placed"),
            "currency":      row.get("currency", "USD"),
            "channel":       row.get("channel", "organic"),
            "qty":           1,
            "unit_price_cents": row.get("total_cents", 0),
            "discount_cents":   row.get("discount_cents", 0),
            "shipping_cents":   row.get("shipping_cents", 0),
            "total_cents":      row.get("total_cents", 0),
            "occurred_at":      row.get("created_at",
                                        datetime.now(timezone.utc).isoformat()),
            "_generated_at_mono": time.monotonic(),
            "_is_replay":        True,
        }

    def run(self) -> None:
        log.info("EventReplayer starting at %.1fx speed", self._speed)
        self._state._running.wait()
        self.started_at = time.monotonic()

        while not self._stop_event.is_set():
            events = self._load_events()
            if not events:
                log.warning("Replay: no events found in DB, sleeping 5s")
                time.sleep(5)
                continue

            prev_ts: datetime | None = None
            for row in events:
                if self._stop_event.is_set():
                    break

                # Parse timestamp
                try:
                    raw_ts = row.get("created_at", "")
                    if raw_ts:
                        ts = datetime.fromisoformat(
                            raw_ts.replace("Z", "+00:00")
                        )
                    else:
                        ts = datetime.now(timezone.utc)
                except ValueError:
                    ts = datetime.now(timezone.utc)

                # Sleep proportional to gap since previous event
                if prev_ts is not None:
                    gap_s = (ts - prev_ts).total_seconds()
                    if gap_s > 0:
                        sleep_s = gap_s / self._speed
                        # Cap to 2s max so demo doesn't stall
                        time.sleep(min(sleep_s, 2.0))
                prev_ts = ts

                event = self._row_to_event(row)
                try:
                    self._state.raw_queue.put_nowait(event)
                    self._state.increment("events_generated")
                    self._state.record_event_ts(event["total_cents"])
                    self.replayed_count += 1
                except queue.Full:
                    self.skipped_count += 1

                if self.replayed_count % 500 == 0:
                    elapsed = time.monotonic() - self.started_at
                    log.info(
                        "Replay progress: %d events in %.1fs (%.1f eps)",
                        self.replayed_count, elapsed,
                        self.replayed_count / max(elapsed, 0.001)
                    )

            if self._loop:
                log.info("Replay: loop complete, restarting …")
                time.sleep(1.0)
            else:
                log.info(
                    "Replay complete: %d replayed, %d skipped",
                    self.replayed_count, self.skipped_count
                )
                break

    def stop(self) -> None:
        self._stop_event.set()

    def get_status(self) -> dict:
        elapsed = (time.monotonic() - self.started_at) if self.started_at else 0
        return {
            "replayed_count": self.replayed_count,
            "skipped_count":  self.skipped_count,
            "speed_multiplier":self._speed,
            "elapsed_seconds": round(elapsed, 1),
            "replay_eps":      round(self.replayed_count / max(elapsed, 0.001), 2),
            "is_running":      self.is_alive(),
        }
