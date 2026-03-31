"""
realtime/backpressure.py

Adaptive backpressure controller.
Monitors queue depths and automatically slows the EventGenerator
when downstream stages can't keep up — preventing memory exhaustion
and maintaining end-to-end latency SLA.

Design:
  - Green  (queue < 30%):  run at full configured rate
  - Yellow (queue 30-70%): reduce rate by 50%
  - Red    (queue > 70%):  reduce rate by 80%, log warning
  - Critical (queue > 90%): pause generator, drain queue first

This is a core pattern in production streaming systems (Kafka consumer
lag management, Flink backpressure, Spark streaming rate control).
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

log = logging.getLogger("retailpulse.backpressure")

# Thresholds (% of maxsize)
GREEN_THRESHOLD    = 0.30
YELLOW_THRESHOLD   = 0.70
RED_THRESHOLD      = 0.90

# Multipliers applied to base EPS
GREEN_MULTIPLIER   = 1.0
YELLOW_MULTIPLIER  = 0.50
RED_MULTIPLIER     = 0.20
CRITICAL_MULTIPLIER= 0.0   # pause


class PressureLevel(Enum):
    GREEN    = "green"
    YELLOW   = "yellow"
    RED      = "red"
    CRITICAL = "critical"


@dataclass
class BackpressureSnapshot:
    level:           PressureLevel
    raw_queue_pct:   float
    proc_queue_pct:  float
    rate_multiplier: float
    latency_ms:      float
    recommendation:  str
    ts:              float = field(default_factory=time.monotonic)


class BackpressureController(threading.Thread):
    """
    Continuously monitors pipeline queue depths and adjusts the
    EventGenerator rate via a shared multiplier.

    Usage:
        controller = BackpressureController(pipeline_state)
        controller.start()

        # EventGenerator reads controller.rate_multiplier before sleeping
        sleep_time = base_interval / controller.rate_multiplier
    """
    daemon = True
    name   = "BackpressureController"

    QUEUE_MAXSIZE = 1000   # matches stream_engine.py Queue(maxsize=1000)
    CHECK_INTERVAL_S = 0.5

    def __init__(self, pipeline_state: Any) -> None:
        super().__init__()
        self._state = pipeline_state
        self._lock  = threading.Lock()
        self._rate_multiplier: float = 1.0
        self._current_level   = PressureLevel.GREEN
        self._history: list[BackpressureSnapshot] = []
        self._paused_until: float = 0.0

        # Stats
        self.throttle_events: int = 0
        self.pause_events:    int = 0

    @property
    def rate_multiplier(self) -> float:
        with self._lock:
            return self._rate_multiplier

    @property
    def current_level(self) -> PressureLevel:
        with self._lock:
            return self._current_level

    def _assess(self) -> BackpressureSnapshot:
        raw_q   = self._state.raw_queue.qsize()
        proc_q  = self._state.processed_queue.qsize()
        latency = self._state.metrics.get("pipeline_latency_ms", 0.0)

        raw_pct  = raw_q  / self.QUEUE_MAXSIZE
        proc_pct = proc_q / self.QUEUE_MAXSIZE
        worst    = max(raw_pct, proc_pct)

        if worst >= RED_THRESHOLD:
            level      = PressureLevel.CRITICAL
            multiplier = CRITICAL_MULTIPLIER
            rec        = f"PAUSING: queues at {worst*100:.0f}% — draining"
        elif worst >= YELLOW_THRESHOLD:
            level      = PressureLevel.RED
            multiplier = RED_MULTIPLIER
            rec        = f"Severe throttle: queues at {worst*100:.0f}%"
        elif worst >= GREEN_THRESHOLD:
            level      = PressureLevel.YELLOW
            multiplier = YELLOW_MULTIPLIER
            rec        = f"Mild throttle: queues at {worst*100:.0f}%"
        else:
            level      = PressureLevel.GREEN
            multiplier = GREEN_MULTIPLIER
            rec        = "Running at full rate"

        # Latency override: if latency > 100ms, cap multiplier at 0.5
        if latency > 100 and multiplier > 0.5:
            multiplier = 0.5
            rec += f" (latency cap: {latency:.0f}ms)"

        return BackpressureSnapshot(
            level=level,
            raw_queue_pct=raw_pct,
            proc_queue_pct=proc_pct,
            rate_multiplier=multiplier,
            latency_ms=latency,
            recommendation=rec,
        )

    def run(self) -> None:
        log.info("BackpressureController started")
        self._state._running.wait()

        while self._state._running.is_set():
            snap = self._assess()

            prev_level = self._current_level
            with self._lock:
                self._rate_multiplier = snap.rate_multiplier
                self._current_level   = snap.level

            # Log level transitions
            if snap.level != prev_level:
                if snap.level == PressureLevel.GREEN:
                    log.info("Backpressure: GREEN — full rate restored")
                elif snap.level == PressureLevel.YELLOW:
                    log.warning("Backpressure: YELLOW — throttling to 50%%")
                    self.throttle_events += 1
                elif snap.level == PressureLevel.RED:
                    log.warning("Backpressure: RED — throttling to 20%%")
                    self.throttle_events += 1
                elif snap.level == PressureLevel.CRITICAL:
                    log.error("Backpressure: CRITICAL — pausing generator!")
                    self.pause_events += 1

            # Keep last 120 snapshots (60 seconds of history)
            self._history.append(snap)
            if len(self._history) > 120:
                self._history.pop(0)

            # Broadcast to dashboard every 2 seconds
            if len(self._history) % 4 == 0:
                self._state.broadcast("backpressure", {
                    "level":           snap.level.value,
                    "raw_queue_pct":   round(snap.raw_queue_pct * 100, 1),
                    "proc_queue_pct":  round(snap.proc_queue_pct * 100, 1),
                    "rate_multiplier": snap.rate_multiplier,
                    "latency_ms":      round(snap.latency_ms, 1),
                    "recommendation":  snap.recommendation,
                    "throttle_count":  self.throttle_events,
                    "pause_count":     self.pause_events,
                })

            time.sleep(self.CHECK_INTERVAL_S)

    def get_history(self, last_n: int = 60) -> list[dict]:
        return [
            {
                "level":           s.level.value,
                "raw_queue_pct":   round(s.raw_queue_pct * 100, 1),
                "proc_queue_pct":  round(s.proc_queue_pct * 100, 1),
                "rate_multiplier": s.rate_multiplier,
                "latency_ms":      round(s.latency_ms, 1),
            }
            for s in self._history[-last_n:]
        ]

    def get_status(self) -> dict:
        snap = self._assess()
        return {
            "level":             snap.level.value,
            "rate_multiplier":   snap.rate_multiplier,
            "raw_queue_pct":     round(snap.raw_queue_pct * 100, 1),
            "proc_queue_pct":    round(snap.proc_queue_pct * 100, 1),
            "latency_ms":        round(snap.latency_ms, 1),
            "recommendation":    snap.recommendation,
            "throttle_events":   self.throttle_events,
            "pause_events":      self.pause_events,
        }
