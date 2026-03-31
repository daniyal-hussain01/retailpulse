"""
scripts/simulate_load.py

Load test and traffic simulation for the real-time pipeline.
Runs configurable traffic patterns and reports throughput, latency,
queue depths, and error rates.

Patterns:
  steady   - constant rate for N seconds
  ramp     - linearly increase from low to high EPS
  burst    - alternate between high and low rates
  spike    - steady rate with sudden 10× spikes
  stress   - maximum sustainable throughput test

Usage:
    python scripts/simulate_load.py --pattern steady --eps 10 --duration 30
    python scripts/simulate_load.py --pattern ramp   --eps-min 1 --eps-max 20 --duration 60
    python scripts/simulate_load.py --pattern burst  --eps 5 --duration 45
    python scripts/simulate_load.py --pattern stress --duration 30
    python scripts/simulate_load.py --pattern spike  --eps 5 --duration 60
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


def _snapshot(state) -> dict:
    m = state.get_metrics()
    return {
        "ts":         time.monotonic(),
        "generated":  m["events_generated"],
        "processed":  m["events_processed"],
        "eps":        m["events_per_second"],
        "latency_ms": m["pipeline_latency_ms"],
        "raw_q":      m["queue_depth_raw"],
        "proc_q":     m["queue_depth_proc"],
        "churn":      m["churn_alerts_fired"],
        "anomalies":  m["anomalies_detected"],
        "r1m":        m["revenue_1m_cents"],
    }


def run_steady(state, eps: float, duration: float) -> list[dict]:
    """Constant event rate."""
    print(f"\n  Pattern: STEADY  |  EPS: {eps}  |  Duration: {duration}s")
    os.environ["RT_EVENTS_PER_SECOND"] = str(eps)
    # Adjust generator rate at runtime
    from realtime.stream_engine import state as s
    for t in s._threads:
        if hasattr(t, "_eps"):
            t._eps = eps
            t._interval = 1.0 / max(eps, 0.1)
    return _collect_snapshots(state, duration)


def run_ramp(state, eps_min: float, eps_max: float, duration: float) -> list[dict]:
    """Linearly increase EPS from min to max."""
    print(f"\n  Pattern: RAMP  |  {eps_min}→{eps_max} EPS  |  Duration: {duration}s")
    snapshots = []
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= duration:
            break
        frac = elapsed / duration
        current_eps = eps_min + (eps_max - eps_min) * frac
        from realtime.stream_engine import state as s
        for t in s._threads:
            if hasattr(t, "_eps"):
                t._eps = current_eps
                t._interval = 1.0 / max(current_eps, 0.1)
        snapshots.append(_snapshot(state))
        _print_row(snapshots[-1], elapsed)
        time.sleep(1.0)
    return snapshots


def run_burst(state, eps: float, duration: float) -> list[dict]:
    """Alternate 5s high / 5s low."""
    print(f"\n  Pattern: BURST  |  Base EPS: {eps}  |  Duration: {duration}s")
    snapshots = []
    start = time.monotonic()
    phase_s = 5.0
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= duration:
            break
        high = (elapsed // phase_s) % 2 == 0
        current_eps = eps * 4 if high else eps * 0.5
        from realtime.stream_engine import state as s
        for t in s._threads:
            if hasattr(t, "_eps"):
                t._eps = current_eps
                t._interval = 1.0 / max(current_eps, 0.1)
        snapshots.append(_snapshot(state))
        phase = "HIGH" if high else "low "
        _print_row(snapshots[-1], elapsed, extra=f"[{phase} {current_eps:.1f} eps]")
        time.sleep(1.0)
    return snapshots


def run_stress(state, duration: float) -> list[dict]:
    """Maximum rate — find the sustainable throughput ceiling."""
    print(f"\n  Pattern: STRESS  |  Max rate  |  Duration: {duration}s")
    from realtime.stream_engine import state as s
    for t in s._threads:
        if hasattr(t, "_eps"):
            t._eps = 200
            t._interval = 0.005
    return _collect_snapshots(state, duration)


def run_spike(state, eps: float, duration: float) -> list[dict]:
    """Steady with random 10× spikes every ~15s."""
    import random
    print(f"\n  Pattern: SPIKE  |  Base EPS: {eps}  |  Duration: {duration}s")
    snapshots = []
    start = time.monotonic()
    next_spike = start + random.uniform(8, 15)
    spike_end  = 0.0
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= duration:
            break
        now = time.monotonic()
        in_spike = now < spike_end
        if not in_spike and now >= next_spike:
            spike_end  = now + random.uniform(2, 4)
            next_spike = spike_end + random.uniform(10, 20)
            in_spike   = True
        current_eps = eps * 10 if in_spike else eps
        from realtime.stream_engine import state as s
        for t in s._threads:
            if hasattr(t, "_eps"):
                t._eps = current_eps
                t._interval = 1.0 / max(current_eps, 0.1)
        snapshots.append(_snapshot(state))
        extra = "⚡ SPIKE" if in_spike else ""
        _print_row(snapshots[-1], elapsed, extra=extra)
        time.sleep(1.0)
    return snapshots


def _collect_snapshots(state, duration: float) -> list[dict]:
    snapshots = []
    start = time.monotonic()
    while time.monotonic() - start < duration:
        snap = _snapshot(state)
        snapshots.append(snap)
        _print_row(snap, time.monotonic() - start)
        time.sleep(1.0)
    return snapshots


def _print_row(snap: dict, elapsed: float, extra: str = "") -> None:
    print(
        f"  t={elapsed:5.1f}s  "
        f"eps={snap['eps']:5.1f}  "
        f"lat={snap['latency_ms']:5.1f}ms  "
        f"raw_q={snap['raw_q']:3d}  "
        f"proc_q={snap['proc_q']:3d}  "
        f"churn={snap['churn']:3d}  "
        f"r1m=${snap['r1m']/100:6.0f}  "
        + extra
    )


def _print_summary(snapshots: list[dict], pattern: str) -> None:
    if not snapshots:
        print("  No data collected.")
        return

    eps_vals = [s["eps"] for s in snapshots]
    lat_vals = [s["latency_ms"] for s in snapshots]
    q_vals   = [max(s["raw_q"], s["proc_q"]) for s in snapshots]

    total_generated = snapshots[-1]["generated"] - snapshots[0]["generated"]
    total_processed = snapshots[-1]["processed"] - snapshots[0]["processed"]
    drop_rate = 1.0 - (total_processed / max(total_generated, 1))

    print("\n" + "═" * 58)
    print(f"  LOAD TEST SUMMARY — {pattern.upper()}")
    print("═" * 58)
    print(f"  Total events generated:  {total_generated:,}")
    print(f"  Total events processed:  {total_processed:,}")
    print(f"  Drop rate:               {drop_rate*100:.2f}%")
    print(f"  Avg EPS:                 {sum(eps_vals)/len(eps_vals):.1f}")
    print(f"  Peak EPS:                {max(eps_vals):.1f}")
    print(f"  Avg latency:             {sum(lat_vals)/len(lat_vals):.1f}ms")
    print(f"  P95 latency:             {sorted(lat_vals)[int(len(lat_vals)*.95)]:.1f}ms")
    print(f"  Max queue depth:         {max(q_vals)}")
    print(f"  Churn alerts fired:      {snapshots[-1]['churn']}")
    print("═" * 58)

    if drop_rate > 0.05:
        print("  ⚠️  >5% drop rate — consider increasing RT_CHURN_WORKERS")
    if max(lat_vals) > 200:
        print("  ⚠️  Peak latency >200ms — pipeline is under stress")
    if max(q_vals) > 500:
        print("  ⚠️  Queue depth >500 — backpressure kicking in")
    if drop_rate <= 0.01 and max(lat_vals) < 50:
        print("  ✓  Pipeline healthy — low latency, no drops")


def main() -> None:
    parser = argparse.ArgumentParser(description="RetailPulse load simulator")
    parser.add_argument("--pattern",  default="steady",
                        choices=["steady","ramp","burst","stress","spike"])
    parser.add_argument("--eps",      type=float, default=5.0,
                        help="Base events per second")
    parser.add_argument("--eps-min",  type=float, default=1.0)
    parser.add_argument("--eps-max",  type=float, default=20.0)
    parser.add_argument("--duration", type=float, default=30.0,
                        help="Test duration in seconds")
    parser.add_argument("--no-start", action="store_true",
                        help="Don't start the pipeline (assume already running)")
    args = parser.parse_args()

    from realtime.stream_engine import start_pipeline, stop_pipeline, state

    print("\nRetailPulse Load Simulator")
    print(f"  Pattern:  {args.pattern}")
    print(f"  Duration: {args.duration}s")
    print(f"  Columns:  t | eps | latency | raw_q | proc_q | churn | r1m\n")

    started_here = False
    if not args.no_start and not state._running.is_set():
        start_pipeline()
        started_here = True
        time.sleep(1.0)   # warm up

    try:
        if args.pattern == "steady":
            snaps = run_steady(state, args.eps, args.duration)
        elif args.pattern == "ramp":
            snaps = run_ramp(state, args.eps_min, args.eps_max, args.duration)
        elif args.pattern == "burst":
            snaps = run_burst(state, args.eps, args.duration)
        elif args.pattern == "stress":
            snaps = run_stress(state, args.duration)
        elif args.pattern == "spike":
            snaps = run_spike(state, args.eps, args.duration)
        else:
            snaps = []

        _print_summary(snaps, args.pattern)

    finally:
        if started_here:
            stop_pipeline()


if __name__ == "__main__":
    main()
