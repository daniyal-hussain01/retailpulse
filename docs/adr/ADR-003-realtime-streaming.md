# ADR-003: Real-Time Streaming Architecture

**Status:** Accepted  
**Date:** 2024-04-25  
**Deciders:** Data Engineering Team

---

## Context

RetailPulse v0.1 used a batch pipeline (daily Airflow DAG) with a 15-minute
data freshness SLA. The business now requires:

1. **Churn scoring in real-time** — flag at-risk users within seconds of their
   last interaction, not 24 hours later
2. **Live dashboard** — executives want to see today's revenue updating
   continuously, not on page refresh
3. **Instant anomaly detection** — detect unusual order volumes or high-value
   transactions as they happen
4. **Zero new infrastructure** — must run locally with no additional services

## Decision: In-process threading with SSE, not a message broker

### Chosen architecture

```
EventGenerator thread
       ↓ (queue, non-blocking)
StreamProcessor threads (×N)
       ↓ (queue, non-blocking)
ChurnScorer thread (ML scoring)
       ↓ (SQLite WAL + ring buffer)
MetricsAggregator thread
       ↓ (Server-Sent Events)
Browser Dashboard
```

All components run as Python daemon threads within the Flask process.
Communication uses `queue.Queue` (thread-safe FIFO, zero dependencies).
The browser receives live updates via SSE — a native browser API over HTTP/1.1,
no WebSocket library needed.

### Rejected alternatives

| Alternative | Why rejected |
|---|---|
| **Apache Kafka** | Requires JVM + ZooKeeper/KRaft broker; adds 4 GB RAM overhead; overkill for single-node demo |
| **Redis Streams** | Requires Redis server; another process to manage; not in stdlib |
| **WebSockets** | `flask-socketio` needs `eventlet` or `gevent` install; SSE is simpler and sufficient for one-directional push |
| **Celery** | Designed for task queues, not streaming; adds broker dependency |
| **asyncio** | Would require rewriting all sync Flask routes; mixing sync/async in Flask 2.x is complex |
| **Apache Flink** | Excellent for production but far exceeds the zero-infra requirement |

### Why Python threads + Queue

The pipeline's bottleneck is I/O (SQLite writes, SSE pushes), not CPU.
Python's GIL does not hurt I/O-bound threads. The `queue.Queue` class is
designed exactly for producer-consumer pipelines and handles backpressure
naturally (blocking `put` when full, or `put_nowait` with graceful drop).

At 2 events/second (default), the pipeline is comfortably within capacity.
The `BackpressureController` dynamically throttles the generator if queue
depth exceeds 30%, ensuring sub-100ms end-to-end latency even under load.

### Why SSE over WebSockets

Server-Sent Events are:
- **Unidirectional** — the pipeline only pushes data to the browser; the browser
  doesn't need to send data back (it uses regular REST POSTs for that)
- **HTTP/1.1 native** — works through proxies, firewalls, and load balancers
  without special configuration
- **Auto-reconnect** — browsers reconnect automatically on disconnect
- **No library** — `text/event-stream` MIME type + `data: ...\n\n` format is all
  that's needed on the server; `EventSource` API is built into all browsers

### SQLite as the streaming store

SQLite in WAL (Write-Ahead Logging) mode supports:
- **Concurrent reads** — multiple threads read while one thread writes
- **Batch writes** — the `DBWriter` batches 50 rows per commit for throughput
- **Ring buffer** — a `AFTER INSERT` trigger deletes rows older than 10,000,
  bounding storage growth

The `rt_orders` table acts as a sliding-window store: the REST API can query
"last 5 minutes" without scanning the full history.

---

## Consequences

**Positive:**
- Zero additional pip installs — runs with Flask + stdlib only
- Sub-100ms end-to-end latency at ≤10 events/second
- Graceful backpressure: generator slows, never crashes, never OOMs
- All components are independently testable (pure Python classes)
- SSE auto-reconnects on network interruption — dashboard is resilient

**Negative:**
- Single-process — not horizontally scalable without switching to a real broker
- Python GIL limits CPU-bound throughput to ~50 events/second sustained
- SQLite WAL still has a single writer; high write throughput (>1000 eps)
  would require switching to Postgres or a time-series DB
- No message durability — events in the queue are lost if the process crashes
  (mitigated by the SQLite ring buffer for already-processed events)

---

## Scaling path (when to upgrade)

| Trigger | Upgrade |
|---|---|
| Need >50 eps sustained | Replace EventGenerator with Debezium → Kafka |
| Need horizontal scaling | Replace queue.Queue with Kafka topics |
| Need <10ms latency | Replace SQLite WAL with Redis Streams or Flink |
| Need message durability | Add Kafka with replication factor ≥ 2 |
| Need multiple consumers | Add Kafka consumer groups |

The current threading architecture is designed to be replaced stage-by-stage:
`EventGenerator` → Debezium connector, `queue.Queue` → Kafka topic,
`DBWriter` → Kafka Connect S3 sink. The `StreamProcessor` and `ChurnScorer`
logic migrates directly to Spark Structured Streaming jobs.

---

## Review date

Revisit when: sustained throughput exceeds 20 eps, or when the team grows
beyond 3 engineers (at which point separate deployable services become valuable).
