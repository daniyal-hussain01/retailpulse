# RetailPulse — Real-Time Pipeline Quickstart

## What it does

This layer adds a live streaming pipeline on top of the existing batch system.
Orders are generated, scored for churn risk, and pushed to the dashboard
**within 100ms** — no extra installs, no Docker, no Kafka.

```
EventGenerator  →  Queue  →  StreamProcessor  →  ChurnScorer  →  SQLite
      2 eps           ↓             2 workers           ML           WAL
                 BackpressureController                  ↓
                 (auto-throttles on lag)            Dashboard
                                                  (SSE stream)
```

---

## Run it (one command)

```powershell
# In your venv terminal, inside the retailpulse folder:
python realtime_app.py
```

Open **http://localhost:5000** — the live dashboard loads automatically.

---

## Dashboard tabs

| What you see | Where it comes from |
|---|---|
| **Live Order Feed** | SSE push, every order within ~50ms |
| **Churn Alerts** | ChurnScorer fires when score ≥ 0.70 |
| **Events/sec chart** | MetricsAggregator, 30-second rolling window |
| **Revenue/min** | Sliding 60-second sum from ring buffer |
| **Anomaly Log** | Traffic bursts and high-value orders (>$200) |
| **Pipeline flow** | Queue depths and thread counts, live |

---

## API endpoints (real-time layer)

```
GET  /stream/events              SSE stream — connect once, receive forever
GET  /api/rt/orders?limit=50     Latest N processed orders (JSON)
GET  /api/rt/metrics             Pipeline KPIs (eps, latency, queue depths)
GET  /api/rt/churn-alerts        High-risk users from ring buffer
GET  /api/rt/anomalies           Detected anomalies from ring buffer
GET  /api/rt/stats?minutes=5     Aggregated stats from SQLite (last N min)
POST /api/rt/churn-score         Score any user on-demand
GET  /api/health                 Full system health (batch + real-time)
```

All batch endpoints (`/api/metrics/daily-revenue`, `/api/orders`, etc.) continue
to work unchanged.

---

## Configuration (environment variables)

Set these in your `.env` file or before running:

| Variable | Default | Description |
|---|---|---|
| `RT_EVENTS_PER_SECOND` | `2` | Event generation rate |
| `RT_CHURN_WORKERS` | `2` | StreamProcessor threads |
| `RT_RING_BUFFER` | `500` | Max orders in memory |
| `PORT` | `5000` | Server port |
| `ALERT_WEBHOOK_URL` | — | Slack/Teams webhook for alerts |
| `ALERT_CHURN_THRESHOLD` | `0.80` | Score to trigger alert |
| `ALERT_COOLDOWN_SECONDS` | `300` | Minimum seconds between repeat alerts |

Example — higher throughput:
```powershell
$env:RT_EVENTS_PER_SECOND = "10"
$env:RT_CHURN_WORKERS = "4"
python realtime_app.py
```

---

## Run tests

```powershell
# All tests including real-time (76 total)
python run_tests.py

# Real-time tests only (12 tests)
python run_tests.py realtime -v

# Individual suites
python run_tests.py transform   # PySpark transform logic
python run_tests.py api         # Flask API endpoints
python run_tests.py security    # PII + injection tests
python run_tests.py churn       # Churn scoring model
python run_tests.py quality     # Data quality suite
python run_tests.py session     # Sessionisation logic
```

---

## Load test

```powershell
# Steady 5 eps for 30 seconds
python scripts/simulate_load.py --pattern steady --eps 5 --duration 30

# Ramp from 1 to 20 eps over 60 seconds
python scripts/simulate_load.py --pattern ramp --eps-min 1 --eps-max 20 --duration 60

# Burst pattern (5s high / 5s low)
python scripts/simulate_load.py --pattern burst --eps 5 --duration 45

# Find maximum throughput
python scripts/simulate_load.py --pattern stress --duration 30

# Spike pattern (random 10× spikes)
python scripts/simulate_load.py --pattern spike --eps 5 --duration 60
```

Expected results on a modern laptop:
- Sustainable throughput: **15–30 eps**
- End-to-end latency: **< 50ms** at ≤10 eps
- Queue depth: **< 10** at steady state

---

## Replay historical data

```python
# In Python, replay the last 7 days at 10× speed:
from realtime.replay import EventReplayer
from realtime.stream_engine import state, start_pipeline

start_pipeline()

replayer = EventReplayer(
    db_path          = Path("data/gold/retailpulse.db"),
    pipeline_state   = state,
    speed_multiplier = 10.0,
    date_from        = "2024-04-18",
    date_to          = "2024-04-25",
    loop             = False,
)
replayer.start()
```

---

## Alert configuration

### Slack webhook

1. Create a Slack App at api.slack.com → Incoming Webhooks
2. Copy the webhook URL
3. Set in `.env`:
   ```
   ALERT_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../xxx
   ALERT_CHURN_THRESHOLD=0.80
   ```

### Email alerts

```
ALERT_EMAIL_TO=team@yourcompany.com
ALERT_EMAIL_FROM=retailpulse@yourcompany.com
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=you@gmail.com
ALERT_SMTP_PASSWORD=your-app-password
```

All alerts are always written to `data/alerts.jsonl` regardless of other config.

---

## File structure (new files)

```
retailpulse/
├── realtime_app.py              ← main entry point (run this)
├── realtime/
│   ├── stream_engine.py         ← all pipeline threads + SSE
│   ├── backpressure.py          ← adaptive rate control
│   ├── replay.py                ← historical event replay
│   └── alerting.py              ← Slack/email/log alert dispatch
├── dashboard/
│   ├── realtime.html            ← live SSE dashboard (auto-loaded)
│   └── index.html               ← classic batch dashboard (/classic)
├── scripts/
│   └── simulate_load.py         ← load test patterns
└── docs/adr/
    └── ADR-003-realtime-streaming.md
```

---

## Troubleshooting

**Port already in use:**
```powershell
$env:PORT = "5001"
python realtime_app.py
```

**Dashboard shows "Disconnected":**
- The SSE stream auto-reconnects — wait 3 seconds
- Check the terminal for errors
- Ensure `realtime_app.py` is running (not `app.py`)

**No churn alerts appearing:**
- Churn alerts require score ≥ 0.70 AND 2-minute cooldown per user
- With 200 user pool rotating randomly, alerts appear every 30–60 seconds
- Lower threshold: `ALERT_CHURN_THRESHOLD=0.50` in `.env`

**Tests failing with import error:**
```powershell
# Make sure you're in the retailpulse folder
cd retailpulse
python run_tests.py
```
