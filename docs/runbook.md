# RetailPulse Operations Runbook

## On-call contacts

| Role | Slack | PagerDuty |
|---|---|---|
| Data Engineering | `#data-engineering` | `retailpulse-de` |
| Platform/Infra | `#platform` | `retailpulse-platform` |

---

## Daily health checks

Run every morning before standup:

```bash
# 1. Check last pipeline run
airflow dags list-runs -d retailpulse_full_pipeline --limit 3

# 2. Verify Silver freshness (should be < 2 hours old)
python scripts/check_freshness.py

# 3. Check API health
curl -s http://localhost:8000/health | python -m json.tool

# 4. Check Kafka consumer lag (should be < 10K messages)
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group retailpulse-s3-sink
```

---

## Incident response

### Pipeline failed / stale data

**Symptoms:** Airflow task in `failed` state; dashboard shows stale timestamps; Slack alert in `#data-alerts`.

**Steps:**
1. Check Airflow UI → click failed task → Logs tab
2. Identify the failing stage: `produce_events`, `bronze_to_silver`, `dbt_run`, or `data_quality`
3. Fix the root cause (see stage-specific guides below)
4. Re-run from failed task: `airflow tasks run retailpulse_full_pipeline <task_id> <run_date>`
5. Monitor next run to confirm recovery

**Bronze → Silver failures — common causes:**
- `No bronze data at ...` → Kafka producer or S3 sink not running. Check `docker compose ps`
- `KeyError: 'after'` → schema change in source Postgres. Check Debezium connector logs
- `Parquet write error` → disk space. Run `df -h` on the worker node

**dbt failures — common causes:**
- `Table not found: ...silver/orders/*.parquet` → Silver job didn't complete. Re-run `bronze_to_silver` first
- `not_null test failed on order_id` → upstream data quality issue. Check Great Expectations report
- `Column X does not exist` → schema drift. Run `dbt run --full-refresh` once, then normal runs resume

**Data quality failures:**
1. Open the GE data docs: `open data_quality/target/data_docs/index.html`
2. Find the failed expectation and the failing rows
3. Determine if it's a source data issue (fix upstream) or a pipeline bug (fix transform)
4. If the failure is acceptable (e.g. known bad data in source), add to `ignore_list` in the expectation suite and document the decision in the ADR

### DuckDB locked / API returning 500

DuckDB is single-writer. If multiple processes open the file for writing, the second will fail.

```bash
# Find the process holding the lock
lsof ./data/gold/retailpulse.duckdb

# Kill it (replace PID)
kill -9 <PID>

# Restart the API
make serve
```

### Kafka consumer lag growing

**Threshold:** Alert at > 100K messages. Critical at > 1M messages.

```bash
# Check per-partition lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group retailpulse-s3-sink

# Reset offset to latest (drops messages — only if data is recoverable from source)
# USE WITH CAUTION
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group retailpulse-s3-sink --reset-offsets --to-latest \
  --topic retailpulse.orders --execute
```

---

## Backfill procedure

To reprocess a historical date range:

```bash
# Reprocess Silver for a single date
python -m spark.jobs.bronze_to_silver --date 2024-04-01 --local

# Reprocess a date range (runs sequentially)
for date in $(seq -f "2024-04-%02g" 1 30); do
  echo "Processing $date..."
  python -m spark.jobs.bronze_to_silver --date "$date" --local
done

# Rebuild Gold after Silver backfill
cd dbt && dbt run --full-refresh --profiles-dir ../.dbt
```

---

## GDPR deletion procedure

```bash
# Trigger deletion for a specific user
airflow dags trigger gdpr_deletion \
  --conf '{"user_id": "user@example.com", "request_id": "GDPR-2024-042"}'

# Monitor the run
airflow dags list-runs -d gdpr_deletion --limit 5

# Verify audit log
tail -5 data/gold/audit/erasure_audit.jsonl | python -m json.tool
```

**SLA:** Must complete within 30 days of request (GDPR Art. 17).  
**Confirmation:** Email the requester with the `request_id` and `completed_at` timestamp from the audit log.

---

## Scaling checklist

When daily data volume exceeds 100 GB:
- [ ] Increase Spark executor memory: `spark.executor.memory=8g`
- [ ] Add partitioning to Silver parquet: partition by `status` or `order_date`
- [ ] Enable Delta Lake Z-ordering: `OPTIMIZE fact_orders ZORDER BY (order_date, user_id_hash)`
- [ ] Move DuckDB to Snowflake for serving (update `.dbt/profiles.yml`)
- [ ] Add Kafka partitions: increase from 1 to 8 for `orders.cdc` topic

---

## Security incident response

If a credential is exposed (e.g. `.env` committed to git):
1. Immediately rotate the affected credential (Postgres password, API key, PII salt)
2. If PII salt is rotated, all Silver data must be reprocessed (backfill from Bronze)
3. Revoke git history: `git filter-branch` or `git-secrets`
4. Notify security team within 24 hours per incident response policy
5. Add the secret pattern to `.gitignore` and `git-secrets` allowlist

**PII salt rotation impact:**
- All `user_id_hash` values will change
- Silver must be fully reprocessed from Bronze
- Gold must be rebuilt with `dbt run --full-refresh`
- ML models referencing `user_id_hash` must be retrained
