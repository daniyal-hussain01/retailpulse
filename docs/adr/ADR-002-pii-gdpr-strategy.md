# ADR-002: PII handling and GDPR compliance strategy

**Status:** Accepted  
**Date:** 2024-04-25  
**Deciders:** Data Engineering + Legal  

---

## Context

RetailPulse processes user data including email addresses, names, and IP addresses. We must comply with GDPR (EU users), CCPA (California users), and general data minimisation principles. The pipeline must:

1. Prevent PII from reaching the analytics/Gold layer
2. Support right-to-erasure requests within 30 days
3. Enable debugging without exposing PII to engineers
4. Pass a security audit with no HIGH-severity findings

## Decision: Hash at the Bronze→Silver boundary, tombstone for erasure

### PII hashing approach

**Chosen:** SHA-256 HMAC with a project-level secret salt stored in AWS Secrets Manager

**Rejected alternatives:**

| Approach | Why rejected |
|---|---|
| No hashing (raw user_id as UUID) | UUIDs are pseudonymous but reversible with the users table — fails GDPR pseudonymisation requirement |
| Encryption (AES) | Reversible — legal would require us to decrypt for law enforcement on request, exposing all users |
| Tokenisation (lookup table) | Requires maintaining a mapping table; adds infrastructure; token table itself becomes a PII liability |
| Full deletion | Breaks referential integrity in time-series analytics — gaps in order data distort cohort analysis |

**Rationale for SHA-256 + salt:**
- One-way: given only the hash, the original user_id cannot be recovered without the salt
- Deterministic: the same user's events are joinable across tables without revealing identity
- Saltable: rotating the salt immediately pseudonymises the entire dataset (no re-hashing needed — old hashes simply stop matching)
- Standard: SHA-256 is auditable and widely accepted for pseudonymisation under GDPR Recital 26

### Where hashing occurs

The hash is applied in `spark/jobs/bronze_to_silver.py` at the Bronze→Silver boundary. This is the earliest defensible point — we need the raw `user_id` for deduplication and CDC merge operations in Bronze (matching Debezium creates to updates requires the stable ID), but it must not appear in Silver or downstream.

### Salt management

- **Local dev:** `PII_SALT` in `.env` (never committed; documented in `.env.example`)
- **Production:** AWS Secrets Manager secret `retailpulse/pii_salt`; loaded at container startup via `boto3.get_secret_value()`
- **Rotation policy:** Salt rotated annually or on suspected compromise. Rotation requires a full Bronze→Silver reprocess (see runbook)

### GDPR erasure: tombstone over delete

**Chosen:** Replace `user_id_hash` with the string `"ERASED"` in Silver and Gold

**Rejected:** Physical row deletion

**Rationale:**
- Deleting rows from partitioned Parquet files requires rewriting the entire partition — expensive and error-prone at scale
- Delta Lake `DELETE` + `VACUUM` works but VACUUM has a minimum 7-day retention window, meaning data isn't immediately gone
- Tombstoning preserves aggregate integrity (daily revenue totals remain correct) while making the user unidentifiable
- The tombstone `"ERASED"` is clearly distinguishable from a legitimate hash value (which is always 64 hex chars)
- GDPR Art. 17 requires erasure of *personal data*, not erasure of *derived aggregates* — our tombstone approach satisfies this

### Audit trail

All erasure events are appended to `data/gold/audit/erasure_audit.jsonl`. The log contains:
- The `request_id` (from the requestor's ticket system)
- A **prefix only** of the `user_id_hash` (first 8 chars + "...") — never the full hash
- Timestamp of completion
- Count of records affected in Silver and Gold

This log is **never purged** and is stored in an append-only S3 bucket (object lock enabled in production).

---

## Consequences

**Positive:**
- Engineers can debug pipeline issues using `user_id_hash` without ever seeing raw PII
- GDPR deletion is a fast, idempotent operation (overwrite hash → done)
- Hashing is transparent to dbt models — they treat `user_id_hash` as an opaque key
- Passing a GDPR audit requires demonstrating the boundary and the deletion mechanism — both are coded and testable

**Negative:**
- The PII salt is a project-level secret. If it leaks alongside a breach of the users table, pseudonymisation is broken
- You cannot join `user_id_hash` back to the users table without the salt — intentional, but means user lookups require the application layer, not SQL
- Annual salt rotation is operationally expensive (full Silver reprocess ~2–4 hours at current scale)

---

## Review date

Revisit if: legal advises that tombstoning doesn't satisfy a specific jurisdiction's erasure requirements, or if scale makes full-partition rewrites feasible for physical deletion.
