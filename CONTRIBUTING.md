# Contributing to RetailPulse

Thank you for contributing. This document covers everything you need to go from zero to first PR.

---

## Development setup

### Requirements

- Python 3.11 (exactly — other versions may work but are untested)
- Docker Desktop 26+ (for the full integration stack)
- Git 2.40+
- 8 GB RAM minimum

### First-time setup

```bash
# 1. Clone
git clone <repo-url> && cd retailpulse

# 2. Copy environment config
cp .env.example .env
# Edit .env and generate secrets per the instructions inside

# 3. Create virtual environment and install all dependencies
make setup

# 4. Install pre-commit hooks (runs lint + security on every commit)
source .venv/bin/activate
pre-commit install

# 5. Verify everything works
make test-unit      # should pass with no Docker
make lint
make security

# 6. Start the Docker stack (optional — needed for integration tests)
make docker-up
```

### VS Code

Open `retailpulse.code-workspace` rather than the folder directly. This sets:
- Python interpreter to `.venv/bin/python`
- Format-on-save with ruff
- pytest integration with the correct test paths
- Launch configs for FastAPI, bronze→silver, producer

---

## Branching strategy

```
main          ← production-ready code only. Protected. Requires PR + CI pass.
develop       ← integration branch. PRs merge here first.
feature/xxx   ← your work. Branch from develop.
fix/xxx       ← bug fixes. Branch from develop (or main for hotfixes).
```

Branch naming examples:
- `feature/demand-forecast-model`
- `fix/silver-dedup-edge-case`
- `chore/upgrade-dbt-1.9`

---

## Making a change

### Step 1 — Create your branch

```bash
git checkout develop
git pull origin develop
git checkout -b feature/your-feature-name
```

### Step 2 — Write code and tests

All new code requires tests. Coverage must not drop below 70%.

| Change type | Test location |
|---|---|
| Spark/Pandas transformation | `spark/tests/unit/` |
| API endpoint | `tests/unit/test_api.py` |
| Airflow DAG | `airflow/tests/unit/` |
| Data quality suite | `tests/unit/test_data_quality.py` |
| dbt model | `dbt/tests/` (custom SQL test) + `schema.yml` |
| Security-relevant code | `tests/unit/test_security.py` |

Run tests frequently:
```bash
make test-unit        # fast, no Docker
make test             # full suite
```

### Step 3 — Commit

Pre-commit hooks run automatically on `git commit`:
- `ruff` lint + format
- `bandit` security scan
- Secret detection
- YAML/JSON validation

If hooks fail, fix the issues and re-stage. Do not use `--no-verify`.

Commit message format (Conventional Commits):
```
feat(spark): add sessionisation watermark for late-arriving events
fix(api): reject UUIDs with uppercase hex chars
chore(deps): bump dbt-duckdb to 1.8.2
docs(adr): add ADR-003 for stream processing choice
test(gdpr): add audit log append test
```

### Step 4 — Open a PR

```bash
git push origin feature/your-feature-name
```

Then open a PR against `develop`. Fill in the PR template:
- What does this change?
- How was it tested?
- Any breaking changes?

CI will run automatically. All checks must pass before merge.

---

## Code standards

### Python

- Type hints required on all function signatures
- Docstrings required on all public functions and modules
- No bare `except:` — always catch specific exceptions
- No hardcoded secrets or paths — use environment variables
- PII handling: if a function receives a raw user_id, hash it immediately and do not pass it onward

```python
# Good
def process_order(order_id: str, user_id_hash: str) -> dict[str, Any]:
    """Process a single order event into Silver format."""
    ...

# Bad
def process_order(order_id, user_id):  # no types, no docstring, receives PII
    ...
```

### SQL (dbt)

- All models must have a `description` in `schema.yml`
- Primary key columns must have `unique` + `not_null` tests
- Status/enum columns must have `accepted_values` tests
- No `SELECT *` in mart models — list columns explicitly
- Use `{{ ref() }}` for all inter-model references
- Dollar amounts always in cents (integer), never floats

### Airflow DAGs

- Every DAG must have `max_active_runs=1` unless explicitly justified
- Default args must include `retries >= 2` and `retry_exponential_backoff=True`
- All tasks must have `doc_md` explaining what they do
- Secrets accessed via `os.environ.get()` — never hardcoded

---

## Adding a new data source

1. Add the connector config to `ingestion/` (Debezium JSON or Airbyte connection spec)
2. Add the schema to `scripts/sql/01_schema.sql` if it's a Postgres source
3. Add a staging dbt model in `dbt/models/staging/stg_<source>.sql`
4. Add schema tests in `dbt/models/staging/schema.yml`
5. Update the data dictionary in `docs/data_dictionary.md`
6. Add the source to the Airflow DAG if it needs scheduled ingestion

---

## Release process

Releases happen from `main` branch. Version follows [Semantic Versioning](https://semver.org).

```bash
# After PR merged to main:
git tag v0.2.0
git push origin v0.2.0
```

Update `CHANGELOG.md` with the changes in this release.

---

## Getting help

- Open a GitHub Issue for bugs or feature requests
- Ask in `#data-engineering` Slack for urgent questions
- Tag `@data-engineering` on the PR for review
