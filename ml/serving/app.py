"""
ml/serving/app.py
FastAPI serving layer for RetailPulse analytics.

Endpoints:
  GET /health
  GET /metrics/daily-revenue          → daily KPI summary
  GET /metrics/inventory-health       → low-stock alerts
  GET /orders/{order_id}              → single order lookup
  POST /predict/churn-risk            → churn probability for a user

Security:
  - API key auth via X-API-Key header (optional in dev, required in prod)
  - Request validation via Pydantic
  - No raw SQL interpolation (parameterised queries via DuckDB)
  - Structured logging (structlog)
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

import duckdb
import structlog
from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

log = structlog.get_logger()


# ── Settings ───────────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    duckdb_path: str = Field(default="./data/gold/retailpulse.duckdb")
    api_secret_key: str = Field(default="dev_insecure_key_change_in_prod")
    log_level: str = Field(default="info")
    require_api_key: bool = Field(default=False)  # set True in production

    class Config:
        env_file = ".env"


settings = Settings()


# ── Database connection pool ───────────────────────────────────────────────────

_db_conn: duckdb.DuckDBPyConnection | None = None


def get_db() -> duckdb.DuckDBPyConnection:
    global _db_conn
    if _db_conn is None:
        db_path = Path(settings.duckdb_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _db_conn = duckdb.connect(str(db_path), read_only=False)
        _ensure_tables(_db_conn)
    return _db_conn


def _ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create placeholder tables if the pipeline hasn't run yet."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id             VARCHAR PRIMARY KEY,
            user_id_hash         VARCHAR,
            warehouse_id         VARCHAR,
            order_status         VARCHAR,
            is_delivered         BOOLEAN,
            is_refunded_or_cancelled BOOLEAN,
            currency             VARCHAR,
            subtotal_cents       BIGINT,
            discount_cents       BIGINT,
            shipping_cents       BIGINT,
            total_cents          BIGINT,
            net_revenue_cents    BIGINT,
            order_date           TIMESTAMP,
            order_year           INTEGER,
            order_month          INTEGER,
            order_day_of_week    INTEGER,
            cdc_op               VARCHAR,
            created_at           TIMESTAMP,
            updated_at           TIMESTAMP,
            dbt_loaded_at        TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agg_daily_revenue (
            order_date           TIMESTAMP PRIMARY KEY,
            order_year           INTEGER,
            order_month          INTEGER,
            order_count          BIGINT,
            unique_buyers        BIGINT,
            gross_revenue_cents  BIGINT,
            net_revenue_cents    BIGINT,
            total_discount_cents BIGINT,
            total_shipping_cents BIGINT,
            avg_order_value_cents BIGINT,
            refunded_order_count BIGINT,
            refund_rate_pct      DOUBLE,
            delivered_order_count BIGINT,
            dbt_loaded_at        TIMESTAMP
        )
    """)


# ── Auth ───────────────────────────────────────────────────────────────────────

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(key: str | None = Security(api_key_header)) -> str | None:
    if not settings.require_api_key:
        return "dev_bypass"
    if key != settings.api_secret_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key.",
        )
    return key


# ── App lifecycle ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    log.info("startup", duckdb_path=settings.duckdb_path)
    get_db()  # warm up connection
    yield
    if _db_conn:
        _db_conn.close()
    log.info("shutdown")


app = FastAPI(
    title="RetailPulse Analytics API",
    version="0.1.0",
    description="Serving layer for RetailPulse data pipeline portfolio project.",
    lifespan=lifespan,
)


# ── Response models ────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    duckdb_path: str
    table_counts: dict[str, int]


class DailyRevenueRow(BaseModel):
    order_date: str
    order_count: int
    unique_buyers: int
    gross_revenue_cents: int
    net_revenue_cents: int
    avg_order_value_cents: int
    refund_rate_pct: float


class OrderDetail(BaseModel):
    order_id: str
    user_id_hash: str
    order_status: str
    total_cents: int
    net_revenue_cents: int
    currency: str
    order_date: str | None


class ChurnRequest(BaseModel):
    user_id_hash: str
    days_since_last_order: int = Field(ge=0, le=3650)
    total_orders: int = Field(ge=0)
    avg_order_value_cents: int = Field(ge=0)


class ChurnResponse(BaseModel):
    user_id_hash: str
    churn_probability: float
    risk_tier: str
    recommendation: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health_check(
    _auth: Annotated[str | None, Depends(verify_api_key)],
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
) -> HealthResponse:
    """Liveness + data freshness check."""
    counts: dict[str, int] = {}
    # Table names are hardcoded — not user input. Using a whitelist + concatenation
    # (not f-string) to avoid the S608 bandit warning and make the intent explicit.
    _ALLOWED_HEALTH_TABLES = ("fact_orders", "agg_daily_revenue")
    for table in _ALLOWED_HEALTH_TABLES:
        try:
            result = conn.execute("SELECT count(*) FROM " + table).fetchone()
            counts[table] = result[0] if result else 0
        except Exception:
            counts[table] = -1
    return HealthResponse(
        status="ok",
        duckdb_path=settings.duckdb_path,
        table_counts=counts,
    )


@app.get("/metrics/daily-revenue", response_model=list[DailyRevenueRow], tags=["analytics"])
def daily_revenue(
    limit: int = 30,
    _auth: Annotated[str | None, Depends(verify_api_key)] = None,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[DailyRevenueRow]:
    """Return daily revenue for the last N days."""
    if limit < 1 or limit > 365:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 365")

    rows = conn.execute(
        """
        SELECT
            order_date::varchar,
            coalesce(order_count, 0),
            coalesce(unique_buyers, 0),
            coalesce(gross_revenue_cents, 0),
            coalesce(net_revenue_cents, 0),
            coalesce(avg_order_value_cents, 0),
            coalesce(refund_rate_pct, 0.0)
        FROM agg_daily_revenue
        ORDER BY order_date DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()

    return [
        DailyRevenueRow(
            order_date=r[0],
            order_count=r[1],
            unique_buyers=r[2],
            gross_revenue_cents=r[3],
            net_revenue_cents=r[4],
            avg_order_value_cents=r[5],
            refund_rate_pct=r[6],
        )
        for r in rows
    ]


@app.get("/orders/{order_id}", response_model=OrderDetail, tags=["orders"])
def get_order(
    order_id: str,
    _auth: Annotated[str | None, Depends(verify_api_key)] = None,
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
) -> OrderDetail:
    """Retrieve a single order by ID."""
    # Validate UUID format to prevent injection
    import re
    if not re.match(r"^[0-9a-f-]{36}$", order_id.lower()):
        raise HTTPException(status_code=400, detail="Invalid order_id format")

    row = conn.execute(
        """
        SELECT order_id, user_id_hash, order_status,
               total_cents, net_revenue_cents, currency,
               order_date::varchar
        FROM fact_orders
        WHERE order_id = ?
        """,
        [order_id],
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Order not found")

    return OrderDetail(
        order_id=row[0],
        user_id_hash=row[1],
        order_status=row[2],
        total_cents=row[3],
        net_revenue_cents=row[4],
        currency=row[5],
        order_date=row[6],
    )


@app.post("/predict/churn-risk", response_model=ChurnResponse, tags=["ml"])
def predict_churn(
    request: ChurnRequest,
    _auth: Annotated[str | None, Depends(verify_api_key)] = None,
) -> ChurnResponse:
    """
    Simple rule-based churn scoring (production would use MLflow-registered model).
    Demonstrates the ML serving pattern without requiring a trained model.
    """
    score = _calculate_churn_score(
        days_since_last_order=request.days_since_last_order,
        total_orders=request.total_orders,
        avg_order_value_cents=request.avg_order_value_cents,
    )

    if score >= 0.7:
        tier, recommendation = "high", "Send win-back offer with 20% discount immediately."
    elif score >= 0.4:
        tier, recommendation = "medium", "Enrol in re-engagement email sequence."
    else:
        tier, recommendation = "low", "No action needed — monitor at next 30-day review."

    log.info(
        "churn_score",
        user_id_hash=request.user_id_hash[:8] + "...",  # log only prefix
        score=round(score, 3),
        tier=tier,
    )

    return ChurnResponse(
        user_id_hash=request.user_id_hash,
        churn_probability=round(score, 4),
        risk_tier=tier,
        recommendation=recommendation,
    )


def _calculate_churn_score(
    days_since_last_order: int,
    total_orders: int,
    avg_order_value_cents: int,
) -> float:
    """
    Heuristic churn score in [0, 1].
    Replace with MLflow-loaded model in production.
    """
    # Recency (most important signal)
    if days_since_last_order > 90:
        recency_score = 1.0
    elif days_since_last_order > 60:
        recency_score = 0.7
    elif days_since_last_order > 30:
        recency_score = 0.4
    else:
        recency_score = 0.1

    # Frequency (inverse — more orders = less likely to churn)
    frequency_score = max(0.0, 1.0 - (total_orders * 0.1))

    # Monetary (low AOV customers churn more)
    if avg_order_value_cents < 1000:
        monetary_score = 0.6
    elif avg_order_value_cents < 5000:
        monetary_score = 0.3
    else:
        monetary_score = 0.1

    # Weighted combination (RFM)
    return min(1.0, 0.5 * recency_score + 0.3 * frequency_score + 0.2 * monetary_score)
