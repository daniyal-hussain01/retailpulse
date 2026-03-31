"""
ml/feature_store/feature_definitions.py

Feast feature store definitions for RetailPulse.
Features are materialised daily from the Gold DuckDB warehouse.

Setup:
    pip install feast[duckdb]
    cd ml/feature_store
    feast apply
    feast materialize-incremental $(date +%Y-%m-%dT%H:%M:%S)

Usage in training:
    from feast import FeatureStore
    store = FeatureStore(repo_path="ml/feature_store")
    features = store.get_historical_features(
        entity_df=entity_df,
        features=["user_features:days_since_last_order", "user_features:ltv_segment"]
    ).to_df()

Usage in serving (online):
    online_features = store.get_online_features(
        features=["user_features:churn_score"],
        entity_rows=[{"user_id_hash": "abc123..."}]
    ).to_dict()
"""
from __future__ import annotations

from datetime import timedelta

# NOTE: feast import is optional — this file documents the feature definitions.
# If feast is not installed, the serving layer uses the heuristic churn score instead.
try:
    from feast import (
        Entity,
        Feature,
        FeatureView,
        ValueType,
    )
    from feast.infra.offline_stores.duckdb import DuckDBOfflineStoreConfig
    from feast.repo_config import RepoConfig
    FEAST_AVAILABLE = True
except ImportError:
    FEAST_AVAILABLE = False


# ── Entities ──────────────────────────────────────────────────────────────────

if FEAST_AVAILABLE:
    user_entity = Entity(
        name="user_id_hash",
        value_type=ValueType.STRING,
        description="SHA-256 hash of user_id. Never the raw user_id.",
        join_keys=["user_id_hash"],
    )

    # ── Feature views ──────────────────────────────────────────────────────────

    user_features = FeatureView(
        name="user_features",
        entities=["user_id_hash"],
        ttl=timedelta(days=30),
        features=[
            Feature(name="total_orders",           dtype=ValueType.INT64),
            Feature(name="days_since_last_order",  dtype=ValueType.INT64),
            Feature(name="avg_order_value_cents",  dtype=ValueType.INT64),
            Feature(name="lifetime_value_cents",   dtype=ValueType.INT64),
            Feature(name="refund_rate",            dtype=ValueType.DOUBLE),
            Feature(name="active_months",          dtype=ValueType.INT64),
            Feature(name="customer_age_days",      dtype=ValueType.INT64),
            Feature(name="ltv_segment",            dtype=ValueType.STRING),   # low/mid/high
            Feature(name="churn_risk_score",       dtype=ValueType.DOUBLE),   # model output
        ],
        online=True,
        tags={"team": "data-engineering", "pii": "false"},
    )


# ── LTV segmentation logic (used in feature materialisation) ──────────────────

def assign_ltv_segment(lifetime_value_cents: int) -> str:
    """Bucket users into LTV tiers. Thresholds should be reviewed quarterly."""
    if lifetime_value_cents >= 50_000:   # $500+
        return "high"
    elif lifetime_value_cents >= 10_000:  # $100–$500
        return "mid"
    else:
        return "low"


# ── Feature view SQL query (materialisation source) ───────────────────────────

FEATURE_MATERIALISATION_QUERY = """
SELECT
    user_id_hash,
    COUNT(*)                                                   AS total_orders,
    MAX(order_date)                                            AS event_timestamp,
    date_diff('day', MAX(order_date), CURRENT_DATE)           AS days_since_last_order,
    AVG(total_cents)::BIGINT                                   AS avg_order_value_cents,
    SUM(total_cents)                                           AS lifetime_value_cents,
    ROUND(
        SUM(CASE WHEN is_refunded_or_cancelled THEN 1.0 ELSE 0.0 END) / COUNT(*),
        4
    )                                                          AS refund_rate,
    COUNT(DISTINCT order_month)                                AS active_months,
    date_diff('day', MIN(order_date), MAX(order_date))        AS customer_age_days
FROM fact_orders
WHERE user_id_hash != 'ERASED'
GROUP BY user_id_hash
HAVING COUNT(*) >= 1
"""
