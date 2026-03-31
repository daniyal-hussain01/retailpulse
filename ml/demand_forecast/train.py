"""
ml/demand_forecast/train.py

Per-SKU demand forecasting using Facebook Prophet.
Reads daily order volume from Gold, trains one model per SKU,
writes forecasts back to the Gold layer as a Parquet table.

Run:
    python -m ml.demand_forecast.train --horizon-days 30

Airflow triggers this nightly; output feeds the inventory health dashboard.
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def load_daily_demand(duckdb_path: str) -> "pd.DataFrame":
    """
    Returns a DataFrame with columns:
        warehouse_id, product_id (proxy: order_date grain), ds, y
    For a real deployment, join fact_orders to order_items to get product-level demand.
    For MVP, we forecast aggregate daily order volume.
    """
    import duckdb

    conn = duckdb.connect(duckdb_path, read_only=True)
    df = conn.execute("""
        select
            order_date::date  as ds,
            count(*)          as y
        from fact_orders
        where order_date is not null
          and is_refunded_or_cancelled = false
        group by 1
        order by 1
    """).df()
    conn.close()

    log.info("Loaded %d daily demand records", len(df))
    return df


def train_forecast(
    df: "pd.DataFrame",
    horizon_days: int = 30,
    output_path: Path = Path("./data/gold/forecasts"),
) -> "pd.DataFrame":
    """
    Train a Prophet model and generate a forecast.
    Returns the forecast DataFrame.
    """
    from prophet import Prophet
    import pandas as pd

    if len(df) < 14:
        log.warning("Only %d data points — need at least 14 for Prophet. Returning empty.", len(df))
        return pd.DataFrame()

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10.0,
    )
    model.fit(df)

    future = model.make_future_dataframe(periods=horizon_days, freq="D")
    forecast = model.predict(future)

    result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    result["yhat"] = result["yhat"].clip(lower=0).round(1)
    result["yhat_lower"] = result["yhat_lower"].clip(lower=0).round(1)
    result["yhat_upper"] = result["yhat_upper"].clip(lower=0).round(1)
    result["model_trained_at"] = pd.Timestamp.utcnow()

    output_path.mkdir(parents=True, exist_ok=True)
    out_file = output_path / "demand_forecast.parquet"
    result.to_parquet(out_file, index=False)
    log.info("Forecast written to %s (%d rows)", out_file, len(result))

    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Train RetailPulse demand forecast")
    parser.add_argument("--duckdb-path", default=os.environ.get("DUCKDB_PATH", "./data/gold/retailpulse.duckdb"))
    parser.add_argument("--horizon-days", type=int, default=30)
    parser.add_argument("--output-path", default="./data/gold/forecasts")
    args = parser.parse_args()

    df = load_daily_demand(args.duckdb_path)
    if df.empty:
        log.warning("No demand data available — skipping forecast.")
        return

    train_forecast(df, horizon_days=args.horizon_days, output_path=Path(args.output_path))


if __name__ == "__main__":
    main()
