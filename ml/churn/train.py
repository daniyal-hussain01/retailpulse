"""
ml/churn/train.py

Churn propensity model training pipeline.
Reads features from Gold DuckDB, trains an XGBoost classifier,
logs everything to MLflow, and registers the best model.

Run:
    python -m ml.churn.train --duckdb-path ./data/gold/retailpulse.duckdb

Requirements (installed separately from core deps):
    xgboost>=2.0.0
    scikit-learn>=1.5.0
    mlflow>=2.13.0
    duckdb>=0.10.0
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ── Feature engineering ────────────────────────────────────────────────────────

def build_feature_table(duckdb_path: str) -> "pd.DataFrame":
    """
    Build the training feature table from Gold DuckDB.
    Returns a DataFrame with columns: user_id_hash, <features>, is_churned (label).

    Churn definition: no order in the last 60 days.
    """
    import duckdb
    import pandas as pd

    conn = duckdb.connect(duckdb_path, read_only=True)

    df = conn.execute("""
        with order_stats as (
            select
                user_id_hash,
                count(*)                                         as total_orders,
                max(created_at)                                  as last_order_date,
                min(created_at)                                  as first_order_date,
                avg(total_cents)                                 as avg_order_value_cents,
                sum(total_cents)                                 as lifetime_value_cents,
                sum(case when is_refunded_or_cancelled then 1 else 0 end)
                                                                 as refund_count,
                count(distinct order_month)                      as active_months,
                max(order_date) filter (where is_delivered)      as last_delivered_date
            from fact_orders
            where user_id_hash != 'ERASED'
            group by user_id_hash
        )
        select
            user_id_hash,
            total_orders,
            avg_order_value_cents,
            lifetime_value_cents,
            refund_count,
            active_months,
            date_diff('day', first_order_date, last_order_date) as customer_age_days,
            date_diff('day', last_order_date, current_date)     as days_since_last_order,
            safe_divide(refund_count, total_orders)             as refund_rate,
            safe_divide(lifetime_value_cents, active_months)    as avg_monthly_spend,
            -- Label: churned if no order in last 60 days
            case when date_diff('day', last_order_date, current_date) > 60
                 then 1 else 0 end                              as is_churned
        from order_stats
        where total_orders >= 1
    """).df()

    conn.close()
    log.info("Feature table: %d rows, %d columns", len(df), len(df.columns))
    return df


FEATURE_COLUMNS = [
    "total_orders",
    "avg_order_value_cents",
    "lifetime_value_cents",
    "refund_count",
    "active_months",
    "customer_age_days",
    "days_since_last_order",
    "refund_rate",
    "avg_monthly_spend",
]


def train_model(
    df: "pd.DataFrame",
    experiment_name: str = "retailpulse-churn",
    n_cv_folds: int = 5,
) -> str:
    """
    Train XGBoost churn classifier with cross-validation.
    Logs metrics, params, and model artifact to MLflow.
    Returns the MLflow run_id.
    """
    import mlflow
    import mlflow.xgboost
    import numpy as np
    from sklearn.model_selection import StratifiedKFold, cross_validate
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    from sklearn.metrics import roc_auc_score
    import xgboost as xgb

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "mlruns"))
    mlflow.set_experiment(experiment_name)

    X = df[FEATURE_COLUMNS].fillna(0).values
    y = df["is_churned"].values

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", xgb.XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            scale_pos_weight=float((y == 0).sum()) / max(1, (y == 1).sum()),
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42,
        )),
    ])

    cv = StratifiedKFold(n_splits=n_cv_folds, shuffle=True, random_state=42)
    cv_results = cross_validate(
        model, X, y, cv=cv,
        scoring=["roc_auc", "f1", "precision", "recall"],
        return_train_score=False,
    )

    with mlflow.start_run() as run:
        # Log parameters
        mlflow.log_params({
            "model_type": "xgboost",
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.05,
            "n_cv_folds": n_cv_folds,
            "n_training_samples": len(df),
            "churn_rate": float(y.mean()),
            "feature_columns": ",".join(FEATURE_COLUMNS),
            "churn_definition_days": 60,
        })

        # Log CV metrics (mean ± std)
        for metric in ["roc_auc", "f1", "precision", "recall"]:
            vals = cv_results[f"test_{metric}"]
            mlflow.log_metrics({
                f"cv_{metric}_mean": float(vals.mean()),
                f"cv_{metric}_std":  float(vals.std()),
            })

        log.info(
            "CV results | AUC=%.3f±%.3f | F1=%.3f±%.3f",
            cv_results["test_roc_auc"].mean(),
            cv_results["test_roc_auc"].std(),
            cv_results["test_f1"].mean(),
            cv_results["test_f1"].std(),
        )

        # Retrain on full dataset for artifact
        model.fit(X, y)

        # Log feature importances
        importances = model.named_steps["clf"].feature_importances_
        for fname, imp in zip(FEATURE_COLUMNS, importances):
            mlflow.log_metric(f"feature_importance_{fname}", float(imp))

        # Log model
        mlflow.xgboost.log_model(
            model.named_steps["clf"],
            artifact_path="model",
            registered_model_name="retailpulse-churn-v1",
            input_example=dict(zip(FEATURE_COLUMNS, X[0].tolist())),
        )

        run_id = run.info.run_id
        log.info("MLflow run complete | run_id=%s", run_id)

    return run_id


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Train RetailPulse churn model")
    parser.add_argument(
        "--duckdb-path",
        default=os.environ.get("DUCKDB_PATH", "./data/gold/retailpulse.duckdb"),
    )
    parser.add_argument("--experiment", default="retailpulse-churn")
    parser.add_argument("--cv-folds", type=int, default=5)
    args = parser.parse_args()

    log.info("Loading features from %s", args.duckdb_path)
    df = build_feature_table(args.duckdb_path)

    if len(df) < 10:
        log.warning("Only %d training samples — model will not generalise. Skipping.", len(df))
        return

    run_id = train_model(df, experiment_name=args.experiment, n_cv_folds=args.cv_folds)
    log.info("Training complete. Run ID: %s", run_id)
    log.info("View in MLflow UI: mlflow ui --port 5000")


if __name__ == "__main__":
    main()
