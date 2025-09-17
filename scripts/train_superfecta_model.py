"""Train and score a simple Superfecta runner model directly in BigQuery.

The script pulls labeled training rows, fits a calibrated logistic regression
using scikit-learn, scores currently open products, and writes the per-runner
probability of finishing first into BigQuery.

Usage: `python scripts/train_superfecta_model.py`
"""

from __future__ import annotations

import datetime as dt
from typing import Iterable

import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score


PROJECT_ID = "autobet-470818"
PRIMARY_DATASET = "autobet"
MODEL_DATASET = "autobet_model"

TRAIN_SQL = """
SELECT *
FROM `autobet-470818.autobet.vw_superfecta_runner_training_features`
"""

LIVE_SQL = """
SELECT *
FROM `autobet-470818.autobet.vw_superfecta_runner_live_features`
WHERE COALESCE(status, '') NOT IN ('CLOSED', 'SETTLED', 'RESULTED')
"""

FEATURE_COLUMNS: Iterable[str] = (
    "recent_runs",
    "avg_finish",
    "wins_last5",
    "places_last5",
    "days_since_last_run",
    "weight_kg",
    "weight_lbs",
)


def fetch_dataframe(client: bigquery.Client, sql: str) -> pd.DataFrame:
    return client.query(sql).result().to_dataframe(create_bqstorage_client=True)


def prepare_features(df: pd.DataFrame, with_labels: bool) -> tuple[np.ndarray, np.ndarray | None]:
    # Ensure every expected feature column exists, even if entirely null in BQ.
    missing_cols = [col for col in FEATURE_COLUMNS if col not in df.columns]
    for col in missing_cols:
        df[col] = np.nan

    X = df[list(FEATURE_COLUMNS)].apply(pd.to_numeric, errors="coerce")

    # Replace pd.NA with standard NaN before numeric operations.
    X = X.replace({pd.NA: np.nan})

    medians = X.median(numeric_only=True)
    X = X.fillna(medians)
    # If a column was entirely null, median will be NaN; fall back to zero in that case.
    X = X.fillna(0.0)

    y = None
    if with_labels:
        y = (df["finish_pos"] == 1).astype(int).to_numpy()

    return X.to_numpy(dtype=float), y


def ensure_model_dataset(client: bigquery.Client) -> None:
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{MODEL_DATASET}")
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset_ref.location = "EU"
        client.create_dataset(dataset_ref)


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    ensure_model_dataset(client)

    df_train = fetch_dataframe(client, TRAIN_SQL)
    if df_train.empty:
        raise SystemExit("Training view returned no rows; check data freshness.")

    X_train, y_train = prepare_features(df_train, with_labels=True)

    base_model = LogisticRegression(max_iter=200, solver="lbfgs", class_weight="balanced")
    clf = CalibratedClassifierCV(base_model, method="isotonic", cv=5)
    clf.fit(X_train, y_train)

    train_auc = roc_auc_score(y_train, clf.predict_proba(X_train)[:, 1])
    print(f"Train AUC: {train_auc:.3f}")

    df_live = fetch_dataframe(client, LIVE_SQL)
    if df_live.empty:
        print("No live runners to score.")
        return

    X_live, _ = prepare_features(df_live, with_labels=False)
    p_first = clf.predict_proba(X_live)[:, 1]

    timestamp = dt.datetime.utcnow()
    model_version = timestamp.strftime("%Y-%m-%d.%H%M")

    output = df_live.copy()
    output["model_id"] = "superfecta-logreg"
    output["model_version"] = model_version
    output["scored_at"] = timestamp.isoformat()
    output["runner_key"] = output["horse_id"].fillna("")
    output["p_place1"] = p_first
    output[["p_place2", "p_place3", "p_place4"]] = np.nan
    output["features_json"] = output[list(FEATURE_COLUMNS)].to_json(orient="records")

    result_df = output[
        [
            "model_id",
            "model_version",
            "scored_at",
            "product_id",
            "event_id",
            "horse_id",
            "runner_key",
            "p_place1",
            "p_place2",
            "p_place3",
            "p_place4",
            "features_json",
        ]
    ]

    destination = f"{PROJECT_ID}.{MODEL_DATASET}.superfecta_runner_predictions"
    job = client.load_table_from_dataframe(result_df, destination)
    job.result()
    print(f"Wrote {len(result_df)} rows to {destination} (version {model_version}).")


if __name__ == "__main__":
    main()
