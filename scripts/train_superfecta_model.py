"""Train and score the Superfecta runner model directly in BigQuery.

This script pulls labeled training rows, fits a calibrated logistic regression
using scikit-learn, scores upcoming runners, and writes per-runner
probabilities into the `autobet_model.superfecta_runner_predictions` table.

Examples
--------
# Train on all history and score every open race
python scripts/train_superfecta_model.py

# Focus on today's GB/IE races that start within the next 4 hours
python scripts/train_superfecta_model.py --countries GB IE --start-date $(date +%F) --end-date $(date +%F) \
    --window-start-minutes -30 --window-end-minutes 240

The script prefers Application Default Credentials. Ensure the environment is
authenticated (`gcloud auth application-default login`) or provide a service
account via GOOGLE_APPLICATION_CREDENTIALS.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from typing import Iterable, Mapping, MutableMapping, Sequence

import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

DEFAULT_PROJECT = "autobet-470818"
DEFAULT_DATASET = "autobet"
DEFAULT_MODEL_DATASET = "autobet_model"

FEATURE_COLUMNS: Iterable[str] = (
    "recent_runs",
    "avg_finish",
    "wins_last5",
    "places_last5",
    "days_since_last_run",
    "weight_kg",
    "weight_lbs",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and score the Superfecta model in BigQuery.")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="BigQuery project ID")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Primary BigQuery dataset")
    parser.add_argument(
        "--model-dataset",
        default=DEFAULT_MODEL_DATASET,
        help="Dataset where model predictions are written",
    )
    parser.add_argument("--model-id", default="superfecta-logreg", help="Identifier stored with predictions")
    parser.add_argument(
        "--training-since",
        help="Optional YYYY-MM-DD lower bound for training data",
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        help="Filter live scoring to these two-letter country codes (e.g. GB IE)",
    )
    parser.add_argument("--start-date", help="Inclusive event_date lower bound (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Inclusive event_date upper bound (YYYY-MM-DD)")
    parser.add_argument(
        "--window-start-minutes",
        type=int,
        default=-60,
        help="Include races starting at least this many minutes from now",
    )
    parser.add_argument(
        "--window-end-minutes",
        type=int,
        default=360,
        help="Include races starting at most this many minutes from now",
    )
    parser.add_argument(
        "--limit-live",
        type=int,
        help="Optional LIMIT to cap the number of live rows scored",
    )
    parser.add_argument("--dry-run", action="store_true", help="Train & score but skip writing predictions")
    return parser.parse_args()


def _resolve_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def _array_type(sample: object) -> str:
    if isinstance(sample, bool):
        return "BOOL"
    if isinstance(sample, int):
        return "INT64"
    if isinstance(sample, float):
        return "FLOAT64"
    if isinstance(sample, dt.datetime):
        return "TIMESTAMP"
    if isinstance(sample, dt.date):
        return "DATE"
    return "STRING"


def make_query_config(params: Mapping[str, object] | None) -> bigquery.QueryJobConfig | None:
    if not params:
        return None
    query_parameters: list[bigquery.QueryParameter] = []
    for key, value in params.items():
        name = str(key)
        if isinstance(value, (list, tuple, set)):
            seq = list(value)
            param_type = _array_type(seq[0]) if seq else "STRING"
            query_parameters.append(bigquery.ArrayQueryParameter(name, param_type, seq))
        elif isinstance(value, bool):
            query_parameters.append(bigquery.ScalarQueryParameter(name, "BOOL", value))
        elif isinstance(value, int):
            query_parameters.append(bigquery.ScalarQueryParameter(name, "INT64", value))
        elif isinstance(value, float):
            query_parameters.append(bigquery.ScalarQueryParameter(name, "FLOAT64", value))
        elif isinstance(value, dt.datetime):
            query_parameters.append(bigquery.ScalarQueryParameter(name, "TIMESTAMP", value))
        elif isinstance(value, dt.date):
            query_parameters.append(bigquery.ScalarQueryParameter(name, "DATE", value))
        else:
            query_parameters.append(bigquery.ScalarQueryParameter(name, "STRING", str(value)))
    return bigquery.QueryJobConfig(query_parameters=query_parameters)


def fetch_dataframe(
    client: bigquery.Client,
    sql: str,
    params: Mapping[str, object] | None = None,
) -> pd.DataFrame:
    job_config = make_query_config(params)
    result = client.query(sql, job_config=job_config).result()
    try:
        return result.to_dataframe(create_bqstorage_client=True)
    except Exception:
        return result.to_dataframe(create_bqstorage_client=False)


def build_training_query(args: argparse.Namespace) -> tuple[str, MutableMapping[str, object]]:
    sql = f"SELECT * FROM `{args.project}.{args.dataset}.vw_superfecta_runner_training_features`"
    params: MutableMapping[str, object] = {}
    clauses: list[str] = []
    if args.training_since:
        clauses.append("event_date >= @train_since")
        params["train_since"] = _resolve_date(args.training_since)
    if args.countries:
        clauses.append("UPPER(country) IN UNNEST(@train_countries)")
        params["train_countries"] = [c.upper() for c in args.countries]
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY event_date DESC"
    return sql, params


def build_live_query(args: argparse.Namespace) -> tuple[str, MutableMapping[str, object]]:
    sql = f"""
    SELECT *
    FROM `{args.project}.{args.dataset}.vw_superfecta_runner_live_features`
    WHERE COALESCE(status, '') NOT IN ('CLOSED', 'SETTLED', 'RESULTED')
    """
    params: MutableMapping[str, object] = {}
    if args.countries:
        sql += " AND UPPER(country) IN UNNEST(@countries)"
        params["countries"] = [c.upper() for c in args.countries]
    start_date = _resolve_date(args.start_date)
    end_date = _resolve_date(args.end_date)
    if start_date or end_date:
        if not start_date:
            start_date = end_date
        if not end_date:
            end_date = start_date
        sql += " AND event_date BETWEEN @start_date AND @end_date"
        params["start_date"] = start_date
        params["end_date"] = end_date
    if args.window_start_minutes is not None or args.window_end_minutes is not None:
        min_minutes = args.window_start_minutes if args.window_start_minutes is not None else -60
        max_minutes = args.window_end_minutes if args.window_end_minutes is not None else 360
        sql += (
            " AND TIMESTAMP_DIFF(TIMESTAMP(start_iso), CURRENT_TIMESTAMP(), MINUTE)"
            " BETWEEN @min_minutes AND @max_minutes"
        )
        params["min_minutes"] = min_minutes
        params["max_minutes"] = max_minutes
    if args.limit_live:
        sql += " ORDER BY start_iso ASC LIMIT @live_limit"
        params["live_limit"] = int(args.limit_live)
    else:
        sql += " ORDER BY start_iso ASC"
    return sql, params


def prepare_features(df: pd.DataFrame, with_labels: bool) -> tuple[np.ndarray, np.ndarray | None]:
    missing_cols = [col for col in FEATURE_COLUMNS if col not in df.columns]
    for col in missing_cols:
        df[col] = np.nan

    X = df[list(FEATURE_COLUMNS)].apply(pd.to_numeric, errors="coerce")
    X = X.replace({pd.NA: np.nan})
    medians = X.median(numeric_only=True)
    X = X.fillna(medians)
    X = X.fillna(0.0)

    y = None
    if with_labels:
        y = (df["finish_pos"] == 1).astype(int).to_numpy()

    return X.to_numpy(dtype=float), y


def ensure_model_dataset(client: bigquery.Client, project: str, dataset: str) -> None:
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset_ref.location = "EU"
        client.create_dataset(dataset_ref)


def encode_features(row: pd.Series) -> str:
    payload = {}
    for key, value in row.items():
        if pd.isna(value):
            payload[key] = None
        else:
            payload[key] = float(value)
    return json.dumps(payload, sort_keys=True)


def main() -> None:
    args = parse_args()
    client = bigquery.Client(project=args.project)
    ensure_model_dataset(client, args.project, args.model_dataset)

    train_sql, train_params = build_training_query(args)
    df_train = fetch_dataframe(client, train_sql, train_params)
    if df_train.empty:
        raise SystemExit("Training view returned no rows; ensure historic data is loaded.")

    X_train, y_train = prepare_features(df_train, with_labels=True)
    base_model = LogisticRegression(max_iter=200, solver="lbfgs", class_weight="balanced")
    clf = CalibratedClassifierCV(base_model, method="isotonic", cv=5)
    clf.fit(X_train, y_train)
    train_auc = roc_auc_score(y_train, clf.predict_proba(X_train)[:, 1])
    print(f"Train AUC: {train_auc:.3f} on {len(df_train):,} rows")

    live_sql, live_params = build_live_query(args)
    df_live = fetch_dataframe(client, live_sql, live_params)
    if df_live.empty:
        print("No live runners matched the filters; nothing to score.")
        return

    X_live, _ = prepare_features(df_live, with_labels=False)
    p_first = clf.predict_proba(X_live)[:, 1]

    timestamp = dt.datetime.utcnow()
    model_version = timestamp.strftime("%Y-%m-%d.%H%M")

    scored = df_live.copy()
    scored["model_id"] = args.model_id
    scored["model_version"] = model_version
    scored["scored_at"] = timestamp.isoformat()
    scored["runner_key"] = scored["horse_id"].fillna("")
    scored["p_place1"] = p_first
    scored[["p_place2", "p_place3", "p_place4"]] = np.nan
    scored["features_json"] = scored[list(FEATURE_COLUMNS)].apply(encode_features, axis=1)

    result_df = scored[
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
    result_df = result_df[result_df["horse_id"].notna()].reset_index(drop=True)
    if result_df.empty:
        print("Scoring produced no rows with horse_id; nothing to write.")
        return

    destination = f"{args.project}.{args.model_dataset}.superfecta_runner_predictions"
    if args.dry_run:
        print(f"Dry run: {len(result_df)} rows prepared for {destination} (version {model_version}).")
        return

    load_job = client.load_table_from_dataframe(result_df, destination)
    load_job.result()
    print(f"Wrote {len(result_df)} rows to {destination} (model {args.model_id}, version {model_version}).")


if __name__ == "__main__":
    main()
