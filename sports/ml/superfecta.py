from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from ..bq import BigQuerySink

NUMERIC_FEATURES = [
    "total_net",
    "weather_temp_c",
    "weather_wind_kph",
    "weather_precip_mm",
    "cloth_number",
    "recent_runs",
    "avg_finish",
    "wins_last5",
    "places_last5",
    "days_since_last_run",
    "weight_kg",
]
CATEGORICAL_FEATURES = ["going", "country"]


@dataclass
class TrainingResult:
    metrics: Dict[str, Any]
    params: Dict[str, Any]
    ts_ms: int
    predictions_written: int
    predicted_events: int


def _query_training_frame(
    sink: BigQuerySink, since: Optional[str], max_rows: Optional[int]
) -> pd.DataFrame:
    sink._client_obj()
    where = ["finish_pos IS NOT NULL"]
    params = []
    if since:
        where.append("event_date >= @since_date")
        params.append(
            sink._bq.ScalarQueryParameter("since_date", "DATE", since)  # type: ignore[attr-defined]
        )
    limit_clause = f" LIMIT {int(max_rows)}" if max_rows else ""
    sql = (
        f"SELECT * FROM `{sink.project}.{sink.dataset}.vw_superfecta_runner_training_features`"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY event_date DESC" + limit_clause
    job_config = (
        sink._bq.QueryJobConfig(query_parameters=params, location=sink.location)  # type: ignore[attr-defined]
        if params
        else None
    )
    df = sink.query_dataframe(sql, job_config=job_config)
    return df


def _query_prediction_frame(
    sink: BigQuerySink, horizon_days: int
) -> pd.DataFrame:
    sink._client_obj()
    params = [
        sink._bq.ScalarQueryParameter("horizon_days", "INT64", int(max(horizon_days, 0)))  # type: ignore[attr-defined]
    ]
    sql = f"""
    SELECT *
    FROM `{sink.project}.{sink.dataset}.vw_superfecta_runner_live_features`
    WHERE event_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL @horizon_days DAY)
      AND UPPER(COALESCE(status, '')) IN ('OPEN', 'UNKNOWN')
    """
    job_config = sink._bq.QueryJobConfig(query_parameters=params, location=sink.location)  # type: ignore[attr-defined]
    return sink.query_dataframe(sql, job_config=job_config)


def _prepare_pipeline() -> Pipeline:
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "onehot",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, NUMERIC_FEATURES),
            ("cat", categorical_transformer, CATEGORICAL_FEATURES),
        ]
    )
    model = LogisticRegression(max_iter=1000)
    return Pipeline(steps=[("preprocess", preprocessor), ("model", model)])


def _ensure_binary_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df[df["finish_pos"].notnull()]
    df["is_winner"] = (df["finish_pos"].astype(float) == 1.0).astype(int)
    positives = int(df["is_winner"].sum())
    negatives = int(len(df) - positives)
    if positives == 0 or negatives == 0:
        raise RuntimeError(
            "Training data needs both winners and non-winners to fit a classifier"
        )
    return df


def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            df[col] = None
    df["cloth_number"].replace(0, np.nan, inplace=True)
    return df


def _fit_model(df: pd.DataFrame) -> tuple[Pipeline, Dict[str, Any]]:
    df = _ensure_binary_labels(df)
    df = _prepare_features(df)
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    X = df[features]
    y = df["is_winner"].to_numpy()
    stratify = y if len(np.unique(y)) > 1 else None
    use_holdout = len(df) >= 80 and stratify is not None
    if use_holdout:
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        scope = "holdout"
    else:
        X_train, X_val, y_train, y_val = X, X, y, y
        scope = "training"
    pipeline = _prepare_pipeline()
    pipeline.fit(X_train, y_train)
    proba = pipeline.predict_proba(X_val)[:, 1]
    metrics: Dict[str, Any] = {
        "evaluation_scope": scope,
        "rows": int(len(df)),
        "positives": int(y.sum()),
        "negatives": int(len(y) - int(y.sum())),
    }
    try:
        metrics["log_loss"] = float(log_loss(y_val, proba, labels=[0, 1]))
    except ValueError:
        metrics["log_loss"] = None
    try:
        metrics["brier_score"] = float(brier_score_loss(y_val, proba))
    except ValueError:
        metrics["brier_score"] = None
    try:
        metrics["roc_auc"] = float(roc_auc_score(y_val, proba))
    except ValueError:
        metrics["roc_auc"] = None
    return pipeline, metrics


def _build_prediction_payloads(
    df: pd.DataFrame, pipeline: Pipeline, model_id: str, ts_ms: int
) -> tuple[pd.DataFrame, list[Dict[str, Any]]]:
    if df.empty:
        return pd.DataFrame(), []
    df = _prepare_features(df)
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    proba = pipeline.predict_proba(df[features])[:, 1]
    df = df.copy()
    df["raw_proba"] = np.clip(proba, 1e-9, None)
    df["group_sum"] = df.groupby("event_id")["raw_proba"].transform("sum")
    df["group_count"] = df.groupby("event_id")["raw_proba"].transform("count")
    df["proba"] = df.apply(
        lambda row: row["raw_proba"] / row["group_sum"]
        if row["group_sum"] > 0
        else 1.0 / float(row["group_count"]),
        axis=1)
    df["rank"] = (
        df.groupby("event_id")["proba"].rank(method="first", ascending=False).astype(int)
    )
    df["model_id"] = model_id
    df["ts_ms"] = int(ts_ms)
    df["market"] = "SUPERFECTA"
    df["runner_key"] = df["horse_id"].fillna("")
    scored_at = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    df["model_version"] = scored_at.strftime("%Y-%m-%d.%H%M")
    df["scored_at"] = scored_at.isoformat()
    df["p_place1"] = df["proba"].astype(float)
    df["p_place2"] = None
    df["p_place3"] = None
    df["p_place4"] = None
    feature_cols = NUMERIC_FEATURES + CATEGORICAL_FEATURES

    def _encode_features(row: pd.Series) -> str:
        payload: Dict[str, Any] = {}
        for col in feature_cols:
            val = row.get(col)
            if pd.isna(val):
                payload[col] = None
            elif col in NUMERIC_FEATURES:
                payload[col] = float(val)
            else:
                payload[col] = str(val)
        return json.dumps(payload, sort_keys=True)

    df["features_json"] = df.apply(_encode_features, axis=1)

    predictions_rows = [
        {
            "model_id": rec.model_id,
            "ts_ms": int(rec.ts_ms),
            "event_id": rec.event_id,
            "horse_id": rec.horse_id,
            "market": "SUPERFECTA",
            "proba": float(rec.proba),
            "rank": int(rec.rank),
        }
        for rec in df.itertuples(index=False)
    ]

    runner_df = df[
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
    ].copy()
    runner_df = runner_df.where(pd.notnull(runner_df), None)
    return runner_df, predictions_rows


def train_superfecta_model(
    sink: BigQuerySink,
    *,
    model_id: str,
    since: Optional[str] = None,
    max_rows: Optional[int] = None,
    predict_horizon_days: int = 2,
    dry_run: bool = False) -> TrainingResult:
    training_df = _query_training_frame(sink, since=since, max_rows=max_rows)
    if training_df.empty:
        raise RuntimeError("No training data found in BigQuery")
    pipeline, metrics = _fit_model(training_df)
    horizon = max(predict_horizon_days, 0)
    predictions_df = _query_prediction_frame(sink, horizon)
    ts_ms = int(time.time() * 1000)
    runner_predictions_df, prediction_rows = _build_prediction_payloads(
        predictions_df, pipeline, model_id, ts_ms
    )
    predicted_events = (
        int(predictions_df["event_id"].nunique()) if not predictions_df.empty else 0
    )

    params = {
        "since": since,
        "max_rows": max_rows,
        "predict_horizon_days": horizon,
        "features": {
            "numeric": NUMERIC_FEATURES,
            "categorical": CATEGORICAL_FEATURES,
        },
    }

    model_row = {
        "model_id": model_id,
        "created_ts": ts_ms,
        "market": "SUPERFECTA",
        "algo": "logistic_regression",
        "params_json": json.dumps(params, sort_keys=True),
        "metrics_json": json.dumps(metrics, sort_keys=True),
        "path": None,
    }
    sink.upsert_models([model_row])

    written = 0
    if not dry_run and not runner_predictions_df.empty:
        model_dataset = os.getenv("BQ_MODEL_DATASET") or os.getenv(
            "ML_BQ_MODEL_DATASET", f"{sink.dataset}_model"
        )
        sink.load_superfecta_predictions(
            runner_predictions_df.to_dict("records"), model_dataset=model_dataset
        )
        sink.upsert_predictions(prediction_rows)
        sink.set_active_model(model_id, ts_ms)
        written = len(prediction_rows)
    elif not dry_run:
        # Ensure a placeholder row keeps the view aligned even when no races are scheduled.
        model_dataset = os.getenv("BQ_MODEL_DATASET") or os.getenv(
            "ML_BQ_MODEL_DATASET", f"{sink.dataset}_model"
        )
        sink.load_superfecta_predictions(
            [
                {
                    "model_id": model_id,
                    "model_version": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime(
                        "%Y-%m-%d.%H%M"
                    ),
                    "scored_at": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                    "product_id": None,
                    "event_id": None,
                    "horse_id": None,
                    "runner_key": "",
                    "p_place1": None,
                    "p_place2": None,
                    "p_place3": None,
                    "p_place4": None,
                    "features_json": "{}",
                }
            ],
            model_dataset=model_dataset)

    return TrainingResult(
        metrics=metrics,
        params=params,
        ts_ms=ts_ms,
        predictions_written=written,
        predicted_events=predicted_events)
