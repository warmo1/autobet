from __future__ import annotations
import os
import json
import time
from flask import Flask, request, jsonify
from google.cloud import bigquery
from typing import Any

from .gcp import publish_pubsub_message
from .config import cfg
from .bq import get_bq_sink
import uuid
from .db import get_db

from .superfecta_automation import (
    execute_ready_recommendations,
    run_live_monitor,
    run_morning_scan,
)

app = Flask(__name__)

@app.get("/")
def health() -> tuple[str, int]:
    return ("ok", 200)


def _log_job_run(task: str, status: str, *, payload: dict[str, Any] | None = None,
                 metrics: dict[str, Any] | None = None, error: str | None = None) -> None:
    """Persist orchestrator job execution metadata for the status dashboard."""
    sink = get_bq_sink()
    if not sink:
        return
    now_ms = int(time.time() * 1000)
    try:
        sink.upsert_ingest_job_runs([
            {
                "job_id": f"orchestrator-{uuid.uuid4().hex}",
                "component": "orchestrator",
                "task": task,
                "status": status,
                "started_ts": now_ms,
                "ended_ts": now_ms,
                "duration_ms": 0,
                "payload_json": json.dumps(payload) if payload else None,
                "error": error,
                "metrics_json": json.dumps(metrics) if metrics else None,
            }
        ])
    except Exception as exc:
        print(f"Failed to log job run for {task}: {exc}")

def scan_and_publish_pre_race_jobs():
    """
    Scans for races starting soon and publishes specific fetch jobs for them.
    This is intended to be called by a Cloud Scheduler job every few minutes.
    """
    db = get_db()
    # Scan for any open products starting in the next 30 minutes to trigger
    # more frequent pool and odds updates.
    try:
        upcoming_df = db.query("""
            SELECT p.product_id, p.event_id
            FROM `autobet-470818.autobet.vw_products_latest_totals` p
            LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id)
            WHERE p.status = 'OPEN'
              AND TIMESTAMP(p.start_iso) BETWEEN CURRENT_TIMESTAMP() AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
              AND COALESCE(e.country, '') NOT IN ('FR', 'ZA')
            GROUP BY p.product_id, p.event_id
        """).to_dataframe()

    except Exception as e:
        err = str(e)
        print(f"Error querying for upcoming races: {err}")
        _log_job_run("pre-race-scanner", "ERROR", error=err)
        return "Query for upcoming races failed", 500

    if upcoming_df.empty:
        # Log a no-op run
        _log_job_run("pre-race-scanner", "OK", metrics={})
        return "No upcoming races to process", 200

    print(f"Found {len(upcoming_df)} upcoming products to refresh.")
    
    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id:
        return "GCP_PROJECT not set", 500

    published_count = 0
    product_ids_to_refresh = upcoming_df["product_id"].unique().tolist()
    event_ids_to_refresh = upcoming_df["event_id"].unique().tolist()

    # Publish one job for all products
    if product_ids_to_refresh:
        products_job = {"task": "ingest_multiple_products", "product_ids": product_ids_to_refresh}
        publish_pubsub_message(project_id, topic_id, products_job)
        published_count += 1

    # Publish one job for all events needing odds refresh
    if event_ids_to_refresh:
        odds_job = {"task": "ingest_multiple_probable_odds", "event_ids": event_ids_to_refresh}
        publish_pubsub_message(project_id, topic_id, odds_job)
        published_count += 1

    # Log run with metrics
    _log_job_run(
        "pre-race-scanner",
        "OK",
        metrics={"published_count": int(published_count), "products": int(len(upcoming_df))},
    )
    return f"Published {published_count} pre-race jobs for {len(upcoming_df)} products.", 200


def scan_and_publish_results_jobs():
    """
    Scans for recently finished events that are missing results and triggers
    an ingest job for them.
    """
    db = get_db()
    try:
        # Find products that are CLOSED, whose start time was in the last 3 hours,
        # and for which no results exist in hr_horse_runs.
        results_needed_df = db.query("""
            SELECT p.event_id, p.product_id
            FROM `autobet-470818.autobet.tote_products` p
            LEFT JOIN `autobet-470818.autobet.hr_horse_runs` r ON p.event_id = r.event_id
            WHERE p.status = 'CLOSED'
              AND TIMESTAMP(p.start_iso) BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR) AND CURRENT_TIMESTAMP()
              AND r.event_id IS NULL
            GROUP BY 1, 2
            LIMIT 100
        """).to_dataframe()
    except Exception as e:
        err = str(e)
        print(f"Error querying for events needing results: {err}")
        _log_job_run("post-race-results-scanner", "ERROR", error=err)
        return "Query for events needing results failed", 500
    
    if results_needed_df.empty:
        _log_job_run("post-race-results-scanner", "OK", metrics={})
        return "No events found needing results.", 200

    print(f"Found {len(results_needed_df)} events needing results to be ingested.")
    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id: return "GCP_PROJECT not set", 500

    unique_event_ids = results_needed_df['event_id'].unique()
    try:
        for event_id in unique_event_ids:
            job = {"task": "ingest_event_results", "event_id": event_id}
            publish_pubsub_message(project_id, topic_id, job)
    except Exception as e:
        err = str(e)
        print(f"Failed to publish result ingest jobs: {err}")
        _log_job_run("post-race-results-scanner", "ERROR", metrics={"published_count": 0}, error=err)
        return "Failed to publish result ingest jobs", 500

    _log_job_run(
        "post-race-results-scanner",
        "OK",
        metrics={"published_count": int(len(unique_event_ids))},
    )
    return f"Published {len(unique_event_ids)} result ingestion jobs.", 200


def scan_and_publish_probable_sweep():
    """Publish probable-odds jobs for a broader window of events.

    Strategy:
    - Include events with an OPEN WIN product starting within next 6 hours.
    - Also include events that started within the last 15 minutes (catch‑up).
    - Upper bound to 500 events per run to avoid overload.
    """
    db = get_db()
    try:
        df = db.query(
            """
            SELECT DISTINCT p.event_id
            FROM `autobet-470818.autobet.tote_products` p
            LEFT JOIN `autobet-470818.autobet.tote_events` e USING(event_id)
            WHERE UPPER(p.bet_type)='WIN' AND p.status='OPEN'
              AND COALESCE(e.country, '') NOT IN ('FR', 'ZA')
              AND TIMESTAMP(p.start_iso) BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
                                           AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
            LIMIT 500
            """
        ).to_dataframe()
    except Exception as e:
        err = str(e)
        print(f"Error querying probable sweep: {err}")
        _log_job_run("probable-odds-sweep", "ERROR", error=err)
        return "Query for probable sweep failed", 500

    if df.empty:
        _log_job_run("probable-odds-sweep", "OK", metrics={})
        return "No events for probable sweep.", 200

    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id:
        return "GCP_PROJECT not set", 500

    n = 0
    try:
        for _, row in df.iterrows():
            eid = row["event_id"]
            publish_pubsub_message(project_id, topic_id, {"task": "ingest_probable_odds", "event_id": eid})
            n += 1
    except Exception as e:
        err = str(e)
        print(f"Failed to publish probable odds jobs: {err}")
        _log_job_run("probable-odds-sweep", "ERROR", metrics={"published_count": int(n)}, error=err)
        return "Failed to publish probable odds jobs", 500

    _log_job_run("probable-odds-sweep", "OK", metrics={"published_count": int(n)})
    return f"Published {n} probable odds jobs.", 200


def publish_probable_bulk_jobs(window_hours: int = 12):
    """Publish a bulk probable-odds refresh covering a longer horizon."""

    try:
        window_hours = max(1, int(window_hours))
    except (TypeError, ValueError):
        window_hours = 12

    db = get_db()
    try:
        df = db.query(
            f"""
            SELECT DISTINCT p.event_id
            FROM `{cfg.bq_project}.{cfg.bq_dataset}.vw_products_latest_totals` p
            LEFT JOIN `{cfg.bq_project}.{cfg.bq_dataset}.tote_events` e USING(event_id)
            WHERE p.status = 'OPEN'
              AND UPPER(p.bet_type) = 'WIN'
              AND COALESCE(e.country, '') NOT IN ('FR', 'ZA')
              AND TIMESTAMP(p.start_iso) BETWEEN CURRENT_TIMESTAMP()
                                       AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {int(window_hours)} HOUR)
            LIMIT 500
            """
        ).to_dataframe()
    except Exception as e:
        err = str(e)
        print(f"Error querying bulk probable window: {err}")
        _log_job_run("probable-odds-bulk", "ERROR", metrics={"published_count": 0}, error=err)
        return "Query for probable bulk window failed", 500

    if df.empty:
        _log_job_run("probable-odds-bulk", "OK", metrics={"published_count": 0})
        return "No events found for probable bulk refresh.", 200

    event_ids = df["event_id"].dropna().unique().tolist()
    if not event_ids:
        _log_job_run("probable-odds-bulk", "OK", metrics={"published_count": 0})
        return "No valid event IDs for probable bulk refresh.", 200

    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id:
        _log_job_run("probable-odds-bulk", "ERROR", metrics={"published_count": 0}, error="GCP_PROJECT not set")
        return "GCP_PROJECT not set", 500

    job_payload = {"task": "ingest_multiple_probable_odds", "event_ids": event_ids}
    try:
        publish_pubsub_message(project_id, topic_id, job_payload)
    except Exception as e:
        err = str(e)
        print(f"Failed to publish probable bulk job: {err}")
        _log_job_run("probable-odds-bulk", "ERROR", metrics={"published_count": 0}, error=err)
        return "Failed to publish probable bulk job", 500

    _log_job_run(
        "probable-odds-bulk",
        "OK",
        payload={"window_hours": window_hours, "event_count": len(event_ids)},
        metrics={"published_count": int(len(event_ids))},
    )
    return f"Published bulk probable odds job for {len(event_ids)} events.", 200


def publish_daily_event_ingest():
    """Publishes a job to ingest all of today's events."""
    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id:
        return "GCP_PROJECT not set", 500

    job = {"task": "ingest_events_for_day", "date": "today"}
    try:
        publish_pubsub_message(project_id, topic_id, job)
    except Exception as e:
        err = str(e)
        print(f"Failed to publish daily event ingest job: {err}")
        _log_job_run("daily-event-ingest-trigger", "ERROR", payload=job, error=err)
        return "Failed to publish daily event ingest job", 500

    # Log run
    _log_job_run(
        "daily-event-ingest-trigger",
        "OK",
        payload=job,
        metrics={"published_count": 1},
    )

    return "Published daily event ingest job.", 200


@app.post("/")
def handle_scheduler() -> tuple[str, int]:
    """Entrypoint for Cloud Scheduler hitting this service via HTTP."""
    try:
        # Scheduler can send a simple JSON body
        payload = request.get_json()
        if not payload:
            return ("No payload received", 400)
        job_name = payload.get("job_name")
    except (ValueError, AttributeError):
        return ("Bad Request or invalid payload", 400)

    try:
        if job_name == "pre-race-scanner":
            return scan_and_publish_pre_race_jobs()
        elif job_name == "post-race-results-scanner":
            return scan_and_publish_results_jobs()
        elif job_name == "probable-odds-sweep":
            return scan_and_publish_probable_sweep()
        elif job_name == "probable-odds-bulk":
            window = payload.get("window_hours", 12)
            try:
                window_int = int(window)
            except (TypeError, ValueError):
                window_int = 12
            if window_int <= 0:
                window_int = 12
            return publish_probable_bulk_jobs(window_int)
        elif job_name == "daily-event-ingest":
            return publish_daily_event_ingest()
        else:
            return (f"Unknown job_name: {job_name}", 400)
    except Exception as e:
        print(f"Orchestrator error for job '{job_name}': {e}")
        # Return 200 to prevent Cloud Scheduler from retrying a failed job logic.
        # The error is logged for debugging.
        return (f"Internal error processing job: {job_name}", 200)


@app.post("/jobs/superfecta/morning")
def superfecta_morning_job():
    """Trigger the morning superfecta triage and persist recommendations."""
    run_date = (request.args.get("date") or "").strip() or None
    try:
        summary = run_morning_scan(target_date=run_date)
        _log_job_run("superfecta-morning", "OK", metrics=summary)
        return jsonify(summary), 200
    except Exception as exc:
        _log_job_run("superfecta-morning", "ERROR", error=str(exc))
        return (str(exc), 500)


@app.post("/jobs/superfecta/live")
def superfecta_live_job():
    """Re-evaluate live EV for monitored superfecta recommendations."""
    run_date = (request.args.get("date") or "").strip() or None
    try:
        summary = run_live_monitor(run_date=run_date)
        _log_job_run("superfecta-live", "OK", metrics=summary)
        return jsonify(summary), 200
    except Exception as exc:
        _log_job_run("superfecta-live", "ERROR", error=str(exc))
        return (str(exc), 500)


@app.post("/jobs/superfecta/execute")
def superfecta_execute_job():
    """Place bets for ready superfecta recommendations."""
    auto_param = request.args.get("auto_place")
    auto_place = None
    if auto_param is not None:
        auto_place = auto_param.lower() in ("1", "true", "yes", "on")
    try:
        summary = execute_ready_recommendations(auto_place=auto_place)
        _log_job_run("superfecta-execute", "OK", metrics=summary)
        return jsonify(summary), 200
    except Exception as exc:
        _log_job_run("superfecta-execute", "ERROR", error=str(exc))
        return (str(exc), 500)
