from __future__ import annotations
import os
import json
import time
from flask import Flask, request
from google.cloud import bigquery

from .gcp import parse_pubsub_envelope, publish_pubsub_message
from .config import cfg
from .bq import get_bq_sink
import uuid
from .db import get_db

app = Flask(__name__)

@app.get("/")
def health() -> tuple[str, int]:
    return ("ok", 200)

def scan_and_publish_pre_race_jobs():
    """
    Scans for races starting soon and publishes specific fetch jobs for them.
    This is intended to be called by a Cloud Scheduler job every few minutes.
    """
    db = get_db()
    # This view `vw_gb_open_superfecta_next60` is defined in bq.py and finds
    # open products for GB races starting in the next 60 minutes.
    try:
        upcoming_df = db.query(
            "SELECT product_id, event_id FROM vw_gb_open_superfecta_next60"
        ).to_dataframe()
    except Exception as e:
        print(f"Error querying for upcoming races: {e}")
        return "Query for upcoming races failed", 500

    if upcoming_df.empty:
        # Log a no-op run
        try:
            sink = get_bq_sink()
            now_ms = int(time.time() * 1000)
            sink.upsert_ingest_job_runs([
                {
                    "job_id": f"orchestrator-{uuid.uuid4().hex}",
                    "component": "orchestrator",
                    "task": "pre-race-scanner",
                    "status": "OK",
                    "started_ts": now_ms,
                    "ended_ts": now_ms,
                    "duration_ms": 0,
                    "payload_json": None,
                    "error": None,
                    "metrics_json": "{}",
                }
            ])
        except Exception:
            pass
        return "No upcoming races to process", 200

    print(f"Found {len(upcoming_df)} upcoming products to refresh.")
    
    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id:
        return "GCP_PROJECT not set", 500

    published_count = 0
    for _, row in upcoming_df.iterrows():
        product_id = row["product_id"]
        event_id = row["event_id"]

        # Job to refresh pool totals (via products endpoint)
        pool_job = {"task": "ingest_single_product", "product_id": product_id}
        publish_pubsub_message(project_id, topic_id, pool_job)
        published_count += 1

        # Job to refresh probable odds (for the WIN market of the same event)
        probable_odds_job = {"task": "ingest_probable_odds", "event_id": event_id}
        publish_pubsub_message(project_id, topic_id, probable_odds_job)
        published_count += 1

    # Log run with metrics
    try:
        sink = get_bq_sink()
        now_ms = int(time.time() * 1000)
        sink.upsert_ingest_job_runs([
            {
                "job_id": f"orchestrator-{uuid.uuid4().hex}",
                "component": "orchestrator",
                "task": "pre-race-scanner",
                "status": "OK",
                "started_ts": now_ms,
                "ended_ts": now_ms,
                "duration_ms": 0,
                "payload_json": None,
                "error": None,
                "metrics_json": json.dumps({"published_count": int(published_count), "products": int(len(upcoming_df))}),
            }
        ])
    except Exception:
        pass
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
            FROM tote_products p
            LEFT JOIN hr_horse_runs r ON p.event_id = r.event_id
            WHERE p.status = 'CLOSED'
              AND TIMESTAMP(p.start_iso) BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR) AND CURRENT_TIMESTAMP()
              AND r.event_id IS NULL
            GROUP BY 1, 2
            LIMIT 100
        """).to_dataframe()
    except Exception as e:
        print(f"Error querying for events needing results: {e}")
        return "Query for events needing results failed", 500
    
    if results_needed_df.empty:
        try:
            sink = get_bq_sink()
            now_ms = int(time.time() * 1000)
            sink.upsert_ingest_job_runs([
                {
                    "job_id": f"orchestrator-{uuid.uuid4().hex}",
                    "component": "orchestrator",
                    "task": "post-race-results-scanner",
                    "status": "OK",
                    "started_ts": now_ms,
                    "ended_ts": now_ms,
                    "duration_ms": 0,
                    "payload_json": None,
                    "error": None,
                    "metrics_json": "{}",
                }
            ])
        except Exception:
            pass
        return "No events found needing results.", 200

    print(f"Found {len(results_needed_df)} events needing results to be ingested.")
    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id: return "GCP_PROJECT not set", 500

    unique_event_ids = results_needed_df['event_id'].unique()
    for event_id in unique_event_ids:
        job = {"task": "ingest_event_results", "event_id": event_id}
        publish_pubsub_message(project_id, topic_id, job)
    try:
        sink = get_bq_sink()
        now_ms = int(time.time() * 1000)
        sink.upsert_ingest_job_runs([
            {
                "job_id": f"orchestrator-{uuid.uuid4().hex}",
                "component": "orchestrator",
                "task": "post-race-results-scanner",
                "status": "OK",
                "started_ts": now_ms,
                "ended_ts": now_ms,
                "duration_ms": 0,
                "payload_json": None,
                "error": None,
                "metrics_json": json.dumps({"published_count": int(len(unique_event_ids))}),
            }
        ])
    except Exception:
        pass
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
            SELECT DISTINCT event_id
            FROM tote_products
            WHERE UPPER(bet_type)='WIN' AND status='OPEN'
              AND TIMESTAMP(start_iso) BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
                                           AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
            LIMIT 500
            """
        ).to_dataframe()
    except Exception as e:
        print(f"Error querying probable sweep: {e}")
        return "Query for probable sweep failed", 500

    if df.empty:
        try:
            sink = get_bq_sink()
            now_ms = int(time.time() * 1000)
            sink.upsert_ingest_job_runs([
                {
                    "job_id": f"orchestrator-{uuid.uuid4().hex}",
                    "component": "orchestrator",
                    "task": "probable-odds-sweep",
                    "status": "OK",
                    "started_ts": now_ms,
                    "ended_ts": now_ms,
                    "duration_ms": 0,
                    "payload_json": None,
                    "error": None,
                    "metrics_json": "{}",
                }
            ])
        except Exception:
            pass
        return "No events for probable sweep.", 200

    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id:
        return "GCP_PROJECT not set", 500

    n = 0
    for _, row in df.iterrows():
        eid = row["event_id"]
        publish_pubsub_message(project_id, topic_id, {"task": "ingest_probable_odds", "event_id": eid})
        n += 1

    try:
        sink = get_bq_sink()
        now_ms = int(time.time() * 1000)
        sink.upsert_ingest_job_runs([
            {
                "job_id": f"orchestrator-{uuid.uuid4().hex}",
                "component": "orchestrator",
                "task": "probable-odds-sweep",
                "status": "OK",
                "started_ts": now_ms,
                "ended_ts": now_ms,
                "duration_ms": 0,
                "payload_json": None,
                "error": None,
                "metrics_json": json.dumps({"published_count": int(n)}),
            }
        ])
    except Exception:
        pass
    return f"Published {n} probable odds jobs.", 200

def publish_daily_event_ingest():
    """Publishes a job to ingest all of today's events."""
    project_id = os.getenv("GCP_PROJECT") or cfg.bq_project
    topic_id = os.getenv("PUBSUB_TOPIC_ID", "ingest-jobs")
    if not project_id:
        return "GCP_PROJECT not set", 500

    job = {"task": "ingest_events_for_day", "date": "today"}
    publish_pubsub_message(project_id, topic_id, job)

    # Log run
    try:
        sink = get_bq_sink()
        now_ms = int(time.time() * 1000)
        sink.upsert_ingest_job_runs([
            {
                "job_id": f"orchestrator-{uuid.uuid4().hex}",
                "component": "orchestrator",
                "task": "daily-event-ingest-trigger",
                "status": "OK",
                "started_ts": now_ms,
                "ended_ts": now_ms,
                "duration_ms": 0,
                "payload_json": json.dumps(job),
                "error": None,
                "metrics_json": json.dumps({"published_count": 1}),
            }
        ])
    except Exception:
        pass

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
        elif job_name == "daily-event-ingest":
            return publish_daily_event_ingest()
        else:
            return (f"Unknown job_name: {job_name}", 400)
    except Exception as e:
        print(f"Orchestrator error for job '{job_name}': {e}")
        # Return 200 to prevent Cloud Scheduler from retrying a failed job logic.
        # The error is logged for debugging.
        return (f"Internal error processing job: {job_name}", 200)
