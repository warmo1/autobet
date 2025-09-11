"""Cloud Run entrypoint to handle ingestion jobs from Pub/Sub.

The service expects Pub/Sub push messages containing JSON with the keys
``url`` (HTTP resource to fetch), ``bucket`` (Cloud Storage bucket), and
``name`` (destination object name). The referenced resource is downloaded
and stored in the given bucket.
"""
from __future__ import annotations

import os
import requests
import time
import json
import traceback
from flask import Flask, request
from google.cloud import bigquery

import pandas as pd
from .gcp import parse_pubsub_envelope, upload_text_to_bucket
from .config import cfg
from .providers.tote_api import ToteClient, ToteError, rate_limited_get
from .bq import get_bq_sink
import uuid
from .ingest.tote_events import ingest_tote_events
from .ingest.tote_products import ingest_products

app = Flask(__name__)

@app.get("/")
def health() -> tuple[str, int]:
    return ("ok", 200)

@app.post("/")
def handle_pubsub() -> tuple[str, int]:
    """Endpoint for Pub/Sub push subscriptions."""
    try:
        payload = parse_pubsub_envelope(request.get_json())
    except ValueError:
        return ("Bad Request", 400)

    task = payload.get("task")
    if not task:
        return ("Missing 'task' in payload", 400)

    job_id = f"ingest-{uuid.uuid4().hex}"
    started_ms = int(time.time() * 1000)
    status = "OK"; err = None; metrics = {}
    try:
        sink = get_bq_sink()
        client = ToteClient()
        if not sink or not client:
            return ("Service not configured (BQ/Tote)", 500)

        print(f"Executing task: {task} with payload: {json.dumps(payload)} (job_id={job_id})")

        if task == "ingest_products_for_day":
            date_iso = payload.get("date", time.strftime("%Y-%m-%d"))
            if date_iso == "today": date_iso = time.strftime("%Y-%m-%d")
            # status=None fetches all statuses (OPEN, CLOSED, etc.)
            status_filter = payload.get("status")
            bet_types = payload.get("bet_types")
            ingest_products(sink, client, date_iso=date_iso, status=status_filter, first=1000, bet_types=bet_types)

        elif task == "ingest_single_product":
            product_id = payload.get("product_id")
            if not product_id: return ("Missing 'product_id' for task", 400)
            # This will update the tote_products table
            n_ingested = ingest_products(sink, client, date_iso=None, status=None, first=1, bet_types=None, product_ids=[product_id])

            # Only proceed if we actually got the product data from the API.
            if n_ingested == 0:
                print(f"Product {product_id} not found via API, skipping snapshot creation.")
                return ("", 204)

            # Also create a snapshot from the data just ingested.
            # This makes the polling mechanism behave like a subscription for the UI.
            prod_df = sink.query(
                "SELECT * FROM tote_products WHERE product_id = @pid LIMIT 1",
                job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("pid", "STRING", product_id)])
            ).to_dataframe()

            if not prod_df.empty:
                # Replace pandas NaN/NaT with None before converting to dict to ensure JSON compatibility
                prod = prod_df.where(pd.notnull(prod_df), None).iloc[0].to_dict()
                snapshot = {k: prod.get(k) for k in ["product_id", "event_id", "bet_type", "status", "currency", "start_iso", "total_gross", "total_net", "rollover", "deduction_rate"]}
                snapshot["ts_ms"] = int(time.time() * 1000)
                sink.upsert_tote_pool_snapshots([snapshot])
                metrics["created_pool_snapshot"] = True
                print(f"Created pool snapshot for product {product_id}")

        elif task == "ingest_probable_odds":
            event_id = payload.get("event_id")
            if not event_id: return ("Missing 'event_id' for task", 400)
            
            win_prod_df = sink.query(
                "SELECT product_id FROM tote_products WHERE event_id = @eid AND bet_type = 'WIN' AND status = 'OPEN' LIMIT 1",
                job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("eid", "STRING", event_id)])
            ).to_dataframe()
            if win_prod_df.empty:
                print(f"No open WIN product found for event {event_id}")
                return ("", 204)
            
            win_product_id = win_prod_df.iloc[0]["product_id"]
            # Build candidate probable-odds endpoints from configured GraphQL base.
            # Many partners expose REST under /partner/gateway/ as well.
            base = cfg.tote_graphql_url or ""
            host_root = ""
            try:
                if "/partner/" in base:
                    host_root = base.split("/partner/")[0].rstrip("/")
                else:
                    from urllib.parse import urlparse
                    u = urlparse(base)
                    if u.scheme and u.netloc:
                        host_root = f"{u.scheme}://{u.netloc}"
            except Exception:
                host_root = ""
            if not host_root:
                host_root = "https://hub.production.racing.tote.co.uk"

            candidates = [
                f"{host_root}/partner/gateway/v1/products/{win_product_id}/probable-odds",
                f"{host_root}/partner/gateway/probable-odds/v1/products/{win_product_id}",
                f"{host_root}/partner/gateway/probable-odds/v1/products/{win_product_id}/probable-odds",
                f"{host_root}/v1/products/{win_product_id}/probable-odds",
            ]

            headers = {"Authorization": f"Api-Key {cfg.tote_api_key}", "Accept": "application/json"}
            resp = None
            last_status = None
            for url in candidates:
                try:
                    resp = rate_limited_get(url, headers=headers, timeout=20)
                    if 200 <= resp.status_code < 300:
                        break
                    last_status = resp.status_code
                except requests.exceptions.RequestException:
                    last_status = None
                    continue
            if resp is None or not (200 <= resp.status_code < 300):
                # Gracefully handle missing endpoints (often 403/404); skip without failing.
                st = last_status if last_status is not None else "n/a"
                print(f"Could not fetch probable odds for {win_product_id} (event: {event_id}). Status: {st}. Skipping.")
                return ("", 204)

            rid = f"probable:{int(time.time()*1000)}:{win_product_id}"
            ts_ms = int(time.time()*1000)
            sink.upsert_raw_tote_probable_odds([{"raw_id": rid, "fetched_ts": ts_ms, "payload": resp.text, "product_id": win_product_id}])
            metrics = {"probable_for_product": win_product_id}
            print(f"Ingested probable odds for product {win_product_id}")

        elif task == "ingest_event_results":
            event_id = payload.get("event_id")
            if not event_id: return ("Missing 'event_id' for task", 400)
            
            event_details_df = sink.query(
                "SELECT start_iso FROM tote_events WHERE event_id = @eid LIMIT 1",
                job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("eid", "STRING", event_id)])
            ).to_dataframe()
            if event_details_df.empty:
                return (f"Event {event_id} not found in DB", 204)
            
            start_iso = event_details_df.iloc[0]['start_iso']
            date_iso = start_iso.split('T')[0]
            ingest_tote_events(sink, client, since_iso=f"{date_iso}T00:00:00Z", until_iso=f"{date_iso}T23:59:59Z", first=1000)
            metrics = {"refreshed_event_results_for_date": date_iso, "event_id": event_id}
            print(f"Refreshed events for date {date_iso} to get results for event {event_id}")

        elif task == "cleanup_bq_temps":
            try:
                older = payload.get("older_than_days") or payload.get("older_days") or 7
                deleted = sink.cleanup_temp_tables(prefix="_tmp_", older_than_days=int(older))
                metrics = {"deleted_tmp_tables": int(deleted)}
                print(f"Temp table cleanup: deleted={deleted} older_than_days={older}")
            except Exception as e:
                raise

        else:
            return (f"Unknown task: {task}", 400)

    except ToteError as te:
        status = "ERROR"; err = f"Tote error: {str(te)[:500]}"; print(err)
        return (err, 204)
    except requests.exceptions.Timeout:
        status = "ERROR"; err = "Timeout"; print(err)
        return ("Timeout", 204)
    except Exception as e:
        status = "ERROR"; err = str(e)[:500]
        traceback.print_exc()
        return (f"Error: {err}", 500)
    finally:
        try:
            sink = get_bq_sink()
            ended_ms = int(time.time() * 1000)
            payload_json = json.dumps(payload) if payload else None
            metrics_json = json.dumps(metrics) if metrics else None
            sink.upsert_ingest_job_runs([
                {
                    "job_id": job_id,
                    "component": "ingest",
                    "task": task or "",
                    "status": status,
                    "started_ts": started_ms,
                    "ended_ts": ended_ms,
                    "duration_ms": max(0, ended_ms - started_ms),
                    "payload_json": payload_json,
                    "error": err,
                    "metrics_json": metrics_json,
                }
            ])
        except Exception:
            pass

    return ("", 204)
