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
from .gcp import parse_pubsub_envelope, publish_pubsub_message
from .config import cfg
from .providers.tote_api import ToteClient, ToteError, rate_limited_get, normalize_probable_lines
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

        elif task == "ingest_multiple_products":
            product_ids = payload.get("product_ids")
            if not product_ids: return ("Missing 'product_ids' for task", 400)
            # Ingest all products in one batch
            n_ingested = ingest_products(sink, client, date_iso=None, status=None, first=len(product_ids), bet_types=None, product_ids=product_ids)
            metrics["ingested_products"] = n_ingested
            print(f"Ingested {n_ingested} products from batch.")

        elif task == "ingest_events_for_day":
            date_iso = payload.get("date", time.strftime("%Y-%m-%d"))
            if date_iso == "today": date_iso = time.strftime("%Y-%m-%d")
            since = f"{date_iso}T00:00:00Z"
            until = f"{date_iso}T23:59:59Z"
            n = ingest_tote_events(sink, client, since_iso=since, until_iso=until, first=1000)
            metrics["ingested_events"] = n
            print(f"Ingested {n} events for {date_iso}")

        elif task == "ingest_single_product":
            product_id = payload.get("product_id")
            if not product_id: return ("Missing 'product_id' for task", 400)
            # This task is now a simple wrapper around the batch ingest for one product.
            # This maintains compatibility with any manual triggers.
            n_ingested = ingest_products(sink, client, date_iso=None, status=None, first=1, bet_types=None, product_ids=[product_id])
            metrics["ingested_products"] = n_ingested
            print(f"Ingested single product {product_id}")

        elif task == "ingest_multiple_probable_odds":
            event_ids = payload.get("event_ids")
            if not event_ids: return ("Missing 'event_ids' for task", 400)

            # This task is now a simple wrapper around the `ingest_probable_odds` task for multiple events.
            # This avoids duplicating logic and ensures consistency.
            published_count = 0
            for event_id in event_ids:
                try:
                    # Re-use the existing `ingest_probable_odds` task logic for each event.
                    # This is more robust as it uses the GraphQL endpoint directly.
                    job_payload = {"task": "ingest_probable_odds", "event_id": event_id}
                    publish_pubsub_message(cfg.bq_project, "ingest-jobs", job_payload)
                    published_count += 1
                except Exception as e:
                    print(f"Failed to publish probable odds job for event {event_id}: {e}")
            n_ingested = published_count
            metrics["refreshed_probable_odds_for_events"] = len(event_ids)
            metrics["ingested_products_for_odds"] = n_ingested

        elif task == "ingest_probable_odds":
            event_id = payload.get("event_id")
            if not event_id: return ("Missing 'event_id' for task", 400)
            
            win_prod_df = sink.query(
                "SELECT product_id FROM `autobet-470818.autobet.tote_products` WHERE event_id = @eid AND bet_type = 'WIN' AND status = 'OPEN' LIMIT 1",
                job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("eid", "STRING", event_id)])
            ).to_dataframe()
            if win_prod_df.empty:
                print(f"No open WIN product found for event {event_id}")
                return ("", 204)
            
            win_product_id = win_prod_df.iloc[0]["product_id"]
            # Use GraphQL to fetch lines (probable odds) for the WIN product
            GQL = """
            query GetProduct($id: String!) {
              product(id: $id) {
                id
                ... on BettingProduct {
                  lines { nodes { legs { legId lineSelections { selectionId } } odds { decimal } } }
                }
              }
            }
            """
            try:
                data = client.graphql(GQL, {"id": win_product_id})
            except ToteError as te:
                print(f"GraphQL probable fetch failed for {win_product_id}: {te}")
                return ("", 204)
            prod = (data or {}).get("product") or {}
            lines_container = prod.get("lines") if isinstance(prod, dict) else None
            if isinstance(lines_container, dict):
                raw_lines = lines_container.get("nodes") or []
            elif isinstance(lines_container, list):
                raw_lines = lines_container
            else:
                raw_lines = []

            norm_lines = normalize_probable_lines(raw_lines)
            if not norm_lines:
                print(f"No probable odds lines found via GraphQL for product {win_product_id}")
                return ("", 204)
            payload = {"products": {"nodes": [{"id": win_product_id, "lines": {"nodes": norm_lines}}]}}
            rid = f"probable:{int(time.time()*1000)}:{win_product_id}"
            ts_ms = int(time.time()*1000)
            sink.upsert_raw_tote_probable_odds([{"raw_id": rid, "fetched_ts": ts_ms, "payload": json.dumps(payload), "product_id": win_product_id}])
            metrics = {"probable_for_product": win_product_id, "lines": len(norm_lines)}
            print(f"Ingested probable odds (GraphQL) for product {win_product_id} (lines={len(norm_lines)})")

        elif task == "ingest_event_results":
            event_id = payload.get("event_id")
            if not event_id: return ("Missing 'event_id' for task", 400)
            
            event_details_df = sink.query(
                "SELECT start_iso FROM `autobet-470818.autobet.tote_events` WHERE event_id = @eid LIMIT 1",
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
