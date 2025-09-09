"""Cloud Run entrypoint to handle ingestion jobs from Pub/Sub.

Expected payloads (task-based):
  {"task": "ingest_probable_odds", "event_id": "..."}
  {"task": "ingest_events_for_day", "date": "YYYY-MM-DD|today"}
  {"task": "ingest_products_for_day", "date": "YYYY-MM-DD|today", "status": "OPEN|CLOSED|..."}
  ...

Legacy REST-shaped envelopes (provider/op/path) are no longer used by this service.
"""
from __future__ import annotations

import os
import requests
import time
import json
import traceback
from flask import Flask, request
from google.cloud import bigquery

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

        elif task == "ingest_events_for_day":
            date_iso = payload.get("date", time.strftime("%Y-%m-%d"))
            if date_iso == "today":
                date_iso = time.strftime("%Y-%m-%d")
            since_iso = f"{date_iso}T00:00:00Z"
            until_iso = f"{date_iso}T23:59:59Z"
            # Pull a generous page size to reduce pagination overhead
            n = ingest_tote_events(sink, client, first=1000, since_iso=since_iso, until_iso=until_iso)
            metrics = {"events_ingested": int(n)}
            print(f"Ingested {n} events for {date_iso}")

        elif task == "ingest_single_product":
            product_id = payload.get("product_id")
            if not product_id: return ("Missing 'product_id' for task", 400)
            ingest_products(sink, client, date_iso=None, status=None, first=1, bet_types=None, product_ids=[product_id])

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
            path = f"/v1/products/{win_product_id}/probable-odds"
            # Derive API base root robustly from configured GraphQL URL; fallback to production hub
            base = cfg.tote_graphql_url or ""
            base_root = ""
            try:
                # Prefer stripping at /partner/... if present
                if "/partner/" in base:
                    base_root = base.split("/partner/")[0].rstrip("/")
                else:
                    # Parse scheme+host as a safe default
                    from urllib.parse import urlparse
                    u = urlparse(base)
                    if u.scheme and u.netloc:
                        base_root = f"{u.scheme}://{u.netloc}"
            except Exception:
                base_root = ""
            if not base_root:
                base_root = "https://hub.production.racing.tote.co.uk"
            full_url = f"{base_root}{path}"
            try:
                headers = {"Authorization": f"Api-Key {cfg.tote_api_key}", "Accept": "application/json"}
                resp = rate_limited_get(full_url, headers=headers, timeout=20)
                resp.raise_for_status()
            except requests.exceptions.HTTPError as http_err:
                # Gracefully handle 403/404 errors for probable odds; attempt GraphQL fallback via lines
                code = getattr(getattr(http_err, 'response', None), 'status_code', None)
                print(f"Could not fetch probable odds (REST) for {win_product_id} (event: {event_id}). Status: {code}. URL: {full_url}. Trying GraphQL lines fallback.")
                try:
                    # GraphQL fallback to fetch lines and emulate the REST payload shape expected by our view
                    PROBABLES_BY_PRODUCT_QUERY = """
                    query LinesForProduct($id: String!) {
                      product(id: $id) {
                        ... on BettingProduct {
                          id
                          lines {
                            nodes {
                              odds { decimal }
                              legs { lineSelections { selectionId } }
                            }
                          }
                        }
                      }
                    }
                    """
                    data = client.graphql(PROBABLES_BY_PRODUCT_QUERY, {"id": win_product_id})
                    prod = (data.get("product") or {})
                    lines = ((prod.get("lines") or {}).get("nodes") or [])
                    payload_obj = {
                        "products": {
                            "nodes": [
                                {
                                    "id": win_product_id,
                                    "lines": {
                                        "nodes": [
                                            {
                                                "odds": ([{"decimal": ln.get("odds")[0].get("decimal")}]
                                                          if isinstance(ln.get("odds"), list) and ln.get("odds") else []),
                                                "legs": [
                                                    {"lineSelections": [
                                                        {"selectionId": sel.get("selectionId")}
                                                        for sel in (leg.get("lineSelections") or []) if sel.get("selectionId")
                                                    ]}
                                                    for leg in (ln.get("legs") or [])
                                                ],
                                            }
                                            for ln in lines if isinstance(ln, dict)
                                        ]
                                    },
                                }
                            ]
                        }
                    }
                    rid = f"probable:{int(time.time()*1000)}"
                    ts_ms = int(time.time()*1000)
                    sink.upsert_raw_tote_probable_odds([{"raw_id": rid, "fetched_ts": ts_ms, "payload": json.dumps(payload_obj)}])
                    print(f"Ingested probable odds via GraphQL fallback for product {win_product_id}")
                    return ("", 204)
                except Exception as ge:
                    print(f"GraphQL fallback failed for {win_product_id}: {ge}")
                    return ("", 204) # Still ack the message

            rid = f"probable:{int(time.time()*1000)}"
            ts_ms = int(time.time()*1000)
            sink.upsert_raw_tote_probable_odds([{"raw_id": rid, "fetched_ts": ts_ms, "payload": resp.text}])
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
