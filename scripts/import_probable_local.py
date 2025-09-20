from __future__ import annotations

"""Local importer for probable odds.

Use this to test end-to-end without Pub/Sub:

1) Fetch from Tote REST using your creds and write to BigQuery raw:
   python autobet/scripts/import_probable_local.py --event-id <EID>
   python autobet/scripts/import_probable_local.py --product-id <PID>

2) Load from a local JSON file into BigQuery raw (must match expected shape):
   python autobet/scripts/import_probable_local.py --file path/to/probable.json

Requirements:
  - BQ_* env and BQ_WRITE_ENABLED=true for writing
  - TOTE_GRAPHQL_URL and TOTE_API_KEY for fetch modes
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.bq import get_bq_sink
from sports.config import cfg


def _rest_base_from_graphql(url: str) -> str:
    try:
        if "/partner/" in url:
            return url.split("/partner/")[0].rstrip("/")
        from urllib.parse import urlparse
        u = urlparse(url)
        if u.scheme and u.netloc:
            return f"{u.scheme}://{u.netloc}"
    except Exception:
        pass
    return "https://hub.production.racing.tote.co.uk"


def _pid_for_event(eid: str) -> str | None:
    client = bigquery.Client(project=cfg.bq_project)
    sql = (
        "SELECT product_id FROM `" + cfg.bq_project + "." + cfg.bq_dataset + 
        "`.tote_products WHERE event_id=@eid AND UPPER(bet_type)='WIN' ORDER BY (status='OPEN') DESC, start_iso DESC LIMIT 1"
    )
    rows = list(
        client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("eid", "STRING", eid,
        location=cfg.bq_location
    )]
            ),
        ).result()
    )
    return rows[0][0] if rows else None


def main() -> None:
    ap = argparse.ArgumentParser(description="Local probable odds importer")
    ap.add_argument("--event-id", help="Event ID to fetch")
    ap.add_argument("--product-id", help="WIN Product ID to fetch")
    ap.add_argument("--file", help="Path to JSON file to load into raw table")
    args = ap.parse_args()

    sink = get_bq_sink()
    if not sink:
        raise SystemExit("BigQuery not configured. Set BQ_* and BQ_WRITE_ENABLED=true")

    # File load mode
    if args.file:
        p = Path(args.file)
        if not p.exists():
            raise SystemExit(f"File not found: {p}")
        payload_text = p.read_text(encoding="utf-8")
        ts_ms = int(time.time() * 1000)
        rid = f"probable:file:{ts_ms}"
        sink.upsert_raw_tote_probable_odds([
            {"raw_id": rid, "fetched_ts": ts_ms, "payload": payload_text}
        ])
        print(f"Loaded probable payload from {p} -> raw_tote_probable_odds")
        return

    # Fetch mode
    if not (args.event_id or args.product_id):
        raise SystemExit("Provide --event-id, --product-id, or --file")

    if not (cfg.tote_graphql_url and cfg.tote_api_key):
        raise SystemExit("TOTE_GRAPHQL_URL/TOTE_API_KEY not set in env/.env")

    product_id = args.product_id
    if not product_id:
        product_id = _pid_for_event(args.event_id)
        if not product_id:
            raise SystemExit(f"No product found for event {args.event_id}")

    base_root = _rest_base_from_graphql(cfg.tote_graphql_url)
    url = f"{base_root}/v1/products/{product_id}/probable-odds"
    headers = {"Authorization": f"Api-Key {cfg.tote_api_key}", "Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    ts_ms = int(time.time() * 1000)
    rid = f"probable:{product_id}:{ts_ms}"
    sink.upsert_raw_tote_probable_odds([
        {"raw_id": rid, "fetched_ts": ts_ms, "payload": resp.text}
    ])
    print(f"Fetched probable odds for product {product_id} -> raw_tote_probable_odds")


if __name__ == "__main__":
    main()
