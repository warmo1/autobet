from __future__ import annotations

"""Fetch probable odds for today's open WIN products directly (no Pub/Sub).

Writes rows into raw_tote_probable_odds for parsing by vw_tote_probable_odds.

Usage:
  python autobet/scripts/fetch_probable_today_local.py --limit 200
"""

import argparse
import os
from pathlib import Path
import sys
import time
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.bq import get_bq_sink
from sports.config import cfg
from google.cloud import bigquery


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch probable odds for today's open WIN products")
    ap.add_argument("--limit", type=int, default=100)
    args = ap.parse_args()

    sink = get_bq_sink()
    if not sink:
        raise SystemExit("BigQuery not configured. Set BQ_* and BQ_WRITE_ENABLED=true")

    client = bigquery.Client(project=cfg.bq_project)
    sql = f"""
    SELECT product_id
    FROM `{cfg.bq_project}.{cfg.bq_dataset}.tote_products`
    WHERE UPPER(bet_type)='WIN' AND status='OPEN' AND DATE(SUBSTR(start_iso,1,10))=CURRENT_DATE()
    ORDER BY start_iso
    LIMIT {max(1, int(args.limit))}
    """
    rows = list(client.query(sql).result())
    if not rows:
        print("No open WIN products found for today.")
        return

    # Build REST base from GraphQL URL
    if not cfg.tote_graphql_url or not cfg.tote_api_key:
        raise SystemExit("TOTE_GRAPHQL_URL/TOTE_API_KEY not set in env/.env")
    base_root = cfg.tote_graphql_url.split('/partner/')[0].rstrip('/')
    headers = {"Authorization": f"Api-Key {cfg.tote_api_key}", "Accept": "application/json"}

    n = 0
    for r in rows:
        pid = r[0]
        url = f"{base_root}/v1/products/{pid}/probable-odds"
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        payload = resp.text
        ts_ms = int(time.time() * 1000)
        sink.upsert_raw_tote_probable_odds([
            {"raw_id": f"probable:{pid}:{ts_ms}", "fetched_ts": ts_ms, "payload": payload}
        ])
        print(f"Stored probable odds for {pid}")
        n += 1
    print(f"Done. Fetched {n} probable odds.")


if __name__ == "__main__":
    main()

