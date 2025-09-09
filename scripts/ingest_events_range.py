"""Ingest Tote events for a date range directly into BigQuery (local, one-off).

Usage:
  python autobet/scripts/ingest_events_range.py --from 2024-01-01 --to 2024-12-31 --first 500 --sleep 1.0

Notes:
  - This uses the Tote GraphQL "events" endpoint and writes to BigQuery via
    the BigQuerySink upsert helpers.
  - Ensure env vars are set locally: BQ_PROJECT, BQ_DATASET, TOTE_API_KEY, TOTE_GRAPHQL_URL.
  - Rate limiting is built into the Tote client (TOTE_RPS / TOTE_BURST).
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.db import get_db
from sports.providers.tote_api import ToteClient
from sports.ingest.tote_events import ingest_tote_events


def daterange(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur = cur + timedelta(days=1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Tote events for a date range into BigQuery")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    ap.add_argument("--first", type=int, default=500, help="Page size for GraphQL (default 500)")
    ap.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between days (default 1.0)")
    args = ap.parse_args()

    d0 = date.fromisoformat(args.date_from)
    d1 = date.fromisoformat(args.date_to)
    if d1 < d0:
        raise SystemExit("--to must be >= --from")

    db = get_db()
    client = ToteClient()
    total_events = 0
    for cur in daterange(d0, d1):
        day = cur.isoformat()
        since_iso = f"{day}T00:00:00Z"; until_iso = f"{day}T23:59:59Z"
        print(f"[Events Range] {day} ...")
        try:
            n = ingest_tote_events(db, client, first=max(1, int(args.first)), since_iso=since_iso, until_iso=until_iso)
            print(f"[Events Range] {day} ingested {n} events")
            total_events += int(n or 0)
        except Exception as e:
            print(f"[Events Range] {day} ERROR: {e}")
        time.sleep(max(0.0, float(args.sleep)))
    print(f"Done. Total events ingested: {total_events}")


if __name__ == "__main__":
    main()

