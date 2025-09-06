"""Ingest Tote products for a date range directly into BigQuery.

Usage:
  python autobet/scripts/ingest_tote_range.py --from 2022-01-01 --to 2025-09-05 \
    --bet-types WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA --first 400

This runs OPEN then CLOSED for each day in the range.
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.db import get_db
from sports.providers.tote_api import ToteClient
from sports.ingest.tote_products import ingest_products


def daterange(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur = cur + timedelta(days=1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Tote products for a date range into BigQuery")
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    ap.add_argument("--bet-types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA")
    ap.add_argument("--first", type=int, default=400)
    args = ap.parse_args()

    d0 = date.fromisoformat(args.date_from)
    d1 = date.fromisoformat(args.date_to)
    if d1 < d0:
        raise SystemExit("--to must be >= --from")

    bet_types = [s.strip().upper() for s in (args.bet_types or '').split(',') if s.strip()]
    db = get_db(); client = ToteClient()

    total = 0
    for cur in daterange(d0, d1):
        ds = cur.isoformat()
        print(f"[Ingest Range] {ds} OPEN...")
        try:
            n_open = ingest_products(db, client, date_iso=ds, status="OPEN", first=args.first, bet_types=bet_types)
        except Exception as e:
            print(f"[Ingest Range] {ds} OPEN ERROR: {e}")
            n_open = 0
        print(f"[Ingest Range] {ds} CLOSED...")
        try:
            n_closed = ingest_products(db, client, date_iso=ds, status="CLOSED", first=args.first, bet_types=bet_types)
        except Exception as e:
            print(f"[Ingest Range] {ds} CLOSED ERROR: {e}")
            n_closed = 0
        day_total = n_open + n_closed
        print(f"[Ingest Range] {ds} total={day_total}")
        total += day_total
    print(f"Done. Total rows: {total}")


if __name__ == "__main__":
    main()

