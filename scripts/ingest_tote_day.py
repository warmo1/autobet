"""Ingest Tote products (OPEN and CLOSED) for a date directly into BigQuery.

Usage:
  python autobet/scripts/ingest_tote_day.py --date YYYY-MM-DD \
      --bet-types WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA --first 400

Requires env: BQ_PROJECT, BQ_DATASET, TOTE_API_KEY, TOTE_GRAPHQL_URL
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.db import get_db
from sports.providers.tote_api import ToteClient
from sports.ingest.tote_products import ingest_products


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Tote products for a date into BigQuery")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--bet-types", default="", help="Comma-separated list. Leave blank to fetch ALL types.")
    ap.add_argument("--first", type=int, default=400)
    args = ap.parse_args()

    bet_types = [s.strip().upper() for s in (args.bet_types or '').split(',') if s.strip()]
    if not bet_types:
        bet_types = None
    db = get_db(); client = ToteClient()

    total = 0
    for status in ("OPEN", "CLOSED"):
        try:
            n = ingest_products(db, client, date_iso=args.date, status=status, first=args.first, bet_types=bet_types)
        except Exception as e:
            print(f"[{status}] ERROR: {e}")
            n = 0
        print(f"[{status}] inserted/updated: {n}")
        total += n
    print(f"Done. Total rows: {total}")


if __name__ == "__main__":
    main()
