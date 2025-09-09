"""Publish standard Tote ingestion jobs for a given date.

This publishes two jobs to Pub/Sub using the provided GraphQL files:
- Products OPEN (with bet types filter)
- Products CLOSED (same types)

Each job lands JSON in GCS and appends a copy to BigQuery raw_tote.

Example:
  python scripts/publish_tote_daily_jobs.py \
    --project autobet-470818 --topic ingest-jobs \
    --bucket autobet-470818-data --date 2025-09-05 \
    --bet-types WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys, os

# Ensure repo root (containing 'sports') is on sys.path when run as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.gcp import publish_pubsub_message


def _load(p: str) -> str:
    return Path(p).read_text(encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish Tote daily GraphQL jobs")
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--page-size", type=int, default=400)
    ap.add_argument("--bet-types", default="WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA")
    ap.add_argument("--products-query", default=str(Path(__file__).resolve().parents[1] / "sql" / "tote_products.graphql"))
    ap.add_argument("--paginate", action="store_true", help="Enable server-side pagination in Cloud Run (uses pageInfo)")
    args = ap.parse_args()

    bet_types = [s.strip().upper() for s in (args.bet_types or '').split(',') if s.strip()]
    q_products = _load(args.products_query)

    for status in ("OPEN", "CLOSED"):
        name = f"raw/tote/products_{args.date}_{status.lower()}.json"
        payload = {
            "provider": "tote",
            "op": "graphql",
            "query": q_products,
            "variables": {"date": args.date, "status": status, "first": args.page_size, "betTypes": bet_types},
            "bucket": args.bucket,
            "name": name,
            "bq": {"table": "raw_tote", "sport": "horse_racing", "entity_id": status},
            "paginate": bool(args.paginate),
        }
        msg_id = publish_pubsub_message(args.project, args.topic, payload)
        print(f"Published {status} products -> {name}: {msg_id}")

    print("Done.")


if __name__ == "__main__":
    main()
