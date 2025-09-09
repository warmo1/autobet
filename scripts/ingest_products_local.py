from __future__ import annotations

"""Directly ingest Tote products + selections into BigQuery (no Pub/Sub).

Usage:
  python autobet/scripts/ingest_products_local.py --date YYYY-MM-DD --status OPEN --first 400 --bet-types SUPERFECTA,WIN
  Repeat with --after cursor to page; or use --pages N to auto paginate.
"""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.providers.tote_api import ToteClient
from sports.bq import get_bq_sink
from sports.ingest.tote_products import ingest_products, PRODUCTS_QUERY

# Fallback query for providers exposing a Discover-style schema
DISCOVER_QUERY = """
query GetRacingNextOff($sport: Sport!, $date: String!, $limit: Int) {
  discover(filter: {sport: $sport, date: $date}) {
    events(first: $limit) {
      nodes {
        eventId
        eventName
        eventDate
        venue
        raceNumber
        sport
        status
        resultStatus
        start
        products {
          nodes {
            productId
            betType
            status
            currency
            start
            legs {
              nodes {
                productLegId
                selections {
                  nodes {
                    selectionId
                    competitor {
                      name
                      details {
                        ... on HorseDetails { clothNumber }
                        ... on GreyhoundDetails { trapNumber }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


def main() -> None:
    ap = argparse.ArgumentParser(description="Direct Tote products ingest to BigQuery")
    ap.add_argument("--date", required=True)
    ap.add_argument("--status", default="OPEN")
    ap.add_argument("--first", type=int, default=500)
    ap.add_argument("--bet-types", default="", help="Comma-separated bet types. Empty = ALL")
    ap.add_argument("--pages", type=int, default=0, help="Pages to fetch. 0 = until hasNextPage=false")
    args = ap.parse_args()

    sink = get_bq_sink()
    if not sink:
        raise SystemExit("BigQuery not configured. Ensure BQ_WRITE_ENABLED=true and BQ_PROJECT/BQ_DATASET envs are set.")
    client = ToteClient()

    bet_types = [s.strip().upper() for s in (args.bet_types or '').split(',') if s.strip()]
    variables = {"date": args.date, "status": args.status, "first": args.first}
    if bet_types:
        variables["betTypes"] = bet_types

    cursor = None
    pages = args.pages
    try:
        i = 0
        while True:
            if cursor:
                variables["after"] = cursor
            else:
                variables.pop("after", None)
            data = client.graphql(PRODUCTS_QUERY, variables)
            prod = (data.get("products") or {})
            nodes = prod.get("nodes")
            if nodes is None:
                edges = prod.get("edges") or []
                nodes = [e.get("node") for e in edges if isinstance(e, dict)]
            page_info = prod.get("pageInfo") or {}
            cursor = page_info.get("endCursor")
            has_next = bool(page_info.get("hasNextPage"))
            # Wrap fake client to feed ingest function without refetch
            class C:
                def graphql(self, q, v):
                    return {"products": {"nodes": nodes}}
            count = ingest_products(sink, C(), args.date, args.status, args.first, bet_types)
            print(f"Inserted/updated products: {count}")
            i += 1
            if pages > 0:
                if i >= pages:
                    break
            if not has_next:
                break
    except Exception as e:
        # Fallback to Discover schema
        print(f"Primary products query failed ({e}); trying Discover schema...")
        data = client.graphql(DISCOVER_QUERY, {"sport": "HorseRacing", "date": args.date, "limit": args.first})
        events = ((data.get("discover") or {}).get("events") or {}).get("nodes") or []
        rows_events = []
        rows_products = []
        rows_selections = []
        for ev in events:
            eid = ev.get("eventId")
            if not eid:
                continue
            rows_events.append({
                "event_id": eid,
                "name": ev.get("eventName"),
                "sport": ev.get("sport") or "horse_racing",
                "venue": ev.get("venue"),
                "country": None,
                "start_iso": ev.get("start") or ev.get("eventDate"),
                "status": ev.get("status"),
                "result_status": ev.get("resultStatus"),
                "competitors_json": None,
                "source": "tote_discover",
                "comp": None, "home": None, "away": None,
            })
            for prod in ((ev.get("products") or {}).get("nodes") or []):
                pid = prod.get("productId")
                if not pid:
                    continue
                rows_products.append({
                    "product_id": pid,
                    "event_id": eid,
                    "bet_type": (prod.get("betType") or '').upper(),
                    "status": prod.get("status"),
                    # store country in currency for filtering consistency
                    "currency": prod.get("currency"),
                    "start_iso": prod.get("start") or ev.get("start"),
                    "total_gross": None,
                    "total_net": None,
                    "event_name": ev.get("eventName"),
                    "venue": ev.get("venue"),
                    "rollover": None,
                    "deduction_rate": None,
                    "source": "tote_discover",
                })
                for leg in ((prod.get("legs") or {}).get("nodes") or []):
                    product_leg_id = leg.get("productLegId")
                    for sel in ((leg.get("selections") or {}).get("nodes") or []):
                        comp = (sel.get("competitor") or {})
                        det = (comp.get("details") or {})
                        num = det.get("clothNumber") if "clothNumber" in det else det.get("trapNumber")
                        rows_selections.append({
                            "product_id": pid,
                            "leg_index": 1,
                            "selection_id": sel.get("selectionId"),
                            "product_leg_id": product_leg_id,
                            "competitor": comp.get("name"),
                            "number": int(num) if isinstance(num, (int, float, str)) and str(num).isdigit() else None,
                            "leg_event_id": eid,
                            "leg_event_name": ev.get("eventName"),
                            "leg_venue": ev.get("venue"),
                            "leg_start_iso": ev.get("start") or ev.get("eventDate"),
                        })
        if rows_events:
            sink.upsert_tote_events(rows_events)
        if rows_products:
            sink.upsert_tote_products(rows_products)
        if rows_selections:
            sink.upsert_tote_product_selections(rows_selections)
        print(f"Inserted via Discover: events={len(rows_events)} products={len(rows_products)} selections={len(rows_selections)}")


if __name__ == "__main__":
    main()
