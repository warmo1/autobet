from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..providers.tote_api import ToteClient
from ..bq import BigQuerySink

PRODUCTS_QUERY = """
query Products($date: Date, $betTypes: [BetTypeCode!], $status: BettingProductSellingStatus, $first: Int){
  products(date:$date, betTypes:$betTypes, sellingStatus:$status, first:$first){
    nodes{
      id
      name
      ... on BettingProduct {
        country { alpha2Code }
        betType { code }
        selling { status }
        pool { total { grossAmount { decimalAmount } netAmount { decimalAmount } } }
        legs {
          nodes {
            event {
              id
              name
              venue { name }
              scheduledStartDateTime { iso8601 }
            }
          }
        }
      }
    }
  }
}
"""

def ingest_products(db: BigQuerySink, client: ToteClient, date_iso: str, status: str, first: int, bet_types: list[str] | None) -> int:
    """
    Ingests product data from the Tote API into BigQuery.
    """
    print(f"Ingesting products for date {date_iso}, status {status}, bet_types {bet_types}")
    variables = {
        "date": date_iso,
        "status": status,
        "first": first,
    }
    if bet_types:
        variables["betTypes"] = bet_types
    try:
        data = client.graphql(PRODUCTS_QUERY, variables)
        print(f"Successfully fetched product data from Tote API for date: {date_iso}")
    except Exception as e:
        print(f"Failed to fetch product data from Tote API: {e}")
        return 0

    products_nodes = (data.get("products", {}).get("nodes", []))
    if not products_nodes:
        print("No products found.")
        return 0

    rows_products: List[Dict[str, Any]] = []
    rows_selections: List[Dict[str, Any]] = []
    rows_events: List[Dict[str, Any]] = []
    rows_dividends: List[Dict[str, Any]] = []
    for p in products_nodes:
        # Map current schema (BettingProduct fragment)
        bp = p or {}
        legs = ((bp.get("legs") or {}).get("nodes") or []) if "legs" in bp else []
        event = None
        if legs:
            evt = (legs[0].get("event") or {})
            event = {
                "id": evt.get("id"),
                "name": evt.get("name"),
                "venue": ((evt.get("venue") or {}).get("name")),
                "start_iso": ((evt.get("scheduledStartDateTime") or {}).get("iso8601")),
            }
            # Collect event row (minimal) for tote_events
            rows_events.append({
                "event_id": event.get("id"),
                "name": event.get("name"),
                "sport": "horse_racing",
                "venue": event.get("venue"),
                "country": ((bp.get("country") or {}).get("alpha2Code")),
                "start_iso": event.get("start_iso"),
                "status": None,
                "result_status": (bp.get("result") or {}).get("status"),
                "competitors_json": None,
                "comp": None,
                "home": None,
                "away": None,
                "source": "tote_api",
            })
        # Pool totals
        pool = (bp.get("pool") or {})
        total = (pool.get("total") or {})
        gross = ((total.get("grossAmount") or {}).get("decimalAmount"))
        net = ((total.get("netAmount") or {}).get("decimalAmount"))
        # Product fields
        bet_type = ((bp.get("betType") or {}).get("code"))
        status_v = ((bp.get("selling") or {}).get("status"))
        country = ((bp.get("country") or {}).get("alpha2Code"))

        rows_products.append({
            "product_id": bp.get("id"),
            "event_id": (event or {}).get("id"),
            "bet_type": bet_type,
            "status": status_v,
            # Using alpha2 country code in currency column to preserve existing filters
            "currency": country,
            "start_iso": (event or {}).get("start_iso"),
            "total_gross": float(gross) if gross is not None else None,
            "total_net": float(net) if net is not None else None,
            # Rollover/deduction not exposed in this query variant
            "rollover": None,
            "deduction_rate": None,
            "event_name": (event or {}).get("name"),
            "venue": (event or {}).get("venue"),
            "source": "tote_api",
        })

        # Collect selections for each leg
        for idx, leg in enumerate(legs, start=1):
            product_leg_id = leg.get("productLegId") or leg.get("id")
            levt = leg.get("event") or {}
            l_ev_id = levt.get("id")
            l_ev_name = levt.get("name")
            l_ev_venue = ((levt.get("venue") or {}).get("name"))
            l_ev_start = ((levt.get("scheduledStartDateTime") or {}).get("iso8601"))
            sels = ((leg.get("selections") or {}).get("nodes")) or []
            for sel in sels:
                sid = sel.get("id")
                comp = (sel.get("competitor") or {})
                det = (comp.get("details") or {})
                n = None
                try:
                    if det.get("__typename") == "HorseDetails":
                        n = det.get("clothNumber")
                    elif det.get("__typename") == "GreyhoundDetails":
                        n = det.get("trapNumber")
                except Exception:
                    n = None
                rows_selections.append({
                    "product_id": bp.get("id"),
                    "leg_index": idx,
                    "selection_id": sid,
                    "product_leg_id": product_leg_id,
                    "competitor": comp.get("name"),
                    "number": int(n) if n is not None else None,
                    "leg_event_id": l_ev_id,
                    "leg_event_name": l_ev_name,
                    "leg_venue": l_ev_venue,
                    "leg_start_iso": l_ev_start,
                })

        # Dividends (if present)
        res = (bp.get("result") or {})
        dvs = ((res.get("dividends") or {}).get("nodes")) or []
        # Use event start time or current time as ts
        import time as _t
        ts_iso = (event or {}).get("start_iso") or _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime())
        for d in dvs:
            amt = (((d.get("dividend") or {}).get("amount") or {}).get("decimalAmount"))
            dlegs = ((d.get("dividendLegs") or {}).get("nodes")) or []
            for dl in dlegs:
                dsel = ((dl.get("dividendSelections") or {}).get("nodes")) or []
                for ds in dsel:
                    sel_id = ds.get("id")
                    if sel_id and amt is not None:
                        rows_dividends.append({
                            "product_id": bp.get("id"),
                            "selection": sel_id,
                            "dividend": float(amt),
                            "ts": ts_iso,
                        })
    
    try:
        if rows_products:
            print(f"Inserting {len(rows_products)} rows into tote_products")
            db.upsert_tote_products(rows_products)
        if rows_selections:
            print(f"Inserting {len(rows_selections)} rows into tote_product_selections")
            db.upsert_tote_product_selections(rows_selections)
        if rows_dividends:
            print(f"Inserting {len(rows_dividends)} rows into tote_product_dividends")
            db.upsert_tote_product_dividends(rows_dividends)
        if rows_events:
            # De-dup rows by event_id to reduce upsert work
            dedup = {}
            for r in rows_events:
                if r.get("event_id"):
                    dedup[r["event_id"]] = r
            ev_rows = list(dedup.values()) if dedup else rows_events
            print(f"Inserting {len(ev_rows)} rows into tote_events")
            db.upsert_tote_events(ev_rows)
        print("Successfully ingested product data.")
        return len(rows_products)
    except Exception as e:
        print(f"Failed to insert product data into BigQuery: {e}")
        return 0
