from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from sports.providers.tote_api import ToteClient
from sports.bq import BigQuerySink

PRODUCTS_QUERY = """
query Products($date: Date, $betTypes: [BetTypeCode!], $status: BettingProductSellingStatus, $first: Int){
  products(date:$date, betTypes:$betTypes, sellingStatus:$status, first:$first){
    nodes{
      id
      eventId
      betType
      status
      currency
      start
      total{
        grossAmounts{ decimalAmount }
        netAmounts{ decimalAmount }
      }
      rollover
      deductionRate
      event{
        name
        venue
      }
    }
  }
}
"""

def ingest_products(db: BigQuerySink, client: ToteClient, date_iso: str, status: str, first: int, bet_types: list[str]) -> int:
    """
    Ingests product data from the Tote API into BigQuery.
    """
    print(f"Ingesting products for date {date_iso}, status {status}, bet_types {bet_types}")
    variables = {
        "date": date_iso,
        "betTypes": bet_types,
        "status": status,
        "first": first,
    }
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

    rows_products = []
    for p in products_nodes:
        event_name = None
        venue = None
        if p.get("event"):
            event_name = p["event"].get("name")
            venue = p["event"].get("venue")

        total_gross = None
        if p.get("total") and p["total"].get("grossAmounts"):
            total_gross = p["total"]["grossAmounts"][0].get("decimalAmount")
        
        total_net = None
        if p.get("total") and p["total"].get("netAmounts"):
            total_net = p["total"]["netAmounts"][0].get("decimalAmount")

        rows_products.append({
            "product_id": p.get("id"),
            "event_id": p.get("eventId"),
            "bet_type": p.get("betType"),
            "status": p.get("status"),
            "currency": p.get("currency"),
            "start_iso": p.get("start"),
            "total_gross": float(total_gross) if total_gross is not None else None,
            "total_net": float(total_net) if total_net is not None else None,
            "rollover": float(p.get("rollover")) if p.get("rollover") is not None else None,
            "deduction_rate": float(p.get("deductionRate")) if p.get("deductionRate") is not None else None,
            "event_name": event_name,
            "venue": venue,
            "source": "tote_api",
        })
    
    try:
        if rows_products:
            print(f"Inserting {len(rows_products)} rows into tote_products")
            db.upsert_tote_products(rows_products)
        print("Successfully ingested product data.")
        return len(rows_products)
    except Exception as e:
        print(f"Failed to insert product data into BigQuery: {e}")
        return 0
