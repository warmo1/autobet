
from __future__ import annotations

import datetime
import json
from typing import Any, Dict, Optional

from sports.providers.tote_api import ToteClient, store_raw

# GraphQL query to fetch upcoming racing events and their products.
# This is a best-effort query based on observed patterns.
# It fetches events for HorseRacing, their products (WIN, PLACE, SUPERFECTA),
# and the selections (runners) for each product.
RACING_NEXT_OFF_QUERY = """
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
                        ... on HorseDetails {
                          clothNumber
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
}
"""


def ingest_racing_next_off(db: Any, dry_run: bool = False) -> int:
    """
    Ingests upcoming HorseRacing data from the Tote API into BigQuery.
    """
    print("Starting ingest_racing_next_off")
    client = ToteClient()
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    variables = {"sport": "HorseRacing", "date": today, "limit": 25}

    try:
        data = client.graphql(RACING_NEXT_OFF_QUERY, variables)
        print(f"Successfully fetched data from Tote API for date: {today}")
    except Exception as e:
        print(f"Failed to fetch data from Tote API: {e}")
        return 0

    events = (data.get("discover", {}).get("events", {}).get("nodes", []))
    if not events:
        print("No events found for today.")
        return 0

    print(f"Found {len(events)} events to process.")

    # For BigQuery, we'll collect rows and use load_table_from_json
    rows_events = []
    rows_products = []
    rows_selections = []

    for event in events:
        event_id = event.get("eventId")
        if not event_id:
            continue

        rows_events.append({
            "event_id": event_id,
            "event_name": event.get("eventName"),
            "event_date": event.get("eventDate"),
            "venue": event.get("venue"),
            "race_number": event.get("raceNumber"),
            "sport": event.get("sport"),
            "status": event.get("status"),
            "result_status": event.get("resultStatus"),
            "start_iso": event.get("start"),
            "raw_id": None, # Raw ID would be stored separately if needed
        })

        for product in event.get("products", {}).get("nodes", []):
            product_id = product.get("productId")
            if not product_id:
                continue

            rows_products.append({
                "product_id": product_id,
                "event_id": event_id,
                "bet_type": product.get("betType"),
                "status": product.get("status"),
                "currency": product.get("currency"),
                "start_iso": product.get("start"),
                "raw_id": None,
            })

            for leg in product.get("legs", {}).get("nodes", []):
                product_leg_id = leg.get("productLegId")
                for selection in leg.get("selections", {}).get("nodes", []):
                    selection_id = selection.get("selectionId")
                    competitor = selection.get("competitor", {})
                    details = competitor.get("details", {})
                    number = details.get("clothNumber") if details else None

                    rows_selections.append({
                        "product_id": product_id,
                        "leg_index": 1, # Assuming single leg for now
                        "selection_id": selection_id,
                        "product_leg_id": product_leg_id,
                        "competitor": competitor.get("name"),
                        "number": int(number) if number is not None else None,
                    })

    if dry_run:
        print(f"Dry run: Would insert {len(rows_events)} events, {len(rows_products)} products, {len(rows_selections)} selections.")
        return len(rows_events)

    # Insert data into BigQuery
    try:
        if rows_events:
            print(f"Inserting {len(rows_events)} rows into tote_events")
            db.load_table_from_json("tote_events", rows_events, replace=True)
        if rows_products:
            print(f"Inserting {len(rows_products)} rows into tote_products")
            db.load_table_from_json("tote_products", rows_products, replace=True)
        if rows_selections:
            print(f"Inserting {len(rows_selections)} rows into tote_product_selections")
            db.load_table_from_json("tote_product_selections", rows_selections, replace=True)
        
        print("Successfully inserted data into BigQuery.")
        return len(rows_events)
    except Exception as e:
        print(f"Failed to insert data into BigQuery: {e}")
        return 0
