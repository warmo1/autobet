#!/usr/bin/env python3
"""
Manual Superfecta Pool Import Script

This script manually imports Superfecta pool data from the Tote API to fix
pool data issues. It can import by product ID, event ID, or date range.

Usage:
  # Import specific product
  python scripts/manual_superfecta_import.py --product-id b38870ed-1647-436e-8a8b-b4c0fbfc8b1a
  
  # Import all Superfecta products for today
  python scripts/manual_superfecta_import.py --date 2025-09-19
  
  # Import all Superfecta products for an event
  python scripts/manual_superfecta_import.py --event-id HORSERACING-NEWCASTLE-GB-2025-09-19-1915
"""

import argparse
import sys
import json
import time
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.db import get_db
from sports.providers.tote_api import ToteClient
from sports.ingest.tote_products import ingest_products
from sports.config import cfg


def import_superfecta_by_product_id(product_id: str) -> int:
    """Import a specific Superfecta product by ID."""
    print(f"Importing Superfecta product: {product_id}")
    
    db = get_db()
    client = ToteClient()
    
    try:
        # Use the existing ingest function with specific product ID
        n = ingest_products(
            db=db,
            client=client,
            date_iso=None,
            status=None,
            first=1,
            bet_types=["SUPERFECTA"],
            product_ids=[product_id]
        )
        print(f"✅ Successfully imported {n} Superfecta product(s)")
        return n
    except Exception as e:
        print(f"❌ Error importing product {product_id}: {e}")
        return 0


def import_superfecta_by_event_id(event_id: str) -> int:
    """Import all Superfecta products for a specific event."""
    print(f"Importing Superfecta products for event: {event_id}")
    
    db = get_db()
    client = ToteClient()
    
    # First, get all products for the event
    query = """
    query GetEventProducts($eventId: String!) {
      event(id: $eventId) {
        products {
          nodes {
            id
            ... on BettingProduct {
              betType { code }
              selling { status }
            }
          }
        }
      }
    }
    """
    
    try:
        data = client.graphql(query, {"eventId": event_id})
        products = data.get("event", {}).get("products", {}).get("nodes", [])
        
        superfecta_products = [
            p["id"] for p in products 
            if p.get("betType", {}).get("code") == "SUPERFECTA"
        ]
        
        if not superfecta_products:
            print(f"❌ No Superfecta products found for event {event_id}")
            return 0
        
        print(f"Found {len(superfecta_products)} Superfecta products for event")
        
        total_imported = 0
        for product_id in superfecta_products:
            n = import_superfecta_by_product_id(product_id)
            total_imported += n
        
        print(f"✅ Total imported: {total_imported} Superfecta products")
        return total_imported
        
    except Exception as e:
        print(f"❌ Error importing event {event_id}: {e}")
        return 0


def import_superfecta_by_date(date_iso: str) -> int:
    """Import all Superfecta products for a specific date."""
    print(f"Importing Superfecta products for date: {date_iso}")
    
    db = get_db()
    client = ToteClient()
    
    total_imported = 0
    
    # Import both OPEN and CLOSED products
    for status in ["OPEN", "CLOSED"]:
        try:
            print(f"Importing {status} Superfecta products...")
            n = ingest_products(
                db=db,
                client=client,
                date_iso=date_iso,
                status=status,
                first=500,
                bet_types=["SUPERFECTA"]
            )
            print(f"✅ Imported {n} {status} Superfecta products")
            total_imported += n
        except Exception as e:
            print(f"❌ Error importing {status} products: {e}")
    
    print(f"✅ Total imported: {total_imported} Superfecta products")
    return total_imported


def check_pool_data(product_id: str) -> None:
    """Check current pool data for a product."""
    print(f"Checking pool data for product: {product_id}")
    
    client = ToteClient()
    
    query = """
    query GetProductPool($id: String!) {
      product(id: $id) {
        id
        ... on BettingProduct {
          betType { code }
          selling { status }
          pool {
            total { 
              grossAmounts { decimalAmount currency { code } }
              netAmounts { decimalAmount currency { code } }
            }
            carryIn { 
              grossAmounts { decimalAmount currency { code } }
              netAmounts { decimalAmount currency { code } }
            }
            guarantee { 
              grossAmounts { decimalAmount currency { code } }
              netAmounts { decimalAmount currency { code } }
            }
            takeout { percentage }
          }
        }
      }
    }
    """
    
    try:
        data = client.graphql(query, {"id": product_id})
        product = data.get("product", {})
        
        if not product:
            print(f"❌ Product {product_id} not found")
            return
        
        bet_type = product.get("betType", {}).get("code", "UNKNOWN")
        status = product.get("selling", {}).get("status", "UNKNOWN")
        pool = product.get("pool", {})
        
        print(f"Product: {product_id}")
        print(f"Bet Type: {bet_type}")
        print(f"Status: {status}")
        
        total = pool.get("total", {})
        gross_amounts = total.get("grossAmounts", [])
        net_amounts = total.get("netAmounts", [])
        
        gross = gross_amounts[0].get("decimalAmount") if gross_amounts else None
        net = net_amounts[0].get("decimalAmount") if net_amounts else None
        
        carry_in = pool.get("carryIn", {})
        carry_in_gross = carry_in.get("grossAmounts", [])
        rollover = carry_in_gross[0].get("decimalAmount") if carry_in_gross else None
        
        takeout = pool.get("takeout", {}).get("percentage")
        
        print(f"Pool Data:")
        print(f"  Gross: {gross}")
        print(f"  Net: {net}")
        print(f"  Rollover: {rollover}")
        print(f"  Takeout: {takeout}%")
        
        if gross is None or net is None:
            print("⚠️  Pool data is missing - this indicates an API issue")
        elif gross == 0 and net == 0:
            print("⚠️  Pool data is zero - this may indicate the pool is not active")
        else:
            print("✅ Pool data looks good")
            
    except Exception as e:
        print(f"❌ Error checking pool data: {e}")


def main():
    parser = argparse.ArgumentParser(description="Manual Superfecta Pool Import")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--product-id", help="Specific product ID to import")
    group.add_argument("--event-id", help="Event ID to import all Superfecta products")
    group.add_argument("--date", help="Date (YYYY-MM-DD) to import all Superfecta products")
    
    parser.add_argument("--check-only", action="store_true", help="Only check pool data, don't import")
    
    args = parser.parse_args()
    
    if not cfg.bq_project or not cfg.bq_dataset:
        print("❌ BigQuery not configured. Set BQ_PROJECT and BQ_DATASET")
        sys.exit(1)
    
    if not cfg.tote_api_key or not cfg.tote_graphql_url:
        print("❌ Tote API not configured. Set TOTE_API_KEY and TOTE_GRAPHQL_URL")
        sys.exit(1)
    
    if args.check_only:
        if args.product_id:
            check_pool_data(args.product_id)
        else:
            print("❌ --check-only requires --product-id")
            sys.exit(1)
        return
    
    total_imported = 0
    
    if args.product_id:
        total_imported = import_superfecta_by_product_id(args.product_id)
    elif args.event_id:
        total_imported = import_superfecta_by_event_id(args.event_id)
    elif args.date:
        total_imported = import_superfecta_by_date(args.date)
    
    if total_imported > 0:
        print(f"\n🎉 Successfully imported {total_imported} Superfecta product(s)")
        print("Pool data should now be updated in your web app")
    else:
        print("\n❌ No products were imported")


if __name__ == "__main__":
    main()
