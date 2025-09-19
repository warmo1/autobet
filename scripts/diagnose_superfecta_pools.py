#!/usr/bin/env python3
"""
Superfecta Pool Diagnostic Script

This script helps diagnose why Superfecta pools are showing 0.00 values.
It checks both the API response and the database data.

Usage:
  python scripts/diagnose_superfecta_pools.py --product-id b38870ed-1647-436e-8a8b-b4c0fbfc8b1a
  python scripts/diagnose_superfecta_pools.py --event-id HORSERACING-NEWCASTLE-GB-2025-09-19-1915
"""

import argparse
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.db import get_db
from sports.providers.tote_api import ToteClient
from sports.config import cfg


def check_api_pool_data(product_id: str) -> Dict[str, Any]:
    """Check pool data directly from Tote API."""
    print(f"🔍 Checking API pool data for product: {product_id}")
    
    client = ToteClient()
    
    query = """
    query GetProductPool($id: String!) {
      product(id: $id) {
        id
        name
        type {
          ... on BettingProduct {
            betType { code }
            selling { status }
            pool {
              total { 
                grossAmount { decimalAmount }
                netAmount { decimalAmount }
              }
              carryIn { 
                grossAmount { decimalAmount }
                netAmount { decimalAmount }
              }
              takeout { 
                percentage
                amount { decimalAmount }
              }
            }
          }
        }
      }
    }
    """
    
    try:
        data = client.graphql(query, {"id": product_id})
        product = data.get("product", {})
        
        if not product:
            return {"error": "Product not found in API"}
        
        # Data is now under product.type
        product_type = product.get("type", {})
        bet_type = product_type.get("betType", {}).get("code", "UNKNOWN")
        status = product_type.get("selling", {}).get("status", "UNKNOWN")
        pool = product_type.get("pool", {})
        
        total = pool.get("total", {})
        gross = total.get("grossAmount", {}).get("decimalAmount")
        net = total.get("netAmount", {}).get("decimalAmount")
        
        carry_in = pool.get("carryIn", {})
        rollover = carry_in.get("grossAmount", {}).get("decimalAmount")
        
        takeout = pool.get("takeout", {}).get("percentage")
        
        return {
            "product_id": product_id,
            "bet_type": bet_type,
            "status": status,
            "gross": gross,
            "net": net,
            "rollover": rollover,
            "takeout": takeout,
            "raw_pool": pool
        }
        
    except Exception as e:
        return {"error": f"API error: {e}"}


def check_database_pool_data(product_id: str) -> Dict[str, Any]:
    """Check pool data in BigQuery database."""
    print(f"🗄️  Checking database pool data for product: {product_id}")
    
    db = get_db()
    
    # Check latest pool snapshot
    query = """
    SELECT 
        product_id,
        total_gross,
        total_net,
        rollover,
        ts_ms,
        created_at
    FROM `autobet-470818.autobet.tote_pool_snapshots`
    WHERE product_id = ?
    ORDER BY ts_ms DESC
    LIMIT 1
    """
    
    try:
        rows = list(db.query(query, (product_id,)))
        if not rows:
            return {"error": "No pool snapshots found in database"}
        
        row = rows[0]
        return {
            "product_id": row.product_id,
            "total_gross": row.total_gross,
            "total_net": row.total_net,
            "rollover": row.rollover,
            "ts_ms": row.ts_ms,
            "created_at": row.created_at
        }
        
    except Exception as e:
        return {"error": f"Database error: {e}"}


def check_product_data(product_id: str) -> Dict[str, Any]:
    """Check product data in BigQuery database."""
    print(f"📊 Checking product data for product: {product_id}")
    
    db = get_db()
    
    query = """
    SELECT 
        product_id,
        bet_type,
        status,
        total_gross,
        total_net,
        rollover,
        deduction_rate,
        event_id,
        start_iso
    FROM `autobet-470818.autobet.vw_products_latest_totals`
    WHERE product_id = ?
    """
    
    try:
        rows = list(db.query(query, (product_id,)))
        if not rows:
            return {"error": "Product not found in database"}
        
        row = rows[0]
        return {
            "product_id": row.product_id,
            "bet_type": row.bet_type,
            "status": row.status,
            "total_gross": row.total_gross,
            "total_net": row.total_net,
            "rollover": row.rollover,
            "deduction_rate": row.deduction_rate,
            "event_id": row.event_id,
            "start_iso": row.start_iso
        }
        
    except Exception as e:
        return {"error": f"Database error: {e}"}


def find_superfecta_products_for_event(event_id: str) -> list:
    """Find all Superfecta products for an event."""
    print(f"🔍 Finding Superfecta products for event: {event_id}")
    
    db = get_db()
    
    query = """
    SELECT 
        product_id,
        bet_type,
        status,
        total_gross,
        total_net,
        rollover
    FROM `autobet-470818.autobet.vw_products_latest_totals`
    WHERE event_id = ? AND UPPER(bet_type) = 'SUPERFECTA'
    ORDER BY product_id
    """
    
    try:
        rows = list(db.query(query, (event_id,)))
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"❌ Error finding products: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Diagnose Superfecta Pool Issues")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--product-id", help="Specific product ID to check")
    group.add_argument("--event-id", help="Event ID to check all Superfecta products")
    
    args = parser.parse_args()
    
    if not cfg.bq_project or not cfg.bq_dataset:
        print("❌ BigQuery not configured. Set BQ_PROJECT and BQ_DATASET")
        sys.exit(1)
    
    if not cfg.tote_api_key or not cfg.tote_graphql_url:
        print("❌ Tote API not configured. Set TOTE_API_KEY and TOTE_GRAPHQL_URL")
        sys.exit(1)
    
    if args.product_id:
        print(f"\n{'='*60}")
        print(f"DIAGNOSING PRODUCT: {args.product_id}")
        print(f"{'='*60}")
        
        # Check API data
        api_data = check_api_pool_data(args.product_id)
        print(f"\n📡 API Data:")
        if "error" in api_data:
            print(f"❌ {api_data['error']}")
        else:
            print(f"  Bet Type: {api_data['bet_type']}")
            print(f"  Status: {api_data['status']}")
            print(f"  Gross: {api_data['gross']}")
            print(f"  Net: {api_data['net']}")
            print(f"  Rollover: {api_data['rollover']}")
            print(f"  Takeout: {api_data['takeout']}%")
        
        # Check database data
        db_data = check_database_pool_data(args.product_id)
        print(f"\n🗄️  Database Pool Snapshots:")
        if "error" in db_data:
            print(f"❌ {db_data['error']}")
        else:
            print(f"  Gross: {db_data['total_gross']}")
            print(f"  Net: {db_data['total_net']}")
            print(f"  Rollover: {db_data['rollover']}")
            print(f"  Last Update: {db_data['created_at']}")
        
        # Check product data
        product_data = check_product_data(args.product_id)
        print(f"\n📊 Database Product Data:")
        if "error" in product_data:
            print(f"❌ {product_data['error']}")
        else:
            print(f"  Bet Type: {product_data['bet_type']}")
            print(f"  Status: {product_data['status']}")
            print(f"  Gross: {product_data['total_gross']}")
            print(f"  Net: {product_data['total_net']}")
            print(f"  Rollover: {product_data['rollover']}")
            print(f"  Event: {product_data['event_id']}")
        
        # Analysis
        print(f"\n🔍 Analysis:")
        if "error" not in api_data and api_data.get("gross") is not None:
            if api_data["gross"] == 0 and api_data["net"] == 0:
                print("⚠️  API shows zero pool values - this may be normal for closed/empty pools")
            else:
                print("✅ API shows non-zero pool values")
        else:
            print("❌ API data unavailable")
        
        if "error" not in product_data and product_data.get("total_gross") is not None:
            if product_data["total_gross"] == 0 and product_data["total_net"] == 0:
                print("⚠️  Database shows zero pool values")
            else:
                print("✅ Database shows non-zero pool values")
        else:
            print("❌ Database data unavailable")
    
    elif args.event_id:
        print(f"\n{'='*60}")
        print(f"DIAGNOSING EVENT: {args.event_id}")
        print(f"{'='*60}")
        
        products = find_superfecta_products_for_event(args.event_id)
        
        if not products:
            print("❌ No Superfecta products found for this event")
            return
        
        print(f"Found {len(products)} Superfecta products:")
        
        for i, product in enumerate(products, 1):
            print(f"\n{i}. Product: {product['product_id']}")
            print(f"   Status: {product['status']}")
            print(f"   Gross: {product['total_gross']}")
            print(f"   Net: {product['total_net']}")
            print(f"   Rollover: {product['rollover']}")
            
            if product['total_gross'] == 0 and product['total_net'] == 0:
                print("   ⚠️  Zero pool values detected")
            else:
                print("   ✅ Non-zero pool values")


if __name__ == "__main__":
    main()
