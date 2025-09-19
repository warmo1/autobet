#!/usr/bin/env python3
"""
Simple test script to check Tote API pool data without full environment setup.
"""

import os
import sys
import json
import requests
from pathlib import Path

# Add the project root to Python path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def test_tote_api():
    """Test Tote API directly with requests."""
    
    # Get API credentials from environment
    api_key = os.getenv('TOTE_API_KEY')
    graphql_url = os.getenv('TOTE_GRAPHQL_URL')
    
    if not api_key or not graphql_url:
        print("❌ Missing TOTE_API_KEY or TOTE_GRAPHQL_URL environment variables")
        print("Please set these in your .env file or environment")
        return
    
    product_id = "b38870ed-1647-436e-8a8b-b4c0fbfc8b1a"
    
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
    
    headers = {
        "Authorization": f"Api-Key {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "query": query,
        "variables": {"id": product_id}
    }
    
    print(f"🔍 Testing Tote API for product: {product_id}")
    print(f"📡 GraphQL URL: {graphql_url}")
    
    try:
        response = requests.post(graphql_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if "errors" in data:
            print("❌ GraphQL Errors:")
            for error in data["errors"]:
                print(f"  - {error.get('message', 'Unknown error')}")
            return
        
        product = data.get("data", {}).get("product", {})
        
        if not product:
            print("❌ Product not found in API response")
            return
        
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
        
        print(f"\n✅ API Response:")
        print(f"  Product ID: {product_id}")
        print(f"  Bet Type: {bet_type}")
        print(f"  Status: {status}")
        print(f"  Gross Pool: {gross}")
        print(f"  Net Pool: {net}")
        print(f"  Rollover: {rollover}")
        print(f"  Takeout: {takeout}%")
        
        if gross is None or net is None:
            print("\n⚠️  Pool data is missing from API response")
        elif gross == 0 and net == 0:
            print("\n⚠️  Pool data shows zero values - this may be normal for closed/empty pools")
        else:
            print("\n✅ Pool data looks good")
            
        # Show raw pool data for debugging
        print(f"\n🔍 Raw Pool Data:")
        print(json.dumps(pool, indent=2))
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API Request failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    test_tote_api()
