#!/usr/bin/env python3
"""Debug script to check raw odds data structure"""

from sports.bq import get_bq_sink

def main():
    sink = get_bq_sink()
    win_product_id = "83cb6ccf-e530-4db4-8c2e-da8df08004ec"
    
    print("=== Raw Odds Data Structure Debug ===")
    
    # Get a sample of raw odds data
    print(f"\n1. Sample Raw Odds Data:")
    try:
        result = sink.query(f"""
        SELECT 
            payload,
            TIMESTAMP_MILLIS(fetched_ts) AS ts
        FROM `autobet-470818.autobet.raw_tote_probable_odds`
        WHERE TIMESTAMP_MILLIS(fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ORDER BY fetched_ts DESC
        LIMIT 1
        """)
        
        for row in result:
            print(f"  Latest fetch: {row.ts}")
            # Print a small sample of the payload
            payload_str = str(row.payload)[:500] + "..." if len(str(row.payload)) > 500 else str(row.payload)
            print(f"  Payload sample: {payload_str}")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Check if the WIN product appears in any raw odds
    print(f"\n2. WIN Product in Raw Odds:")
    try:
        result = sink.query(f"""
        SELECT 
            COUNT(*) as count,
            MAX(TIMESTAMP_MILLIS(fetched_ts)) as latest_fetch
        FROM `autobet-470818.autobet.raw_tote_probable_odds`
        WHERE TIMESTAMP_MILLIS(fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND JSON_EXTRACT(payload, '$.products.nodes') IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM UNNEST(JSON_EXTRACT_ARRAY(payload, '$.products.nodes')) AS prod
            WHERE JSON_EXTRACT_SCALAR(prod, '$.id') = '{win_product_id}'
          )
        """)
        
        for row in result:
            print(f"  Records with WIN product: {row.count}, Latest: {row.latest_fetch}")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Try a simpler approach to find odds
    print(f"\n3. Simple Odds Search:")
    try:
        result = sink.query(f"""
        SELECT 
            SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) AS product_id,
            JSON_EXTRACT_SCALAR(prod, '$.id') AS raw_id,
            TIMESTAMP_MILLIS(r.fetched_ts) AS ts
        FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
        UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod
        WHERE TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND JSON_EXTRACT_SCALAR(prod, '$.id') = '{win_product_id}'
        ORDER BY ts DESC
        LIMIT 5
        """)
        
        count = 0
        for row in result:
            count += 1
            print(f"  Found product: {row.product_id} (raw: {row.raw_id}) at {row.ts}")
        
        if count == 0:
            print("  No matching product found in raw odds!")
        else:
            print(f"  Found {count} matching records")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
