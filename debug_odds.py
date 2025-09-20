#!/usr/bin/env python3
"""Debug script to check odds data"""

from sports.bq import get_bq_sink

def main():
    sink = get_bq_sink()
    event_id = "HORSERACING-AYR-GB-2025-09-19-1228"
    
    print("=== Odds Data Debug ===")
    
    # Get the WIN product ID for this event
    print("\n1. WIN Product ID:")
    try:
        result = sink.query(f"""
        SELECT product_id
        FROM `autobet-470818.autobet.tote_products`
        WHERE event_id = '{event_id}' AND UPPER(bet_type) = 'WIN'
        """)
        
        win_product_id = None
        for row in result:
            win_product_id = row.product_id
            print(f"  WIN Product ID: {win_product_id}")
        
        if not win_product_id:
            print("  No WIN product found!")
            return
            
    except Exception as e:
        print(f"Error: {e}")
        return
    
    # Check recent odds data for this product
    print(f"\n2. Recent Odds for Product {win_product_id}:")
    try:
        result = sink.query(f"""
        SELECT 
            SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) AS product_id,
            COALESCE(
              JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId'),
              JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId')
            ) AS selection_id,
            SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds.decimal') AS FLOAT64) AS decimal_odds,
            TIMESTAMP_MILLIS(r.fetched_ts) AS ts
        FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
        UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
        UNNEST(IFNULL(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes'),
                      JSON_EXTRACT_ARRAY(prod, '$.lines'))) AS line
        WHERE SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) = '{win_product_id}'
          AND JSON_EXTRACT_SCALAR(line, '$.odds.decimal') IS NOT NULL
          AND TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ORDER BY ts DESC
        LIMIT 10
        """)
        
        count = 0
        for row in result:
            count += 1
            print(f"  {row.selection_id}: {row.decimal_odds} (at {row.ts})")
        
        if count == 0:
            print("  No odds found for this product!")
        else:
            print(f"  Found {count} odds records")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Check if there are any odds at all
    print(f"\n3. Any Recent Odds Data:")
    try:
        result = sink.query(f"""
        SELECT 
            COUNT(*) as count,
            MAX(TIMESTAMP_MILLIS(fetched_ts)) as latest_fetch
        FROM `autobet-470818.autobet.raw_tote_probable_odds`
        WHERE TIMESTAMP_MILLIS(fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        """)
        
        for row in result:
            print(f"  Total recent odds records: {row.count}, Latest: {row.latest_fetch}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
