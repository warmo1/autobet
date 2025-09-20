#!/usr/bin/env python3
"""Debug script to check WIN product odds data"""

from sports.bq import get_bq_sink

def main():
    sink = get_bq_sink()
    win_product_id = "83cb6ccf-e530-4db4-8c2e-da8df08004ec"
    
    print("=== WIN Product Odds Debug ===")
    
    # Get the actual odds data for the WIN product
    print(f"\n1. Odds for WIN Product {win_product_id}:")
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
        ORDER BY ts DESC
        LIMIT 20
        """)
        
        count = 0
        for row in result:
            count += 1
            print(f"  {row.selection_id}: {row.decimal_odds} (at {row.ts})")
        
        if count == 0:
            print("  No odds found!")
        else:
            print(f"  Found {count} odds records")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Check the horse names for these selection IDs
    print(f"\n2. Horse Names for Selection IDs:")
    try:
        result = sink.query(f"""
        SELECT 
            s.selection_id,
            s.number,
            s.competitor
        FROM `autobet-470818.autobet.tote_product_selections` s
        WHERE s.product_id = '{win_product_id}'
        ORDER BY SAFE_CAST(s.number AS INT64)
        """)
        
        for row in result:
            print(f"  {row.number}: {row.competitor} ({row.selection_id})")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
