#!/usr/bin/env python3
"""Debug script to check lines structure in odds data"""

from sports.bq import get_bq_sink

def main():
    sink = get_bq_sink()
    win_product_id = "83cb6ccf-e530-4db4-8c2e-da8df08004ec"
    
    print("=== Lines Structure Debug ===")
    
    # Get the lines structure for the WIN product
    print(f"\n1. Lines Structure for WIN Product:")
    try:
        result = sink.query(f"""
        SELECT 
            JSON_EXTRACT(prod, '$.lines') as lines_json,
            TIMESTAMP_MILLIS(r.fetched_ts) AS ts
        FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
        UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod
        WHERE TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND JSON_EXTRACT_SCALAR(prod, '$.id') = '{win_product_id}'
        ORDER BY ts DESC
        LIMIT 1
        """)
        
        for row in result:
            print(f"  Latest fetch: {row.ts}")
            lines_str = str(row.lines_json)[:1000] + "..." if len(str(row.lines_json)) > 1000 else str(row.lines_json)
            print(f"  Lines JSON: {lines_str}")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Try to extract lines data
    print(f"\n2. Lines Data Extraction:")
    try:
        result = sink.query(f"""
        SELECT 
            line,
            TIMESTAMP_MILLIS(r.fetched_ts) AS ts
        FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
        UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
        UNNEST(IFNULL(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes'),
                      JSON_EXTRACT_ARRAY(prod, '$.lines'))) AS line
        WHERE TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND JSON_EXTRACT_SCALAR(prod, '$.id') = '{win_product_id}'
        ORDER BY ts DESC
        LIMIT 3
        """)
        
        count = 0
        for row in result:
            count += 1
            line_str = str(row.line)[:500] + "..." if len(str(row.line)) > 500 else str(row.line)
            print(f"  Line {count} at {row.ts}: {line_str}")
        
        if count == 0:
            print("  No lines found!")
        else:
            print(f"  Found {count} lines")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Try to extract odds specifically
    print(f"\n3. Odds Extraction:")
    try:
        result = sink.query(f"""
        SELECT 
            JSON_EXTRACT_SCALAR(line, '$.odds.decimal') AS decimal_odds,
            JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId') AS selection_id_1,
            JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId') AS selection_id_2,
            TIMESTAMP_MILLIS(r.fetched_ts) AS ts
        FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
        UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
        UNNEST(IFNULL(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes'),
                      JSON_EXTRACT_ARRAY(prod, '$.lines'))) AS line
        WHERE TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND JSON_EXTRACT_SCALAR(prod, '$.id') = '{win_product_id}'
        ORDER BY ts DESC
        LIMIT 5
        """)
        
        count = 0
        for row in result:
            count += 1
            print(f"  Line {count}: odds={row.decimal_odds}, sel1={row.selection_id_1}, sel2={row.selection_id_2}")
        
        if count == 0:
            print("  No odds found!")
        else:
            print(f"  Found {count} odds records")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
