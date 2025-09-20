#!/usr/bin/env python3
"""Debug script to check what products have odds data"""

from sports.bq import get_bq_sink

def main():
    sink = get_bq_sink()
    
    print("=== Odds Products Debug ===")
    
    # Check what products have recent odds data
    print("\n1. Products with Recent Odds:")
    try:
        result = sink.query(f"""
        SELECT 
            SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) AS product_id,
            COUNT(*) as odds_count,
            MAX(TIMESTAMP_MILLIS(r.fetched_ts)) as latest_fetch
        FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
        UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod
        WHERE TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY product_id
        ORDER BY latest_fetch DESC
        LIMIT 10
        """)
        
        for row in result:
            print(f"  {row.product_id}: {row.odds_count} odds (latest: {row.latest_fetch})")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Check if any of these products are for AYR event
    print(f"\n2. AYR Event Products:")
    try:
        result = sink.query(f"""
        SELECT 
            p.product_id,
            p.bet_type,
            p.status,
            p.event_id
        FROM `autobet-470818.autobet.tote_products` p
        WHERE p.event_id = 'HORSERACING-AYR-GB-2025-09-19-1228'
        ORDER BY p.bet_type
        """)
        
        for row in result:
            print(f"  {row.bet_type}: {row.product_id} ({row.status})")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Check if there are any odds for AYR event products
    print(f"\n3. Odds for AYR Event Products:")
    try:
        result = sink.query(f"""
        WITH ayr_products AS (
          SELECT product_id FROM `autobet-470818.autobet.tote_products`
          WHERE event_id = 'HORSERACING-AYR-GB-2025-09-19-1228'
        )
        SELECT 
            SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) AS product_id,
            COUNT(*) as odds_count,
            MAX(TIMESTAMP_MILLIS(r.fetched_ts)) as latest_fetch
        FROM `autobet-470818.autobet.raw_tote_probable_odds` r,
        UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod
        WHERE TIMESTAMP_MILLIS(r.fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) IN (SELECT product_id FROM ayr_products)
        GROUP BY product_id
        ORDER BY latest_fetch DESC
        """)
        
        count = 0
        for row in result:
            count += 1
            print(f"  {row.product_id}: {row.odds_count} odds (latest: {row.latest_fetch})")
        
        if count == 0:
            print("  No odds found for any AYR event products!")
        else:
            print(f"  Found odds for {count} AYR products")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
