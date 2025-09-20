#!/usr/bin/env python3
"""Debug script to check AYR event data"""

from sports.bq import get_bq_sink

def main():
    sink = get_bq_sink()
    event_id = "HORSERACING-AYR-GB-2025-09-19-1228"
    
    print("=== AYR Event Debug ===")
    
    # Check WIN product selections
    print("\n1. WIN Product Selections:")
    try:
        result = sink.query(f"""
        SELECT 
            s.number, 
            s.competitor, 
            s.selection_id,
            s.leg_index
        FROM `autobet-470818.autobet.tote_product_selections` s
        JOIN `autobet-470818.autobet.tote_products` p ON s.product_id = p.product_id
        WHERE p.event_id = '{event_id}' 
          AND UPPER(p.bet_type) = 'WIN'
        ORDER BY SAFE_CAST(s.number AS INT64)
        """)
        
        for row in result:
            print(f"  {row.number}: '{row.competitor}' ({row.selection_id}) [leg: {row.leg_index}]")
    except Exception as e:
        print(f"Error: {e}")
    
    # Check all products for this event
    print("\n2. All Products for Event:")
    try:
        result = sink.query(f"""
        SELECT 
            product_id,
            bet_type,
            status
        FROM `autobet-470818.autobet.tote_products`
        WHERE event_id = '{event_id}'
        ORDER BY bet_type
        """)
        
        for row in result:
            print(f"  {row.bet_type}: {row.product_id} ({row.status})")
    except Exception as e:
        print(f"Error: {e}")
    
    # Check runner count view
    print("\n3. Runner Count View:")
    try:
        result = sink.query(f"""
        SELECT event_id, n_runners 
        FROM `autobet-470818.autobet.vw_event_runner_counts` 
        WHERE event_id = '{event_id}'
        """)
        
        for row in result:
            print(f"  Event: {row.event_id}, Runners: {row.n_runners}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Check if there's any odds data
    print("\n4. Recent Odds Data:")
    try:
        result = sink.query(f"""
        SELECT 
            COUNT(*) as count,
            MAX(TIMESTAMP_MILLIS(fetched_ts)) as latest_fetch
        FROM `autobet-470818.autobet.raw_tote_probable_odds`
        WHERE TIMESTAMP_MILLIS(fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        """)
        
        for row in result:
            print(f"  Recent odds records: {row.count}, Latest: {row.latest_fetch}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
