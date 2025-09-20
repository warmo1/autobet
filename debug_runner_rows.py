#!/usr/bin/env python3
"""Debug script to check runner_rows data"""

from sports.bq import get_bq_sink

def main():
    sink = get_bq_sink()
    event_id = "HORSERACING-AYR-GB-2025-09-19-1228"
    
    print("=== Runner Rows Debug ===")
    
    # Check hr_horse_runs data
    print(f"\n1. HR Horse Runs Data:")
    try:
        result = sink.query(f"""
        SELECT 
            cloth_number,
            horse_name,
            finish_pos,
            status
        FROM `autobet-470818.autobet.hr_horse_runs`
        WHERE event_id = '{event_id}'
        ORDER BY SAFE_CAST(cloth_number AS INT64)
        """)
        
        count = 0
        for row in result:
            count += 1
            print(f"  {row.cloth_number}: {row.horse_name} (finish: {row.finish_pos}, status: {row.status})")
        
        if count == 0:
            print("  No HR horse runs data found!")
        else:
            print(f"  Found {count} HR horse runs records")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Check competitors_json
    print(f"\n2. Competitors JSON Data:")
    try:
        result = sink.query(f"""
        SELECT 
            event_id,
            competitors_json
        FROM `autobet-470818.autobet.tote_events`
        WHERE event_id = '{event_id}'
        """)
        
        for row in result:
            print(f"  Event: {row.event_id}")
            if row.competitors_json:
                print(f"  Competitors JSON: {row.competitors_json[:200]}...")
            else:
                print("  No competitors JSON data")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
