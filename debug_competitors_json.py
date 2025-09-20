#!/usr/bin/env python3
"""Debug script to check competitors_json parsing"""

from sports.bq import get_bq_sink
import json

def main():
    sink = get_bq_sink()
    event_id = "HORSERACING-AYR-GB-2025-09-19-1228"
    
    print("=== Competitors JSON Debug ===")
    
    # Get the competitors_json data
    try:
        result = sink.query(f"""
        SELECT 
            event_id,
            competitors_json
        FROM `autobet-470818.autobet.tote_events`
        WHERE event_id = '{event_id}'
        """)
        
        for row in result:
            print(f"Event: {row.event_id}")
            if row.competitors_json:
                print(f"Raw JSON: {row.competitors_json}")
                
                # Parse the JSON
                try:
                    competitors_data = json.loads(row.competitors_json)
                    print(f"Parsed JSON: {competitors_data}")
                    
                    # Check each competitor
                    for i, c in enumerate(competitors_data):
                        print(f"  Competitor {i}:")
                        print(f"    id: {c.get('id')}")
                        print(f"    name: {c.get('name')}")
                        print(f"    clothNumber: {c.get('details', {}).get('clothNumber')}")
                        
                except Exception as e:
                    print(f"Error parsing JSON: {e}")
            else:
                print("No competitors JSON data")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
