#!/usr/bin/env python3
"""
Simple data check script to diagnose horse numbers and odds issues.
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def check_data_sources():
    """Check what data sources are available."""
    print("🔍 Checking Data Sources")
    print("=" * 50)
    
    # Check if we can access BigQuery
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project="autobet-470818")
        print("✅ BigQuery client connected")
    except Exception as e:
        print(f"❌ BigQuery connection failed: {e}")
        return False
    
    # Check recent events
    try:
        query = """
        SELECT event_id, name, venue, start_iso, status
        FROM `autobet-470818.autobet.tote_events`
        WHERE start_iso >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        ORDER BY start_iso DESC
        LIMIT 3
        """
        
        result = client.query(query)
        events = list(result)
        
        if events:
            print(f"✅ Found {len(events)} recent events")
            for event in events:
                print(f"  • {event.name} at {event.venue} - {event.start_iso} ({event.status})")
        else:
            print("❌ No recent events found")
            return False
            
    except Exception as e:
        print(f"❌ Error querying events: {e}")
        return False
    
    # Check products for recent events
    try:
        if events:
            event_id = events[0].event_id
            query = """
            SELECT product_id, bet_type, status, total_net
            FROM `autobet-470818.autobet.tote_products`
            WHERE event_id = @event_id
            ORDER BY bet_type
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("event_id", "STRING", event_id)
                ]
            )
            
            result = client.query(query, job_config=job_config)
            products = list(result)
            
            if products:
                print(f"✅ Found {len(products)} products for event {event_id}")
                for product in products:
                    print(f"  • {product.bet_type} - {product.status} - Net: {product.total_net}")
            else:
                print(f"❌ No products found for event {event_id}")
                
    except Exception as e:
        print(f"❌ Error querying products: {e}")
        return False
    
    # Check selections for WIN products
    try:
        if events:
            event_id = events[0].event_id
            query = """
            SELECT p.product_id, COUNT(s.selection_id) as selection_count
            FROM `autobet-470818.autobet.tote_products` p
            LEFT JOIN `autobet-470818.autobet.tote_product_selections` s ON p.product_id = s.product_id
            WHERE UPPER(p.bet_type) = 'WIN' AND p.event_id = @event_id
            GROUP BY p.product_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("event_id", "STRING", event_id)
                ]
            )
            
            result = client.query(query, job_config=job_config)
            selections = list(result)
            
            if selections:
                print(f"✅ Found selections for WIN products")
                for selection in selections:
                    print(f"  • Product {selection.product_id}: {selection.selection_count} selections")
            else:
                print(f"❌ No selections found for WIN products")
                
    except Exception as e:
        print(f"❌ Error querying selections: {e}")
        return False
    
    # Check probable odds data
    try:
        query = """
        SELECT COUNT(*) as odds_count, MAX(TIMESTAMP_MILLIS(fetched_ts)) as latest_fetch
        FROM `autobet-470818.autobet.raw_tote_probable_odds`
        WHERE TIMESTAMP_MILLIS(fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        """
        
        result = client.query(query)
        odds_data = list(result)
        
        if odds_data:
            odds_count = odds_data[0].odds_count
            latest_fetch = odds_data[0].latest_fetch
            if odds_count > 0:
                print(f"✅ Found {odds_count} odds records in last 7 days")
                print(f"  • Latest fetch: {latest_fetch}")
            else:
                print("❌ No odds data found in last 7 days")
        else:
            print("❌ No odds data found")
            
    except Exception as e:
        print(f"❌ Error querying odds: {e}")
        return False
    
    # Check vw_tote_probable_odds view
    try:
        query = """
        SELECT COUNT(*) as view_count, MAX(latest_ts) as latest_odds
        FROM `autobet-470818.autobet.vw_tote_probable_odds`
        WHERE latest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        """
        
        result = client.query(query)
        view_data = list(result)
        
        if view_data:
            view_count = view_data[0].view_count
            latest_odds = view_data[0].latest_odds
            if view_count > 0:
                print(f"✅ vw_tote_probable_odds has {view_count} records")
                print(f"  • Latest odds: {latest_odds}")
            else:
                print("❌ vw_tote_probable_odds view is empty")
        else:
            print("❌ vw_tote_probable_odds view not accessible")
            
    except Exception as e:
        print(f"❌ Error querying vw_tote_probable_odds: {e}")
        return False
    
    return True

def suggest_fixes():
    """Suggest fixes for data issues."""
    print("\n🔧 Suggested Fixes")
    print("=" * 50)
    
    print("1. **Data Ingestion Issues:**")
    print("   • Run data ingestion for recent events:")
    print("     python3 -m sports.run tote-events --first 100")
    print("     python3 -m sports.run tote-products --first 500")
    print("     python3 -m sports.run tote-probable --first 100")
    
    print("\n2. **Odds Data Issues:**")
    print("   • Check if odds ingestion is working:")
    print("     python3 -m sports.run tote-probable --first 50")
    print("   • Verify the vw_tote_probable_odds view is up to date")
    
    print("\n3. **Product Selections Issues:**")
    print("   • Ensure product ingestion includes selections:")
    print("     python3 -m sports.run tote-products --first 200")
    
    print("\n4. **Manual Data Refresh:**")
    print("   • Use the 'Refresh Odds' button on the event page")
    print("   • Check Cloud Scheduler jobs are running")
    print("   • Verify BigQuery permissions and quotas")

def main():
    """Main function."""
    print("🏁 Data Issues Diagnosis")
    print("=" * 50)
    
    success = check_data_sources()
    suggest_fixes()
    
    if success:
        print("\n✅ Diagnosis completed!")
    else:
        print("\n❌ Diagnosis failed!")

if __name__ == "__main__":
    main()
