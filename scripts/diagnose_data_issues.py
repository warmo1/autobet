#!/usr/bin/env python3
"""
Diagnose data issues with horse numbers and odds.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sports.config import cfg
from sports.bq import get_bq_sink

def diagnose_data_issues():
    """Diagnose data issues with horse numbers and odds."""
    sink = get_bq_sink()
    if not sink.enabled:
        print("❌ BigQuery not enabled")
        return False
    
    print("🔍 Diagnosing Data Issues")
    print("=" * 50)
    
    try:
        # Check if we have recent events
        print("\n1. Checking recent events...")
        events_query = """
        SELECT event_id, name, venue, start_iso, status
        FROM `autobet-470818.autobet.tote_events`
        WHERE start_iso >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        ORDER BY start_iso DESC
        LIMIT 5
        """
        
        events_df = sink.query(events_query).to_dataframe()
        if not events_df.empty:
            print(f"✅ Found {len(events_df)} recent events")
            for _, event in events_df.head(3).iterrows():
                print(f"  • {event['name']} at {event['venue']} - {event['start_iso']} ({event['status']})")
        else:
            print("❌ No recent events found")
            return False
        
        # Check if we have products for recent events
        print("\n2. Checking products for recent events...")
        if not events_df.empty:
            event_id = events_df.iloc[0]['event_id']
            products_query = """
            SELECT product_id, bet_type, status, total_net
            FROM `autobet-470818.autobet.tote_products`
            WHERE event_id = @event_id
            ORDER BY bet_type
            """
            
            products_df = sink.query(products_query, params={'event_id': event_id}).to_dataframe()
            if not products_df.empty:
                print(f"✅ Found {len(products_df)} products for event {event_id}")
                for _, product in products_df.iterrows():
                    print(f"  • {product['bet_type']} - {product['status']} - Net: {product['total_net']}")
            else:
                print(f"❌ No products found for event {event_id}")
        
        # Check if we have selections for WIN products
        print("\n3. Checking selections for WIN products...")
        win_products_query = """
        SELECT p.product_id, p.event_id, COUNT(s.selection_id) as selection_count
        FROM `autobet-470818.autobet.tote_products` p
        LEFT JOIN `autobet-470818.autobet.tote_product_selections` s ON p.product_id = s.product_id
        WHERE UPPER(p.bet_type) = 'WIN' 
        AND p.event_id = @event_id
        GROUP BY p.product_id, p.event_id
        """
        
        if not events_df.empty:
            event_id = events_df.iloc[0]['event_id']
            selections_df = sink.query(win_products_query, params={'event_id': event_id}).to_dataframe()
            if not selections_df.empty:
                print(f"✅ Found selections for WIN products")
                for _, selection in selections_df.iterrows():
                    print(f"  • Product {selection['product_id']}: {selection['selection_count']} selections")
            else:
                print(f"❌ No selections found for WIN products")
        
        # Check if we have probable odds data
        print("\n4. Checking probable odds data...")
        odds_query = """
        SELECT COUNT(*) as odds_count, MAX(TIMESTAMP_MILLIS(fetched_ts)) as latest_fetch
        FROM `autobet-470818.autobet.raw_tote_probable_odds`
        WHERE TIMESTAMP_MILLIS(fetched_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        """
        
        odds_df = sink.query(odds_query).to_dataframe()
        if not odds_df.empty:
            odds_count = odds_df.iloc[0]['odds_count']
            latest_fetch = odds_df.iloc[0]['latest_fetch']
            if odds_count > 0:
                print(f"✅ Found {odds_count} odds records in last 7 days")
                print(f"  • Latest fetch: {latest_fetch}")
            else:
                print("❌ No odds data found in last 7 days")
        else:
            print("❌ No odds data found")
        
        # Check the vw_tote_probable_odds view
        print("\n5. Checking vw_tote_probable_odds view...")
        view_query = """
        SELECT COUNT(*) as view_count, MAX(latest_ts) as latest_odds
        FROM `autobet-470818.autobet.vw_tote_probable_odds`
        WHERE latest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        """
        
        view_df = sink.query(view_query).to_dataframe()
        if not view_df.empty:
            view_count = view_df.iloc[0]['view_count']
            latest_odds = view_df.iloc[0]['latest_odds']
            if view_count > 0:
                print(f"✅ vw_tote_probable_odds has {view_count} records")
                print(f"  • Latest odds: {latest_odds}")
            else:
                print("❌ vw_tote_probable_odds view is empty")
        else:
            print("❌ vw_tote_probable_odds view not accessible")
        
        # Check specific event data
        print("\n6. Checking specific event data...")
        if not events_df.empty:
            event_id = events_df.iloc[0]['event_id']
            event_detail_query = """
            SELECT 
                e.event_id,
                e.name,
                e.start_iso,
                e.status,
                COUNT(DISTINCT p.product_id) as product_count,
                COUNT(DISTINCT s.selection_id) as selection_count,
                COUNT(DISTINCT o.selection_id) as odds_count
            FROM `autobet-470818.autobet.tote_events` e
            LEFT JOIN `autobet-470818.autobet.tote_products` p ON e.event_id = p.event_id
            LEFT JOIN `autobet-470818.autobet.tote_product_selections` s ON p.product_id = s.product_id
            LEFT JOIN `autobet-470818.autobet.vw_tote_probable_odds` o ON p.product_id = o.product_id
            WHERE e.event_id = @event_id
            GROUP BY e.event_id, e.name, e.start_iso, e.status
            """
            
            detail_df = sink.query(event_detail_query, params={'event_id': event_id}).to_dataframe()
            if not detail_df.empty:
                detail = detail_df.iloc[0]
                print(f"✅ Event detail for {detail['name']}:")
                print(f"  • Products: {detail['product_count']}")
                print(f"  • Selections: {detail['selection_count']}")
                print(f"  • Odds records: {detail['odds_count']}")
            else:
                print(f"❌ No detail data found for event {event_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during diagnosis: {e}")
        return False

def suggest_fixes():
    """Suggest fixes for data issues."""
    print("\n🔧 Suggested Fixes")
    print("=" * 50)
    
    print("1. **Data Ingestion Issues:**")
    print("   • Run data ingestion for recent events:")
    print("     python -m sports.run tote-events --first 100")
    print("     python -m sports.run tote-products --first 500")
    print("     python -m sports.run tote-probable --first 100")
    
    print("\n2. **Odds Data Issues:**")
    print("   • Check if odds ingestion is working:")
    print("     python -m sports.run tote-probable --first 50")
    print("   • Verify the vw_tote_probable_odds view is up to date")
    
    print("\n3. **Product Selections Issues:**")
    print("   • Ensure product ingestion includes selections:")
    print("     python -m sports.run tote-products --first 200")
    
    print("\n4. **Manual Data Refresh:**")
    print("   • Use the 'Refresh Odds' button on the event page")
    print("   • Check Cloud Scheduler jobs are running")
    print("   • Verify BigQuery permissions and quotas")

def main():
    """Main function."""
    print("🏁 Data Issues Diagnosis")
    print("=" * 50)
    
    # Load environment
    env_path = project_root / '.env'
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    success = diagnose_data_issues()
    suggest_fixes()
    
    if success:
        print("\n✅ Diagnosis completed!")
    else:
        print("\n❌ Diagnosis failed!")

if __name__ == "__main__":
    main()
