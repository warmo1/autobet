#!/usr/bin/env python3
"""
Test script to verify the runners column is working correctly.
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sports.config import cfg
from sports.bq import get_bq_sink

def test_runners_column():
    """Test the runners column functionality."""
    sink = get_bq_sink()
    if not sink.enabled:
        print("❌ BigQuery not enabled")
        return False
    
    try:
        # Test the main page query
        print("🧪 Testing main page events query...")
        main_query = """
        SELECT e.event_id, e.name, e.venue, e.country, e.start_iso, e.sport, e.status, 
        COALESCE(erc.n_runners, 0) AS n_runners 
        FROM `autobet-470818.autobet.tote_events` e 
        LEFT JOIN `autobet-470818.autobet.vw_event_runner_counts` erc ON e.event_id = erc.event_id 
        WHERE e.start_iso >= CURRENT_TIMESTAMP() 
        ORDER BY e.start_iso ASC 
        LIMIT 5
        """
        
        result = sink.query(main_query)
        events = list(result)
        
        if events:
            print(f"✅ Found {len(events)} events with runners data")
            for event in events[:3]:  # Show first 3
                print(f"  • {event.name} at {event.venue}: {event.n_runners} runners")
        else:
            print("⚠️  No upcoming events found")
        
        # Test the events page query
        print("\n🧪 Testing events page query...")
        events_query = """
        SELECT e.event_id, e.name, e.venue, e.country, e.start_iso, e.sport, e.status, 
        COALESCE(erc.n_runners, 0) AS n_runners 
        FROM `autobet-470818.autobet.tote_events` e 
        LEFT JOIN `autobet-470818.autobet.vw_event_runner_counts` erc ON e.event_id = erc.event_id
        WHERE e.start_iso >= CURRENT_TIMESTAMP() 
        ORDER BY e.start_iso ASC 
        LIMIT 5
        """
        
        result = sink.query(events_query)
        events = list(result)
        
        if events:
            print(f"✅ Found {len(events)} events with runners data")
            for event in events[:3]:  # Show first 3
                print(f"  • {event.name} at {event.venue}: {event.n_runners} runners")
        else:
            print("⚠️  No upcoming events found")
        
        # Test the views exist
        print("\n🧪 Testing views exist...")
        views_query = """
        SELECT table_name 
        FROM `autobet-470818.autobet.INFORMATION_SCHEMA.TABLES` 
        WHERE table_name IN ('vw_product_competitor_counts', 'vw_event_runner_counts')
        """
        
        result = sink.query(views_query)
        views = [row.table_name for row in result]
        
        if 'vw_product_competitor_counts' in views:
            print("✅ vw_product_competitor_counts view exists")
        else:
            print("❌ vw_product_competitor_counts view missing")
        
        if 'vw_event_runner_counts' in views:
            print("✅ vw_event_runner_counts view exists")
        else:
            print("❌ vw_event_runner_counts view missing")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing runners column: {e}")
        return False

def main():
    """Main function."""
    print("🏁 Testing Runners Column Implementation")
    print("=" * 50)
    
    # Load environment
    env_path = project_root / '.env'
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    success = test_runners_column()
    
    if success:
        print("\n🎉 Runners column test completed successfully!")
        print("\n📋 Next steps:")
        print("1. Deploy the performance optimizations: ./scripts/deploy_performance_optimizations.sh")
        print("2. Check the main page and events page for the new runners column")
        print("3. Verify the data is displaying correctly")
    else:
        print("\n❌ Runners column test failed!")
        print("Please check the error messages above and fix any issues.")

if __name__ == "__main__":
    main()
