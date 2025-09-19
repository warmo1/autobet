#!/usr/bin/env python3
"""
Performance status checker for Autobet optimizations.

This script checks the current performance status of the race status updates
and displays key metrics and any issues found.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sports.config import cfg
from sports.bq import get_bq_sink

def check_performance_status() -> Dict[str, Any]:
    """Check the current performance status."""
    sink = get_bq_sink()
    if not sink.enabled:
        return {"error": "BigQuery not enabled"}
    
    results = {}
    
    try:
        # Check peak hours status
        peak_hours_sql = """
        SELECT * FROM `autobet-470818.autobet.vw_peak_racing_hours`
        """
        peak_df = sink.query(peak_hours_sql).to_dataframe()
        if not peak_df.empty:
            results['peak_hours'] = peak_df.to_dict('records')[0]
        
        # Check status update performance
        performance_sql = """
        SELECT * FROM `autobet-470818.autobet.vw_status_update_performance`
        """
        perf_df = sink.query(performance_sql).to_dataframe()
        if not perf_df.empty:
            results['performance'] = perf_df.to_dict('records')[0]
        
        # Check races that should be closed
        should_be_closed_sql = """
        SELECT 
          product_id,
          event_name,
          venue,
          start_iso,
          status,
          minutes_since_start
        FROM `autobet-470818.autobet.vw_races_should_be_closed`
        ORDER BY minutes_since_start DESC
        LIMIT 10
        """
        closed_df = sink.query(should_be_closed_sql).to_dataframe()
        results['should_be_closed'] = closed_df.to_dict('records') if not closed_df.empty else []
        
        # Check races that should be open
        should_be_open_sql = """
        SELECT 
          product_id,
          event_name,
          venue,
          start_iso,
          status,
          minutes_since_start
        FROM `autobet-470818.autobet.vw_races_should_be_open`
        ORDER BY start_iso ASC
        LIMIT 10
        """
        open_df = sink.query(should_be_open_sql).to_dataframe()
        results['should_be_open'] = open_df.to_dict('records') if not open_df.empty else []
        
        # Check materialized view status
        mv_status_sql = """
        SELECT 
          table_name,
          table_type,
          creation_time
        FROM `autobet-470818.autobet.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'mv_%'
        ORDER BY creation_time DESC
        """
        mv_df = sink.query(mv_status_sql).to_dataframe()
        results['materialized_views'] = mv_df.to_dict('records') if not mv_df.empty else []
        
    except Exception as e:
        results['error'] = str(e)
    
    return results

def format_timestamp(ts_str: str) -> str:
    """Format timestamp for display."""
    try:
        if ts_str:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    except:
        pass
    return ts_str

def print_status_report(results: Dict[str, Any]) -> None:
    """Print a formatted status report."""
    print("=" * 80)
    print("🏁 AUTOBET PERFORMANCE STATUS REPORT")
    print("=" * 80)
    print(f"Generated at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    
    if 'error' in results:
        print(f"❌ ERROR: {results['error']}")
        return
    
    # Peak hours status
    if 'peak_hours' in results:
        peak = results['peak_hours']
        print("🕐 PEAK HOURS STATUS")
        print("-" * 40)
        print(f"UK Hour: {peak.get('uk_hour', 'N/A')}")
        print(f"Peak Hours: {'Yes' if peak.get('is_peak_hours') else 'No'}")
        print(f"Recommended Refresh Interval: {peak.get('recommended_refresh_interval_seconds', 'N/A')} seconds")
        print()
    
    # Performance metrics
    if 'performance' in results:
        perf = results['performance']
        print("📊 STATUS UPDATE PERFORMANCE")
        print("-" * 40)
        print(f"Total Races Checked: {perf.get('total_races_checked', 0)}")
        print(f"Status Correct: {perf.get('races_status_correct', 0)}")
        print(f"Should Be Closed: {perf.get('races_should_be_closed', 0)}")
        print(f"Should Be Open: {perf.get('races_should_be_open', 0)}")
        print(f"Accuracy: {perf.get('accuracy_percentage', 0)}%")
        print()
    
    # Races that should be closed
    if results.get('should_be_closed'):
        print("⚠️  RACES THAT SHOULD BE CLOSED")
        print("-" * 40)
        for race in results['should_be_closed']:
            print(f"• {race['event_name']} at {race['venue']}")
            print(f"  Started: {format_timestamp(race['start_iso'])} ({race['minutes_since_start']} minutes ago)")
            print(f"  Status: {race['status']}")
            print()
    else:
        print("✅ NO RACES THAT SHOULD BE CLOSED")
        print()
    
    # Races that should be open
    if results.get('should_be_open'):
        print("⚠️  RACES THAT SHOULD BE OPEN")
        print("-" * 40)
        for race in results['should_be_open']:
            print(f"• {race['event_name']} at {race['venue']}")
            print(f"  Starts: {format_timestamp(race['start_iso'])}")
            print(f"  Status: {race['status']}")
            print()
    else:
        print("✅ NO RACES THAT SHOULD BE OPEN")
        print()
    
    # Materialized views status
    if results.get('materialized_views'):
        print("🔄 MATERIALIZED VIEWS STATUS")
        print("-" * 40)
        for mv in results['materialized_views']:
            print(f"• {mv['table_name']}")
            print(f"  Type: {mv['table_type']}")
            print(f"  Created: {format_timestamp(mv.get('creation_time', ''))}")
            print()
    
    print("=" * 80)

def main():
    """Main function."""
    print("Checking Autobet performance status...")
    
    # Load environment
    env_path = project_root / '.env'
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    results = check_performance_status()
    print_status_report(results)

if __name__ == "__main__":
    main()
