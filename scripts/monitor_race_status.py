#!/usr/bin/env python3
"""
Real-time race status monitoring script for Autobet.

This script monitors race statuses and identifies races that should be closed
but are still showing as OPEN, or races that should be open but are closed.
It can also trigger automatic status updates if configured.
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sports.config import cfg
from sports.bq import get_bq_sink

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RaceStatusMonitor:
    def __init__(self):
        self.sink = get_bq_sink()
        if not self.sink.enabled:
            raise RuntimeError("BigQuery not enabled")
    
    def check_status_anomalies(self) -> Dict[str, Any]:
        """Check for races with status anomalies."""
        try:
            # Check races that should be closed but are still open
            should_be_closed_sql = """
            SELECT 
              product_id,
              event_id,
              event_name,
              venue,
              start_iso,
              status,
              minutes_since_start
            FROM `autobet-470818.autobet.vw_races_should_be_closed`
            ORDER BY minutes_since_start DESC
            LIMIT 20
            """
            
            should_be_closed_df = self.sink.query(should_be_closed_sql).to_dataframe()
            
            # Check races that should be open but are closed
            should_be_open_sql = """
            SELECT 
              product_id,
              event_id,
              event_name,
              venue,
              start_iso,
              status,
              minutes_since_start
            FROM `autobet-470818.autobet.vw_races_should_be_open`
            ORDER BY start_iso ASC
            LIMIT 20
            """
            
            should_be_open_df = self.sink.query(should_be_open_sql).to_dataframe()
            
            # Get overall performance metrics
            performance_sql = """
            SELECT * FROM `autobet-470818.autobet.vw_status_update_performance`
            """
            
            performance_df = self.sink.query(performance_sql).to_dataframe()
            
            return {
                'should_be_closed': should_be_closed_df.to_dict('records') if not should_be_closed_df.empty else [],
                'should_be_open': should_be_open_df.to_dict('records') if not should_be_open_df.empty else [],
                'performance': performance_df.to_dict('records')[0] if not performance_df.empty else {},
                'check_time': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking status anomalies: {e}")
            return {'error': str(e)}
    
    def log_status_anomalies(self, anomalies: Dict[str, Any]) -> None:
        """Log status anomalies for monitoring."""
        if 'error' in anomalies:
            logger.error(f"Status check failed: {anomalies['error']}")
            return
        
        should_be_closed = anomalies.get('should_be_closed', [])
        should_be_open = anomalies.get('should_be_open', [])
        performance = anomalies.get('performance', {})
        
        if should_be_closed:
            logger.warning(f"Found {len(should_be_closed)} races that should be closed but are still OPEN")
            for race in should_be_closed[:5]:  # Log first 5
                logger.warning(f"  - {race['event_name']} at {race['venue']} started {race['minutes_since_start']} minutes ago but still OPEN")
        
        if should_be_open:
            logger.warning(f"Found {len(should_be_open)} races that should be open but are CLOSED")
            for race in should_be_open[:5]:  # Log first 5
                logger.warning(f"  - {race['event_name']} at {race['venue']} starts at {race['start_iso']} but already CLOSED")
        
        if performance:
            accuracy = performance.get('accuracy_percentage', 0)
            total_races = performance.get('total_races_checked', 0)
            logger.info(f"Status accuracy: {accuracy}% ({performance.get('races_status_correct', 0)}/{total_races} races)")
    
    def trigger_status_refresh(self, product_ids: List[str]) -> bool:
        """Trigger a refresh of specific product statuses."""
        try:
            # This would trigger a job to refresh product statuses
            # For now, we'll just log the action
            logger.info(f"Would trigger status refresh for {len(product_ids)} products: {product_ids[:5]}...")
            
            # In a real implementation, you might:
            # 1. Publish a Pub/Sub message to trigger product status refresh
            # 2. Call the Tote API directly to get fresh status
            # 3. Update the status in BigQuery
            
            return True
        except Exception as e:
            logger.error(f"Error triggering status refresh: {e}")
            return False
    
    def check_peak_hours(self) -> Dict[str, Any]:
        """Check if we're in peak racing hours."""
        try:
            peak_hours_sql = """
            SELECT * FROM `autobet-470818.autobet.vw_peak_racing_hours`
            """
            
            peak_df = self.sink.query(peak_hours_sql).to_dataframe()
            if not peak_df.empty:
                return peak_df.to_dict('records')[0]
            return {}
        except Exception as e:
            logger.error(f"Error checking peak hours: {e}")
            return {}
    
    def get_recommended_refresh_interval(self) -> int:
        """Get the recommended refresh interval based on peak hours."""
        peak_info = self.check_peak_hours()
        if peak_info:
            return peak_info.get('recommended_refresh_interval_seconds', 300)
        return 300  # Default to 5 minutes
    
    def run_monitoring_cycle(self) -> None:
        """Run one monitoring cycle."""
        logger.info("Starting race status monitoring cycle")
        
        # Check for status anomalies
        anomalies = self.check_status_anomalies()
        self.log_status_anomalies(anomalies)
        
        # Check if we should trigger any actions
        if anomalies.get('should_be_closed'):
            should_be_closed = anomalies['should_be_closed']
            # Only trigger refresh for races that have been running for more than 5 minutes
            urgent_races = [r for r in should_be_closed if r.get('minutes_since_start', 0) > 5]
            if urgent_races:
                product_ids = [r['product_id'] for r in urgent_races]
                self.trigger_status_refresh(product_ids)
        
        # Log peak hours status
        peak_info = self.check_peak_hours()
        if peak_info:
            is_peak = peak_info.get('is_peak_hours', False)
            uk_hour = peak_info.get('uk_hour', 0)
            logger.info(f"Peak hours status: {'Yes' if is_peak else 'No'} (UK hour: {uk_hour})")

def main():
    """Main monitoring loop."""
    logger.info("Starting Autobet race status monitoring service")
    
    # Load environment
    env_path = project_root / '.env'
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    monitor = RaceStatusMonitor()
    
    # Get initial refresh interval
    refresh_interval = monitor.get_recommended_refresh_interval()
    logger.info(f"Initial refresh interval: {refresh_interval} seconds")
    
    last_peak_check = time.time()
    
    while True:
        try:
            start_time = time.time()
            
            # Run monitoring cycle
            monitor.run_monitoring_cycle()
            
            # Check if we need to adjust refresh interval (every 10 minutes)
            if time.time() - last_peak_check > 600:  # 10 minutes
                new_interval = monitor.get_recommended_refresh_interval()
                if new_interval != refresh_interval:
                    refresh_interval = new_interval
                    logger.info(f"Adjusted refresh interval to: {refresh_interval} seconds")
                last_peak_check = time.time()
            
            duration = time.time() - start_time
            logger.info(f"Monitoring cycle completed in {duration:.2f}s")
            
            # Wait for next cycle
            time.sleep(refresh_interval)
            
        except KeyboardInterrupt:
            logger.info("Race status monitoring service stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    main()
