#!/usr/bin/env python3
"""
Background cache refresh script for Autobet performance optimization.

This script runs in the background to pre-compute expensive queries
and refresh materialized views, keeping the web app responsive.
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta

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

def refresh_materialized_views():
    """Refresh BigQuery materialized views."""
    try:
        sink = get_bq_sink()
        if not sink.enabled:
            logger.warning("BigQuery not enabled, skipping materialized view refresh")
            return
        
        # Refresh materialized views
        views_to_refresh = [
            "mv_event_filters_country",
            "mv_event_filters_sport", 
            "mv_event_filters_venue",
            "mv_latest_win_odds",
            "vw_superfecta_dashboard_cache"
        ]
        
        for view_name in views_to_refresh:
            try:
                logger.info(f"Refreshing materialized view: {view_name}")
                # BigQuery materialized views refresh automatically, but we can trigger
                # a query to ensure they're up to date
                sink.query(f"SELECT COUNT(*) FROM `{sink.project}.{sink.dataset}.{view_name}` LIMIT 1")
                logger.info(f"Successfully refreshed: {view_name}")
            except Exception as e:
                logger.error(f"Failed to refresh {view_name}: {e}")
                
    except Exception as e:
        logger.error(f"Error refreshing materialized views: {e}")

def warm_cache():
    """Warm up common caches by running frequent queries."""
    try:
        sink = get_bq_sink()
        if not sink.enabled:
            logger.warning("BigQuery not enabled, skipping cache warming")
            return
        
        # Common queries to warm up
        warmup_queries = [
            # Dashboard data
            """
            SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, total_net
            FROM `{project}.{dataset}.vw_gb_open_superfecta_next60_be` 
            ORDER BY start_iso LIMIT 20
            """,
            
            # Filter options
            """
            SELECT country FROM `{project}.{dataset}.mv_event_filters_country` ORDER BY country
            """,
            
            # Recent events
            """
            SELECT event_id, name, sport, venue, country, start_iso, status
            FROM `{project}.{dataset}.tote_events` 
            WHERE start_iso >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            ORDER BY start_iso DESC LIMIT 100
            """,
            
            # Superfecta products
            """
            SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, total_net
            FROM `{project}.{dataset}.vw_products_latest_totals` 
            WHERE UPPER(bet_type) = 'SUPERFECTA'
            AND start_iso >= CURRENT_TIMESTAMP()
            ORDER BY start_iso LIMIT 50
            """
        ]
        
        for i, query in enumerate(warmup_queries):
            try:
                formatted_query = query.format(project=sink.project, dataset=sink.dataset)
                logger.info(f"Warming cache query {i+1}/{len(warmup_queries)}")
                result = sink.query(formatted_query)
                logger.info(f"Cache warmup query {i+1} completed")
            except Exception as e:
                logger.error(f"Cache warmup query {i+1} failed: {e}")
                
    except Exception as e:
        logger.error(f"Error warming cache: {e}")

def cleanup_old_data():
    """Clean up old temporary tables and data."""
    try:
        sink = get_bq_sink()
        if not sink.enabled:
            logger.warning("BigQuery not enabled, skipping cleanup")
            return
        
        # Clean up temporary tables older than 3 days
        deleted_count = sink.cleanup_temp_tables(prefix="_tmp_", older_than_days=3)
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} temporary tables")
        
        # Clean up old job runs (older than 30 days)
        cleanup_sql = f"""
        DELETE FROM `{sink.project}.{sink.dataset}.ingest_job_runs`
        WHERE started_ts < TIMESTAMP_MILLIS(UNIX_MILLIS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)))
        """
        try:
            sink.query(cleanup_sql)
            logger.info("Cleaned up old job runs")
        except Exception as e:
            logger.warning(f"Failed to clean up old job runs: {e}")
            
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

def monitor_performance():
    """Monitor query performance and log metrics."""
    try:
        sink = get_bq_sink()
        if not sink.enabled:
            return
        
        # Check materialized view freshness
        check_sql = f"""
        SELECT 
            table_name,
            creation_time
        FROM `{sink.project}.{sink.dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'mv_%'
        ORDER BY creation_time DESC
        """
        
        try:
            result = sink.query(check_sql)
            for row in result:
                logger.info(f"Materialized view {row.table_name}: created {row.creation_time}")
        except Exception as e:
            logger.warning(f"Could not check materialized view status: {e}")
            
    except Exception as e:
        logger.error(f"Error monitoring performance: {e}")

def get_peak_hours_refresh_interval(sink) -> int:
    """Get refresh interval based on peak racing hours."""
    try:
        peak_hours_sql = """
        SELECT recommended_refresh_interval_seconds, is_peak_hours, uk_hour
        FROM `autobet-470818.autobet.vw_peak_racing_hours`
        """
        
        result = sink.query(peak_hours_sql)
        for row in result:
            interval = row.recommended_refresh_interval_seconds
            is_peak = row.is_peak_hours
            uk_hour = row.uk_hour
            logger.info(f"Peak hours: {'Yes' if is_peak else 'No'} (UK hour: {uk_hour}), refresh interval: {interval}s")
            return interval
    except Exception as e:
        logger.warning(f"Could not determine peak hours, using default: {e}")
    
    return 300  # Default to 5 minutes

def main():
    """Main cache refresh loop."""
    logger.info("Starting Autobet cache refresh service")
    
    # Load environment
    env_path = project_root / '.env'
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    sink = get_bq_sink()
    if not sink.enabled:
        logger.error("BigQuery not enabled, cannot run cache refresh service")
        return
    
    # Get initial refresh interval based on peak hours
    refresh_interval = get_peak_hours_refresh_interval(sink)
    cleanup_interval = int(os.getenv("CACHE_CLEANUP_INTERVAL", "3600"))  # 1 hour
    peak_check_interval = 600  # Check peak hours every 10 minutes
    
    last_cleanup = time.time()
    last_peak_check = time.time()
    
    while True:
        try:
            start_time = time.time()
            
            # Check if we need to adjust refresh interval based on peak hours
            if time.time() - last_peak_check > peak_check_interval:
                new_interval = get_peak_hours_refresh_interval(sink)
                if new_interval != refresh_interval:
                    refresh_interval = new_interval
                    logger.info(f"Adjusted refresh interval to: {refresh_interval} seconds")
                last_peak_check = time.time()
            
            # Refresh materialized views
            refresh_materialized_views()
            
            # Warm up caches
            warm_cache()
            
            # Monitor performance
            monitor_performance()
            
            # Periodic cleanup
            if time.time() - last_cleanup > cleanup_interval:
                cleanup_old_data()
                last_cleanup = time.time()
            
            duration = time.time() - start_time
            logger.info(f"Cache refresh cycle completed in {duration:.2f}s (next in {refresh_interval}s)")
            
            # Wait for next cycle
            time.sleep(refresh_interval)
            
        except KeyboardInterrupt:
            logger.info("Cache refresh service stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in cache refresh cycle: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    main()
