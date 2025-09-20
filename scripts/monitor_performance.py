#!/usr/bin/env python3
"""
Performance monitoring script for Autobet web app.

This script monitors query performance, cache hit rates, and system health.
"""

import os
import sys
import time
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

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

class PerformanceMonitor:
    def __init__(self):
        self.sink = get_bq_sink()
        self.metrics = {
            'query_times': [],
            'cache_hits': 0,
            'cache_misses': 0,
            'slow_queries': 0,
            'errors': 0
        }
    
    def test_query_performance(self) -> Dict[str, Any]:
        """Test performance of common queries."""
        results = {}
        
        if not self.sink.enabled:
            return {'error': 'BigQuery not enabled'}
        
        test_queries = {
            'dashboard_superfecta': """
                SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, total_net
                FROM `{project}.{dataset}.vw_gb_open_superfecta_next60_be` 
                ORDER BY start_iso LIMIT 20
            """,
            'event_filters': """
                SELECT country FROM `{project}.{dataset}.mv_event_filters_country` ORDER BY country
            """,
            'recent_events': """
                SELECT event_id, name, sport, venue, country, start_iso, status
                FROM `{project}.{dataset}.tote_events` 
                WHERE start_iso >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
                ORDER BY start_iso DESC LIMIT 100
            """,
            'products_list': """
                SELECT product_id, event_id, event_name, venue, country, start_iso, status, currency, total_net
                FROM `{project}.{dataset}.vw_products_latest_totals` 
                WHERE UPPER(bet_type) = 'SUPERFECTA'
                AND start_iso >= CURRENT_TIMESTAMP()
                ORDER BY start_iso LIMIT 50
            """
        }
        
        for query_name, query_template in test_queries.items():
            try:
                start_time = time.time()
                query = query_template.format(project=self.sink.project, dataset=self.sink.dataset)
                result = self.sink.query(query)
                
                # Convert to list to get row count
                rows = list(result)
                duration = time.time() - start_time
                
                results[query_name] = {
                    'duration': duration,
                    'row_count': len(rows),
                    'status': 'success'
                }
                
                if duration > 2.0:
                    self.metrics['slow_queries'] += 1
                    logger.warning(f"Slow query detected: {query_name} took {duration:.2f}s")
                
                self.metrics['query_times'].append(duration)
                
            except Exception as e:
                logger.error(f"Query {query_name} failed: {e}")
                results[query_name] = {
                    'duration': 0,
                    'row_count': 0,
                    'status': 'error',
                    'error': str(e)
                }
                self.metrics['errors'] += 1
        
        return results
    
    def check_materialized_views(self) -> Dict[str, Any]:
        """Check status of materialized views."""
        if not self.sink.enabled:
            return {'error': 'BigQuery not enabled'}
        
        try:
            check_sql = f"""
            SELECT 
                table_name,
                table_type,
                creation_time,
                row_count,
                size_bytes
            FROM `{self.sink.project}.{self.sink.dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name LIKE 'mv_%' OR table_name LIKE 'vw_%'
            ORDER BY creation_time DESC
            """
            
            result = self.sink.query(check_sql)
            views = []
            
            for row in result:
                views.append({
                    'name': row.table_name,
                    'type': row.table_type,
                    'created': str(row.creation_time),
                    'row_count': getattr(row, 'row_count', 0),
                    'size_bytes': getattr(row, 'size_bytes', 0)
                })
            
            return {'views': views, 'status': 'success'}
            
        except Exception as e:
            logger.error(f"Failed to check materialized views: {e}")
            return {'error': str(e), 'status': 'error'}
    
    def check_cache_performance(self) -> Dict[str, Any]:
        """Check cache performance metrics."""
        # This would integrate with Redis if available
        redis_client = None
        try:
            if cfg.redis_url:
                import redis
                redis_client = redis.from_url(cfg.redis_url)
                redis_client.ping()
        except Exception:
            redis_client = None
        
        cache_stats = {
            'redis_available': redis_client is not None,
            'local_cache_size': len(getattr(self, '_SQLDF_CACHE', {})),
            'cache_hit_rate': 0.0
        }
        
        if redis_client:
            try:
                info = redis_client.info('memory')
                cache_stats.update({
                    'redis_memory_used': info.get('used_memory', 0),
                    'redis_memory_peak': info.get('used_memory_peak', 0),
                    'redis_keys': redis_client.dbsize()
                })
            except Exception as e:
                logger.warning(f"Could not get Redis stats: {e}")
        
        # Calculate cache hit rate from metrics
        total_requests = self.metrics['cache_hits'] + self.metrics['cache_misses']
        if total_requests > 0:
            cache_stats['cache_hit_rate'] = self.metrics['cache_hits'] / total_requests
        
        return cache_stats
    
    def check_system_health(self) -> Dict[str, Any]:
        """Check overall system health."""
        health = {
            'timestamp': datetime.now().isoformat(),
            'bigquery_enabled': self.sink.enabled,
            'storage_api_enabled': cfg.bq_use_storage_api,
            'cache_enabled': cfg.web_sqldf_cache_enabled,
            'redis_enabled': bool(cfg.redis_url),
            'avg_query_time': 0.0,
            'slow_query_count': self.metrics['slow_queries'],
            'error_count': self.metrics['errors']
        }
        
        if self.metrics['query_times']:
            health['avg_query_time'] = sum(self.metrics['query_times']) / len(self.metrics['query_times'])
            health['max_query_time'] = max(self.metrics['query_times'])
            health['min_query_time'] = min(self.metrics['query_times'])
        
        return health
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate a comprehensive performance report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'query_performance': self.test_query_performance(),
            'materialized_views': self.check_materialized_views(),
            'cache_performance': self.check_cache_performance(),
            'system_health': self.check_system_health()
        }
        
        return report
    
    def log_report(self, report: Dict[str, Any]):
        """Log the performance report."""
        logger.info("=== PERFORMANCE REPORT ===")
        
        # Query performance summary
        query_perf = report.get('query_performance', {})
        for query_name, stats in query_perf.items():
            if stats.get('status') == 'success':
                logger.info(f"Query {query_name}: {stats['duration']:.2f}s, {stats['row_count']} rows")
            else:
                logger.error(f"Query {query_name}: FAILED - {stats.get('error', 'Unknown error')}")
        
        # System health summary
        health = report.get('system_health', {})
        logger.info(f"Average query time: {health.get('avg_query_time', 0):.2f}s")
        logger.info(f"Slow queries: {health.get('slow_query_count', 0)}")
        logger.info(f"Errors: {health.get('error_count', 0)}")
        
        # Cache performance
        cache = report.get('cache_performance', {})
        logger.info(f"Cache hit rate: {cache.get('cache_hit_rate', 0):.2%}")
        logger.info(f"Redis available: {cache.get('redis_available', False)}")
        
        logger.info("=== END REPORT ===")

def main():
    """Main monitoring function."""
    logger.info("Starting Autobet performance monitoring")
    
    # Load environment
    env_path = project_root / '.env'
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    monitor = PerformanceMonitor()
    
    # Generate and log report
    report = monitor.generate_report()
    monitor.log_report(report)
    
    # Save report to file if requested
    output_file = os.getenv("PERFORMANCE_REPORT_FILE")
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"Performance report saved to {output_file}")

if __name__ == "__main__":
    main()
