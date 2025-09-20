#!/usr/bin/env python3
"""BigQuery quota management utilities and monitoring."""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta

from .quota_manager import get_quota_manager, QuotaLimits
from .config import cfg
from .bq import get_db

def check_quota_status():
    """Check current quota usage and display status."""
    quota_manager = get_quota_manager()
    stats = quota_manager.get_usage_stats()
    
    print("=== BigQuery Quota Usage Status ===")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    # Query stats
    query_stats = stats['queries']
    print("QUERIES:")
    print(f"  Per minute: {query_stats['per_minute']} / {quota_manager.limits.queries_per_minute}")
    print(f"  Per hour:   {query_stats['per_hour']} / {quota_manager.limits.queries_per_hour}")
    print(f"  Per day:    {query_stats['per_day']} / {quota_manager.limits.queries_per_day}")
    
    # Check if approaching limits
    if query_stats['per_minute'] > quota_manager.limits.queries_per_minute * 0.8:
        print("  ⚠️  WARNING: Approaching per-minute query limit")
    if query_stats['per_hour'] > quota_manager.limits.queries_per_hour * 0.8:
        print("  ⚠️  WARNING: Approaching per-hour query limit")
    
    print()
    
    # Insert stats
    insert_stats = stats['inserts']
    print("INSERTS:")
    print(f"  Per minute: {insert_stats['per_minute']} / {quota_manager.limits.inserts_per_minute}")
    print(f"  Per hour:   {insert_stats['per_hour']} / {quota_manager.limits.inserts_per_hour}")
    print(f"  Per day:    {insert_stats['per_day']} / {quota_manager.limits.inserts_per_day}")
    
    # Check if approaching limits
    if insert_stats['per_minute'] > quota_manager.limits.inserts_per_minute * 0.8:
        print("  ⚠️  WARNING: Approaching per-minute insert limit")
    if insert_stats['per_hour'] > quota_manager.limits.inserts_per_hour * 0.8:
        print("  ⚠️  WARNING: Approaching per-hour insert limit")
    
    print()
    
    # Failed requests
    failed_stats = stats['failed_requests']
    print(f"FAILED REQUESTS (last hour): {failed_stats['last_hour']}")
    
    if failed_stats['last_hour'] > 10:
        print("  ⚠️  WARNING: High number of failed requests")
    
    print()

def monitor_quota(interval_seconds=60, duration_minutes=10):
    """Monitor quota usage continuously."""
    print(f"Starting quota monitoring for {duration_minutes} minutes...")
    print(f"Checking every {interval_seconds} seconds")
    print()
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    while time.time() < end_time:
        check_quota_status()
        
        if time.time() < end_time:
            print("Waiting for next check...")
            time.sleep(interval_seconds)
            print()

def analyze_bigquery_usage():
    """Analyze BigQuery usage patterns from job logs."""
    try:
        db = get_db()
        
        # Get recent job runs with errors
        print("=== Recent BigQuery Job Analysis ===")
        
        # Query for recent failed jobs
        failed_jobs_df = db.query_dataframe("""
            SELECT 
                component,
                task,
                status,
                error,
                COUNT(*) as count,
                MAX(started_ts) as latest_failure
            FROM ingest_job_runs 
            WHERE started_ts > (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP()) * 1000 - 3600000)
              AND status = 'ERROR'
            GROUP BY component, task, status, error
            ORDER BY count DESC
            LIMIT 10
        """)
        
        if not failed_jobs_df.empty:
            print("Recent Failed Jobs (last hour):")
            for _, row in failed_jobs_df.iterrows():
                print(f"  {row['component']}.{row['task']}: {row['count']} failures")
                if 'quota' in str(row['error']).lower():
                    print(f"    Error: {row['error']}")
            print()
        
        # Get job frequency analysis
        job_freq_df = db.query_dataframe("""
            SELECT 
                component,
                task,
                COUNT(*) as total_jobs,
                COUNT(CASE WHEN status = 'OK' THEN 1 END) as successful_jobs,
                COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as failed_jobs,
                ROUND(COUNT(CASE WHEN status = 'ERROR' THEN 1 END) * 100.0 / COUNT(*), 2) as failure_rate
            FROM ingest_job_runs 
            WHERE started_ts > (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP()) * 1000 - 86400000)
            GROUP BY component, task
            ORDER BY total_jobs DESC
            LIMIT 15
        """)
        
        if not job_freq_df.empty:
            print("Job Frequency Analysis (last 24 hours):")
            print(f"{'Component':<15} {'Task':<25} {'Total':<8} {'Success':<8} {'Failed':<8} {'Failure%':<10}")
            print("-" * 80)
            for _, row in job_freq_df.iterrows():
                print(f"{row['component']:<15} {row['task']:<25} {row['total_jobs']:<8} {row['successful_jobs']:<8} {row['failed_jobs']:<8} {row['failure_rate']:<10}")
        
    except Exception as e:
        print(f"Error analyzing BigQuery usage: {e}")

def reset_quota_tracking():
    """Reset quota tracking (for testing purposes)."""
    quota_manager = get_quota_manager()
    with quota_manager._lock:
        quota_manager._usage = {
            'queries': [],
            'inserts': [],
            'failed_requests': []
        }
    print("Quota tracking has been reset.")

def main():
    parser = argparse.ArgumentParser(description="BigQuery quota management utilities")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Status command
    subparsers.add_parser("status", help="Check current quota usage status")
    
    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Monitor quota usage continuously")
    monitor_parser.add_argument("--interval", type=int, default=60, help="Check interval in seconds")
    monitor_parser.add_argument("--duration", type=int, default=10, help="Duration in minutes")
    
    # Analyze command
    subparsers.add_parser("analyze", help="Analyze BigQuery usage patterns")
    
    # Reset command
    subparsers.add_parser("reset", help="Reset quota tracking")
    
    args = parser.parse_args()
    
    if args.command == "status":
        check_quota_status()
    elif args.command == "monitor":
        monitor_quota(args.interval, args.duration)
    elif args.command == "analyze":
        analyze_bigquery_usage()
    elif args.command == "reset":
        reset_quota_tracking()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
