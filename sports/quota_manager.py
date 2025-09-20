"""BigQuery quota management and rate limiting utilities."""

import time
import threading
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@dataclass
class QuotaLimits:
    """BigQuery quota limits per time window."""
    # Per-minute limits
    queries_per_minute: int = 300
    inserts_per_minute: int = 1000
    
    # Per-hour limits  
    queries_per_hour: int = 10000
    inserts_per_hour: int = 20000
    
    # Per-day limits
    queries_per_day: int = 100000
    inserts_per_day: int = 200000

class QuotaManager:
    """Manages BigQuery quota usage and implements rate limiting."""
    
    def __init__(self, limits: Optional[QuotaLimits] = None):
        self.limits = limits or QuotaLimits()
        self._lock = threading.Lock()
        self._usage: Dict[str, list] = {
            'queries': [],
            'inserts': [],
            'failed_requests': []
        }
        
    def _cleanup_old_entries(self, operation_type: str, window_seconds: int):
        """Remove entries older than the specified window."""
        cutoff = time.time() - window_seconds
        with self._lock:
            self._usage[operation_type] = [
                timestamp for timestamp in self._usage[operation_type] 
                if timestamp > cutoff
            ]
    
    def _get_current_count(self, operation_type: str, window_seconds: int) -> int:
        """Get current count of operations in the specified time window."""
        self._cleanup_old_entries(operation_type, window_seconds)
        with self._lock:
            return len(self._usage[operation_type])
    
    def _record_operation(self, operation_type: str, success: bool = True):
        """Record an operation attempt."""
        timestamp = time.time()
        with self._lock:
            self._usage[operation_type].append(timestamp)
            if not success:
                self._usage['failed_requests'].append(timestamp)
    
    def can_execute_query(self) -> bool:
        """Check if we can execute a query without hitting quota limits."""
        queries_per_minute = self._get_current_count('queries', 60)
        queries_per_hour = self._get_current_count('queries', 3600)
        queries_per_day = self._get_current_count('queries', 86400)
        
        return (
            queries_per_minute < self.limits.queries_per_minute and
            queries_per_hour < self.limits.queries_per_hour and
            queries_per_day < self.limits.queries_per_day
        )
    
    def can_execute_insert(self) -> bool:
        """Check if we can execute an insert without hitting quota limits."""
        inserts_per_minute = self._get_current_count('inserts', 60)
        inserts_per_hour = self._get_current_count('inserts', 3600)
        inserts_per_day = self._get_current_count('inserts', 86400)
        
        return (
            inserts_per_minute < self.limits.inserts_per_minute and
            inserts_per_hour < self.limits.inserts_per_hour and
            inserts_per_day < self.limits.inserts_per_day
        )
    
    def record_query(self, success: bool = True):
        """Record a query operation."""
        self._record_operation('queries', success)
    
    def record_insert(self, success: bool = True):
        """Record an insert operation."""
        self._record_operation('inserts', success)
    
    def get_wait_time(self, operation_type: str) -> float:
        """Get the time to wait before the next operation can be performed."""
        if operation_type == 'query':
            if not self.can_execute_query():
                # Find the oldest entry in the current minute
                with self._lock:
                    if self._usage['queries']:
                        oldest_in_minute = min(
                            ts for ts in self._usage['queries'] 
                            if ts > time.time() - 60
                        )
                        return max(0, 60 - (time.time() - oldest_in_minute))
        elif operation_type == 'insert':
            if not self.can_execute_insert():
                with self._lock:
                    if self._usage['inserts']:
                        oldest_in_minute = min(
                            ts for ts in self._usage['inserts'] 
                            if ts > time.time() - 60
                        )
                        return max(0, 60 - (time.time() - oldest_in_minute))
        return 0.0
    
    def wait_if_needed(self, operation_type: str, max_wait: float = 30.0):
        """Wait if necessary to avoid hitting quota limits."""
        wait_time = min(self.get_wait_time(operation_type), max_wait)
        if wait_time > 0:
            logger.warning(f"Rate limiting: waiting {wait_time:.2f}s before {operation_type}")
            time.sleep(wait_time)
    
    def get_usage_stats(self) -> Dict[str, Dict[str, int]]:
        """Get current usage statistics."""
        return {
            'queries': {
                'per_minute': self._get_current_count('queries', 60),
                'per_hour': self._get_current_count('queries', 3600),
                'per_day': self._get_current_count('queries', 86400),
            },
            'inserts': {
                'per_minute': self._get_current_count('inserts', 60),
                'per_hour': self._get_current_count('inserts', 3600),
                'per_day': self._get_current_count('inserts', 86400),
            },
            'failed_requests': {
                'last_hour': self._get_current_count('failed_requests', 3600),
            }
        }

# Global quota manager instance
_quota_manager = None
_quota_manager_lock = threading.Lock()

def get_quota_manager() -> QuotaManager:
    """Get the global quota manager instance."""
    global _quota_manager
    with _quota_manager_lock:
        if _quota_manager is None:
            _quota_manager = QuotaManager()
        return _quota_manager

def with_quota_management(operation_type: str, max_wait: float = 30.0):
    """Decorator to add quota management to BigQuery operations."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            quota_manager = get_quota_manager()
            
            # Wait if necessary
            quota_manager.wait_if_needed(operation_type, max_wait)
            
            # Check if we can proceed
            can_proceed = (
                quota_manager.can_execute_query() if operation_type == 'query' 
                else quota_manager.can_execute_insert()
            )
            
            if not can_proceed:
                raise RuntimeError(f"Quota limit exceeded for {operation_type} operations")
            
            try:
                result = func(*args, **kwargs)
                quota_manager.record_query(success=True) if operation_type == 'query' else quota_manager.record_insert(success=True)
                return result
            except Exception as e:
                quota_manager.record_query(success=False) if operation_type == 'query' else quota_manager.record_insert(success=False)
                raise e
                
        return wrapper
    return decorator
