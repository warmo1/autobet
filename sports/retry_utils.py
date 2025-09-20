"""Retry utilities with exponential backoff for BigQuery operations."""

import time
import random
import logging
from typing import Callable, Any, Optional, Type, Tuple
from functools import wraps

logger = logging.getLogger(__name__)

class RetryableError(Exception):
    """Base class for errors that should trigger a retry."""
    pass

class QuotaExceededError(RetryableError):
    """Error indicating BigQuery quota has been exceeded."""
    pass

class TemporaryError(RetryableError):
    """Error indicating a temporary issue that might resolve with retry."""
    pass

def is_quota_error(error: Exception) -> bool:
    """Check if an error is related to quota limits."""
    error_str = str(error).lower()
    return any(phrase in error_str for phrase in [
        'quota exceeded',
        'quota limit',
        'rate limit',
        'too many requests',
        'resource exhausted'
    ])

def is_temporary_error(error: Exception) -> bool:
    """Check if an error is likely temporary and worth retrying."""
    error_str = str(error).lower()
    return any(phrase in error_str for phrase in [
        'timeout',
        'connection',
        'network',
        'service unavailable',
        'internal error',
        'deadline exceeded',
        'unavailable'
    ])

def exponential_backoff_with_jitter(
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    max_retries: int = 5,
    backoff_factor: float = 2.0,
    jitter: bool = True
):
    """
    Decorator that implements exponential backoff with jitter for retrying functions.
    
    Args:
        base_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        max_retries: Maximum number of retry attempts
        backoff_factor: Factor to multiply delay by after each retry
        jitter: Whether to add random jitter to avoid thundering herd
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry this error
                    if attempt == max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}")
                        break
                    
                    if is_quota_error(e):
                        logger.warning(f"Quota error on attempt {attempt + 1} for {func.__name__}: {e}")
                        # For quota errors, use longer delays
                        delay = min(base_delay * (backoff_factor ** attempt) * 2, max_delay * 2)
                    elif is_temporary_error(e):
                        logger.warning(f"Temporary error on attempt {attempt + 1} for {func.__name__}: {e}")
                        delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                    else:
                        # Don't retry for non-retryable errors
                        logger.error(f"Non-retryable error in {func.__name__}: {e}")
                        break
                    
                    if jitter:
                        # Add jitter to avoid thundering herd
                        delay = delay * (0.5 + random.random() * 0.5)
                    
                    logger.info(f"Retrying {func.__name__} in {delay:.2f} seconds (attempt {attempt + 2}/{max_retries + 1})")
                    time.sleep(delay)
            
            # If we get here, all retries failed
            raise last_exception
            
        return wrapper
    return decorator

def retry_on_quota_exceeded(max_retries: int = 3):
    """
    Decorator specifically for handling BigQuery quota exceeded errors.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__} due to quota issues")
                        break
                    
                    if is_quota_error(e):
                        # Use progressive backoff for quota errors
                        delay = (attempt + 1) * 30  # 30s, 60s, 90s
                        logger.warning(f"Quota exceeded on attempt {attempt + 1} for {func.__name__}, waiting {delay}s")
                        time.sleep(delay)
                    else:
                        # Don't retry non-quota errors
                        break
            
            raise last_exception
            
        return wrapper
    return decorator

def batch_operations(operations: list, batch_size: int = 10, delay_between_batches: float = 1.0):
    """
    Execute operations in batches with delays to avoid overwhelming the service.
    
    Args:
        operations: List of callable operations to execute
        batch_size: Number of operations per batch
        delay_between_batches: Delay in seconds between batches
    """
    results = []
    
    for i in range(0, len(operations), batch_size):
        batch = operations[i:i + batch_size]
        logger.info(f"Executing batch {i // batch_size + 1}/{(len(operations) + batch_size - 1) // batch_size}")
        
        batch_results = []
        for operation in batch:
            try:
                result = operation()
                batch_results.append(result)
            except Exception as e:
                logger.error(f"Operation failed in batch: {e}")
                batch_results.append(None)
        
        results.extend(batch_results)
        
        # Delay between batches (except for the last batch)
        if i + batch_size < len(operations):
            time.sleep(delay_between_batches)
    
    return results
