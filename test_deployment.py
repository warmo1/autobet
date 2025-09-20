#!/usr/bin/env python3
"""
Simple test script to verify the quota management system works
before deploying to Cloud Run.
"""

import sys
import os
sys.path.append('sports')

def test_imports():
    """Test all critical imports."""
    print("🧪 Testing imports...")
    
    try:
        from sports.quota_manager import get_quota_manager, QuotaLimits
        print("  ✅ quota_manager")
    except Exception as e:
        print(f"  ❌ quota_manager: {e}")
        return False
    
    try:
        from sports.retry_utils import exponential_backoff_with_jitter, is_quota_error
        print("  ✅ retry_utils")
    except Exception as e:
        print(f"  ❌ retry_utils: {e}")
        return False
    
    try:
        from sports.bq import get_bq_sink
        print("  ✅ bq")
    except Exception as e:
        print(f"  ❌ bq: {e}")
        return False
    
    try:
        from sports.webapp import app
        print("  ✅ webapp")
    except Exception as e:
        print(f"  ❌ webapp: {e}")
        return False
    
    return True

def test_quota_manager():
    """Test quota manager functionality."""
    print("\n🧪 Testing quota manager...")
    
    try:
        from sports.quota_manager import get_quota_manager
        
        qm = get_quota_manager()
        stats = qm.get_usage_stats()
        
        print(f"  📊 Query usage: {stats['queries']['per_minute']}/min")
        print(f"  📊 Insert usage: {stats['inserts']['per_minute']}/min")
        print(f"  ✅ Can execute query: {qm.can_execute_query()}")
        print(f"  ✅ Can execute insert: {qm.can_execute_insert()}")
        
        return True
    except Exception as e:
        print(f"  ❌ Quota manager test failed: {e}")
        return False

def test_retry_logic():
    """Test retry logic."""
    print("\n🧪 Testing retry logic...")
    
    try:
        from sports.retry_utils import exponential_backoff_with_jitter, is_quota_error
        
        # Test quota error detection
        quota_error = Exception("quota exceeded")
        temp_error = Exception("timeout")
        
        print(f"  ✅ Quota error detection: {is_quota_error(quota_error)}")
        print(f"  ✅ Temp error detection: {is_quota_error(temp_error)}")
        
        # Test retry decorator
        @exponential_backoff_with_jitter(base_delay=0.01, max_delay=0.1, max_retries=1)
        def test_func():
            return "success"
        
        result = test_func()
        print(f"  ✅ Retry decorator: {result}")
        
        return True
    except Exception as e:
        print(f"  ❌ Retry logic test failed: {e}")
        return False

def test_webapp_endpoints():
    """Test webapp endpoints."""
    print("\n🧪 Testing webapp endpoints...")
    
    try:
        from sports.webapp import app
        
        with app.test_client() as client:
            # Test health endpoint
            response = client.get('/')
            if response.status_code == 200:
                print("  ✅ Health endpoint")
            else:
                print(f"  ❌ Health endpoint: {response.status_code}")
                return False
            
            # Test quota usage endpoint
            response = client.get('/api/status/quota_usage')
            if response.status_code == 200:
                print("  ✅ Quota usage endpoint")
                data = response.get_json()
                print(f"  📊 Response: {list(data.keys())}")
            else:
                print(f"  ❌ Quota usage endpoint: {response.status_code}")
                return False
        
        return True
    except Exception as e:
        print(f"  ❌ Webapp test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Testing Quota Management System")
    print("=" * 40)
    
    tests = [
        ("Imports", test_imports),
        ("Quota Manager", test_quota_manager),
        ("Retry Logic", test_retry_logic),
        ("Webapp Endpoints", test_webapp_endpoints),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    print("\n📋 Test Results:")
    print("=" * 20)
    
    all_passed = True
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name}: {status}")
        if not result:
            all_passed = False
    
    print("\n" + "=" * 40)
    if all_passed:
        print("🎉 All tests passed! Ready for deployment.")
        return 0
    else:
        print("❌ Some tests failed. Fix issues before deployment.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
