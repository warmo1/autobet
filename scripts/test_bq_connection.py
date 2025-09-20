#!/usr/bin/env python3
"""
Simple test script to verify BigQuery connection works after fixes.

Run this from the project root directory:
python3 scripts/test_bq_connection.py
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

def test_bigquery_connection():
    """Test if BigQuery connection works after fixes."""
    try:
        print("🔧 Testing BigQuery connection...")
        print(f"Project root: {project_root}")
        print(f"Python path: {sys.path[:3]}...")
        
        from sports.db import get_db
        print("✅ Successfully imported sports.db")
        
        db = get_db()
        print("✅ Successfully created BigQuery connection")
        
        result = db.query('SELECT 1 as test')
        print("✅ Successfully executed BigQuery query")
        
        print("🎉 BigQuery connection test PASSED!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure you're running from the project root directory")
        return False
    except Exception as e:
        print(f"❌ BigQuery error: {e}")
        return False

if __name__ == "__main__":
    success = test_bigquery_connection()
    sys.exit(0 if success else 1)
