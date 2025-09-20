#!/usr/bin/env python3
"""
Update BigQuery client to the latest version to support location parameter in QueryJobConfig.

This script will:
1. Check current BigQuery version
2. Update to the latest version
3. Test the connection
4. Verify QueryJobConfig location parameter works
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} successful")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ {description} failed")
            print(f"   Error: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"❌ {description} failed: {e}")
        return False

def check_bigquery_version():
    """Check current BigQuery version."""
    print("📋 Checking current BigQuery version...")
    try:
        result = subprocess.run([sys.executable, "-c", "import google.cloud.bigquery; print(google.cloud.bigquery.__version__)"], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"Current BigQuery version: {version}")
            return version
        else:
            print("❌ Could not determine BigQuery version")
            return None
    except Exception as e:
        print(f"❌ Error checking version: {e}")
        return None

def update_bigquery():
    """Update BigQuery to the latest version."""
    print("🚀 Updating BigQuery client...")
    
    # Update BigQuery and related packages
    packages = [
        "google-cloud-bigquery>=3.20.0",
        "google-cloud-bigquery-storage>=2.24.0", 
        "google-cloud-core>=2.4.0"
    ]
    
    for package in packages:
        if not run_command(f"pip install --upgrade '{package}'", f"Installing {package}"):
            return False
    
    return True

def test_bigquery_connection():
    """Test BigQuery connection after update."""
    try:
        # Add project root to Python path
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from sports.db import get_db
        print("🧪 Testing BigQuery connection...")
        db = get_db()
        result = db.query('SELECT 1 as test')
        print("✅ BigQuery connection successful")
        return True
    except Exception as e:
        print(f"❌ BigQuery error: {e}")
        return False

def test_queryjobconfig_location():
    """Test that QueryJobConfig location parameter works."""
    try:
        from google.cloud import bigquery
        from sports.config import cfg
        
        print("🧪 Testing QueryJobConfig location parameter...")
        
        # Test creating QueryJobConfig with location parameter
        job_config = bigquery.QueryJobConfig(
            default_dataset=f"{cfg.bq_project}.{cfg.bq_dataset}",
            location=cfg.bq_location
        )
        
        print("✅ QueryJobConfig with location parameter works")
        return True
    except Exception as e:
        print(f"❌ QueryJobConfig location test failed: {e}")
        return False

def main():
    """Main function to update BigQuery and test."""
    print("🔧 Updating BigQuery client for QueryJobConfig location support...")
    
    # Check current version
    current_version = check_bigquery_version()
    
    # Update BigQuery
    if not update_bigquery():
        print("❌ Failed to update BigQuery")
        return False
    
    # Check new version
    new_version = check_bigquery_version()
    if new_version and current_version:
        print(f"📈 Updated from {current_version} to {new_version}")
    
    # Test QueryJobConfig location parameter
    if not test_queryjobconfig_location():
        print("❌ QueryJobConfig location parameter still not working")
        return False
    
    # Test BigQuery connection
    if not test_bigquery_connection():
        print("❌ BigQuery connection failed")
        return False
    
    print("🎉 BigQuery update successful! QueryJobConfig location parameter is now supported.")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
