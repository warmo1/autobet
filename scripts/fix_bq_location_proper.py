#!/usr/bin/env python3
"""
Fix BigQuery location handling by using the correct approach:
- Remove location from QueryJobConfig (not supported in older versions)
- Pass location to client.query() method instead
"""

import os
import re
import sys
from pathlib import Path

def find_python_files():
    """Find all Python files in the project."""
    root = Path('.')
    files = []
    
    for py_file in root.rglob("*.py"):
        if py_file.name.startswith(".") or "test_env" in str(py_file) or "__pycache__" in str(py_file):
            continue
        files.append(py_file)
    
    return files

def fix_queryjobconfig_remove_location(file_path):
    """Remove location parameter from QueryJobConfig instances."""
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        
        # Pattern 1: Remove location parameter from bigquery.QueryJobConfig
        pattern1 = r'bigquery\.QueryJobConfig\(\s*([^)]*?),\s*location=cfg\.bq_location\s*\)'
        
        def replace_bigquery_queryjobconfig(match):
            params = match.group(1).strip()
            return f"bigquery.QueryJobConfig(\n        {params}\n    )"
        
        content = re.sub(pattern1, replace_bigquery_queryjobconfig, content, flags=re.MULTILINE | re.DOTALL)
        
        # Pattern 2: Remove location parameter from self._bq.QueryJobConfig
        pattern2 = r'self\._bq\.QueryJobConfig\(\s*([^)]*?),\s*location=self\.location\s*\)'
        
        def replace_self_queryjobconfig(match):
            params = match.group(1).strip()
            return f"self._bq.QueryJobConfig(\n            {params}\n        )"
        
        content = re.sub(pattern2, replace_self_queryjobconfig, content, flags=re.MULTILINE | re.DOTALL)
        
        # Pattern 3: Handle multiline cases with location parameter
        pattern3 = r'bigquery\.QueryJobConfig\(\s*([^)]*?)\n\s*location=cfg\.bq_location\s*\)'
        content = re.sub(pattern3, r'bigquery.QueryJobConfig(\n        \1\n    )', content, flags=re.MULTILINE | re.DOTALL)
        
        pattern4 = r'self\._bq\.QueryJobConfig\(\s*([^)]*?)\n\s*location=self\.location\s*\)'
        content = re.sub(pattern4, r'self._bq.QueryJobConfig(\n            \1\n        )', content, flags=re.MULTILINE | re.DOTALL)
        
        # Only write if content changed
        if content != original_content:
            file_path.write_text(content, encoding='utf-8')
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def test_bigquery_connection():
    """Test if BigQuery connection works after fixes."""
    try:
        # Add the project root to Python path
        import sys
        from pathlib import Path
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from sports.db import get_db
        print("Testing BigQuery connection...")
        db = get_db()
        result = db.query('SELECT 1 as test')
        print("✅ BigQuery connection successful")
        return True
    except Exception as e:
        print(f"❌ BigQuery error: {e}")
        return False

def test_queryjobconfig_without_location():
    """Test that QueryJobConfig works without location parameter."""
    try:
        from google.cloud import bigquery
        from sports.config import cfg
        
        print("Testing QueryJobConfig without location parameter...")
        
        # Test creating QueryJobConfig without location parameter
        job_config = bigquery.QueryJobConfig(
            default_dataset=f"{cfg.bq_project}.{cfg.bq_dataset}"
        )
        
        print("✅ QueryJobConfig without location parameter works")
        return True
    except Exception as e:
        print(f"❌ QueryJobConfig test failed: {e}")
        return False

def main():
    """Main function to fix BigQuery location issues."""
    print("🔧 Fixing BigQuery location handling (removing location from QueryJobConfig)...")
    
    # Find all Python files
    files = find_python_files()
    print(f"Found {len(files)} Python files to check")
    
    fixed_files = 0
    
    for file_path in files:
        print(f"Processing: {file_path}")
        
        # Fix QueryJobConfig by removing location parameter
        if fix_queryjobconfig_remove_location(file_path):
            print(f"  ✅ Removed location from QueryJobConfig")
            fixed_files += 1
        else:
            print(f"  ⏭️  No changes needed")
    
    print(f"\n📊 Summary:")
    print(f"  - Fixed QueryJobConfig: {fixed_files} files")
    
    # Test QueryJobConfig without location
    print(f"\n🧪 Testing QueryJobConfig without location parameter...")
    if not test_queryjobconfig_without_location():
        print("❌ QueryJobConfig test failed")
        return False
    
    # Test BigQuery connection
    print(f"\n🧪 Testing BigQuery connection...")
    if test_bigquery_connection():
        print("🎉 All fixes applied successfully!")
        print("💡 Location is now handled at the client level in bq.py")
        return True
    else:
        print("⚠️  Some issues may remain. Check the error messages above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
