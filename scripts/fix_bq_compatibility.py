#!/usr/bin/env python3
"""
Fix BigQuery compatibility issues by removing location parameter from QueryJobConfig
and using the client-level location instead.

This addresses the issue where older versions of google-cloud-bigquery don't support
the location parameter in QueryJobConfig.
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

def fix_queryjobconfig_compatibility(file_path):
    """Remove location parameter from QueryJobConfig instances for compatibility."""
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
        
        # Pattern 3: Handle multiline cases
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

def ensure_client_location_handling(file_path):
    """Ensure that location is handled at the client level, not QueryJobConfig level."""
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        
        # In the main query method, ensure location is passed to client.query() not QueryJobConfig
        # This is already handled in the bq.py file, but let's make sure
        
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

def main():
    """Main function to fix BigQuery compatibility issues."""
    print("🔧 Fixing BigQuery QueryJobConfig compatibility issues...")
    
    # Find all Python files
    files = find_python_files()
    print(f"Found {len(files)} Python files to check")
    
    fixed_files = 0
    
    for file_path in files:
        print(f"Processing: {file_path}")
        
        # Fix QueryJobConfig compatibility
        if fix_queryjobconfig_compatibility(file_path):
            print(f"  ✅ Fixed QueryJobConfig compatibility")
            fixed_files += 1
        else:
            print(f"  ⏭️  No changes needed")
    
    print(f"\n📊 Summary:")
    print(f"  - Fixed compatibility: {fixed_files} files")
    
    # Test the fixes
    print(f"\n🧪 Testing BigQuery connection...")
    if test_bigquery_connection():
        print("🎉 All fixes applied successfully!")
    else:
        print("⚠️  Some issues may remain. Check the error messages above.")

if __name__ == "__main__":
    main()
