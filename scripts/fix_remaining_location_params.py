#!/usr/bin/env python3
"""
Fix remaining location parameters in QueryJobConfig instances.
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

def fix_remaining_location_params(file_path):
    """Remove all remaining location parameters from QueryJobConfig instances."""
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        
        # Pattern 1: Remove location=cfg.bq_location from QueryJobConfig
        pattern1 = r'(\s+)location=cfg\.bq_location,?\s*'
        content = re.sub(pattern1, '', content)
        
        # Pattern 2: Remove location=self.location from QueryJobConfig
        pattern2 = r'(\s+)location=self\.location,?\s*'
        content = re.sub(pattern2, '', content)
        
        # Pattern 3: Clean up any trailing commas
        pattern3 = r',\s*\)'
        content = re.sub(pattern3, ')', content)
        
        # Pattern 4: Clean up any trailing commas in multiline QueryJobConfig
        pattern4 = r',\s*\n\s*\)'
        content = re.sub(pattern4, '\n    )', content)
        
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
    """Main function to fix remaining location parameters."""
    print("🔧 Fixing remaining location parameters in QueryJobConfig...")
    
    # Find all Python files
    files = find_python_files()
    print(f"Found {len(files)} Python files to check")
    
    fixed_files = 0
    
    for file_path in files:
        print(f"Processing: {file_path}")
        
        # Fix remaining location parameters
        if fix_remaining_location_params(file_path):
            print(f"  ✅ Removed location parameters")
            fixed_files += 1
        else:
            print(f"  ⏭️  No changes needed")
    
    print(f"\n📊 Summary:")
    print(f"  - Fixed files: {fixed_files}")
    
    # Test BigQuery connection
    print(f"\n🧪 Testing BigQuery connection...")
    if test_bigquery_connection():
        print("🎉 All location parameters removed successfully!")
        return True
    else:
        print("⚠️  Some issues may remain. Check the error messages above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
