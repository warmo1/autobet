#!/usr/bin/env python3
"""
Diagnose and fix BigQuery issues after aggressive regex replacements.
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

def check_syntax_errors(file_path):
    """Check if a Python file has syntax errors."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        compile(content, file_path, 'exec')
        return True, None
    except SyntaxError as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)

def find_corrupted_files():
    """Find files that may have been corrupted by regex replacements."""
    files = find_python_files()
    corrupted = []
    
    for file_path in files:
        is_valid, error = check_syntax_errors(file_path)
        if not is_valid:
            corrupted.append((file_path, error))
    
    return corrupted

def fix_common_regex_issues(file_path):
    """Fix common issues caused by aggressive regex replacements."""
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        
        # Fix common regex issues
        fixes = [
            # Fix malformed function calls
            (r'\(\s*\)\s*\)', ')'),
            # Fix malformed tuples
            (r'\(\s*,\s*\)', '()'),
            # Fix malformed QueryJobConfig
            (r'QueryJobConfig\(\s*\)', 'QueryJobConfig()'),
            # Fix trailing commas in function calls
            (r',\s*\)', ')'),
            # Fix multiple commas
            (r',\s*,', ','),
        ]
        
        for pattern, replacement in fixes:
            content = re.sub(pattern, replacement, content)
        
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
    """Main function to diagnose and fix BigQuery issues."""
    print("🔍 Diagnosing BigQuery issues after regex replacements...")
    
    # Find corrupted files
    print("Checking for syntax errors...")
    corrupted = find_corrupted_files()
    
    if corrupted:
        print(f"❌ Found {len(corrupted)} files with syntax errors:")
        for file_path, error in corrupted[:10]:  # Show first 10
            print(f"  - {file_path}: {error}")
        if len(corrupted) > 10:
            print(f"  ... and {len(corrupted) - 10} more")
    else:
        print("✅ No syntax errors found")
    
    # Fix common issues
    print("\n🔧 Fixing common regex issues...")
    files = find_python_files()
    fixed_files = 0
    
    for file_path in files:
        if fix_common_regex_issues(file_path):
            fixed_files += 1
    
    print(f"Fixed {fixed_files} files")
    
    # Test BigQuery connection
    print(f"\n🧪 Testing BigQuery connection...")
    if test_bigquery_connection():
        print("🎉 BigQuery connection working!")
        return True
    else:
        print("⚠️  BigQuery still has issues")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
