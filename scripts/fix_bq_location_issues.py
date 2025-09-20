#!/usr/bin/env python3
"""
Fix BigQuery QueryJobConfig location issues across the codebase.

This script finds all QueryJobConfig instances that don't have the location parameter
and adds it to ensure compatibility with the latest BigQuery client.
"""

import os
import re
import sys
from pathlib import Path

def find_queryjobconfig_files():
    """Find all Python files that contain QueryJobConfig."""
    root = Path(__file__).parent.parent
    files = []
    
    for py_file in root.rglob("*.py"):
        if py_file.name.startswith(".") or "test_env" in str(py_file):
            continue
            
        try:
            content = py_file.read_text(encoding='utf-8')
            if "QueryJobConfig" in content:
                files.append(py_file)
        except Exception as e:
            print(f"Warning: Could not read {py_file}: {e}")
    
    return files

def fix_queryjobconfig_location(file_path):
    """Fix QueryJobConfig instances in a file to include location parameter."""
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        
        # Pattern to match QueryJobConfig with parameters but no location
        # This matches QueryJobConfig( with parameters but not location=
        pattern = r'bigquery\.QueryJobConfig\(\s*([^)]*?)\s*\)'
        
        def replace_queryjobconfig(match):
            params = match.group(1).strip()
            
            # Skip if location is already present
            if 'location=' in params:
                return match.group(0)
            
            # Skip if it's an empty QueryJobConfig
            if not params:
                return match.group(0)
            
            # Add location parameter
            if params.endswith(','):
                new_params = f"{params}\n        location=cfg.bq_location,"
            else:
                new_params = f"{params},\n        location=cfg.bq_location"
            
            return f"bigquery.QueryJobConfig(
        \n        {new_params}\n,
        location=cfg.bq_location
    )"
        
        # Apply the fix
        content = re.sub(pattern, replace_queryjobconfig, content, flags=re.MULTILINE | re.DOTALL)
        
        # Also handle self._bq.QueryJobConfig
        pattern2 = r'self\._bq\.QueryJobConfig\(\s*([^)]*?)\s*\)'
        
        def replace_self_queryjobconfig(match):
            params = match.group(1).strip()
            
            # Skip if location is already present
            if 'location=' in params:
                return match.group(0)
            
            # Skip if it's an empty QueryJobConfig
            if not params:
                return match.group(0)
            
            # Add location parameter
            if params.endswith(','):
                new_params = f"{params}\n            location=self.location,"
            else:
                new_params = f"{params},\n            location=self.location"
            
            return f"self._bq.QueryJobConfig(
            \n            {new_params}\n,
            location=self.location
        )"
        
        content = re.sub(pattern2, replace_self_queryjobconfig, content, flags=re.MULTILINE | re.DOTALL)
        
        # Only write if content changed
        if content != original_content:
            file_path.write_text(content, encoding='utf-8')
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Main function to fix all QueryJobConfig location issues."""
    print("🔧 Fixing BigQuery QueryJobConfig location issues...")
    
    # Find all files with QueryJobConfig
    files = find_queryjobconfig_files()
    print(f"Found {len(files)} files with QueryJobConfig")
    
    fixed_count = 0
    for file_path in files:
        print(f"Processing: {file_path.relative_to(Path(__file__).parent.parent)}")
        if fix_queryjobconfig_location(file_path):
            print(f"  ✅ Fixed QueryJobConfig location issues")
            fixed_count += 1
        else:
            print(f"  ⏭️  No changes needed")
    
    print(f"\n🎉 Fixed {fixed_count} files")
    
    # Check if we need to add imports
    print("\n📋 Checking for missing imports...")
    
    # Check if any files need the cfg import
    for file_path in files:
        try:
            content = file_path.read_text(encoding='utf-8')
            if 'cfg.bq_location' in content and 'from .config import cfg' not in content and 'import cfg' not in content:
                print(f"⚠️  {file_path.relative_to(Path(__file__).parent.parent)} may need cfg import")
        except Exception:
            pass

if __name__ == "__main__":
    main()
