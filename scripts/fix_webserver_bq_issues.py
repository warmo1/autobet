#!/usr/bin/env python3
"""
Comprehensive script to fix all BigQuery QueryJobConfig location issues on the web server.

This script will:
1. Fix all QueryJobConfig instances to include the location parameter
2. Fix ensure_views function to use self.query() instead of client.query()
3. Add missing imports where needed
4. Test the fixes

Run this script on the web server to fix the BigQuery location errors.
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

def fix_queryjobconfig_location(file_path):
    """Fix QueryJobConfig instances in a file to include location parameter."""
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        
        # Pattern 1: bigquery.QueryJobConfig( with parameters but no location
        pattern1 = r'bigquery\.QueryJobConfig\(\s*([^)]*?)\s*\)'
        
        def replace_bigquery_queryjobconfig(match):
            params = match.group(1).strip()
            
            # Skip if location is already present
            if 'location=' in params:
                return match.group(0)
            
            # Skip if it's an empty QueryJobConfig
            if not params:
                return match.group(0)
            
            # Add location parameter
            if params.endswith(','):
                new_params = f"{params}\n        location=cfg.bq_location"
            else:
                new_params = f"{params},\n        location=cfg.bq_location"
            
            return f"bigquery.QueryJobConfig(\n        {new_params}\n    )"
        
        content = re.sub(pattern1, replace_bigquery_queryjobconfig, content, flags=re.MULTILINE | re.DOTALL)
        
        # Pattern 2: self._bq.QueryJobConfig( with parameters but no location
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
                new_params = f"{params}\n            location=self.location"
            else:
                new_params = f"{params},\n            location=self.location"
            
            return f"self._bq.QueryJobConfig(\n            {new_params}\n        )"
        
        content = re.sub(pattern2, replace_self_queryjobconfig, content, flags=re.MULTILINE | re.DOTALL)
        
        # Only write if content changed
        if content != original_content:
            file_path.write_text(content, encoding='utf-8')
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def fix_ensure_views_client_query(file_path):
    """Fix client.query() calls in ensure_views to use self.query()."""
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        
        # Pattern to match client.query() calls that should be self.query()
        # Look for client.query(sql) and replace with self.query(sql)
        pattern = r'(\s+)client\.query\(sql\)\.result\(\)'
        replacement = r'\1self.query(sql).result()'
        content = re.sub(pattern, replacement, content)
        
        # Also handle cases where it's just client.query(sql) without .result()
        pattern2 = r'(\s+)client\.query\(sql\)'
        replacement2 = r'\1self.query(sql)'
        content = re.sub(pattern2, replacement2, content)
        
        # Handle the specific case with job = client.query(sql); job.result()
        pattern3 = r'(\s+)job = client\.query\(sql\); job\.result\(\)'
        replacement3 = r'\1job = self.query(sql); job.result()'
        content = re.sub(pattern3, replacement3, content)
        
        # Handle other client.query calls that should be self.query
        pattern4 = r'(\s+)client\.query\(sql_ctas\)\.result\(\)'
        replacement4 = r'\1self.query(sql_ctas).result()'
        content = re.sub(pattern4, replacement4, content)
        
        pattern5 = r'(\s+)client\.query\(sql_merge\)\.result\(\)'
        replacement5 = r'\1self.query(sql_merge).result()'
        content = re.sub(pattern5, replacement5, content)
        
        pattern6 = r'(\s+)client\.query\(f"ALTER TABLE `\{ds\}\.tote_params` ADD COLUMN IF NOT EXISTS `\{name\}` \{typ\}"\)\.result\(\)'
        replacement6 = r'\1self.query(f"ALTER TABLE `{ds}.tote_params` ADD COLUMN IF NOT EXISTS `{name}` {typ}").result()'
        content = re.sub(pattern6, replacement6, content)
        
        pattern7 = r'(\s+)client\.query\(f"DROP VIEW IF EXISTS `\{ds\}\.vw_sf_strengths_from_win_horse`"\)\.result\(\)'
        replacement7 = r'\1self.query(f"DROP VIEW IF EXISTS `{ds}.vw_sf_strengths_from_win_horse`").result()'
        content = re.sub(pattern7, replacement7, content)
        
        # Only write if content changed
        if content != original_content:
            file_path.write_text(content, encoding='utf-8')
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def add_missing_imports(file_path):
    """Add missing cfg imports to files that need them."""
    try:
        content = file_path.read_text(encoding='utf-8')
        original_content = content
        
        # Check if file uses cfg.bq_location but doesn't import cfg
        if 'cfg.bq_location' in content and 'from .config import cfg' not in content and 'import cfg' not in content:
            # Find the first import statement
            lines = content.split('\n')
            import_line = -1
            
            for i, line in enumerate(lines):
                if line.strip().startswith('import ') or line.strip().startswith('from '):
                    import_line = i
                    break
            
            if import_line >= 0:
                # Add cfg import after the last import
                lines.insert(import_line + 1, 'from sports.config import cfg')
                content = '\n'.join(lines)
            else:
                # Add at the top after shebang
                lines = content.split('\n')
                if lines[0].startswith('#!'):
                    lines.insert(1, 'from sports.config import cfg')
                else:
                    lines.insert(0, 'from sports.config import cfg')
                content = '\n'.join(lines)
        
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
    """Main function to fix all BigQuery location issues."""
    print("🔧 Fixing BigQuery QueryJobConfig location issues on web server...")
    
    # Find all Python files
    files = find_python_files()
    print(f"Found {len(files)} Python files to check")
    
    fixed_queryjobconfig = 0
    fixed_client_query = 0
    added_imports = 0
    
    for file_path in files:
        print(f"Processing: {file_path}")
        
        # Fix QueryJobConfig location issues
        if fix_queryjobconfig_location(file_path):
            print(f"  ✅ Fixed QueryJobConfig location issues")
            fixed_queryjobconfig += 1
        
        # Fix client.query() calls in ensure_views
        if fix_ensure_views_client_query(file_path):
            print(f"  ✅ Fixed client.query() calls")
            fixed_client_query += 1
        
        # Add missing imports
        if add_missing_imports(file_path):
            print(f"  ✅ Added missing imports")
            added_imports += 1
        
        if not any([fix_queryjobconfig_location(file_path), 
                   fix_ensure_views_client_query(file_path), 
                   add_missing_imports(file_path)]):
            print(f"  ⏭️  No changes needed")
    
    print(f"\n📊 Summary:")
    print(f"  - Fixed QueryJobConfig: {fixed_queryjobconfig} files")
    print(f"  - Fixed client.query(): {fixed_client_query} files")
    print(f"  - Added imports: {added_imports} files")
    
    # Test the fixes
    print(f"\n🧪 Testing BigQuery connection...")
    if test_bigquery_connection():
        print("🎉 All fixes applied successfully!")
    else:
        print("⚠️  Some issues may remain. Check the error messages above.")

if __name__ == "__main__":
    main()
