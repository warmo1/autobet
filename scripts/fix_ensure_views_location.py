#!/usr/bin/env python3
"""
Fix the ensure_views function to use self.query() instead of client.query() 
to ensure proper location parameter handling.
"""

import re
from pathlib import Path

def fix_ensure_views():
    """Fix the ensure_views function in bq.py to use self.query() instead of client.query()."""
    bq_file = Path(__file__).parent.parent / "sports" / "bq.py"
    
    if not bq_file.exists():
        print("❌ bq.py not found")
        return False
    
    content = bq_file.read_text(encoding='utf-8')
    original_content = content
    
    # Pattern to match client.query() calls in ensure_views function
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
    
    if content != original_content:
        bq_file.write_text(content, encoding='utf-8')
        print("✅ Fixed ensure_views function to use self.query()")
        return True
    else:
        print("⏭️  No changes needed")
        return False

if __name__ == "__main__":
    print("🔧 Fixing ensure_views function location handling...")
    fix_ensure_views()
    print("Done!")
