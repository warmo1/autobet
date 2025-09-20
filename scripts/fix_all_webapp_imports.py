#!/usr/bin/env python3
"""
Fix all relative imports in webapp.py
"""

import re
from pathlib import Path

def fix_webapp_imports():
    """Fix all relative imports in webapp.py"""
    
    webapp_path = Path("sports/webapp.py")
    if not webapp_path.exists():
        print("❌ webapp.py not found")
        return False
    
    print("🔧 Fixing all relative imports in webapp.py...")
    
    # Read the current file
    with open(webapp_path, 'r') as f:
        content = f.read()
    
    # Count original relative imports
    original_relative_imports = len(re.findall(r'from \.', content))
    print(f"Found {original_relative_imports} relative imports to fix")
    
    # Fix all relative imports
    content = re.sub(r'from \.', 'from ', content)
    
    # Count remaining relative imports
    remaining_relative_imports = len(re.findall(r'from \.', content))
    
    if remaining_relative_imports == 0:
        # Write the fixed content
        with open(webapp_path, 'w') as f:
            f.write(content)
        print(f"✅ Fixed {original_relative_imports} relative imports in webapp.py")
        return True
    else:
        print(f"❌ Still have {remaining_relative_imports} relative imports")
        return False

def main():
    """Main function"""
    if fix_webapp_imports():
        print("✅ All relative imports fixed!")
    else:
        print("❌ Failed to fix imports")

if __name__ == "__main__":
    main()
