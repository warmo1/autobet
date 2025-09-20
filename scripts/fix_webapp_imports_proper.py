#!/usr/bin/env python3
"""
Fix webapp imports to work with the sports package structure
"""

import re
from pathlib import Path

def fix_webapp_imports():
    """Fix webapp imports to work properly"""
    
    webapp_path = Path("sports/webapp.py")
    if not webapp_path.exists():
        print("❌ webapp.py not found")
        return False
    
    print("🔧 Fixing webapp imports for sports package...")
    
    # Read the current file
    with open(webapp_path, 'r') as f:
        content = f.read()
    
    # Fix imports to use sports package
    fixes = [
        # Core sports modules
        (r'from config import', 'from sports.config import'),
        (r'from db import', 'from sports.db import'),
        (r'from superfecta_automation import', 'from sports.superfecta_automation import'),
        (r'from ml.superfecta import', 'from sports.ml.superfecta import'),
        (r'from providers.tote_bets import', 'from sports.providers.tote_bets import'),
        (r'from providers.tote_api import', 'from sports.providers.tote_api import'),
        (r'from providers.pl_calcs import', 'from sports.providers.pl_calcs import'),
        (r'from providers.tote_subscriptions import', 'from sports.providers.tote_subscriptions import'),
        (r'from gcp import', 'from sports.gcp import'),
        (r'from superfecta_planner import', 'from sports.superfecta_planner import'),
        (r'from realtime import', 'from sports.realtime import'),
        (r'from ingest.tote_products import', 'from sports.ingest.tote_products import'),
        (r'from bq import', 'from sports.bq import'),
    ]
    
    original_content = content
    for pattern, replacement in fixes:
        content = re.sub(pattern, replacement, content)
    
    if content != original_content:
        # Write the fixed content
        with open(webapp_path, 'w') as f:
            f.write(content)
        print("✅ Fixed webapp imports for sports package")
        return True
    else:
        print("ℹ️ No imports needed fixing")
        return True

def main():
    """Main function"""
    if fix_webapp_imports():
        print("✅ Webapp imports fixed!")
    else:
        print("❌ Failed to fix imports")

if __name__ == "__main__":
    main()
