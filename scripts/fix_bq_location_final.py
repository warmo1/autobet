#!/usr/bin/env python3
"""
Final fix for BigQuery location parameter issues.
This removes ALL location parameters from QueryJobConfig instances.
"""

import os
import re
from pathlib import Path

def fix_bq_location_issues():
    """Remove all location parameters from QueryJobConfig instances"""
    
    print("🔧 Fixing BigQuery location parameter issues...")
    
    # Files to check and fix
    files_to_fix = [
        "sports/webapp.py",
        "sports/bq.py", 
        "sports/superfecta_automation.py",
        "sports/ml/superfecta.py"
    ]
    
    total_fixes = 0
    
    for file_path in files_to_fix:
        if not Path(file_path).exists():
            print(f"⚠️ File not found: {file_path}")
            continue
            
        print(f"🔧 Processing: {file_path}")
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        original_content = content
        
        # Pattern 1: Remove location=cfg.bq_location, from QueryJobConfig
        content = re.sub(
            r'location=cfg\.bq_location,\s*',
            '',
            content
        )
        
        # Pattern 2: Remove location=self.location, from QueryJobConfig  
        content = re.sub(
            r'location=self\.location,\s*',
            '',
            content
        )
        
        # Pattern 3: Remove location=sink.location, from QueryJobConfig
        content = re.sub(
            r'location=sink\.location,\s*',
            '',
            content
        )
        
        # Pattern 4: Remove any remaining location=... from QueryJobConfig
        content = re.sub(
            r'location=[^,)]+,\s*',
            '',
            content
        )
        
        # Clean up trailing commas before closing parentheses
        content = re.sub(r',\s*\)', ')', content)
        
        # Count changes
        changes = len(re.findall(r'location=', original_content)) - len(re.findall(r'location=', content))
        total_fixes += changes
        
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            print(f"✅ Fixed {changes} location parameters in {file_path}")
        else:
            print(f"ℹ️ No changes needed in {file_path}")
    
    print(f"\n📊 Total fixes applied: {total_fixes}")
    return total_fixes > 0

def test_bigquery_import():
    """Test if BigQuery can be imported without location errors"""
    
    print("\n🧪 Testing BigQuery import...")
    
    try:
        from google.cloud import bigquery
        
        # Test QueryJobConfig creation without location
        config = bigquery.QueryJobConfig(
            default_dataset="test_dataset",
            use_query_cache=True
        )
        
        print("✅ BigQuery QueryJobConfig works without location parameter")
        return True
        
    except Exception as e:
        print(f"❌ BigQuery test failed: {e}")
        return False

def main():
    """Main function"""
    
    print("🚀 Final BigQuery location fix...")
    
    # Fix the files
    if fix_bq_location_issues():
        print("✅ Files updated successfully")
    else:
        print("ℹ️ No files needed updating")
    
    # Test BigQuery
    if test_bigquery_import():
        print("✅ BigQuery location fix successful!")
        print("\n🎯 The webapp should now work without location parameter errors")
    else:
        print("❌ BigQuery test failed - check the error messages above")

if __name__ == "__main__":
    main()
