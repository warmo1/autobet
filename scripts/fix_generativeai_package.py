#!/usr/bin/env python3
"""
Fix the google_generativeai package installation issue.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} successful")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ {description} failed")
            print(f"   Error: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"❌ {description} failed: {e}")
        return False

def fix_generativeai_package():
    """Fix the google_generativeai package installation."""
    print("🔧 Fixing google_generativeai package...")
    
    # Step 1: Uninstall the problematic package
    if not run_command("pip uninstall google-generativeai -y", "Uninstalling google-generativeai"):
        return False
    
    # Step 2: Clean up any corrupted .pth files
    venv_path = Path(sys.prefix)
    pth_files = list(venv_path.glob("**/*.pth"))
    
    for pth_file in pth_files:
        if "google_generativeai" in pth_file.name:
            print(f"🗑️  Removing corrupted .pth file: {pth_file}")
            try:
                pth_file.unlink()
            except Exception as e:
                print(f"⚠️  Could not remove {pth_file}: {e}")
    
    # Step 3: Reinstall the package
    if not run_command("pip install google-generativeai", "Reinstalling google-generativeai"):
        return False
    
    # Step 4: Test the installation
    if not run_command("python -c 'import google.generativeai; print(\"✅ google.generativeai imported successfully\")'", "Testing google.generativeai import"):
        return False
    
    return True

def test_web_server_start():
    """Test if the web server can start without errors."""
    try:
        print("🧪 Testing web server startup...")
        
        # Add project root to Python path
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        # Test importing the main modules
        from sports.db import get_db
        from sports.config import cfg
        print("✅ Core modules imported successfully")
        
        # Test BigQuery connection
        db = get_db()
        result = db.query('SELECT 1 as test')
        print("✅ BigQuery connection working")
        
        return True
    except Exception as e:
        print(f"❌ Web server test failed: {e}")
        return False

def main():
    """Main function to fix the generativeai package issue."""
    print("🔧 Fixing google_generativeai package installation...")
    
    # Fix the package
    if not fix_generativeai_package():
        print("❌ Failed to fix google_generativeai package")
        return False
    
    # Test web server startup
    if not test_web_server_start():
        print("❌ Web server startup test failed")
        return False
    
    print("🎉 Package fix successful! Web server should now start properly.")
    print("💡 You can now start your web server with: python sports/webapp.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
