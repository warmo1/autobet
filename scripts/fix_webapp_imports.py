#!/usr/bin/env python3
"""
Fix webapp import issues and get the web server running properly.
"""

import os
import sys
from pathlib import Path

def fix_webapp_imports():
    """Fix the relative import issues in webapp.py"""
    
    webapp_path = Path("sports/webapp.py")
    if not webapp_path.exists():
        print("❌ webapp.py not found")
        return False
    
    print("🔧 Fixing webapp imports...")
    
    # Read the current file
    with open(webapp_path, 'r') as f:
        content = f.read()
    
    # Fix relative imports
    fixes = [
        ("from .config import cfg", "from config import cfg"),
        ("from .db import get_db", "from db import get_db"),
        ("from .superfecta_automation import SuperfectaAutomation", "from superfecta_automation import SuperfectaAutomation"),
        ("from .ml.superfecta import SuperfectaML", "from ml.superfecta import SuperfectaML"),
    ]
    
    original_content = content
    for old, new in fixes:
        content = content.replace(old, new)
    
    if content != original_content:
        # Write the fixed content
        with open(webapp_path, 'w') as f:
            f.write(content)
        print("✅ Fixed relative imports in webapp.py")
    else:
        print("ℹ️ No relative imports found to fix")
    
    return True

def create_webapp_runner():
    """Create a simple runner script for the webapp"""
    
    runner_content = '''#!/usr/bin/env python3
"""
Webapp runner script to avoid import issues.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import and run the webapp
from sports.webapp import app

if __name__ == "__main__":
    print("🚀 Starting web server...")
    app.run(host="0.0.0.0", port=8010, debug=True)
'''
    
    runner_path = Path("run_webapp.py")
    with open(runner_path, 'w') as f:
        f.write(runner_content)
    
    # Make it executable
    os.chmod(runner_path, 0o755)
    print(f"✅ Created {runner_path}")
    
    return True

def test_imports():
    """Test if the imports work correctly"""
    
    print("🧪 Testing imports...")
    
    try:
        # Add project root to path
        project_root = Path.cwd()
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        # Test individual imports
        import config
        print("✅ config import works")
        
        import db
        print("✅ db import works")
        
        import superfecta_automation
        print("✅ superfecta_automation import works")
        
        import ml.superfecta
        print("✅ ml.superfecta import works")
        
        # Test webapp import
        from sports.webapp import app
        print("✅ webapp import works")
        
        return True
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

def main():
    """Main function to fix webapp issues"""
    
    print("🔧 Fixing webapp import issues...")
    
    # Fix the imports
    if not fix_webapp_imports():
        return False
    
    # Create runner script
    if not create_webapp_runner():
        return False
    
    # Test imports
    if not test_imports():
        return False
    
    print("\n✅ All fixes applied successfully!")
    print("\n🚀 To run the web server, use one of these methods:")
    print("1. python run_webapp.py")
    print("2. python -m sports.webapp")
    print("3. export FLASK_APP=sports/webapp.py && flask run --host=0.0.0.0 --port=8010")
    
    return True

if __name__ == "__main__":
    main()
