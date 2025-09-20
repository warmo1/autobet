#!/usr/bin/env python3
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
