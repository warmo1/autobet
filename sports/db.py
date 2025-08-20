import os
import sqlite3

def _path_from_url(url: str) -> str:
    """Extracts the file path from a sqlite:/// URL."""
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "", 1)
    raise ValueError("Only sqlite:/// URLs are supported in this version.")

def connect(database_url: str) -> sqlite3.Connection:
    """Establishes a connection to the SQLite database."""
    path = _path_from_url(database_url)
    # Ensure the directory for the database file exists
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    
    # Connect to the database
    conn = sqlite3.connect(path, check_same_thread=False)
    
    # Set recommended PRAGMA settings for performance and reliability
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    
    return conn
