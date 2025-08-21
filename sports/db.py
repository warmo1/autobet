import sqlite3
from urllib.parse import urlparse
from contextlib import contextmanager

def _sqlite_path_from_url(db_url: str) -> str:
    # supports "sqlite:///file.db" and plain "file.db"
    if db_url.startswith("sqlite:///"):
        return db_url.replace("sqlite:///", "", 1)
    if db_url.startswith("sqlite://"):
        parsed = urlparse(db_url)
        return parsed.path or "sports_bot.db"
    return db_url  # assume plain path

def connect(db_url: str) -> sqlite3.Connection:
    path = _sqlite_path_from_url(db_url)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)  # autocommit
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

@contextmanager
def txn(conn: sqlite3.Connection):
    try:
        conn.execute("BEGIN")
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
 def get_conn(db_url: str):
    """Alias for connect() to keep backwards compatibility."""
    return connect(db_url)       
