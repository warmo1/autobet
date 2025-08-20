import sqlite3, os, time
from typing import Iterable, List, Optional, Tuple

def db_path_from_url(url: str) -> str:
    if not url.startswith("sqlite:///"):
        raise ValueError("Only sqlite:/// URLs supported in this starter.")
    return url.replace("sqlite:///", "", 1)

def get_conn(database_url: str) -> sqlite3.Connection:
    path = db_path_from_url(database_url)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(        '''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT NOT NULL,
            date TEXT NOT NULL,
            home TEXT NOT NULL,
            away TEXT NOT NULL,
            fthg INTEGER,
            ftag INTEGER,
            ftr TEXT, -- H/D/A for football; generic labels for others
            comp TEXT,
            season TEXT
        );
        CREATE TABLE IF NOT EXISTS odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            book TEXT NOT NULL,
            odd_home REAL,
            odd_draw REAL,
            odd_away REAL,
            UNIQUE(match_id, book)
        );
        CREATE TABLE IF NOT EXISTS suggestions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_ts INTEGER NOT NULL,
            sport TEXT NOT NULL,
            date TEXT NOT NULL,
            home TEXT NOT NULL,
            away TEXT NOT NULL,
            market TEXT NOT NULL,
            side TEXT NOT NULL, -- back/lay
            selection TEXT NOT NULL, -- home/draw/away
            model_prob REAL NOT NULL,
            market_odds REAL,
            edge REAL,
            stake REAL,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS paper_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            sport TEXT NOT NULL,
            match TEXT NOT NULL,
            market TEXT NOT NULL,
            side TEXT NOT NULL,
            selection TEXT NOT NULL,
            odds REAL NOT NULL,
            stake REAL NOT NULL,
            result TEXT,
            pnl REAL
        );
        CREATE TABLE IF NOT EXISTS bankroll_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            published_ts INTEGER,
            UNIQUE(url)
        );
        CREATE TABLE IF NOT EXISTS insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_ts INTEGER NOT NULL,
            headline TEXT NOT NULL,
            summary TEXT NOT NULL
        );
        '''
    )
    conn.commit()

def insert_match(conn, sport, date, home, away, fthg, ftag, ftr, comp, season):
    conn.execute(
        "INSERT INTO matches (sport,date,home,away,fthg,ftag,ftr,comp,season) VALUES (?,?,?,?,?,?,?,?,?)",
        (sport, date, home, away, fthg, ftag, ftr, comp, season)
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

def insert_odds(conn, match_id, book, odd_home, odd_draw, odd_away):
    conn.execute(
        "INSERT OR REPLACE INTO odds (match_id,book,odd_home,odd_draw,odd_away) VALUES (?,?,?,?,?)",
        (match_id, book, odd_home, odd_draw, odd_away)
    )
    conn.commit()

def list_matches(conn, sport, date_from=None, date_to=None, comp=None):
    q = "SELECT id, date, home, away, fthg, ftag, ftr, comp, season FROM matches WHERE sport=?"
    params = [sport]
    if date_from:
        q += " AND date>=?"; params.append(date_from)
    if date_to:
        q += " AND date<=?"; params.append(date_to)
    if comp:
        q += " AND comp=?"; params.append(comp)
    q += " ORDER BY date ASC"
    return conn.execute(q, params).fetchall()

def bank_get(conn, key: str, default: str) -> str:
    cur = conn.execute("SELECT value FROM bankroll_state WHERE key=?", (key,))
    r = cur.fetchone()
    return r[0] if r else default

def bank_set(conn, key: str, value: str) -> None:
    conn.execute("INSERT INTO bankroll_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key,value))
    conn.commit()

def record_suggestion(conn, **k):
    cols = ",".join(k.keys())
    placeholders = ",".join(["?"]*len(k))
    conn.execute(f"INSERT INTO suggestions ({cols}) VALUES ({placeholders})", tuple(k.values()))
    conn.commit()

def recent_suggestions(conn, limit=50):
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM suggestions ORDER BY created_ts DESC LIMIT ?", conn, params=(limit,))
    if not df.empty:
        df["created_ts"] = pd.to_datetime(df["created_ts"], unit="ms", utc=True)
    return df

def record_paper_bet(conn, **k):
    cols = ",".join(k.keys())
    placeholders = ",".join(["?"]*len(k))
    conn.execute(f"INSERT INTO paper_bets ({cols}) VALUES ({placeholders})", tuple(k.values()))
    conn.commit()

def recent_paper_bets(conn, limit=100):
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM paper_bets ORDER BY ts DESC LIMIT ?", conn, params=(limit,))
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df

def insert_news(conn, source, url, title, published_ts):
    conn.execute("INSERT OR IGNORE INTO news_articles (source,url,title,published_ts) VALUES (?,?,?,?)",
                 (source,url,title,published_ts))
    conn.commit()

def insert_insight(conn, headline, summary):
    conn.execute("INSERT INTO insights(created_ts, headline, summary) VALUES (?,?,?)",
                 (int(time.time()*1000), headline, summary))
    conn.commit()
