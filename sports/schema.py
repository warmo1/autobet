SCHEMA = """
CREATE TABLE IF NOT EXISTS teams (
  team_id   INTEGER PRIMARY KEY,
  name      TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS matches (
  match_id    TEXT PRIMARY KEY,       -- e.g. "{date}:{home}:{away}:{comp}"
  sport       TEXT NOT NULL,
  comp        TEXT,
  season      TEXT,
  date        TEXT NOT NULL,
  home_id     INTEGER NOT NULL,
  away_id     INTEGER NOT NULL,
  fthg        INTEGER,
  ftag        INTEGER,
  ftr         TEXT,
  source      TEXT NOT NULL,
  inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(home_id) REFERENCES teams(team_id),
  FOREIGN KEY(away_id) REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS odds (
  match_id   TEXT NOT NULL,
  book       TEXT NOT NULL,
  market     TEXT NOT NULL,
  sel        TEXT NOT NULL,           -- 'H','D','A'
  price      REAL NOT NULL,
  ts         TEXT NOT NULL,
  PRIMARY KEY (match_id, book, market, sel, ts),
  FOREIGN KEY(match_id) REFERENCES matches(match_id)
);

CREATE TABLE IF NOT EXISTS suggestions (
  match_id   TEXT NOT NULL,
  created_ts TEXT NOT NULL,
  sel        TEXT NOT NULL,
  model_prob REAL NOT NULL,
  book       TEXT,
  price      REAL,
  edge       REAL,
  kelly      REAL,
  PRIMARY KEY (match_id, sel, created_ts),
  FOREIGN KEY(match_id) REFERENCES matches(match_id)
);

CREATE TABLE IF NOT EXISTS subscriptions (
  chat_id    INTEGER PRIMARY KEY,
  created_ts TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date);
CREATE INDEX IF NOT EXISTS idx_odds_match ON odds(match_id);
"""

def init_schema(conn):
    conn.executescript(SCHEMA)

def upsert_team(conn, name: str) -> int:
    conn.execute("INSERT OR IGNORE INTO teams(name) VALUES (?)", (name,))
    row = conn.execute("SELECT team_id FROM teams WHERE name=?", (name,)).fetchone()
    return row[0]

def upsert_match(conn, *, sport, comp, season, date, home, away, fthg, ftag, ftr, source) -> str:
    home_id = upsert_team(conn, home)
    away_id = upsert_team(conn, away)
    match_id = f"{date}:{home}:{away}:{comp or ''}"
    conn.execute(
        """
        INSERT INTO matches(match_id,sport,comp,season,date,home_id,away_id,fthg,ftag,ftr,source)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(match_id) DO UPDATE SET
          fthg=COALESCE(excluded.fthg, matches.fthg),
          ftag=COALESCE(excluded.ftag, matches.ftag),
          ftr =COALESCE(excluded.ftr , matches.ftr ),
          source=excluded.source
        """,
        (match_id, sport, comp, season, date, home_id, away_id, fthg, ftag, ftr, source),
    )
    return match_id

def insert_odds_snapshot(conn, match_id: str, *, book: str, market: str, sel: str, price: float, ts: str):
    conn.execute(
        """
        INSERT OR IGNORE INTO odds(match_id,book,market,sel,price,ts)
        VALUES (?,?,?,?,?,?)
        """,
        (match_id, book, market, sel, price, ts),
    )
