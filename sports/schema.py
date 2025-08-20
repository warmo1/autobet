def init_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS teams(
      team_id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      sport TEXT NOT NULL,
      country TEXT
    );

    CREATE TABLE IF NOT EXISTS events(
      event_id INTEGER PRIMARY KEY AUTOINCREMENT,
      sport TEXT NOT NULL,
      comp TEXT,
      season TEXT,
      start_date TEXT,            -- YYYY-MM-DD (UTC)
      home_team TEXT NOT NULL,
      away_team TEXT NOT NULL,
      status TEXT DEFAULT 'completed'  -- scheduled|inplay|completed
    );

    CREATE TABLE IF NOT EXISTS results(
      event_id INTEGER PRIMARY KEY,
      home_score INTEGER,
      away_score INTEGER,
      outcome TEXT,               -- H/D/A for football
      FOREIGN KEY(event_id) REFERENCES events(event_id)
    );

    CREATE TABLE IF NOT EXISTS odds_snapshots(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id INTEGER NOT NULL,
      market TEXT NOT NULL,       -- e.g. '1X2'
      selection TEXT NOT NULL,    -- 'home'|'draw'|'away'
      bookmaker TEXT,             -- e.g. 'B365'
      price REAL,
      ts_collected TEXT,          -- ISO8601 or date for CSV snapshots
      UNIQUE(event_id, market, selection, bookmaker, ts_collected),
      FOREIGN KEY(event_id) REFERENCES events(event_id)
    );

    CREATE TABLE IF NOT EXISTS suggestions(
      suggestion_id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_ts INTEGER NOT NULL,
      event_id INTEGER NOT NULL,
      market TEXT NOT NULL,
      selection TEXT NOT NULL,
      side TEXT NOT NULL,         -- 'back'|'lay'
      model_prob REAL,
      price REAL,
      edge REAL,
      stake REAL,
      note TEXT,
      FOREIGN KEY(event_id) REFERENCES events(event_id)
    );

    CREATE TABLE IF NOT EXISTS bets(
      bet_id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      mode TEXT NOT NULL,         -- 'paper'|'live'
      event_id INTEGER NOT NULL,
      market TEXT NOT NULL,
      selection TEXT NOT NULL,
      side TEXT NOT NULL,
      price REAL NOT NULL,
      stake REAL NOT NULL,
      result TEXT,                -- 'win'|'lose'|'push'|NULL
      pnl REAL,
      FOREIGN KEY(event_id) REFERENCES events(event_id)
    );

    CREATE TABLE IF NOT EXISTS bankroll_state(
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
    """)
    conn.commit()
