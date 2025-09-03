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

-- Raw Tote payload store for fidelity/debugging
CREATE TABLE IF NOT EXISTS raw_tote (
  raw_id     TEXT PRIMARY KEY,         -- e.g. endpoint:path:id:ts
  endpoint   TEXT NOT NULL,
  entity_id  TEXT,
  sport      TEXT,
  fetched_ts TEXT NOT NULL,
  payload    TEXT NOT NULL
);

-- Horse racing schema (meetings/races/runners)
CREATE TABLE IF NOT EXISTS hr_meetings (
  meeting_id TEXT PRIMARY KEY,
  name       TEXT,
  country    TEXT,
  venue      TEXT,
  date       TEXT,
  source     TEXT
);

CREATE TABLE IF NOT EXISTS hr_races (
  race_id    TEXT PRIMARY KEY,
  meeting_id TEXT NOT NULL,
  number     INTEGER,
  name       TEXT,
  start_time TEXT,
  status     TEXT,
  distance   TEXT,
  surface    TEXT,
  FOREIGN KEY(meeting_id) REFERENCES hr_meetings(meeting_id)
);

CREATE TABLE IF NOT EXISTS hr_runners (
  runner_id  TEXT PRIMARY KEY,
  race_id    TEXT NOT NULL,
  number     INTEGER,
  name       TEXT,
  draw       INTEGER,
  jockey     TEXT,
  trainer    TEXT,
  status     TEXT,
  FOREIGN KEY(race_id) REFERENCES hr_races(race_id)
);

-- Tote pools and results (e.g., Win/Place/Exacta/Trifecta/Superfecta)
CREATE TABLE IF NOT EXISTS hr_pools (
  pool_id    TEXT PRIMARY KEY,
  race_id    TEXT NOT NULL,
  type       TEXT NOT NULL,           -- e.g., WIN, PLC, EXA, TRI, SUPERFECTA
  status     TEXT,
  currency   TEXT,
  total_pool REAL,
  FOREIGN KEY(race_id) REFERENCES hr_races(race_id)
);

CREATE TABLE IF NOT EXISTS hr_pool_dividends (
  pool_id    TEXT NOT NULL,
  selection  TEXT NOT NULL,           -- representation varies (e.g., "3-7-1-5" for superfecta)
  dividend   REAL,
  ts         TEXT,
  PRIMARY KEY(pool_id, selection, ts),
  FOREIGN KEY(pool_id) REFERENCES hr_pools(pool_id)
);

-- Planned/placed Tote bets (paper/live)
CREATE TABLE IF NOT EXISTS hr_bets (
  bet_id     TEXT PRIMARY KEY,
  ts         INTEGER NOT NULL,
  mode       TEXT NOT NULL,           -- 'paper' or 'live'
  pool_id    TEXT NOT NULL,
  selection  TEXT NOT NULL,           -- e.g., "3-7-1-5"
  stake      REAL NOT NULL,
  currency   TEXT,
  status     TEXT,                    -- placed/pending/settled
  return     REAL,                    -- settled return, if any
  FOREIGN KEY(pool_id) REFERENCES hr_pools(pool_id)
);

-- Master horses and their lifecycle performances
CREATE TABLE IF NOT EXISTS hr_horses (
  horse_id   TEXT PRIMARY KEY,
  name       TEXT,
  country    TEXT
);

-- Individual horse runs (placements) tied to Tote Event IDs; race_id if known
CREATE TABLE IF NOT EXISTS hr_horse_runs (
  horse_id     TEXT NOT NULL,
  event_id     TEXT NOT NULL,
  race_id      TEXT,
  finish_pos   INTEGER,
  status       TEXT,
  cloth_number INTEGER,
  jockey       TEXT,
  trainer      TEXT,
  recorded_ts  TEXT,
  PRIMARY KEY(horse_id, event_id),
  FOREIGN KEY(horse_id) REFERENCES hr_horses(horse_id)
);

-- Race conditions (going, weather) keyed by Tote Event; cross-reference to hr_races later
CREATE TABLE IF NOT EXISTS race_conditions (
  event_id     TEXT PRIMARY KEY,
  venue        TEXT,
  country      TEXT,
  start_iso    TEXT,
  going        TEXT,
  surface      TEXT,
  distance     TEXT,
  weather_desc TEXT,
  weather_temp_c REAL,
  weather_wind_kph REAL,
  weather_precip_mm REAL,
  source       TEXT,
  fetched_ts   TEXT
);

-- Cached geocodes for venues (to avoid repeated lookups)
CREATE TABLE IF NOT EXISTS geo_venues (
  venue       TEXT NOT NULL,
  country     TEXT,
  lat         REAL NOT NULL,
  lon         REAL NOT NULL,
  source      TEXT,
  fetched_ts  TEXT,
  PRIMARY KEY(venue, country)
);

-- Generic Tote events (GraphQL) for horse racing, football, etc.
CREATE TABLE IF NOT EXISTS tote_events (
  event_id   TEXT PRIMARY KEY,
  name       TEXT,
  sport      TEXT,
  venue      TEXT,
  country    TEXT,
  start_iso  TEXT,
  status     TEXT,
  result_status TEXT,
  comp       TEXT,
  home       TEXT,
  away       TEXT,
  competitors_json TEXT,
  source     TEXT
);

-- Indexes to speed common UI filters/joins
CREATE INDEX IF NOT EXISTS idx_tote_events_start ON tote_events(start_iso);
CREATE INDEX IF NOT EXISTS idx_tote_events_country ON tote_events(country);
CREATE INDEX IF NOT EXISTS idx_tote_events_sport ON tote_events(sport);

-- Tote products and dividends (e.g., SUPERFECTA)
CREATE TABLE IF NOT EXISTS tote_products (
  product_id TEXT PRIMARY KEY,
  bet_type   TEXT NOT NULL,
  status     TEXT,
  currency   TEXT,
  total_gross REAL,
  total_net   REAL,
  rollover    REAL,
  deduction_rate REAL,
  event_id   TEXT,
  event_name TEXT,
  venue      TEXT,
  start_iso  TEXT,
  source     TEXT
);

CREATE INDEX IF NOT EXISTS idx_tote_products_event ON tote_products(event_id);
CREATE INDEX IF NOT EXISTS idx_tote_products_start ON tote_products(start_iso);
CREATE INDEX IF NOT EXISTS idx_tote_products_type ON tote_products(bet_type);

CREATE TABLE IF NOT EXISTS tote_product_dividends (
  product_id TEXT NOT NULL,
  selection  TEXT NOT NULL,
  dividend   REAL,
  ts         TEXT,
  PRIMARY KEY(product_id, selection, ts),
  FOREIGN KEY(product_id) REFERENCES tote_products(product_id)
);

CREATE INDEX IF NOT EXISTS idx_tote_divs_product ON tote_product_dividends(product_id);

-- Tote product selections per leg (for modeling and UI)
CREATE TABLE IF NOT EXISTS tote_product_selections (
  product_id     TEXT NOT NULL,
  leg_index      INTEGER NOT NULL,      -- 1-based leg index
  product_leg_id TEXT,                  -- GraphQL leg id if available
  selection_id   TEXT NOT NULL,         -- selection id from GraphQL
  competitor     TEXT,                  -- displayed name
  number         INTEGER,               -- clothNumber/trapNumber if available
  leg_event_id   TEXT,                  -- event id for that leg
  leg_event_name TEXT,
  leg_venue      TEXT,
  leg_start_iso  TEXT,
  PRIMARY KEY(product_id, leg_index, selection_id)
);
CREATE INDEX IF NOT EXISTS idx_tps_product ON tote_product_selections(product_id);

CREATE TABLE IF NOT EXISTS tote_bets (
  bet_id     TEXT PRIMARY KEY,
  ts         INTEGER NOT NULL,
  mode       TEXT NOT NULL,          -- 'paper' or 'live'
  product_id TEXT NOT NULL,
  selection  TEXT NOT NULL,
  stake      REAL NOT NULL,
  currency   TEXT,
  status     TEXT,
  return     REAL,
  response_json TEXT,
  error      TEXT,
  settled_ts INTEGER,
  outcome    TEXT,
  result_json TEXT,
  FOREIGN KEY(product_id) REFERENCES tote_products(product_id)
);

-- Additional helpful indexes
CREATE INDEX IF NOT EXISTS idx_hr_runs_event ON hr_horse_runs(event_id);
CREATE INDEX IF NOT EXISTS idx_race_conditions_event ON race_conditions(event_id);

-- Time-series snapshots of product pool totals
CREATE TABLE IF NOT EXISTS tote_pool_snapshots (
  product_id  TEXT NOT NULL,
  event_id    TEXT,
  bet_type    TEXT,
  status      TEXT,
  currency    TEXT,
  start_iso   TEXT,
  ts_ms       INTEGER NOT NULL,      -- snapshot epoch millis
  total_gross REAL,
  total_net   REAL,
  rollover    REAL,
  deduction_rate REAL,
  PRIMARY KEY(product_id, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_tpsnap_event ON tote_pool_snapshots(event_id);

-- Log of event competitor snapshots (to track non-runners/changes)
CREATE TABLE IF NOT EXISTS tote_event_competitors_log (
  event_id   TEXT NOT NULL,
  ts_ms      INTEGER NOT NULL,
  competitors_json TEXT NOT NULL,
  PRIMARY KEY(event_id, ts_ms)
);

-- Raw Tote subscription messages (best-effort)
CREATE TABLE IF NOT EXISTS tote_messages (
  ts_ms     INTEGER NOT NULL,
  channel   TEXT,
  kind      TEXT,
  entity_id TEXT,
  audit     INTEGER,
  payload   TEXT,
  PRIMARY KEY(ts_ms, channel, kind, entity_id)
);

-- Optional runner info (weights/ratings). Populated from REST runners when available.
CREATE TABLE IF NOT EXISTS hr_runner_info (
  runner_id     TEXT PRIMARY KEY,
  weight_kg     REAL,
  weight_lbs    REAL,
  official_rating REAL,
  age           INTEGER,
  speed_rating  REAL,
  source        TEXT,
  fetched_ts    TEXT,
  FOREIGN KEY(runner_id) REFERENCES hr_runners(runner_id)
);

-- Convenience view: horse runs by horse name with event context
DROP VIEW IF EXISTS vw_horse_runs_by_name;
CREATE VIEW vw_horse_runs_by_name AS
WITH ep AS (
  SELECT event_id,
         MIN(event_name) AS event_name,
         MIN(venue) AS venue,
         MIN(start_iso) AS start_iso
  FROM tote_products
  GROUP BY event_id
)
SELECT
  h.name               AS horse_name,
  r.horse_id          AS horse_id,
  r.event_id          AS event_id,
  DATE(COALESCE(te.start_iso, ep.start_iso)) AS event_date,
  COALESCE(te.venue, ep.venue)               AS venue,
  te.country                                  AS country,
  ep.event_name                               AS race_name,
  r.cloth_number                              AS cloth_number,
  r.finish_pos                                 AS finish_pos,
  r.status                                     AS status
FROM hr_horse_runs r
JOIN hr_horses h ON h.horse_id = r.horse_id
LEFT JOIN tote_events te ON te.event_id = r.event_id
LEFT JOIN ep ON ep.event_id = r.event_id;
"""

# Extend schema for RapidAPI odds ingestion (generic live odds)
SCHEMA_ODDS_EXT = """
-- Raw odds payload archive (for troubleshooting and backfills)
CREATE TABLE IF NOT EXISTS raw_odds (
  raw_id     TEXT PRIMARY KEY,         -- provider:path:ts[:id]
  provider   TEXT NOT NULL,
  endpoint   TEXT NOT NULL,
  params     TEXT,
  fetched_ts TEXT NOT NULL,
  payload    TEXT NOT NULL
);

-- Generic live odds snapshots (normalized best-effort)
CREATE TABLE IF NOT EXISTS odds_live (
  event_id   TEXT,                     -- tote or provider event id if known
  runner     TEXT,                     -- runner/selection name or id
  market     TEXT,                     -- e.g., WIN, PLACE, MATCH_ODDS
  bookmaker  TEXT,                     -- source/bookmaker/exchange
  price      REAL,                     -- back/offer price
  ts_ms      INTEGER,                  -- snapshot epoch millis
  source     TEXT,                     -- e.g., rapidapi:uk-betting-odds
  PRIMARY KEY(event_id, runner, market, bookmaker, ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_odds_live_event_ts ON odds_live(event_id, ts_ms);
"""

# Feature and model storage for weighting engine
SCHEMA_WEIGHTS = """
CREATE TABLE IF NOT EXISTS features_runner_event (
  event_id TEXT NOT NULL,
  horse_id TEXT NOT NULL,
  event_date TEXT,
  cloth_number INTEGER,
  total_net REAL,
  weather_temp_c REAL,
  weather_wind_kph REAL,
  weather_precip_mm REAL,
  going TEXT,
  recent_runs INTEGER,
  avg_finish REAL,
  wins_last5 INTEGER,
  places_last5 INTEGER,
  days_since_last_run INTEGER,
  PRIMARY KEY(event_id, horse_id)
);

CREATE TABLE IF NOT EXISTS models (
  model_id TEXT PRIMARY KEY,
  created_ts INTEGER,
  market TEXT,
  algo TEXT,
  params_json TEXT,
  metrics_json TEXT,
  path TEXT
);

CREATE TABLE IF NOT EXISTS predictions (
  model_id TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  event_id TEXT NOT NULL,
  horse_id TEXT NOT NULL,
  market TEXT NOT NULL,
  proba REAL,
  rank INTEGER,
  PRIMARY KEY(model_id, ts_ms, event_id, horse_id)
);
"""

SCHEMA_SF = """
-- Evaluation of SUPERFECTA predictions against actual results
CREATE TABLE IF NOT EXISTS superfecta_eval (
  product_id TEXT NOT NULL,
  event_id   TEXT NOT NULL,
  model_id   TEXT NOT NULL,
  ts_ms      INTEGER NOT NULL,
  predicted_horse_ids TEXT,   -- JSON array order 1-4
  predicted_cloths    TEXT,   -- JSON array order 1-4
  actual_horse_ids    TEXT,   -- JSON array order 1-4
  actual_cloths       TEXT,   -- JSON array order 1-4
  exact4    INTEGER,
  prefix3   INTEGER,
  prefix2   INTEGER,
  any4set   INTEGER,
  eval_ts   INTEGER,
  PRIMARY KEY(product_id, model_id, ts_ms)
);
"""

SCHEMA_SUBSCRIPTIONS_EXT = """
-- Log of Tote subscription messages for dividends
CREATE TABLE IF NOT EXISTS tote_dividend_updates (
  product_id TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  dividend_name TEXT,
  dividend_type INTEGER,
  dividend_status INTEGER,
  dividend_amount REAL,
  dividend_currency TEXT,
  leg_id TEXT,
  selection_id TEXT,
  finishing_position INTEGER,
  PRIMARY KEY (product_id, ts_ms, leg_id, selection_id)
);

-- Log of Tote subscription messages for event results
CREATE TABLE IF NOT EXISTS tote_event_results_log (
  event_id TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  competitor_id TEXT NOT NULL,
  finishing_position INTEGER,
  status TEXT,
  PRIMARY KEY (event_id, ts_ms, competitor_id)
);

-- Log of Tote subscription messages for event status
CREATE TABLE IF NOT EXISTS tote_event_status_log (
  event_id TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  status TEXT,
  PRIMARY KEY (event_id, ts_ms)
);

-- Log of Tote subscription messages for competitor status
CREATE TABLE IF NOT EXISTS tote_competitor_status_log (
  event_id TEXT NOT NULL,
  competitor_id TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  status TEXT,
  PRIMARY KEY (event_id, competitor_id, ts_ms)
);
"""

def init_schema(conn):
    conn.executescript(SCHEMA)
    conn.executescript(SCHEMA_ODDS_EXT)
    conn.executescript(SCHEMA_WEIGHTS)
    conn.executescript(SCHEMA_SF)
    conn.executescript(SCHEMA_SUBSCRIPTIONS_EXT)
    # Lightweight migrations for feature columns
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(features_runner_event)").fetchall()]
        if 'weight_kg' not in cols:
            conn.execute("ALTER TABLE features_runner_event ADD COLUMN weight_kg REAL")
        if 'weight_lbs' not in cols:
            conn.execute("ALTER TABLE features_runner_event ADD COLUMN weight_lbs REAL")
    except Exception:
        pass
    # Lightweight migrations for tote_events
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(tote_events)").fetchall()]
        if 'result_status' not in cols:
            conn.execute("ALTER TABLE tote_events ADD COLUMN result_status TEXT")
    except Exception:
        pass
    # Lightweight migrations for tote_bets (audit/live storage)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(tote_bets)").fetchall()]
        if 'response_json' not in cols:
            conn.execute("ALTER TABLE tote_bets ADD COLUMN response_json TEXT")
        if 'error' not in cols:
            conn.execute("ALTER TABLE tote_bets ADD COLUMN error TEXT")
        if 'settled_ts' not in cols:
            conn.execute("ALTER TABLE tote_bets ADD COLUMN settled_ts INTEGER")
        if 'outcome' not in cols:
            conn.execute("ALTER TABLE tote_bets ADD COLUMN outcome TEXT")
        if 'result_json' not in cols:
            conn.execute("ALTER TABLE tote_bets ADD COLUMN result_json TEXT")
    except Exception:
        pass
    # Migration: add product_leg_id to tote_product_selections
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(tote_product_selections)").fetchall()]
        if 'product_leg_id' not in cols:
            conn.execute("ALTER TABLE tote_product_selections ADD COLUMN product_leg_id TEXT")
    except Exception:
        pass
    # Migration: add rollover and deduction_rate to tote_products
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(tote_products)").fetchall()]
        if 'rollover' not in cols:
            conn.execute("ALTER TABLE tote_products ADD COLUMN rollover REAL")
        if 'deduction_rate' not in cols:
            conn.execute("ALTER TABLE tote_products ADD COLUMN deduction_rate REAL")
    except Exception:
        pass

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
