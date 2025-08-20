import os
import csv
import datetime
import sqlite3

# **FIX**: Changed from %y (two-digit year) to %Y (four-digit year)
FD_DATE_FORMAT = "%d/%m/%Y"

def _parse_date(s: str) -> str:
    """Parses the date string and normalizes it to YYYY-MM-DD."""
    dt = datetime.datetime.strptime(s.strip(), FD_DATE_FORMAT)
    return dt.strftime("%Y-%m-%d")

def ingest_dir(conn: sqlite3.Connection, csv_dir: str, bookmaker="B365") -> int:
    """Ingests a directory of football-data.co.uk CSVs."""
    files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    total = 0
    for f in files:
        path = os.path.join(csv_dir, f)
        with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                date, home, away = row.get("Date"), row.get("HomeTeam"), row.get("AwayTeam")
                if not all([date, home, away]):
                    continue
                
                start_date = _parse_date(date)
                comp, season = row.get("Div"), row.get("Season")

                # Upsert event
                cur = conn.execute("SELECT event_id FROM events WHERE sport='football' AND start_date=? AND home_team=? AND away_team=?", (start_date, home, away))
                existing_event = cur.fetchone()
                if existing_event:
                    event_id = existing_event[0]
                else:
                    cur = conn.execute("INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)", ("football", comp, season, start_date, home, away, "completed" if row.get("FTR") else "scheduled"))
                    event_id = cur.lastrowid

                # Insert result if present
                if row.get("FTR"):
                    conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, _to_int(row.get("FTHG")), _to_int(row.get("FTAG")), row.get("FTR")))

                # Insert odds snapshot
                odds_home, odds_draw, odds_away = _to_float(row.get(f"{bookmaker}H")), _to_float(row.get(f"{bookmaker}D")), _to_float(row.get(f"{bookmaker}A"))
                if any([odds_home, odds_draw, odds_away]):
                    stamp = os.path.basename(path) # Use filename as a unique identifier for the snapshot
                    for selection, price in (("home", odds_home), ("draw", odds_draw), ("away", odds_away)):
                        if price:
                            conn.execute("INSERT OR IGNORE INTO odds_snapshots(event_id, market, selection, bookmaker, price, ts_collected) VALUES (?,?,?,?,?,?)", (event_id, "1X2", selection, bookmaker, float(price), stamp))
                total += 1
        conn.commit()
    return total

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None

def _to_float(x):
    try: return float(x)
    except (ValueError, TypeError): return None
