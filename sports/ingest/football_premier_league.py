import os
import csv
import sqlite3
from datetime import datetime

def ingest_file(conn: sqlite3.Connection, csv_path: str) -> int:
    """
    Ingests the Premier League matches.csv file from the Kaggle dataset.
    Expected headers: Date, HomeTeam, AwayTeam, FTHG, FTAG
    """
    total = 0
    print(f"[PL Ingest] Processing file: {csv_path}")
    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # The date format in this file is YYYY-MM-DD
            date_str = row.get("Date")
            home = row.get("HomeTeam")
            away = row.get("AwayTeam")
            
            if not all([date_str, home, away]):
                continue

            # Normalize date to YYYY-MM-DD format
            try:
                # The dates might be in different formats, so we try to parse them
                date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                try:
                    date = datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    continue # Skip row if date format is unrecognized

            h_score, a_score = _to_int(row.get("FTHG")), _to_int(row.get("FTAG"))
            if h_score is None or a_score is None:
                continue

            outcome = "H" if h_score > a_score else ("A" if a_score > h_score else "D")

            # Upsert event
            cur = conn.execute("SELECT event_id FROM events WHERE sport='football' AND start_date=? AND home_team=? AND away_team=?", (date, home, away))
            existing_event = cur.fetchone()
            if existing_event:
                event_id = existing_event[0]
            else:
                cur = conn.execute("INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)", ("football", "Premier League", row.get("Season"), date, home, away, "completed"))
                event_id = cur.lastrowid

            # Insert result
            conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, h_score, a_score, outcome))
            total += 1
    conn.commit()
    return total

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
