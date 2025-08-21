import os
import csv
import sqlite3
from datetime import datetime

def ingest_dir(conn: sqlite3.Connection, csv_dir: str) -> int:
    """
    Ingests the Premier League dataset from a directory.
    For now, it processes the fixtures.csv file for match results.
    """
    total = 0
    matches_file = os.path.join(csv_dir, 'fixtures.csv')

    if not os.path.exists(matches_file):
        print(f"[PL Ingest Error] fixtures.csv not found in directory: {csv_dir}")
        return 0

    print(f"[PL Ingest] Processing file: {matches_file}")
    with open(matches_file, newline="", encoding="utf-8", errors="ignore") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # The column names in fixtures.csv are different
            date_str = row.get("Date")
            home = row.get("Home")
            away = row.get("Away")
            
            # Scores are in 'Score' column, e.g., '3-1'
            score = row.get("Score")
            if not all([date_str, home, away, score]):
                continue

            try:
                h_score, a_score = map(int, score.split('-'))
                date = datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                continue # Skip row if score or date format is invalid

            outcome = "H" if h_score > a_score else ("A" if a_score > h_score else "D")

            # Upsert event
            cur = conn.execute("SELECT event_id FROM events WHERE sport='football' AND start_date=? AND home_team=? AND away_team=?", (date, home, away))
            existing_event = cur.fetchone()
            if existing_event:
                event_id = existing_event[0]
            else:
                cur = conn.execute("INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)", ("football", "Premier League", "2024/2025", date, home, away, "completed"))
                event_id = cur.lastrowid

            # Insert result
            conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, h_score, a_score, outcome))
            total += 1
    conn.commit()
    return total

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
