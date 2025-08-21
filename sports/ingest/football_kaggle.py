import os
import csv
import sqlite3

def ingest_file(conn: sqlite3.Connection, csv_path: str) -> int:
    """
    Ingests a single CSV file from the Kaggle domestic football dataset.
    Expected headers: season, division, date, home, away, goalshome, goalsaway
    """
    total = 0
    print(f"[Kaggle Ingest] Processing file: {csv_path}")
    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            date, home, away = row.get("date"), row.get("home"), row.get("away")
            if not all([date, home, away]):
                continue

            # Determine the outcome based on goals
            h_score, a_score = _to_int(row.get("goalshome")), _to_int(row.get("goalsaway"))
            outcome = "H" if h_score > a_score else ("A" if a_score > h_score else "D")

            # Upsert event
            cur = conn.execute("SELECT event_id FROM events WHERE sport='football' AND start_date=? AND home_team=? AND away_team=?", (date, home, away))
            existing_event = cur.fetchone()
            if existing_event:
                event_id = existing_event[0]
            else:
                cur = conn.execute("INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)", ("football", row.get("division"), row.get("season"), date, home, away, "completed"))
                event_id = cur.lastrowid

            # Insert result
            conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, h_score, a_score, outcome))
            total += 1
    conn.commit()
    return total

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
