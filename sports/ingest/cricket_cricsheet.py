import os
import csv
import sqlite3

def ingest_dir(conn: sqlite3.Connection, csv_dir: str) -> int:
    """Ingests a directory of Cricsheet match summary CSVs."""
    
    # --- Start Debugging ---
    print(f"--- DEBUG: Checking directory: {csv_dir}")
    abs_path = os.path.abspath(csv_dir)
    print(f"--- DEBUG: Absolute path is: {abs_path}")
    
    try:
        all_files_in_dir = os.listdir(csv_dir)
        print(f"--- DEBUG: Found {len(all_files_in_dir)} total items in directory.")
        print(f"--- DEBUG: Full list: {all_files_in_dir[:10]}") # Print first 10 items
    except Exception as e:
        print(f"--- DEBUG: Error listing directory: {e}")
        return 0
    # --- End Debugging ---

    files = [f for f in all_files_in_dir if f.lower().endswith(".csv")]
    
    print(f"--- DEBUG: Found {len(files)} files ending with .csv")

    total = 0
    for f in files:
        path = os.path.join(csv_dir, f)
        with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                date = (row.get("date") or row.get("start_date") or "").strip()
                home, away = (row.get("team1") or "").strip(), (row.get("team2") or "").strip()
                if not all([date, home, away]):
                    continue

                # Upsert event
                cur = conn.execute("SELECT event_id FROM events WHERE sport='cricket' AND start_date=? AND home_team=? AND away_team=?", (date, home, away))
                existing_event = cur.fetchone()
                if existing_event:
                    event_id = existing_event[0]
                else:
                    cur = conn.execute("INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)", ("cricket", row.get("match_type"), row.get("season"), date, home, away, "completed" if row.get("winner") else "scheduled"))
                    event_id = cur.lastrowid

                # Insert result if present
                if row.get("team1_runs") and row.get("team2_runs"):
                    h_score, a_score = _to_int(row.get("team1_runs")), _to_int(row.get("team2_runs"))
                    outcome = "H" if h_score > a_score else ("A" if a_score > h_score else "D")
                    conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, h_score, a_score, outcome))
                total += 1
        conn.commit()
    return total

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
