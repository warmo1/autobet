import os
import csv
import sqlite3

def ingest_dir(conn: sqlite3.Connection, csv_dir: str) -> int:
    """
    Ingests a directory of Cricsheet CSVs, correctly handling metadata headers.
    """
    files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    total_processed_rows = 0
    
    for f in files:
        path = os.path.join(csv_dir, f)
        with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
            
            # --- FIX: Skip metadata lines at the start of the file ---
            data_lines = []
            for line in fh:
                # Cricsheet metadata starts with 'info,'. The actual data does not.
                if not line.strip().startswith('info,'):
                    data_lines.append(line)
            
            # If no data lines were found after skipping metadata, move to the next file.
            if not data_lines:
                continue

            # Now, use the cleaned data_lines with the CSV reader
            reader = csv.DictReader(data_lines)
            for row in reader:
                # Use flexible column name checking
                date = (row.get("date") or row.get("start_date") or "").strip()
                home = (row.get("team1") or "").strip()
                away = (row.get("team2") or "").strip()

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
                
                total_processed_rows += 1
                
    # Commit all changes at the end for better performance
    conn.commit()
    return total_processed_rows

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
