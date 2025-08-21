import os
import csv
import sqlite3

def ingest_dir(conn: sqlite3.Connection, csv_dir: str) -> int:
    """
    Ingests a directory of Cricsheet CSVs with robust parsing for varied formats.
    """
    files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    total_processed_rows = 0
    
    for f in files:
        path = os.path.join(csv_dir, f)
        with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()
            header_line_index = -1
            
            # **FIX**: Find the first line that looks like a header by checking for key columns.
            for i, line in enumerate(lines):
                # A real header will contain at least one of these. This is more flexible.
                if any(key in line.lower() for key in ['date', 'team1', 'team2', 'winner']):
                    header_line_index = i
                    break
            
            if header_line_index == -1:
                continue

            data_lines = lines[header_line_index:]
            
            reader = csv.DictReader(data_lines)
            for row in reader:
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

                # Insert result
                if row.get("team1_runs") and row.get("team2_runs"):
                    h_score, a_score = _to_int(row.get("team1_runs")), _to_int(row.get("team2_runs"))
                    outcome = "H" if h_score > a_score else ("A" if a_score > h_score else "D")
                    conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, h_score, a_score, outcome))
                
                total_processed_rows += 1
                
    conn.commit()
    return total_processed_rows

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
