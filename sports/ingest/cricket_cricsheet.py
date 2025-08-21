import os
import csv
import sqlite3

def ingest_dir(conn: sqlite3.Connection, csv_dir: str) -> int:
    """
    Ingests a directory of Cricsheet CSVs with robust parsing for varied formats.
    """
    files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    total_processed_rows = 0
    last_file_processed = "None"

    for f in files:
        path = os.path.join(csv_dir, f)
        last_file_processed = f
        with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
            
            # --- Robust Parsing Logic ---
            lines = fh.readlines()
            header_line = None
            data_lines = []

            # Find the first line that is not metadata to use as the header
            for line in lines:
                if not line.strip().startswith('info,'):
                    if header_line is None:
                        header_line = line
                    data_lines.append(line)
            
            if not header_line or not data_lines:
                continue

            # Use the identified header and data with the CSV reader
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

    # --- Self-Diagnosing Debug Report ---
    if total_processed_rows == 0:
        print("\n--- INGESTION FAILED: Processed 0 rows. ---")
        print("This usually means the format of the CSV files is not as expected.")
        print(f"Below is a debug report for the last file attempted: {last_file_processed}\n")
        _run_debug_on_file(os.path.join(csv_dir, last_file_processed))

    return total_processed_rows

def _run_debug_on_file(path):
    """Prints a detailed debug analysis of a single file."""
    print(f"--- Starting detailed analysis of: {path} ---\n")
    with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
        lines = fh.readlines()
        print(f"Total lines in file: {len(lines)}")
        print("\n--- First 10 Lines of Raw File ---")
        for i, line in enumerate(lines[:10]):
            print(f"Line {i+1}: {line.strip()}")

        header_line = None
        data_lines = []
        for line in lines:
            if not line.strip().startswith('info,'):
                if header_line is None:
                    header_line = line
                data_lines.append(line)
        
        print("\n--- Parser Analysis ---")
        print(f"Identified Header Line: {header_line.strip() if header_line else 'None'}")
        print(f"Total Data Lines Found (including header): {len(data_lines)}")

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
