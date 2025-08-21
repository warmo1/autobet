import os
import csv
import sqlite3

def ingest_file(conn: sqlite3.Connection, csv_path: str) -> int:
    """
    Ingests a single CSV file from the Kaggle domestic football dataset with enhanced debugging.
    """
    total = 0
    print(f"[Kaggle Ingest] Processing file: {csv_path}")
    
    # --- Enhanced Debugging ---
    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as fh:
        reader = csv.DictReader(fh)
        
        headers = reader.fieldnames
        print(f"\n--- DEBUG: Headers found in Kaggle file: {headers}\n")
        
        print("--- DEBUG: Analyzing first 5 rows... ---")
        # We need to re-open the file to iterate from the beginning after reading headers
        fh.seek(0) 
        reader = csv.DictReader(fh)

        for i, row in enumerate(reader):
            if i >= 5:
                break

            print(f"\n--- Row {i+1} ---")
            print(f"Original Row Data: {row}")
            
            # This is the logic we are testing
            date, home, away = row.get("date"), row.get("home"), row.get("away")
            print(f"Extracted Date: '{date}'")
            print(f"Extracted Home: '{home}'")
            print(f"Extracted Away: '{away}'")

            if not all([date, home, away]):
                print("Result: SKIPPING ROW (missing essential data)")
                continue
            else:
                 print("Result: WOULD PROCESS ROW")

    # For now, we return 0 as this is just a debug run.
    # The real logic is commented out below to prevent errors during testing.
    """
    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            date, home, away = row.get("date"), row.get("home"), row.get("away")
            if not all([date, home, away]):
                continue

            h_score, a_score = _to_int(row.get("goalshome")), _to_int(row.get("goalsaway"))
            
            if h_score is None or a_score is None:
                continue
            
            outcome = "H" if h_score > a_score else ("A" if a_score > h_score else "D")

            cur = conn.execute("SELECT event_id FROM events WHERE sport='football' AND start_date=? AND home_team=? AND away_team=?", (date, home, away))
            existing_event = cur.fetchone()
            if existing_event:
                event_id = existing_event[0]
            else:
                cur = conn.execute("INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)", ("football", row.get("division"), row.get("season"), date, home, away, "completed"))
                event_id = cur.lastrowid

            conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, h_score, a_score, outcome))
            total += 1
    conn.commit()
    """
    return total

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
