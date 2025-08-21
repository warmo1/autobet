import os
import csv
import sqlite3

def ingest_dir(conn: sqlite3.Connection, csv_dir: str) -> int:
    """Ingests a directory of Cricsheet match summary CSVs with enhanced debugging."""
    
    try:
        files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
        if not files:
            print("--- DEBUG: No CSV files found in the directory.")
            return 0
    except Exception as e:
        print(f"--- DEBUG: Error listing files in directory: {e}")
        return 0

    total = 0
    
    # --- Enhanced Debugging: Process only the first file found ---
    file_to_debug = files[0]
    path = os.path.join(csv_dir, file_to_debug)
    print(f"\n--- DEBUG: Starting detailed analysis of a single file: {file_to_debug} ---\n")

    with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
        reader = csv.DictReader(fh)
        
        # Print headers
        headers = reader.fieldnames
        print(f"--- DEBUG: Headers found in file: {headers}\n")
        
        # Print first 5 rows and the data we extract
        print("--- DEBUG: Analyzing first 5 rows... ---")
        for i, row in enumerate(reader):
            if i >= 5:
                break # Stop after 5 rows for this test

            print(f"\n--- Row {i+1} ---")
            print(f"Original Row Data: {row}")

            # This is the logic we are testing
            date = (row.get("date") or row.get("start_date") or row.get("event_date") or "").strip()
            home = (row.get("team1") or row.get("home_team") or "").strip()
            away = (row.get("team2") or row.get("away_team") or "").strip()
            
            print(f"Extracted Date: '{date}'")
            print(f"Extracted Home: '{home}'")
            print(f"Extracted Away: '{away}'")

            if not all([date, home, away]):
                print("Result: SKIPPING ROW (missing essential data)")
                continue
            else:
                print("Result: PROCESSING ROW")
                # In a real run, this is where the DB insert would happen
                total += 1
    
    print(f"\n--- DEBUG: Finished analysis. Processed {total} rows from the test file. ---")
    # We return 0 because this is just a debug run, not a full ingest.
    return 0

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
