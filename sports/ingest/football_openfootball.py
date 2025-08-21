import os
import re
import sqlite3
from datetime import datetime

# Regex to find match lines, e.g., "[Fri Aug/9] Arsenal 4-1 Leicester City"
MATCH_LINE_REGEX = re.compile(r"\[(.+?)\]\s+(.+?)\s+(\d+-\d+)\s+(.+)")

def ingest_dir(conn: sqlite3.Connection, data_dir: str) -> int:
    """
    Recursively ingests all openfootball .txt files with enhanced debugging.
    """
    total = 0
    files_to_process = []
    for root, dirs, files in os.walk(data_dir):
        for filename in files:
            if filename.endswith('.txt'):
                files_to_process.append(os.path.join(root, filename))

    if not files_to_process:
        print("[OpenFootball] No .txt files found to process.")
        return 0

    # --- Enhanced Debugging: Process only the first file found ---
    file_to_debug = files_to_process[0]
    print(f"\n--- DEBUG: Starting detailed analysis of a single file: {file_to_debug} ---\n")
    
    season = os.path.basename(os.path.dirname(file_to_debug)).replace('-', '/')
    
    with open(file_to_debug, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        print("--- DEBUG: Analyzing first 20 lines... ---")
        for i, line in enumerate(lines[:20]):
            if i >= 20: break
            
            print(f"\n--- Line {i+1}: {line.strip()} ---")
            match = MATCH_LINE_REGEX.match(line.strip())
            
            if not match:
                print("Result: NO REGEX MATCH")
                continue

            date_str, home_team, score_str, away_team = match.groups()
            print(f"Regex Groups: date='{date_str}', home='{home_team}', score='{score_str}', away='{away_team}'")

            try:
                date_part = date_str.split(' ')[-1]
                date_obj = datetime.strptime(date_part, '%b/%d')
                year = int(season.split('/')[0])
                date = date_obj.replace(year=year).strftime('%Y-%m-%d')
                print(f"Parsed Date: '{date}' (SUCCESS)")
            except ValueError as e:
                print(f"Parsed Date: FAILED ({e})")
                continue
            
            print("Result: WOULD PROCESS ROW")
            total += 1

    print(f"\n--- DEBUG: Finished analysis. Processed {total} rows from the test file. ---")
    # We return 0 because this is just a debug run.
    return 0

def _to_int(x):
    try: return int(x)
    except (ValueError, TypeError): return None
