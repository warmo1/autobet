import os
import re
import sqlite3
from datetime import datetime

# --- **FIX**: New, more robust regular expressions ---

# Regex to find a date line, e.g., "[Sat Aug/13]"
DATE_LINE_REGEX = re.compile(r"\[(.+?)\]")

# Regex to find a match line, ignoring optional timestamps and half-time scores
# e.g., "15.00 Blackburn Rovers 1-2 (1-1) Wolverhampton Wanderers" or "Fulham FC 0-0 Aston Villa"
MATCH_LINE_REGEX = re.compile(r"^(?:\d{2}\.\d{2}\s+)?(.+?)\s+(\d+-\d+)(?:\s*\(.+\))?\s+(.+?)$")

def ingest_dir(conn: sqlite3.Connection, data_dir: str) -> int:
    """
    Recursively ingests all openfootball .txt files with a stateful parser.
    """
    total = 0
    print(f"[OpenFootball] Recursively processing directory: {data_dir}")
    
    for root, dirs, files in os.walk(data_dir):
        for filename in files:
            if filename.endswith('.txt'):
                path = os.path.join(root, filename)
                
                try:
                    season = os.path.basename(root).replace('-', '/')
                    competition = filename.split('.')[0].split('-', 1)[-1].replace('_', ' ').title()
                except Exception:
                    continue

                with open(path, 'r', encoding='utf-8') as f:
                    current_date = None # This will hold the last date we've seen
                    for line in f:
                        line = line.strip()
                        
                        # --- **FIX**: New stateful parsing logic ---
                        
                        # 1. Check if the line is a date
                        date_match = DATE_LINE_REGEX.match(line)
                        if date_match:
                            try:
                                date_str = date_match.groups()[0]
                                date_part = date_str.split(' ')[-1]
                                date_obj = datetime.strptime(date_part, '%b/%d')
                                year = int(season.split('/')[0])
                                current_date = date_obj.replace(year=year).strftime('%Y-%m-%d')
                            except ValueError:
                                current_date = None # Reset date if parsing fails
                            continue # Move to the next line

                        # 2. If it's not a date, check if it's a match
                        match = MATCH_LINE_REGEX.match(line)
                        if not match or not current_date:
                            continue # Skip if it's not a match or we haven't seen a date yet

                        home_team, score_str, away_team = match.groups()
                        h_score, a_score = map(int, score_str.split('-'))
                        outcome = "H" if h_score > a_score else ("A" if a_score > h_score else "D")

                        # Upsert event
                        cur = conn.execute("SELECT event_id FROM events WHERE sport='football' AND start_date=? AND home_team=? AND away_team=?", (current_date, home_team.strip(), away_team.strip()))
                        existing_event = cur.fetchone()
                        if existing_event:
                            event_id = existing_event[0]
                        else:
                            cur = conn.execute("INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)", ("football", competition, season, current_date, home_team.strip(), away_team.strip(), "completed"))
                            event_id = cur.lastrowid

                        # Insert result
                        conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, h_score, a_score, outcome))
                        total += 1
    conn.commit()
    return total
