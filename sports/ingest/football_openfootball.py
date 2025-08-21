import os
import re
import sqlite3
from datetime import datetime

# Regex to find match lines, e.g., "[Fri Aug/9] Arsenal 4-1 Leicester City"
MATCH_LINE_REGEX = re.compile(r"\[(.+?)\]\s+(.+?)\s+(\d+-\d+)\s+(.+)")

def ingest_dir(conn: sqlite3.Connection, data_dir: str, competition: str, season: str) -> int:
    """
    Ingests a directory of openfootball .txt files for a given season.
    """
    total = 0
    print(f"[OpenFootball] Processing directory: {data_dir}")
    for filename in os.listdir(data_dir):
        if filename.endswith('.txt'):
            path = os.path.join(data_dir, filename)
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    match = MATCH_LINE_REGEX.match(line.strip())
                    if not match:
                        continue

                    date_str, home_team, score_str, away_team = match.groups()
                    
                    try:
                        # Attempt to parse date, format can vary (e.g., "Fri Aug/9", "Aug/9")
                        # This is a simplified parser; a more robust one might be needed for all files
                        date_obj = datetime.strptime(date_str.split(' ')[-1], '%b/%d')
                        # Assume the season year is correct for the date
                        year = int(season.split('/')[0])
                        date = date_obj.replace(year=year).strftime('%Y-%m-%d')
                    except ValueError:
                        continue # Skip if date format is not recognized

                    h_score, a_score = map(int, score_str.split('-'))
                    outcome = "H" if h_score > a_score else ("A" if a_score > h_score else "D")

                    # Upsert event
                    cur = conn.execute("SELECT event_id FROM events WHERE sport='football' AND start_date=? AND home_team=? AND away_team=?", (date, home_team.strip(), away_team.strip()))
                    existing_event = cur.fetchone()
                    if existing_event:
                        event_id = existing_event[0]
                    else:
                        cur = conn.execute("INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)", ("football", competition, season, date, home_team.strip(), away_team.strip(), "completed"))
                        event_id = cur.lastrowid

                    # Insert result
                    conn.execute("INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)", (event_id, h_score, a_score, outcome))
                    total += 1
    conn.commit()
    return total
