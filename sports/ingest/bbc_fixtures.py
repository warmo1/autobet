import requests
import json
import re
from bs4 import BeautifulSoup
from datetime import date
import sqlite3

# A mapping of sports to their URL paths on the BBC Sport website
SPORT_URL_PATHS = {
    "football": "football",
    "cricket": "cricket",
    "rugby-union": "rugby-union"
}

def ingest_bbc_fixtures(conn: sqlite3.Connection, sport: str) -> int:
    """
    Scrapes the BBC Sport website for a given sport's fixtures by finding and parsing embedded JSON data.
    This version is more robust to website structure changes.
    """
    if sport not in SPORT_URL_PATHS:
        print(f"[BBC Ingest Error] The sport '{sport}' is not supported.")
        return 0

    url = f"https://www.bbc.co.uk/sport/{SPORT_URL_PATHS[sport]}/scores-fixtures"
    print(f"[BBC Ingest] Fetching fixtures for {sport.title()} from: {url}")
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # **FIX**: Instead of looking for a specific ID, search all script tags
        # for the one containing the fixture data.
        all_scripts = soup.find_all('script')
        fixture_data = None
        for script in all_scripts:
            # The data we need is usually a large JSON blob. We look for a key part of it.
            if script.string and '"competitions":' in script.string:
                try:
                    # The JSON is often assigned to a variable, e.g., window.__INITIAL_DATA__ = {...}
                    # We need to extract just the JSON part.
                    json_text = script.string.split('=', 1)[-1].strip()
                    data = json.loads(json_text)
                    fixture_data = data
                    break
                except (json.JSONDecodeError, IndexError):
                    continue # Ignore scripts that aren't valid JSON

        if not fixture_data:
            print("[BBC Ingest Error] Could not find valid fixture JSON data. The BBC website structure may have changed significantly.")
            return 0
        
        # Navigate the complex JSON structure to find the fixture data
        all_events = fixture_data.get('props', {}).get('pageProps', {}).get('payload', {}).get('body', {}).get('content', {}).get('body', [])
        
        count = 0
        for block in all_events:
            if block.get('type') == 'live-scores':
                for competition in block.get('liveScores', {}).get('competitions', []):
                    for event in competition.get('events', []):
                        home_team = event.get('home', {}).get('name', {}).get('full')
                        away_team = event.get('away', {}).get('name', {}).get('full')
                        
                        if home_team and away_team:
                            conn.execute(
                                "INSERT OR IGNORE INTO events(sport, comp, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?)",
                                (sport, competition.get('name'), date.today().strftime('%Y-%m-%d'), home_team, away_team, "scheduled")
                            )
                            count += 1
                            
        conn.commit()
        print(f"[BBC Ingest] Ingested {count} {sport.title()} fixtures.")
        return count

    except Exception as e:
        print(f"[BBC Ingest Error] Could not fetch or parse fixtures: {e}")
        return 0
