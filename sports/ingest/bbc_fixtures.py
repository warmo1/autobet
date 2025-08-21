import requests
import json
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
    Scrapes the BBC Sport website for a given sport's fixtures by parsing embedded JSON data.
    This is more reliable than relying on HTML class names.
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

        # The fixture data is stored in a JSON object within a <script> tag.
        # This is much more stable than scraping HTML elements.
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if not script_tag:
            print("[BBC Ingest Error] Could not find the __NEXT_DATA__ script tag. The BBC website structure may have changed.")
            return 0

        data = json.loads(script_tag.string)
        
        # Navigate the complex JSON structure to find the fixture data
        # This path is based on the current structure and might need updating in the future.
        all_events = data.get('props', {}).get('pageProps', {}).get('payload', {}).get('body', {}).get('content', {}).get('body', [])
        
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
