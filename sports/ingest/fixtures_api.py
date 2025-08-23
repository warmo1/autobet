import requests
from datetime import datetime
from ..config import cfg

# --- ESPN API Configuration ---
# This maps our internal sport names to the specific paths used by the ESPN API.
SPORT_PATHS = {
    'football': 'soccer/eng.1', # English Premier League
    'rugby': 'rugby/league',     # Generic rugby league
    'cricket': 'cricket/ipl'     # Indian Premier League (as a stable example)
}

def ingest_fixtures(conn, sport: str) -> int:
    """
    Fetches and ingests fixtures for a given sport directly from the stable ESPN API.
    """
    if sport not in SPORT_PATHS:
        print(f"[Ingest Error] Sport '{sport}' not supported by the ESPN ingestor.")
        return 0

    url = f"http://site.api.espn.com/apis/site/v2/sports/{SPORT_PATHS[sport]}/scoreboard"
    print(f"[Fixtures] Fetching {sport.title()} fixtures from ESPN API...")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        fixtures = data.get('events', [])
        if not fixtures:
            print(f"[Fixtures] No {sport.title()} fixtures found for today on ESPN.")
            return 0
            
        count = 0
        for event in fixtures:
            # We only want to ingest matches that are scheduled for the future
            if event.get('status', {}).get('type', {}).get('name') != 'STATUS_SCHEDULED':
                continue

            competitors = event.get('competitions', [{}])[0].get('competitors', [])
            if len(competitors) < 2:
                continue

            home = competitors[0].get('team', {}).get('displayName')
            away = competitors[1].get('team', {}).get('displayName')
            
            # ESPN provides dates in ISO 8601 format (e.g., '2025-08-21T19:00Z')
            date_str = event.get('date', '').split('T')[0]
            league = data.get('leagues', [{}])[0].get('name', 'Unknown League')
            
            if home and away and date_str:
                conn.execute(
                    "INSERT OR IGNORE INTO events(sport, comp, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?)",
                    (sport, league, date_str, home, away, "scheduled")
                )
                count += 1
                
        conn.commit()
        print(f"[Fixtures] Ingested {count} {sport.title()} fixtures from ESPN.")
        return count
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
             print(f"[Fixtures] The ESPN API endpoint returned a 404 Not Found error. This may be due to the league being out of season.")
             return 0
        print(f"[API Error] Could not fetch data from ESPN: {e}")
        return 0
    except Exception as e:
        print(f"[API Error] An unexpected error occurred: {e}")
        return 0
