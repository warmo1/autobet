import requests
from datetime import datetime
from ..config import cfg

# --- ESPN API Configuration ---
# This maps our sport names to the specific paths used by the ESPN API.
SPORT_PATHS = {
    'football': 'soccer/eng.1', # English Premier League
    'rugby': 'rugby/league',     # Generic rugby league
    # **FIX**: Changed to the more reliable ESPNCricinfo API endpoint
    'cricket': 'cricket/scores/live'         
}

def ingest_fixtures(conn, sport: str) -> int:
    """
    Fetches and ingests fixtures for a given sport directly from the stable ESPN and ESPNCricinfo APIs.
    """
    if sport not in SPORT_PATHS:
        print(f"[Ingest Error] Sport '{sport}' not supported by the ESPN ingestor.")
        return 0

    # **FIX**: Use the correct base URL for cricket
    base_url = "https://site.api.espn.com/apis/site/v2/sports/"
    if sport == 'cricket':
        base_url = "https://www.espncricinfo.com/apis/v2/scores/"

    url = f"{base_url}{SPORT_PATHS[sport]}"
    print(f"[Fixtures] Fetching {sport.title()} fixtures from: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # The structure of the Cricinfo response is different
        if sport == 'cricket':
            fixtures = data.get('matches', [])
        else:
            fixtures = data.get('events', [])

        if not fixtures:
            print(f"[Fixtures] No {sport.title()} fixtures found for today.")
            return 0
            
        count = 0
        for event in fixtures:
            if sport == 'cricket':
                if event.get('status') != 'Scheduled': continue
                home = event.get('teams', [{}, {}])[0].get('name')
                away = event.get('teams', [{}, {}])[1].get('name')
                date_str = event.get('startDate', '').split('T')[0]
                league = event.get('series', {}).get('name', 'Unknown Series')
            else: # Football and Rugby logic
                if event.get('status', {}).get('type', {}).get('name') != 'STATUS_SCHEDULED': continue
                competitors = event.get('competitions', [{}])[0].get('competitors', [])
                if len(competitors) < 2: continue
                home = competitors[0].get('team', {}).get('displayName')
                away = competitors[1].get('team', {}).get('displayName')
                date_str = event.get('date', '').split('T')[0]
                league = data.get('leagues', [{}])[0].get('name', 'Unknown League')
            
            if home and away and date_str:
                conn.execute(
                    "INSERT OR IGNORE INTO events(sport, comp, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?)",
                    (sport, league, date_str, home, away, "scheduled")
                )
                count += 1
                
        conn.commit()
        print(f"[Fixtures] Ingested {count} {sport.title()} fixtures.")
        return count
        
    except Exception as e:
        print(f"[API Error] Could not fetch data: {e}")
        return 0
