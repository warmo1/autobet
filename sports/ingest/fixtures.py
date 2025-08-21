import requests
from datetime import date
from ..config import cfg # Use .. to go up one directory level

def ingest_football_fixtures(conn) -> int:
    """
    Fetches upcoming football fixtures from SportAPI7 and saves them as 'scheduled' events.
    """
    if not (cfg.rapidapi_key and cfg.rapidapi_host_football):
        print("[Fixtures Error] RapidAPI key or football host not set.")
        return 0

    url = f"https://{cfg.rapidapi_host_football}/api/v1/sport/football/scheduled-events/{date.today().strftime('%Y-%m-%d')}"
    headers = {"X-RapidAPI-Key": cfg.rapidapi_key, "X-RapidAPI-Host": cfg.rapidapi_host_football}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        fixtures = response.json().get('events', [])
        count = 0
        for fix in fixtures:
            home = fix.get('homeTeam', {}).get('name')
            away = fix.get('awayTeam', {}).get('name')
            fixture_date = date.fromtimestamp(fix.get('startTimestamp')).strftime('%Y-%m-%d')
            league = fix.get('tournament', {}).get('name')
            season = fix.get('season', {}).get('name')
            
            if home and away and fixture_date:
                # Insert the event with status 'scheduled'
                conn.execute(
                    "INSERT OR IGNORE INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)",
                    ("football", league, season, fixture_date, home, away, "scheduled")
                )
                count += 1
        conn.commit()
        return count
    except Exception as e:
        print(f"[Fixtures Error] Could not fetch football fixtures: {e}")
        return 0
