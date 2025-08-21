import requests
from datetime import date
from ..config import cfg

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
            # Filter for UK competitions
            if fix.get('tournament', {}).get('category', {}).get('name') == 'England':
                home = fix.get('homeTeam', {}).get('name')
                away = fix.get('awayTeam', {}).get('name')
                fixture_date = date.fromtimestamp(fix.get('startTimestamp')).strftime('%Y-%m-%d')
                league = fix.get('tournament', {}).get('name')
                
                if home and away and fixture_date:
                    conn.execute(
                        "INSERT OR IGNORE INTO events(sport, comp, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?)",
                        ("football", league, fixture_date, home, away, "scheduled")
                    )
                    count += 1
        conn.commit()
        return count
    except Exception as e:
        print(f"[Fixtures Error] Could not fetch football fixtures: {e}")
        return 0

def ingest_cricket_fixtures(conn) -> int:
    # Placeholder for a cricket fixtures API
    print("[Fixtures] Cricket fixture ingestion not yet implemented.")
    return 0

def ingest_rugby_fixtures(conn) -> int:
    # Placeholder for a rugby fixtures API
    print("[Fixtures] Rugby fixture ingestion not yet implemented.")
    return 0

def ingest_horse_racing_fixtures(conn) -> int:
    # Placeholder for a horse racing fixtures API
    print("[Fixtures] Horse racing fixture ingestion not yet implemented.")
    return 0
