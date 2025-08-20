import os
import csv
import requests
from datetime import date
from .db import insert_match, insert_odds
from .config import cfg

# --- Helper Functions ---
def _to_int(x):
    try:
        return int(x)
    except (ValueError, TypeError):
        try:
            return int(float(x))
        except (ValueError, TypeError):
            return None

def _to_float(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None

# --- CSV Ingestion (for historical data) ---
def ingest_football_csv_file(conn, file_path: str) -> int:
    """
    Ingests historical football data from a single CSV file.
    This is used by the web upload feature.
    """
    count = 0
    print(f"[Ingest] Processing CSV file: {file_path}")
    try:
        with open(file_path, newline="", encoding="utf-8", errors="ignore") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                def g(k, default=None): return row.get(k, default)
                
                date_val, home, away = g("Date"), g("HomeTeam"), g("AwayTeam")
                if not all([date_val, home, away]): continue
                
                mid = insert_match(conn, "football", date_val, home, away, _to_int(g("FTHG")), _to_int(g("FTAG")), g("FTR"), g("Div"), g("Season"))
                
                odd_h, odd_d, odd_a = _to_float(g("B365H")), _to_float(g("B365D")), _to_float(g("B365A"))
                if any([odd_h, odd_d, odd_a]):
                    insert_odds(conn, mid, "B365", odd_h, odd_d, odd_a)
                
                count += 1
    except Exception as e:
        print(f"[Ingest Error] Could not process CSV file {file_path}: {e}")
        return 0
    return count

def ingest_football_csv_dir(conn, csv_dir: str) -> int:
    """Ingests all CSV files from a given directory."""
    files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    total_count = 0
    for f in files:
        path = os.path.join(csv_dir, f)
        total_count += ingest_football_csv_file(conn, path)
    return total_count

# --- API Data Ingestion (for live fixtures) ---
def ingest_football_fixtures_from_api(conn):
    """Fetches upcoming football fixtures from API-Football on RapidAPI."""
    if not (cfg.rapidapi_key and cfg.rapidapi_host_football):
        print("[Ingest Error] RapidAPI key or football host not set in .env file.")
        return 0

    url = f"https://{cfg.rapidapi_host_football}/v3/fixtures"
    params = {"date": date.today().strftime("%Y-%m-%d")}
    headers = {"X-RapidAPI-Key": cfg.rapidapi_key, "X-RapidAPI-Host": cfg.rapidapi_host_football}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        fixtures = response.json().get('response', [])
        count = 0
        for fix in fixtures:
            teams = fix.get('teams', {})
            home, away = teams.get('home', {}).get('name'), teams.get('away', {}).get('name')
            fixture_date = fix.get('fixture', {}).get('date', '').split('T')[0]
            league, season = fix.get('league', {}).get('name'), fix.get('league', {}).get('season')
            
            if home and away and fixture_date:
                insert_match(conn, "football", fixture_date, home, away, None, None, None, league, season)
                count += 1
        print(f"[Ingest] Fetched {count} live football fixtures.")
        return count
    except Exception as e:
        print(f"[Ingest Error] Could not fetch football fixtures: {e}")
        return 0

def ingest_tennis_data_from_api(conn):
    """Fetches upcoming tennis matches from RapidAPI."""
    if not (cfg.rapidapi_key and cfg.rapidapi_host_tennis):
        print("[Ingest Error] RapidAPI key or tennis host not set in .env file.")
        return 0

    url = f"https://{cfg.rapidapi_host_tennis}/matches/by-date/{date.today().strftime('%Y-%m-%d')}"
    headers = {"X-RapidAPI-Key": cfg.rapidapi_key, "X-RapidAPI-Host": cfg.rapidapi_host_tennis}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        count = 0
        for event in data.get('events', []):
            home_player, away_player = event.get('home_team', {}).get('name'), event.get('away_team', {}).get('name')
            event_date = date.fromtimestamp(event.get('start_timestamp')).strftime('%Y-%m-%d')
            tournament = event.get('tournament', {}).get('name')

            if home_player and away_player:
                insert_match(conn, "tennis", event_date, home_player, away_player, None, None, None, tournament, None)
                count += 1
        print(f"[Ingest] Fetched {count} live tennis matches.")
        return count
    except Exception as e:
        print(f"[Ingest Error] Could not fetch tennis data: {e}")
        return 0

def ingest_horse_racing_data(conn):
    """Fetches today's horse racing data from RapidAPI."""
    if not (cfg.rapidapi_key and cfg.rapidapi_host_horse_racing):
        print("[Ingest Error] RapidAPI key or horse racing host not set in .env file.")
        return 0
    
    url = f"https://{cfg.rapidapi_host_horse_racing}/races"
    headers = {"X-RapidAPI-Key": cfg.rapidapi_key, "X-RapidAPI-Host": cfg.rapidapi_host_horse_racing}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        races = response.json()
        count = 0
        for race in races:
            for horse in race.get('horses', []):
                insert_match(conn, "horse_racing", race['date'], horse['horse_name'], race['course'], None, None, None, race['race_name'], None)
                count += 1
        print(f"[Ingest] Fetched {count} horse racing entries.")
        return count
    except Exception as e:
        print(f"[Ingest Error] Could not fetch horse racing data: {e}")
        return 0
