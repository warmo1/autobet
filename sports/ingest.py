import os
import csv
import requests
from datetime import date, timedelta
from .db import insert_match, insert_odds
from .config import cfg

# --- API Data Ingestion (for live fixtures) ---
def ingest_football_fixtures_from_api(conn):
    """Fetches upcoming football fixtures from SportAPI7 on RapidAPI."""
    if not (cfg.rapidapi_key and cfg.rapidapi_host_football):
        print("[Ingest Error] RapidAPI key or football host not set in .env file.")
        return 0

    # SportAPI7 endpoint for fixtures by date
    url = f"https://{cfg.rapidapi_host_football}/api/v1/sport/football/scheduled-events/{date.today().strftime('%Y-%m-%d')}"
    headers = {"X-RapidAPI-Key": cfg.rapidapi_key, "X-RapidAPI-Host": cfg.rapidapi_host_football}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # The data is nested in an 'events' key
        fixtures = response.json().get('events', [])
        if not fixtures:
            print("[Ingest] No football fixtures found for today on SportAPI7.")
            return 0
            
        count = 0
        for fix in fixtures:
            home = fix.get('homeTeam', {}).get('name')
            away = fix.get('awayTeam', {}).get('name')
            # The date comes from the startTimestamp
            fixture_date = date.fromtimestamp(fix.get('startTimestamp')).strftime('%Y-%m-%d')
            league = fix.get('tournament', {}).get('name')
            season = fix.get('season', {}).get('name')
            
            if home and away and fixture_date:
                insert_match(conn, "football", fixture_date, home, away, None, None, None, league, season)
                count += 1
        print(f"[Ingest] Fetched {count} live football fixtures from SportAPI7.")
        return count
    except Exception as e:
        print(f"[Ingest Error] Could not fetch football fixtures: {e}")
        return 0

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

# --- Placeholder functions for other sports ---
def ingest_tennis_data_from_api(conn):
    """Fetches upcoming tennis matches from RapidAPI."""
    print("[Ingest] Tennis data ingestion not yet fully implemented.")
    return 0

def ingest_horse_racing_data(conn):
    """Fetches today's horse racing data from RapidAPI."""
    print("[Ingest] Horse racing data ingestion not yet fully implemented.")
    return 0
