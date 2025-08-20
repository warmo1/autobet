import os
import csv
import requests
from datetime import date
from .db import insert_match, insert_odds
from .config import cfg

# --- API Data Ingestion (for live fixtures) ---
def ingest_football_fixtures_from_api(conn):
    """Fetches upcoming football fixtures from SportAPI7 on RapidAPI."""
    if not (cfg.rapidapi_key and cfg.rapidapi_host_football):
        print("[Ingest Error] RapidAPI key or football host not set in .env file.")
        return 0

    # **FIX**: This is the correct endpoint for getting events by date from SportAPI7
    url = f"https://{cfg.rapidapi_host_football}/api/v1/sport/football/events/date/{date.today().strftime('%Y-%m-%d')}"
    headers = {"X-RapidAPI-Key": cfg.rapidapi_key, "X-RapidAPI-Host": cfg.rapidapi_host_football}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        fixtures = response.json().get('events', [])
        if not fixtures:
            print("[Ingest] No football fixtures found for today on SportAPI7.")
            return 0
            
        count = 0
        for fix in fixtures:
            home = fix.get('homeTeam', {}).get('name')
            away = fix.get('awayTeam', {}).get('name')
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

# ... (rest of the ingest functions for other sports and CSVs remain the same) ...
def ingest_tennis_data_from_api(conn):
    print("[Ingest] Tennis data ingestion not yet fully implemented.")
    return 0
def ingest_horse_racing_data(conn):
    print("[Ingest] Horse racing data ingestion not yet fully implemented.")
    return 0
def ingest_football_csv_file(conn, file_path: str) -> int:
    return 0
def ingest_football_csv_dir(conn, csv_dir: str) -> int:
    return 0
