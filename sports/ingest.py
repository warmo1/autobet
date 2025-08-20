import os
import csv
import requests
from datetime import date
from .db import insert_match, insert_odds
from .config import cfg

# --- API Data Ingestion ---

def ingest_football_fixtures_from_api(conn):
    """Fetches upcoming football fixtures from API-Football on RapidAPI."""
    if not (cfg.rapidapi_key and cfg.rapidapi_host_football):
        print("[Ingest Error] RapidAPI key or football host not set in .env file.")
        return 0

    url = f"https://{cfg.rapidapi_host_football}/v3/fixtures"
    params = {"date": date.today().strftime("%Y-%m-%d")}
    headers = {
        "X-RapidAPI-Key": cfg.rapidapi_key,
        "X-RapidAPI-Host": cfg.rapidapi_host_football
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        fixtures = response.json().get('response', [])
        count = 0
        for fix in fixtures:
            teams = fix.get('teams', {})
            home = teams.get('home', {}).get('name')
            away = teams.get('away', {}).get('name')
            fixture_date = fix.get('fixture', {}).get('date', '').split('T')[0]
            league = fix.get('league', {}).get('name')
            season = fix.get('league', {}).get('season')
            
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
    headers = {
        "X-RapidAPI-Key": cfg.rapidapi_key,
        "X-RapidAPI-Host": cfg.rapidapi_host_tennis
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        count = 0
        for event in data.get('events', []):
            home_player = event.get('home_team', {}).get('name')
            away_player = event.get('away_team', {}).get('name')
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
    headers = {
        "X-RapidAPI-Key": cfg.rapidapi_key,
        "X-RapidAPI-Host": cfg.rapidapi_host_horse_racing
    }
    # ... (rest of the horse racing logic remains the same)
    return 0 # Placeholder

# --- Football CSV Ingestion (for historical data) ---
def ingest_football_csv_dir(conn, csv_dir: str) -> int:
    # ... (this function remains for ingesting historical data)
    return 0 # Placeholder
