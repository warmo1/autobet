import os
import csv
import requests
from .db import insert_match, insert_odds
from .config import cfg

# --- Football Ingestion ---
def ingest_football_csv_file(conn, file_path: str) -> int:
    """Ingests historical football data from a single CSV file."""
    count = 0
    try:
        with open(file_path, newline="", encoding="utf-8", errors="ignore") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # ... (existing football ingestion logic)
                count += 1
    except Exception as e:
        print(f"[Ingest Error] Could not process football CSV {file_path}: {e}")
    return count

def ingest_football_csv_dir(conn, csv_dir: str) -> int:
    """Ingests all football CSV files from a directory."""
    # ... (existing logic)

# --- Horse Racing Ingestion ---
def ingest_horse_racing_data(conn):
    """Fetches today's horse racing data from an API."""
    if not cfg.rapidapi_key:
        print("[Ingest Error] RAPIDAPI_KEY not set in .env file.")
        return 0
    
    url = "https://horse-racing.p.rapidapi.com/races"
    headers = {
        "X-RapidAPI-Key": cfg.rapidapi_key,
        "X-RapidAPI-Host": "horse-racing.p.rapidapi.com"
    }
    
    try:
        response = requests.get(url, headers=headers)
        races = response.json()
        count = 0
        for race in races:
            for horse in race.get('horses', []):
                # Simplified: treating each horse as a "match" for suggestion purposes
                insert_match(conn, "horse_racing", race['date'], horse['horse_name'], race['course'], None, None, None, race['race_name'], None)
                count += 1
        return count
    except Exception as e:
        print(f"[Ingest Error] Could not fetch horse racing data: {e}")
        return 0

# --- Tennis Ingestion ---
def ingest_tennis_data(conn):
    """Fetches upcoming tennis matches."""
    # This is a placeholder for a real tennis data API
    # For now, we'll add some dummy data
    print("[Ingest] Ingesting dummy tennis data.")
    matches = [
        {"date": "2025-08-21", "player1": "Carlos Alcaraz", "player2": "Novak Djokovic"},
        {"date": "2025-08-22", "player1": "Iga Swiatek", "player2": "Aryna Sabalenka"},
    ]
    count = 0
    for match in matches:
        insert_match(conn, "tennis", match['date'], match['player1'], match['player2'], None, None, None, "ATP/WTA Tour", None)
        count += 1
    return count

# --- Helper Functions ---
def _to_int(x):
    # ... (existing logic)

def _to_float(x):
    # ... (existing logic)
