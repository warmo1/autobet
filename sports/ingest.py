import os
import csv
from .db import insert_match, insert_odds

def ingest_football_csv_file(conn, file_path: str) -> int:
    """
    Ingests historical football data from a single CSV file into the database.
    Returns the number of rows processed.
    """
    count = 0
    print(f"[Ingest] Processing file: {file_path}")
    try:
        with open(file_path, newline="", encoding="utf-8", errors="ignore") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                def g(k, default=None):
                    return row.get(k, default)
                
                date = g("Date")
                home = g("HomeTeam")
                away = g("AwayTeam")
                if not date or not home or not away:
                    continue
                
                fthg = _to_int(g("FTHG"))
                ftag = _to_int(g("FTAG"))
                ftr = g("FTR")
                comp = g("Div") or ""
                season = g("Season") or ""
                
                mid = insert_match(conn, "football", date, home, away, fthg, ftag, ftr, comp, season)
                
                odd_home = _to_float(g("B365H"))
                odd_draw = _to_float(g("B365D"))
                odd_away = _to_float(g("B365A"))
                if any([odd_home, odd_draw, odd_away]):
                    insert_odds(conn, mid, "B365", odd_home, odd_draw, odd_away)
                
                count += 1
    except Exception as e:
        print(f"[Ingest Error] Could not process file {file_path}: {e}")
        return 0
    return count

def ingest_football_csv_dir(conn, csv_dir: str) -> int:
    """
    Ingests all CSV files from a given directory.
    """
    files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    total_count = 0
    for f in files:
        path = os.path.join(csv_dir, f)
        total_count += ingest_football_csv_file(conn, path)
    return total_count

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
