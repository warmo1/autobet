import os, csv
from .db import insert_match, insert_odds

FOOTBALL_HEADERS = {
    "Date": "date",
    "HomeTeam": "home",
    "AwayTeam": "away",
    "FTHG": "fthg",
    "FTAG": "ftag",
    "FTR": "ftr",
    "Div": "comp",
    "Season": "season",
    # Popular odds columns
    "B365H": "odd_home",
    "B365D": "odd_draw",
    "B365A": "odd_away",
}

def ingest_football_csv_dir(conn, csv_dir: str):
    files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    count = 0
    for f in files:
        path = os.path.join(csv_dir, f)
        with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
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
    return count

def _to_int(x):
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None
