import os
import csv
from datetime import datetime
from sports.schema import upsert_match

def ingest_dir(conn, dir_path: str) -> int:
    """
    Expects each CSV to have fields: match_date, team1, team2, comp (optional), result (optional)
    Adjust this mapping to your actual Cricsheet CSV headers.
    """
    count = 0
    for name in sorted(os.listdir(dir_path)):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(dir_path, name)
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    date_raw = row.get("match_date") or row.get("date")
                    date_iso = datetime.fromisoformat(date_raw).date().isoformat()
                except Exception:
                    try:
                        date_iso = datetime.strptime(date_raw, "%d/%m/%Y").date().isoformat()
                    except Exception:
                        continue
                home = row.get("team1") or row.get("home_team") or row.get("HomeTeam")
                away = row.get("team2") or row.get("away_team") or row.get("AwayTeam")
                comp = row.get("comp") or row.get("competition")
                upsert_match(
                    conn,
                    sport="cricket",
                    comp=comp,
                    season=row.get("season"),
                    date=date_iso,
                    home=home, away=away,
                    fthg=None, ftag=None, ftr=row.get("result"),
                    source="cricsheet",
                )
                count += 1
    return count
