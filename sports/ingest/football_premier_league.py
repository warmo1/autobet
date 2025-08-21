import csv
from datetime import datetime
from sports.schema import upsert_match

def ingest_file(conn, csv_path: str) -> int:
    """
    Expecting columns like: date, home_team, away_team, fthg, ftag, ftr, season, competition (optional)
    """
    count = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                date_raw = row.get("date") or row.get("Date")
                date_iso = datetime.fromisoformat(date_raw).date().isoformat()
            except Exception:
                try:
                    date_iso = datetime.strptime(date_raw, "%d/%m/%Y").date().isoformat()
                except Exception:
                    continue

            home = row.get("home_team") or row.get("HomeTeam")
            away = row.get("away_team") or row.get("AwayTeam")
            if not home or not away:
                continue

            def _to_int(v):
                return int(v) if v not in (None, "", "NA") else None

            upsert_match(
                conn,
                sport="football",
                comp=row.get("competition") or "Premier League",
                season=row.get("season"),
                date=date_iso,
                home=home,
                away=away,
                fthg=_to_int(row.get("fthg") or row.get("FTHG")),
                ftag=_to_int(row.get("ftag") or row.get("FTAG")),
                ftr=row.get("ftr") or row.get("FTR"),
                source="kaggle_pl",
            )
            count += 1
    return count
