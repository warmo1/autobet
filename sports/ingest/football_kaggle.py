import csv
from datetime import datetime
from sports.schema import upsert_match

def ingest_file(conn, csv_path: str) -> int:
    count = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                date = (row.get("date") or row.get("Date") or "").split(" ")[0]
                # attempt ISO first, else fallback common formats
                try:
                    iso = datetime.fromisoformat(date).date().isoformat()
                except Exception:
                    iso = datetime.strptime(date, "%d/%m/%Y").date().isoformat()
                home = row.get("home_team") or row.get("HomeTeam")
                away = row.get("away_team") or row.get("AwayTeam")
                comp = row.get("competition") or row.get("Div")
                fthg = int(row["home_goals"]) if row.get("home_goals") else None
                ftag = int(row["away_goals"]) if row.get("away_goals") else None
                ftr  = row.get("result") or row.get("FTR")
                upsert_match(
                    conn,
                    sport="football",
                    comp=comp,
                    season=row.get("season"),
                    date=iso,
                    home=home, away=away,
                    fthg=fthg, ftag=ftag, ftr=ftr,
                    source="kaggle",
                )
                count += 1
            except Exception:
                continue
    return count
