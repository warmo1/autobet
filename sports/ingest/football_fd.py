import os
import csv
from datetime import datetime
from sports.schema import upsert_match

FD_DATE_FORMATS = ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"]

def _parse_date(s: str) -> str:
    for fmt in FD_DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Unrecognised date: {s}")

def ingest_dir(conn, dir_path: str) -> int:
    count = 0
    for name in sorted(os.listdir(dir_path)):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(dir_path, name)
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    date = _parse_date(row.get("Date") or row.get("date"))
                    home = row.get("HomeTeam") or row.get("Home") or row.get("HomeTeamName")
                    away = row.get("AwayTeam") or row.get("Away") or row.get("AwayTeamName")
                    comp = row.get("Div") or row.get("division")
                    fthg = int(row["FTHG"]) if row.get("FTHG") not in (None, "", "NA") else None
                    ftag = int(row["FTAG"]) if row.get("FTAG") not in (None, "", "NA") else None
                    ftr  = row.get("FTR")
                    upsert_match(
                        conn,
                        sport="football",
                        comp=comp,
                        season=None,
                        date=date,
                        home=home, away=away,
                        fthg=fthg, ftag=ftag, ftr=ftr,
                        source="football-data.co.uk",
                    )
                    count += 1
                except Exception:
                    # skip bad rows; optionally add logging
                    continue
    return count
