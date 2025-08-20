"""
Ingest Cricsheet CSV match summaries.
We assume a summary CSV with columns including: match_id,date,team1,team2,venue,season,gender,match_type,team1_runs,team2_runs,winner
If you unzip cricsheet CSV pack, you may point to a directory and we will load all *.csv.
"""
import os, csv, sqlite3

def ingest_dir(conn: sqlite3.Connection, csv_dir: str) -> int:
    files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    total = 0
    for f in files:
        path = os.path.join(csv_dir, f)
        with open(path, newline="", encoding="utf-8", errors="ignore") as fh:
            r = csv.DictReader(fh)
            for row in r:
                # We do a best-effort normalisation; you can customise per specific cricsheet file.
                date = (row.get("date") or row.get("start_date") or "").strip()  # YYYY-MM-DD
                home = (row.get("team1") or "").strip()
                away = (row.get("team2") or "").strip()
                if not date or not home or not away:
                    continue
                # Insert event
                cur = conn.execute(
                    """SELECT event_id FROM events
                       WHERE sport='cricket' AND start_date=? AND home_team=? AND away_team=?""",
                    (date, home, away)
                )
                got = cur.fetchone()
                if got:
                    event_id = got[0]
                else:
                    conn.execute(
                        "INSERT INTO events(sport, comp, season, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?,?)",
                        ("cricket", row.get("match_type") or None, row.get("season") or None, date, home, away, "completed" if row.get("winner") else "scheduled")
                    )
                    event_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                # Result if present
                if row.get("team1_runs") and row.get("team2_runs"):
                    h = _to_int(row.get("team1_runs")); a = _to_int(row.get("team2_runs"))
                    outcome = "H" if h>a else ("A" if a>h else "D")
                    conn.execute(
                        "INSERT OR REPLACE INTO results(event_id, home_score, away_score, outcome) VALUES (?,?,?,?)",
                        (event_id, h, a, outcome)
                    )
                total += 1
        conn.commit()
    return total

def _to_int(x):
    try: return int(x)
    except: 
        try: return int(float(x))
        except: return None
