# autobet/sports/ingest/fpl_fixtures.py
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from sports.schema import upsert_match

BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
FIXTURES_URL = "https://fantasy.premierleague.com/api/fixtures/"

def _get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = requests.get(url, params=params, timeout=30, headers={"User-Agent": "autobet/1.0"})
    r.raise_for_status()
    return r.json()

def _load_team_map() -> Dict[int, str]:
    data = _get_json(BOOTSTRAP_URL)
    teams = data.get("teams", [])
    return {t["id"]: t["name"] for t in teams if "id" in t and "name" in t}

def ingest(conn, future_only: bool = True) -> int:
    """
    Pull Premier League fixtures from the FPL API and insert/update matches.
    - If future_only=True, only upcoming matches are returned by the API (?future=1).
    - If False, you get all fixtures (past + future).
    """
    params = {"future": 1} if future_only else None
    fixtures = _get_json(FIXTURES_URL, params=params)
    team_map = _load_team_map()
    count = 0

    for f in fixtures:
        try:
            home_name = team_map.get(f["team_h"])
            away_name = team_map.get(f["team_a"])
            if not home_name or not away_name:
                continue

            kt = f.get("kickoff_time")  # UTC ISO, e.g. "2025-08-14T19:00:00Z"
            if kt:
                # normalise to date (local date will be handled downstream if needed)
                try:
                    dt = datetime.fromisoformat(kt.replace("Z", "+00:00")).date().isoformat()
                except Exception:
                    # if parse fails, skip – FPL always sends valid ISO, though
                    continue
            else:
                # rarely missing; fall back to today's date to store minimally
                dt = datetime.now(timezone.utc).date().isoformat()

            # result (if finished)
            ftr = None
            if f.get("finished"):
                hg = f.get("team_h_score")
                ag = f.get("team_a_score")
                if isinstance(hg, int) and isinstance(ag, int):
                    ftr = "H" if hg > ag else ("A" if ag > hg else "D")
            fthg = f.get("team_h_score") if isinstance(f.get("team_h_score"), int) else None
            ftag = f.get("team_a_score") if isinstance(f.get("team_a_score"), int) else None

            upsert_match(
                conn,
                sport="football",
                comp="Premier League",
                season=None,  # can be derived from bootstrap if needed
                date=dt,
                home=home_name,
                away=away_name,
                fthg=fthg,
                ftag=ftag,
                ftr=ftr,
                source="fpl_api",
            )
            count += 1
        except Exception:
            # keep ingestion robust; you can add logging here
            continue

    return count
