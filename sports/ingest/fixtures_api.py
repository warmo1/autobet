import requests
from datetime import datetime, date
from typing import Dict, Any, Optional

from sports.schema import upsert_match  # use the unified matches/teams schema

UA = {"User-Agent": "autobet/fixtures (github.com/warmo1/autobet)"}

# --- ESPN API Configuration ---
# Map internal sport keys to ESPN paths. Start with soccer (EPL, Championship).
# Rugby/Cricket endpoints on ESPN are inconsistent/unofficial; keep stubs for now.
SPORT_PATHS = {
    "football": "soccer/eng.1",          # English Premier League
    "football_championship": "soccer/eng.2",  # EFL Championship
    # "rugby": "rugby/267979",           # Premiership Rugby (numeric league ID) – optional
    # "cricket": "cricket/england",      # Placeholder; verify exact path before enabling
}

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/{path}/scoreboard"


def _dates_param(d: Optional[str]) -> str:
    """Return ESPN dates param as YYYYMMDD for the provided ISO date or today."""
    if not d:
        d = date.today().isoformat()
    try:
        return datetime.fromisoformat(d).strftime("%Y%m%d")
    except ValueError:
        # allow already formatted 8-digit strings
        if len(d) == 8 and d.isdigit():
            return d
        raise


def _espn_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = requests.get(url, headers=UA, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()


def _parse_event(ev: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalise an ESPN scoreboard event into a dict for upsert_match."""
    try:
        comps = ev.get("competitions", [])
        comp0 = comps[0] if comps else {}
        competitors = comp0.get("competitors", [])
        if len(competitors) < 2:
            return None

        # Identify home/away by flag, not order
        home_team = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away_team = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not home_team or not away_team:
            return None

        home_name = (home_team.get("team") or {}).get("name") or (home_team.get("team") or {}).get("displayName")
        away_name = (away_team.get("team") or {}).get("name") or (away_team.get("team") or {}).get("displayName")
        if not home_name or not away_name:
            return None

        iso_dt = ev.get("date")  # e.g. 2025-08-23T14:00Z
        match_date = None
        if iso_dt:
            try:
                match_date = datetime.fromisoformat(iso_dt.replace("Z", "+00:00")).date().isoformat()
            except Exception:
                pass
        if not match_date:
            return None

        # League/competition name – try event then competition then top-level fallback
        league_name = (
            (ev.get("league") or {}).get("name")
            or (comp0.get("league") or {}).get("name")
            or "Football"
        )

        # Status gate: keep scheduled + in-progress (so today’s live games are included)
        status_name = ((ev.get("status") or {}).get("type") or {}).get("name", "")
        if status_name not in {"STATUS_SCHEDULED", "STATUS_IN_PROGRESS", "STATUS_HALFTIME"}:
            # skip finals to avoid re-writing scores over settled data if you only want upcoming/live
            pass  # we still ingest; comment this line to enforce filtering

        return {
            "date": match_date,
            "home": home_name,
            "away": away_name,
            "comp": league_name,
        }
    except Exception:
        return None


def ingest_fixtures(conn, sport: str, *, date_iso: Optional[str] = None) -> int:
    """
    Fetch & upsert fixtures for a given sport key via ESPN scoreboard.
    - Uses unified matches/teams schema via upsert_match().
    - Filters by the provided ISO date (defaults to today) using ESPN's ?dates=YYYYMMDD param.
    """
    if sport not in SPORT_PATHS:
        print(f"[Ingest Error] Sport '{sport}' not supported by the ESPN ingestor.")
        return 0

    path = SPORT_PATHS[sport]
    url = BASE_URL.format(path=path)
    params = {"dates": _dates_param(date_iso)}

    print(f"[Fixtures] ESPN → {sport} ({path}) for {params['dates']} …")

    try:
        data = _espn_get(url, params=params)
        events = data.get("events", []) or []
        if not events:
            print(f"[Fixtures] No {sport} events returned for date {params['dates']}.")
            return 0

        count = 0
        for ev in events:
            norm = _parse_event(ev)
            if not norm:
                continue
            try:
                upsert_match(
                    conn,
                    sport="football" if sport.startswith("football") else sport,
                    comp=norm["comp"],
                    season=None,
                    date=norm["date"],
                    home=norm["home"],
                    away=norm["away"],
                    fthg=None,
                    ftag=None,
                    ftr=None,
                    source=f"espn:{path}",
                )
                count += 1
            except Exception:
                # keep robust; optionally log row
                continue

        print(f"[Fixtures] Ingested {count} fixture(s) from ESPN for {sport}.")
        return count

    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if code == 404:
            print("[Fixtures] ESPN returned 404 (possibly out of season / bad league path).")
            return 0
        print(f"[API Error] ESPN HTTP error: {e}")
        return 0
    except Exception as e:
        print(f"[API Error] Unexpected error: {e}")
        return 0
