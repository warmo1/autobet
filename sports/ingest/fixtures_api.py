import os
import requests
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional

from sports.schema import upsert_match  # use the unified matches/teams schema
import sqlite3

# Debug toggle: set via CLI flag in run.py (exports FIXTURE_DEBUG=1)
DEBUG = os.getenv("FIXTURE_DEBUG") == "1"

# Headers – some ESPN edges throttle bare requests
HEADERS = {
    "User-Agent": "autobet/fixtures (+https://github.com/warmo1/autobet)",
    "Accept-Language": "en-GB,en;q=0.9",
}

# --- ESPN API Configuration ---
# Built-in league paths. You can add more at runtime with ESPN_EXTRA_PATHS="key=path,key2=path2"
SPORT_PATHS = {
    # Football (soccer)
    "football": "soccer/eng.1",                  # English Premier League
    "football_championship": "soccer/eng.2",     # EFL Championship
    # Tennis (tour scoreboards aggregate daily matches)
    "tennis_atp": "tennis/atp",
    "tennis_wta": "tennis/wta",
    # Golf (PGA Tour scoreboard shows current tournament rounds)
    "golf_pga": "golf/pga",
    # The following may vary by season/competition. Provide via ESPN_EXTRA_PATHS to avoid hardcoding:
    # "rugby_premiership": "rugby/eng.1",        # Example placeholder – confirm exact path id first
    # "rugby_urc": "rugby/206",
    # "cricket_international": "cricket/icc",
    # "horse_uk": "horse-racing/uk",           # Horse racing often not available via scoreboard
}

# Allow runtime extension via env, e.g. ESPN_EXTRA_PATHS="rugby_premiership=rugby/eng.1,cricket_blast=cricket/1234"
_extra = os.getenv("ESPN_EXTRA_PATHS")
if _extra:
    for kv in _extra.split(","):
        if "=" in kv:
            k, v = kv.split("=", 1)
            SPORT_PATHS[k.strip()] = v.strip()

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/{path}/scoreboard"
# Some slates only appear on the web subdomain API
BASE_URL_FALLBACK = "https://site.web.api.espn.com/apis/v2/sports/{path}/scoreboard"

# --- Minimal, robust DB helpers (avoid brittle upsert_match assumptions) ---

def _ensure_team(conn: sqlite3.Connection, name: str) -> int:
    # Try fetch existing row by UNIQUE name
    cur = conn.execute("SELECT team_id FROM teams WHERE name=?", (name,))
    row = cur.fetchone()
    if row:
        return int(row[0])
    # Insert new team row; assume schema: team_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE
    cur = conn.execute("INSERT OR IGNORE INTO teams(name) VALUES (?)", (name,))
    if cur.lastrowid:
        return int(cur.lastrowid)
    # If IGNORE due to race, fetch again
    cur = conn.execute("SELECT team_id FROM teams WHERE name=?", (name,))
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _upsert_match_raw(
    conn: sqlite3.Connection,
    *,
    sport: str,
    comp: str,
    date: str,
    home: str,
    away: str,
    source: str,
    fthg: Optional[int] = None,
    ftag: Optional[int] = None,
    ftr: Optional[str] = None,
):
    home_id = _ensure_team(conn, home)
    away_id = _ensure_team(conn, away)
    match_id = f"{sport}:{date}:{home}:{away}"
    # Insert if missing
    conn.execute(
        """
        INSERT OR IGNORE INTO matches(
            match_id, sport, comp, season, date, home_id, away_id, fthg, ftag, ftr, source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            match_id,
            sport,
            comp or "",
            "",
            date,
            int(home_id),
            int(away_id),
            fthg,
            ftag,
            ftr,
            source,
        ),
    )
    # Update comp/source if row already existed
    conn.execute(
        "UPDATE matches SET comp=COALESCE(?, comp), source=COALESCE(?, source) WHERE match_id=?",
        (comp or "", source or "", match_id),
    )


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
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()


def _safe_team_name(side: Dict[str, Any]) -> Optional[str]:
    t = side.get("team") or {}
    return (
        t.get("name")
        or t.get("displayName")
        or t.get("shortDisplayName")
        or t.get("abbreviation")
    )


def _parse_event(ev: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalise an ESPN scoreboard event into a dict for upsert_match."""
    try:
        comps = ev.get("competitions", [])
        comp0 = comps[0] if comps else {}
        competitors = comp0.get("competitors", [])
        if len(competitors) < 2:
            if DEBUG:
                print("[ESPN DEBUG] skip: <2 competitors")
            return None

        # Prefer the explicit homeAway flag; fall back to order/isHome
        home_team = next((c for c in competitors if c.get("homeAway") == "home"), None)
        away_team = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not home_team or not away_team:
            # try boolean flags
            home_team = next((c for c in competitors if c.get("isHome") is True), home_team)
            away_team = next((c for c in competitors if c.get("isHome") is False), away_team)
        if not home_team or not away_team:
            # final fallback: assume first is home, second is away
            if len(competitors) >= 2:
                home_team = home_team or competitors[0]
                away_team = away_team or competitors[1]

        # Tennis sometimes uses athlete blocks instead of team names
        def _athlete_name(cand):
            a = (cand or {}).get("athlete") or {}
            n = a.get("displayName") or a.get("shortName") or a.get("fullName")
            return n

        home_name = _safe_team_name(home_team) if home_team else None
        away_name = _safe_team_name(away_team) if away_team else None
        if not home_name:
            home_name = _athlete_name(home_team)
        if not away_name:
            away_name = _athlete_name(away_team)

        if not home_name or not away_name:
            if DEBUG:
                print(f"[ESPN DEBUG] skip: missing team names h={bool(home_name)} a={bool(away_name)}")
            return None

        # If we still have no two competitors (e.g., golf tournament), try to coerce an event
        if not (home_team and away_team) and (ev.get('name') or comp0.get('venue')):
            title = ev.get('name') or comp0.get('name') or (comp0.get('venue') or {}).get('fullName') or 'Event'
            home_name = title
            away_name = (comp0.get('venue') or {}).get('fullName') or 'Field'

        # Kickoff date – event.date, else competition.date/startDate
        iso_dt = ev.get("date") or comp0.get("date") or comp0.get("startDate")
        match_date = None
        if iso_dt:
            try:
                match_date = datetime.fromisoformat(str(iso_dt).replace("Z", "+00:00")).date().isoformat()
            except Exception:
                match_date = None
        if not match_date:
            if DEBUG:
                print("[ESPN DEBUG] skip: no parsable date")
            return None

        league_name = (
            (ev.get("league") or {}).get("name")
            or (comp0.get("league") or {}).get("name")
            or ev.get("shortName")
            or "Football"
        )

        # Optional scoring (if finished)
        status = (ev.get("status") or {}).get("type") or {}
        state = status.get("state")  # e.g., 'pre', 'in', 'post'
        fthg = ftag = None
        ftr = None
        try:
            comp_scores = comp0.get("competitors", [])
            if len(comp_scores) >= 2:
                # ESPN often provides integers as strings
                def _score(c):
                    s = c.get("score")
                    try:
                        return int(s) if s is not None else None
                    except Exception:
                        return None
                # align scores to resolved home/away
                home_score = next((_score(c) for c in comp_scores if (c.get("homeAway") == "home") or (c is home_team)), None)
                away_score = next((_score(c) for c in comp_scores if (c.get("homeAway") == "away") or (c is away_team)), None)
                if state == "post" and home_score is not None and away_score is not None:
                    fthg, ftag = home_score, away_score
                    ftr = "H" if fthg > ftag else ("A" if ftag > fthg else "D")
        except Exception:
            pass

        return {
            "date": match_date,
            "home": home_name,
            "away": away_name,
            "comp": league_name,
            "fthg": fthg,
            "ftag": ftag,
            "ftr": ftr,
        }
    except Exception as e:
        if DEBUG:
            print(f"[ESPN DEBUG] parse error: {e}")
        return None


def _fetch_events_for_date(path: str, date_str: str) -> list:
    url = BASE_URL.format(path=path)
    params = {"dates": date_str}
    data = _espn_get(url, params=params)
    events = data.get("events", []) or []
    if DEBUG:
        print(f"[ESPN DEBUG] URL: {url} params: {params} events: {len(events)}")
    if not events:
        fb_url = BASE_URL_FALLBACK.format(path=path)
        try:
            data = _espn_get(fb_url, params=params)
            events = data.get("events", []) or []
            if DEBUG:
                print(f"[ESPN DEBUG] Fallback URL: {fb_url} events: {len(events)}")
        except Exception as e:
            if DEBUG:
                print(f"[ESPN DEBUG] Fallback error: {e}")
    if DEBUG and events:
        e0 = events[0]
        comp0 = (e0.get("competitions") or [{}])[0]
        teams = comp0.get("competitors", [])
        print(f"[ESPN DEBUG] sample: keys={list(e0.keys())[:8]} teams={len(teams)} date={e0.get('date') or comp0.get('date')} name={e0.get('name')}")
    return events


def ingest_fixtures(conn, sport: str, *, date_iso: Optional[str] = None) -> int:
    """
    Fetch & upsert fixtures for a given sport key via ESPN scoreboard.
    - Uses unified matches/teams schema via upsert_match().
    - Filters by the provided ISO date (defaults to today) using ESPN's ?dates=YYYYMMDD param.
    - If empty, tries fallback host and a 5-day range (date-2 to date+2).
    """
    if sport not in SPORT_PATHS:
        print(f"[Ingest Error] Sport '{sport}' not supported by the ESPN ingestor.")
        return 0

    path = SPORT_PATHS[sport]
    day_param = _dates_param(date_iso)

    print(f"[Fixtures] ESPN → {sport} ({path}) for {day_param} …")

    try:
        # 1) Try the exact date
        events = _fetch_events_for_date(path, day_param)

        # 2) If none, try a small range around the date (helps across US/EU tz and weekend slates)
        if not events:
            try:
                base_dt = datetime.strptime(day_param, "%Y%m%d").date()
            except Exception:
                base_dt = date.today()
            start = base_dt - timedelta(days=2)
            end = base_dt + timedelta(days=2)
            range_param = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
            if DEBUG:
                print(f"[ESPN DEBUG] Trying range: {range_param}")
            events = _fetch_events_for_date(path, range_param)

        if not events:
            print(f"[Fixtures] No {sport} events returned for date {day_param} (and nearby range).")
            return 0

        count = 0
        for ev in events:
            norm = _parse_event(ev)
            if not norm:
                continue
            if DEBUG:
                try:
                    print(f"[ESPN DEBUG] norm: {norm['comp']} | {norm['home']} vs {norm['away']} on {norm['date']}")
                except Exception:
                    pass
            try:
                _upsert_match_raw(
                    conn,
                    sport="football" if sport.startswith("football") else sport,
                    comp=norm["comp"],
                    date=norm["date"],
                    home=norm["home"],
                    away=norm["away"],
                    source=f"espn:{path}",
                    fthg=norm.get("fthg"),
                    ftag=norm.get("ftag"),
                    ftr=norm.get("ftr"),
                )
                count += 1
            except Exception as e:
                if DEBUG:
                    print(f"[ESPN DEBUG] upsert error: {e} for {norm['home']} vs {norm['away']} on {norm['date']}")
                continue

        print(f"[Fixtures] Ingested {count} fixture(s) from ESPN for {sport}.")
        if DEBUG:
            print(f"[ESPN DEBUG] upserted: {count}")
        if DEBUG and events and count == 0:
            print("[ESPN DEBUG] events present but none normalised – structure may differ; printed a sample above.")
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
