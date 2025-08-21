# autobet/sports/ingest/bbc_fixtures.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Iterable, Tuple, Optional, List
from sports.schema import upsert_match

# BBC competition slugs we support (you can add more)
BBC_COMPS = {
    "premier-league": "Premier League",
    "championship": "Championship",
    "league-one": "League One",
    "league-two": "League Two",
    "fa-cup": "FA Cup",
    "league-cup": "EFL Cup",
}

BASE = "https://www.bbc.co.uk/sport/football/{slug}/scores-fixtures/{date}"  # YYYY-MM-DD

def _fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "autobet/1.0"})
        r.raise_for_status()
        return r.text
    except Exception:
        return None

def _parse_fixtures(html: str) -> Iterable[Tuple[str, str]]:
    """
    Yield (home, away) team names from a BBC fixtures page.
    We try multiple selector patterns to be resilient to minor markup changes.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Primary pattern: each fixture card lists two team rows.
    # Try to capture .qa-match-block elements with team names.
    blocks = soup.select("[data-testid='match-block'], .qa-match-block, .sp-c-fixture")
    if not blocks:
        # fallback: older structures
        blocks = soup.select(".sp-c-fixture")

    for b in blocks:
        # Try multiple ways to get team names
        home = None
        away = None

        # Common structure has spans with data-team-home/away or visually in order
        teams = b.select("[data-testid='team-name'], .sp-c-fixture__team-name, .gs-u-display-inline-block")
        texts = [t.get_text(strip=True) for t in teams if t.get_text(strip=True)]
        # Deduplicate and keep order
        seen, ordered = set(), []
        for t in texts:
            if t not in seen:
                ordered.append(t)
                seen.add(t)
        if len(ordered) >= 2:
            home, away = ordered[0], ordered[1]

        if not home or not away:
            # Another fallback: look for abbreviations or spans in team blocks
            parts = b.select(".sp-c-fixture__team, .sp-c-fixture__wrapper .qa-full-team-name")
            if len(parts) >= 2:
                home = parts[0].get_text(strip=True)
                away = parts[1].get_text(strip=True)

        if home and away:
            yield home, away

def ingest_date(conn, date_iso: str, competitions: Optional[List[str]] = None) -> int:
    """
    Scrape fixtures for given ISO date (YYYY-MM-DD) from BBC for the competitions provided.
    competitions: list of slugs from BBC_COMPS keys. If None, scrape all in BBC_COMPS.
    """
    try:
        datetime.fromisoformat(date_iso)
    except Exception:
        raise ValueError("date_iso must be YYYY-MM-DD")

    comps = competitions or list(BBC_COMPS.keys())
    count = 0

    for slug in comps:
        comp_name = BBC_COMPS.get(slug, slug)
        url = BASE.format(slug=slug, date=date_iso)
        html = _fetch_html(url)
        if not html:
            continue
        for home, away in _parse_fixtures(html):
            try:
                upsert_match(
                    conn,
                    sport="football",
                    comp=comp_name,
                    season=None,
                    date=date_iso,
                    home=home,
                    away=away,
                    fthg=None, ftag=None, ftr=None,
                    source=f"bbc:{slug}",
                )
                count += 1
            except Exception:
                continue

    return count
