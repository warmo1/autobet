
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from typing import Iterable, Tuple, Optional

from sports.schema import upsert_match

HEADERS = {"User-Agent": "autobet/fixtures (+https://github.com/warmo1/autobet)"}

# BBC football uses a date-scoped fixtures page
FOOTBALL_URL = "https://www.bbc.co.uk/sport/football/scores-fixtures/{date}"  # YYYY-MM-DD

SUPPORTED_SPORTS = {"football"}


def _fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def _parse_football_day(html: str, date_iso: str) -> Iterable[Tuple[str, str, str]]:
    """
    Yield (competition, home, away) tuples for the given BBC football fixtures HTML.
    The BBC markup changes occasionally; we try a few robust selectors.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Each competition is typically in a <section> with fixture cards inside
    sections = soup.select("[data-component='sport-fixtures'] section") or soup.select("section")

    for sec in sections:
        # Competition name can appear in various header tags
        header = sec.select_one("h3, h2, .sp-c-fixtures__date, [data-testid='link-text']")
        comp_name = (header.get_text(strip=True) if header else "").strip()
        if not comp_name:
            # attempt a fallback if header missing
            comp_name = "Football"

        # Fixture cards within this competition
        cards = sec.select("[data-testid='match-block'], .sp-c-fixture")
        if not cards:
            cards = sec.select("li.sp-c-fixture, .gs-o-list-ui__item")

        for card in cards:
            # Try to read two team names in order (home, away)
            names = [
                t.get_text(strip=True)
                for t in card.select(
                    "[data-testid='team-name'], .sp-c-fixture__team-name, .qa-full-team-name, .gs-u-display-inline-block"
                )
                if t.get_text(strip=True)
            ]
            # de-dupe keep order
            uniq = []
            seen = set()
            for n in names:
                if n not in seen:
                    uniq.append(n)
                    seen.add(n)

            home = away = None
            if len(uniq) >= 2:
                home, away = uniq[0], uniq[1]
            else:
                # fallback older structure
                parts = card.select(".sp-c-fixture__team")
                if len(parts) >= 2:
                    home = parts[0].get_text(strip=True)
                    away = parts[1].get_text(strip=True)

            if home and away:
                yield (comp_name, home, away)


def ingest_bbc_fixtures(conn: sqlite3.Connection, sport: str, *, date_iso: Optional[str] = None) -> int:
    """
    Ingest BBC fixtures for the given sport and date into the unified matches schema.
    Currently supports: football (date-scoped).
    Returns count of fixtures upserted.
    """
    if sport not in SUPPORTED_SPORTS:
        print(f"[BBC Ingest] Sport '{sport}' not yet supported by this ingestor. Use football.")
        return 0

    # Validate/normalise date
    if not date_iso:
        date_iso = date.today().isoformat()
    else:
        try:
            datetime.fromisoformat(date_iso)
        except Exception:
            raise ValueError("date_iso must be YYYY-MM-DD")

    url = FOOTBALL_URL.format(date=date_iso)
    print(f"[BBC Ingest] Fetching Football fixtures for {date_iso} → {url}")

    html = _fetch_html(url)
    if not html:
        print("[BBC Ingest] No HTML returned from BBC; skipping.")
        return 0

    count = 0
    for comp, home, away in _parse_football_day(html, date_iso):
        try:
            upsert_match(
                conn,
                sport="football",
                comp=comp,
                season=None,
                date=date_iso,
                home=home,
                away=away,
                fthg=None,
                ftag=None,
                ftr=None,
                source="bbc_html",
            )
            count += 1
        except Exception:
            continue

    print(f"[BBC Ingest] Upserted {count} football fixtures for {date_iso}.")
    return count
