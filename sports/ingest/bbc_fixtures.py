import os
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from typing import Iterable, Tuple, Optional
import json

from sports.schema import upsert_match

DEBUG = os.getenv("FIXTURE_DEBUG") == "1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.bbc.co.uk/sport/football/scores-fixtures",
    "Cache-Control": "no-cache",
}
# Helps bypass the BBC consent wall when fetching HTML without a browser
BBC_CONSENT_COOKIE = {"ckns_policy": "111", "ckns_policy_exp": "4102444800000"}

# BBC football uses a date-scoped fixtures page
FOOTBALL_URL = "https://www.bbc.co.uk/sport/football/scores-fixtures/{date}"  # YYYY-MM-DD

SUPPORTED_SPORTS = {"football"}


def _fetch_html(url: str) -> Optional[str]:
    try:
        s = requests.Session()
        r = s.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        text = r.text
        if DEBUG:
            print(f"[BBC DEBUG] status={r.status_code} len={len(text)}")
        # If we don't see any obvious fixture markup, retry with consent cookie set
        if ("sp-c-fixture" not in text) and ("data-testid=\"match-block\"" not in text):
            s.cookies.update(BBC_CONSENT_COOKIE)
            r2 = s.get(url, headers=HEADERS, timeout=30)
            r2.raise_for_status()
            if DEBUG:
                print(f"[BBC DEBUG] retry status={r2.status_code} len={len(r2.text)}")
            return r2.text
        return text
    except Exception as e:
        if DEBUG:
            print(f"[BBC DEBUG] fetch error: {e}")
        return None


def _parse_ld_json(html: str, date_iso: str) -> Iterable[Tuple[str, str, str]]:
    """Fallback: parse application/ld+json blocks to extract SportsEvent entries."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        scripts = soup.find_all("script", {"type": "application/ld+json"})
        for sc in scripts:
            try:
                data = json.loads(sc.string or "{}")
            except Exception:
                continue
            # Sometimes it's a single object, sometimes a list
            items = data if isinstance(data, list) else [data]
            for it in items:
                if not isinstance(it, dict):
                    continue
                if it.get("@type") not in ("SportsEvent", "Event"):
                    continue
                # only take events on the requested date
                start = it.get("startDate") or it.get("startTime") or it.get("date")
                if not start:
                    continue
                try:
                    d = datetime.fromisoformat(start.replace("Z", "+00:00")).date().isoformat()
                except Exception:
                    d = None
                if d and d != date_iso:
                    continue
                comp = (it.get("superEvent") or {}).get("name") or it.get("name") or "Football"
                home = (it.get("homeTeam") or {}).get("name")
                away = (it.get("awayTeam") or {}).get("name")
                if home and away:
                    yield (comp, home, away)
    except Exception:
        return []


def _parse_football_day(html: str, date_iso: str) -> Iterable[Tuple[str, str, str]]:
    """
    Yield (competition, home, away) tuples for the given BBC football fixtures HTML.
    The BBC markup changes occasionally; we try a few robust selectors.
    """
    soup = BeautifulSoup(html, "html.parser")

    found = 0
    # Each competition is typically in a <section> with fixture cards inside
    sections = soup.select("[data-component='sport-fixtures'] section, section.sp-c-fixture__wrapper, section")
    sections_count = len(sections)

    for sec in sections:
        # Competition name can appear in various header tags
        header = sec.select_one("h3, h2, .sp-c-fixtures__date, [data-testid='link-text']")
        comp_name = (header.get_text(strip=True) if header else "").strip()
        if not comp_name:
            comp_name = "Football"

        # Fixture cards within this competition
        cards = sec.select("[data-testid='match-block'], .sp-c-fixture, li.sp-c-fixture, .sp-c-match-list .sp-c-fixture")
        cards_count = len(cards)
        if DEBUG:
            print(f"[BBC DEBUG] Section '{comp_name}' -> {cards_count} cards")

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
                found += 1
                yield (comp_name, home, away)

    if DEBUG:
        print(f"[BBC DEBUG] Parsed fixtures: {found} (sections: {sections_count})")


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
    if DEBUG:
        print(f"[BBC DEBUG] HTML bytes: {len(html.encode('utf-8')) if html else 0}")
    if not html:
        print("[BBC Ingest] No HTML returned from BBC; skipping.")
        return 0

    count = 0
    parsed = list(_parse_football_day(html, date_iso))
    if DEBUG:
        print(f"[BBC DEBUG] primary parser found: {len(parsed)} fixtures")
    if not parsed:
        ld = list(_parse_ld_json(html, date_iso))
        if DEBUG:
            print(f"[BBC DEBUG] ld+json fallback found: {len(ld)} fixtures")
        parsed = ld

    for comp, home, away in parsed:
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
        except Exception as e:
            if DEBUG:
                print(f"[BBC DEBUG] upsert error: {e} for {home} vs {away}")
            continue

    print(f"[BBC Ingest] Upserted {count} football fixtures for {date_iso}.")
    return count
