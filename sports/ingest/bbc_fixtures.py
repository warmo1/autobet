import requests
from bs4 import BeautifulSoup
from datetime import date
import sqlite3

def ingest_bbc_football_fixtures(conn: sqlite3.Connection) -> int:
    """
    Scrapes the BBC Sport football fixtures page and ingests the data.
    """
    url = "https://www.bbc.co.uk/sport/football/scores-fixtures"
    print(f"[BBC Ingest] Fetching fixtures from: {url}")
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # This selector is specific to the BBC's current page structure and may need updating if they change their website.
        fixture_blocks = soup.select('.ssrc-532k2b-PromoLink')
        
        count = 0
        for block in fixture_blocks:
            try:
                home_team = block.select_one('span[data-reactid*="home-team-name"]').get_text(strip=True)
                away_team = block.select_one('span[data-reactid*="away-team-name"]').get_text(strip=True)
                competition = block.select_one('span.ssrc-17sirc6-Metadata').get_text(strip=True)
                
                if home_team and away_team:
                    conn.execute(
                        "INSERT OR IGNORE INTO events(sport, comp, start_date, home_team, away_team, status) VALUES (?,?,?,?,?,?)",
                        ("football", competition, date.today().strftime('%Y-%m-%d'), home_team, away_team, "scheduled")
                    )
                    count += 1
            except AttributeError:
                # This handles cases where a block is not a valid fixture
                continue
                
        conn.commit()
        print(f"[BBC Ingest] Ingested {count} football fixtures.")
        return count

    except Exception as e:
        print(f"[BBC Ingest Error] Could not fetch or parse fixtures: {e}")
        return 0
