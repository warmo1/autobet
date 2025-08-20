# Sports Betting Bot (Starter)

A pragmatic, extensible sports betting bot for football, rugby, and cricket:
- Ingest historical results/odds (CSV)
- Train simple models (Poisson for football; basic Elo for others)
- Generate value **back**/**lay** suggestions (including **draw**)
- Paper-bet with bankroll management (fractional Kelly)
- Optional live Betfair integration (with `--confirm BET` guard)
- Flask dashboard with weekly suggestions + AI news/insights

> This project is for research/education. **Gambling involves risk.** Bet responsibly and comply with local laws.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env, e.g. DATABASE_URL and provider keys if you want AI insights
```

### 1) Ingest historical CSVs
Supported now: **football-data.co.uk**-style CSV (Europe leagues), or custom CSV with columns:
`Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,B365H,B365D,B365A` (extra columns ignored).

```bash
python -m sports.run ingest --sport football --csv-dir /path/to/csvs
```

### 2) Train models
```bash
python -m sports.run train --sport football
```

### 3) Suggestions (next 7 days)
```bash
python -m sports.run suggest --sport football --days 7 --min-edge 0.03 --kelly-mult 0.25
```

### 4) Paper bet execution (simulated staking)
```bash
python -m sports.run paper --stake-plan kelly0.25
```

### 5) News + Insights
```bash
python -m sports.run news
python -m sports.run insights
```

### 6) Web dashboard
```bash
python -m sports.run web
# open http://localhost:8010
```

### 7) Live (optional, Betfair - disabled by default)
```bash
python -m sports.run live --symbol 'Home vs Away (Match Odds)' --side back --odds 2.2 --stake 5 --confirm BET
```

## Folder layout
- `sports/db.py` – SQLite schema + helpers
- `sports/ingest.py` – CSV ingestion
- `sports/model_football.py` – Poisson goals model
- `sports/model_generic.py` – simple Elo for rugby/cricket (stub)
- `sports/odds.py` – odds & staking utilities
- `sports/suggest.py` – suggestion engine
- `sports/betfair_api.py` – Betfair connector (safe by default)
- `sports/llm.py`, `sports/news.py` – news fetch + LLM summaries
- `sports/webapp.py` – Flask dashboard
- `sports/run.py` – CLI entrypoint
