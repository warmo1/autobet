# Horse Racing Betting Bot (Tote)

A pragmatic, extensible betting bot for horse racing, focused on the Tote API:
- Ingests events, products (pools), results, and horse data from the Tote API.
- Ingests historical horse racing results from Kaggle CSVs.
- Provides a framework for building and training predictive models (e.g., for WIN or SUPERFECTA markets).
- Includes a Flask web dashboard for viewing data, placing audit bets, and analyzing pool viability.
- Supports exporting data to Google BigQuery for advanced analytics.

> This project is for research/education. **Gambling involves risk.** Bet responsibly and comply with local laws.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env, e.g. DATABASE_URL, TOTE_API_KEY, and BQ_* variables
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

## GCP ingestion pipeline

A basic pipeline using Google Cloud native services is included. Deploy
`sports/gcp_ingest_service.py` to Cloud Run and create a Pub/Sub topic with a
push subscription pointing at the service. Jobs can then be published with
`scripts/publish_ingest_job.py`:

```bash
python scripts/publish_ingest_job.py --project <PROJECT> --topic <TOPIC> \
    --url https://example.com/file.csv --bucket my-bucket --name raw/file.csv
```

The Cloud Run service downloads the referenced URL and stores the contents in
the specified Cloud Storage bucket for downstream processing.
