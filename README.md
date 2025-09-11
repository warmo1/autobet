# Horse Racing Betting Bot (Tote)

A pragmatic, extensible betting bot for horse racing, focused on the Tote API:
- Ingests events, products (pools), results, and horse data from the Tote API.
- Provides a framework for building and training predictive models (e.g., for WIN or SUPERFECTA markets).
- Includes a Flask web dashboard for viewing data, placing audit bets, and analyzing pool viability.
- Uses Google BigQuery as the primary data store for the web app and analytics.

> This project is for research/education. **Gambling involves risk.** Bet responsibly and comply with local laws.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env: TOTE_API_KEY, TOTE_GRAPHQL_URL, and BQ_* variables
# authenticate to GCP (choose one):
#  - gcloud: gcloud auth application-default login
#  - service account: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
```

### Web dashboard (BigQuery-only)

```bash
python -m sports.run web
# open http://localhost:8010
```
The dashboard reads directly from BigQuery. Ensure `BQ_PROJECT` and `BQ_DATASET` are set and you are authenticated.

Key pages:
- `/tote-superfecta` – list of SUPERFECTA products (with upcoming 60m widget)
- `/tote/calculators` – calculators using selection units or probable odds
- `/tote/viability` – breakeven/threshold calculator (S, O_min, ROI)
- `/tote/bet` – page for placing single-line audit bets
- `/tote-events` and `/event/<id>` – event lists/details
- `/audit/bets` – audit bets via Tote API (read-only in BQ mode)
- `/imports` – latest import stats (raw + structured)

### Performance options

- BigQuery Storage API: set `BQ_USE_STORAGE_API=true` to speed dataframe fetches.
- SQL result cache: the web app caches identical SELECT queries for a short TTL.
  - Configure with env vars:
    - `WEB_SQLDF_CACHE=true|false` (default true)
    - `WEB_SQLDF_CACHE_TTL=30` (seconds; default 30)
    - `WEB_SQLDF_CACHE_MAX=512` (max entries; default 512)
    - `WEB_SQLDF_MAX_ROWS=0` (soft cap on returned rows; 0 disables)
  - The cache applies only to `SELECT`/`WITH` queries. Mutating queries are blocked via `sql_df` and should use the internal BigQuery execute helper.

### Historical Tote backfill

Two options to ingest past Tote data directly into BigQuery:

- Range ingest (OPEN then CLOSED) by date:

```
python -m sports.run tote-backfill --from 2023-01-01 --to 2023-12-31 \
  --bet-types WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA --first 400
```

- Events range ingest (Event API):

```
python -m sports.run tote-events-range --from 2024-01-01 --to 2024-12-31 --first 500
```

This writes explicit Event rows (incl. status and competitors list when available). Note that finishing order is not always exposed on the Event object; dividends/subscriptions will still enrich results.

### BigQuery temp-table cleanup

Bulk upserts create temporary staging tables (prefix `_tmp_`). Clean them up periodically:

```bash
python -m sports.run bq-cleanup --older 3   # delete _tmp_ tables older than 3 days
```

Alternatively, from Python:

```python
from autobet.sports.bq import get_bq_sink
sink = get_bq_sink(); print(sink.cleanup_temp_tables(older_than_days=3))
```

### Subscriptions (optional)

Set `SUBSCRIBE_POOLS=1` and configure `TOTE_SUBSCRIPTIONS_URL` and `TOTE_API_KEY` to enable the background pool subscription (writes pool snapshots to BigQuery if your subscriber is adapted).
### Fetch schema SDL

Some partner deployments disable GraphQL introspection. You can still download the SDL via the gateway endpoint using auth:

```bash
python -m sports.run tote-graphql-sdl
```

Make sure `TOTE_GRAPHQL_URL` is set to the gateway path in `autobet/.env`:

```
TOTE_GRAPHQL_URL=https://hub.production.racing.tote.co.uk/partner/gateway/graphql
TOTE_SUBSCRIPTIONS_URL=wss://hub.production.racing.tote.co.uk/partner/connections/graphql/
```
