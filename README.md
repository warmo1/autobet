# Horse Racing Betting Bot (Tote)

A pragmatic, extensible betting bot for horse racing, focused on the Tote API:
- Ingests events, products (pools), results, and horse data from the Tote API.
- Ingests historical horse racing results from Kaggle CSVs.
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

### Web dashboard (BigQuery-only)
```bash
python -c "from autobet.sports.webapp import app; app.run(host='0.0.0.0', port=8010)"
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

### Historical Tote backfill

Two options to ingest past Tote data directly into BigQuery:

- Range ingest (OPEN then CLOSED) by date:

```
python autobet/scripts/ingest_tote_range.py --from 2023-01-01 --to 2023-12-31 \
  --bet-types WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA --first 400
```

- CLOSED‑only backfill loop:

```
python -m autobet.sports.backfill --start 2023-01-01 --end 2023-12-31 --types SUPERFECTA,TRIFECTA
```

These populate:
- `tote_products`, `tote_product_selections`, `tote_events`, and `tote_product_dividends`.
After backfill, use `/tote-pools` and `/event/<id>` to browse by date.

- Events range ingest (Event API):

```
python autobet/scripts/ingest_events_range.py --from 2024-01-01 --to 2024-12-31 --first 500 --sleep 1.0
```

This writes explicit Event rows (incl. status and competitors list when available). Note that finishing order is not always exposed on the Event object; dividends/subscriptions will still enrich results.

### BigQuery temp-table cleanup

Temporary staging tables named `_tmp_*` are created during upserts and removed after merges. If jobs fail mid-flight, leftover tables can accumulate.
Temporary staging tables named `_tmp_*` are created during bulk upserts and are normally removed after a successful `MERGE` operation. If jobs fail mid-flight, these temporary tables can accumulate.

There are several ways to clean them up:

**1. Scheduled Cleanup (Recommended)**

This is deployed via Terraform as part of the `make deploy` process. A Cloud Scheduler job (`bq-tmp-cleanup`) periodically publishes a message like `{ "task": "cleanup_bq_temps", "older_than_days": 1 }` to the `ingest-jobs` Pub/Sub topic. The `ingestion-fetcher` Cloud Run service receives this message and deletes any temporary tables older than the specified number of days.

**2. Manual Script**

For one-off cleanups, you can run a dedicated script:
```bash
python autobet/scripts/bq_cleanup.py --older 3   # delete _tmp_ tables older than 3 days
```

- One-off cleanup:
**3. Manual Python Snippet**

You can also trigger the cleanup directly from a Python shell:
```
python - <<'PY'
from autobet.sports.bq import get_bq_sink
sink = get_bq_sink(); print('deleted:', sink.cleanup_temp_tables(older_than_days=1))
PY
python -c "from autobet.sports.bq import get_bq_sink; sink = get_bq_sink(); print('deleted:', sink.cleanup_temp_tables(older_than_days=1))"
```

- Scheduled cleanup (deployed via Terraform):
  Cloud Scheduler job `bq-tmp-cleanup` publishes `{ "task": "cleanup_bq_temps", "older_than_days": 1 }` to the ingest topic. The Cloud Run fetcher handles it and logs how many tables were deleted.

### 7) Live (optional, Betfair - disabled by default)
```bash
python -m sports.run live --symbol 'Home vs Away (Match Odds)' --side back --odds 2.2 --stake 5 --confirm BET
```

## Folder layout
- `sports/bq.py` – BigQuery sink, upsert helpers, and view creation
- `sports/db.py` – Thin wrapper returning a configured BigQuerySink
- `sports/ingest/` – Tote and historical data ingestors
- `sports/webapp.py` – Flask dashboard (reads from BigQuery)
- `sports/gcp_ingest_service.py` – Cloud Run handler for Pub/Sub → GCS (+ optional BQ raw)
- `scripts/` – helper scripts (publish jobs, cleanup)

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

### Tote → GCS JSON → BigQuery (raw)

You can now fetch Tote API JSON to Cloud Storage and append the payload to BigQuery `raw_tote` in one step via Pub/Sub:

1) Prepare a GraphQL file with your query (e.g., `sql/tote_products.graphql`).

2) Publish a job (London region project example):

```bash
python scripts/publish_tote_graphql_job.py \
  --project autobet-470818 \
  --topic ingest-jobs \
  --bucket autobet-470818-data \
  --name raw/tote/products_2025-09-05.json \
  --query-file path/to/query.graphql \
  --vars '{"date":"2025-09-05","first":100,"status":"OPEN","betTypes":["WIN","PLACE","EXACTA","TRIFECTA","SUPERFECTA"]}'
```

The Cloud Run service will:
- Execute the Tote GraphQL using `TOTE_API_KEY`/`TOTE_GRAPHQL_URL`.
- Write the JSON response to `gs://<bucket>/<name>`.
- If `BQ_*` envs are set and the message includes `bq.table=raw_tote` (default in the script), it appends the payload to BigQuery `raw_tote`.

Notes:
- The included query requests BettingProduct fields as per `docs/tote_queries.graphql`.
- You can materialize structured tables directly from the webapp/ingestors (below), or transform `raw_tote` via scheduled jobs/BigQuery SQL.

Troubleshooting GraphQL endpoints:
- Ensure you are editing the inner env file: `autobet/.env` (this repo contains an inner `autobet/` folder).
- Verify values: `python - <<'PY'\nfrom autobet.sports.config import cfg\nprint(cfg.tote_graphql_url)\nprint(bool(cfg.tote_api_key))\nPY`
- Some partners disable introspection and/or expose a reduced Query. If `products`/`discover` fail:
  - Ensure `TOTE_GRAPHQL_URL` points to the gateway endpoint: `https://hub.production.racing.tote.co.uk/partner/gateway/graphql`
  - Keep subscriptions at: `wss://hub.production.racing.tote.co.uk/partner/connections/graphql/`
  - Confirm your API key has access to the product subgraph; otherwise `products` will not be present on `Query`.
  - Run the probe: `python autobet/scripts/probe_tote_graphql.py` — it prints the effective endpoint and fetches SDL via `?sdl` if introspection is disabled.

### Tote probable odds

Fetch probable odds payloads via Pub/Sub and store in GCS + BigQuery raw:

```bash
python autobet/scripts/publish_tote_probable_job.py \
  --project autobet-470818 --topic ingest-jobs \
  --bucket autobet-470818-data \
  --path /v1/products/<WIN_PRODUCT_ID>/probable-odds \
  --name raw/tote/probable/<WIN_PRODUCT_ID>.json

# Or publish for all today's open WIN products:
python autobet/scripts/publish_probable_for_today.py \
  --project autobet-470818 --topic ingest-jobs --bucket autobet-470818-data \
  --limit 50 --bq-project autobet-470818 --bq-dataset autobet
```

Parsed view (created by `init_db`): `vw_tote_probable_odds` exposes product_id, cloth_number, selection_id, decimal_odds, ts_ms.

### Direct Tote → BigQuery (structured)

To load products (with events and selections) for a specific day directly into BigQuery from your laptop, use the `tote-products` command:

```bash
python -m sports.run tote-products --date 2024-09-05 --status OPEN \
  --types WIN,PLACE,EXACTA,TRIFECTA,SUPERFECTA
```

This populates:
- `tote_products` (product totals, status, event linkage)
- `tote_events` (basic metadata inferred from leg event)
- `tote_product_selections` (leg/selection map including cloth/trap numbers)
- `tote_product_dividends` (latest known dividends per selection)


### A Note on Code Modernization

While reviewing the code, I noticed that many other commands in `sports/run.py` still use the legacy SQLite-first pattern. For better consistency and to fully embrace the BigQuery-native strategy outlined in your documentation, these could also be updated over time. This would be a great next step to modernize the project's command-line tools.

Let me know if you'd like me to proceed with refactoring another command, or if you have any other questions!

<!--
[PROMPT_SUGGESTION]Can you refactor the `tote-products` command to also write directly to BigQuery?[/PROMPT_SUGGESTION]
[PROMPT_SUGGESTION]Create a new command to backfill a range of dates for products, results, and weather, writing directly to BigQuery.[/PROMPT_SUGGESTION]
-->

### A Note on Code Modernization

While reviewing the code, I noticed that many other commands in `sports/run.py` still use the legacy SQLite-first pattern. For better consistency and to fully embrace the BigQuery-native strategy outlined in your documentation, these could also be updated over time. This would be a great next step to modernize the project's command-line tools.

Let me know if you'd like me to proceed with refactoring another command, or if you have any other questions!

<!--
[PROMPT_SUGGESTION]Can you refactor the `tote-products` command to also write directly to BigQuery?[/PROMPT_SUGGESTION]
[PROMPT_SUGGESTION]Create a new command to backfill a range of dates for products, results, and weather, writing directly to BigQuery.[/PROMPT_SUGGESTION]
-->
**Range Ingest (Events)**

To ingest a year's worth of historical events directly into BigQuery:
```bash
python -m sports.run tote-events-range --from 2023-01-01 --to 2023-12-31
```

### BigQuery temp-table cleanup

Bulk upserts create temporary staging tables (prefix `_tmp_`). Clean them up periodically:

```bash
python autobet/scripts/bq_cleanup.py --older 3   # delete _tmp_ tables older than 3 days
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
python autobet/scripts/fetch_tote_schema.py --out autobet/docs/tote_schema.graphqls
```

Make sure `TOTE_GRAPHQL_URL` is set to the gateway path in `autobet/.env`:

```
TOTE_GRAPHQL_URL=https://hub.production.racing.tote.co.uk/partner/gateway/graphql
TOTE_SUBSCRIPTIONS_URL=wss://hub.production.racing.tote.co.uk/partner/connections/graphql/
```
