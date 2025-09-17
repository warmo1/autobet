# Database Schema and Data Import

## Overview

This document outlines the database schema, data import processes, and BigQuery integration for the Autobet project. The primary and only supported data store for the app is Google BigQuery.

## BigQuery Schema (active)

Structured tables and views are created on demand by the BigQuery sink (`sports/bq.py`) when you run `init_db()` or the ingestors. Key tables:

- `tote_events` – tote events with basic metadata
- `tote_products` – products (bet type, status, totals, event linkage)
- `tote_product_selections` – selections per leg (cloth/trap numbers)
- `tote_product_dividends` – latest known dividends per selection
- `tote_pool_snapshots` – optional time-series of pool totals (from subscriptions)
- `raw_tote` – raw API payloads
- `raw_tote_probable_odds` – raw probable odds payloads

Important views:

- `vw_today_gb_events`, `vw_today_gb_superfecta`, `vw_today_gb_superfecta_be`
- `vw_gb_open_superfecta_next60`, `vw_gb_open_superfecta_next60_be`
- `vw_superfecta_dividends_latest`
- `mv_sf_strengths_from_win_horse` (materialized fallback strengths), `vw_sf_strengths_from_win_horse`, `vw_superfecta_runner_strength_any`
- `vw_tote_probable_odds`, `vw_probable_odds_by_event`
- `vw_products_coverage`, `vw_products_coverage_issues`

Run:

```bash
python -c "from autobet.sports.db import init_db; init_db()"
```

<!-- Removed legacy local DB references; project is BigQuery-first. -->

## Data Imports

Data is imported from various sources using scripts located in the `sports/ingest/` directory. Each script is responsible for fetching data from a specific source and inserting it into the appropriate database tables.

### Tote Events → Horses and Runs

- `sports/ingest/tote_events.py` calls the Tote GraphQL `events` API and extracts:
  - `eventCompetitors` → upsert into `hr_horses` (`horse_id`, `name`, `country`).
    - Note: The event response does not expose a horse nationality; we store the event venue’s `country.alpha2Code` as a proxy for country.
  - Event metadata → upsert into `tote_events` (incl. status/result status and a competitors JSON snapshot for reference).
  - If present, finishing positions → upsert into `hr_horse_runs` (with `cloth_number`/`trapNumber` mapped from competitor details).

### Tote Products → Runners, Pools, Rules and Probable Odds

- `sports/ingest/tote_products.py` queries `products` and upserts:
  - `tote_products` (bet type, selling status, pool totals, event linkage)
  - `tote_product_selections` (per-leg selections with cloth/trap numbers)
  - `tote_bet_rules` (min/max/increment)
  - `tote_product_dividends` (latest known dividends)
- Probable odds are captured from `lines { nodes { legs { lineSelections { selectionId } } odds { decimal } } }` and stored as JSON batches in `raw_tote_probable_odds`. Views materialize these into tables for convenience:
  - `vw_tote_probable_odds` – latest decimal odds per (product_id, selection_id)
  - `vw_tote_probable_history` – time series of parsed odds

### Data Sources

*   **Tote (`tote_events.py`, `tote_horse.py`, `tote_products.py`, `tote_results.py`)**: Ingests a wide range of betting data from the Tote API.
*   **Weather (`weather.py`)**: Imports weather data.
*   **Kaggle (`hr_kaggle_results.py`)**: Imports historical horse racing data from Kaggle datasets.

## BigQuery Integration

For more advanced analysis and to handle larger datasets, data is periodically exported to Google BigQuery.

### Setup

The BigQuery integration is configured via the following environment variables:

*   `BQ_WRITE_ENABLED`: Set to `true` to enable writing to BigQuery.
*   `BQ_PROJECT`: The Google Cloud project ID.
*   `BQ_DATASET`: The BigQuery dataset to use.
*   `BQ_LOCATION`: The location of the BigQuery dataset (e.g., `EU`).

The core logic for the BigQuery integration is in `sports/bq.py`. The `BigQuerySink` class handles the creation of tables and the upserting of data.

### Synced Tables

The following tables are synced to BigQuery:

*   `tote_products`
*   `tote_product_dividends`
*   `tote_events`
*   `tote_event_competitors_log`
*   `raw_tote`
*   `tote_product_selections`
*   `tote_pool_snapshots`
*   `hr_horse_runs`
*   `hr_horses`
*   `race_conditions`
*   `models`
*   `predictions`
*   `features_runner_event`
*   `odds_live`

### BigQuery Views

A number of views are created in BigQuery to facilitate analysis. These views are defined in `sports/bq.py` in the `ensure_views` method.

*   **`vw_horse_runs_by_name`**: Joins horse runs with horse names and event context.
*   **`vw_today_gb_events`**: Shows today's Great Britain events with a competitor count.
*   **`vw_today_gb_superfecta`**: Displays today's Great Britain Superfecta products with pool totals and competitor counts.
*   **`vw_today_gb_superfecta_latest`**: Provides the latest pool snapshot for each Superfecta product for today's GB races.
*   **`vw_today_gb_superfecta_be`**: Calculates breakeven metrics for today's GB Superfecta pools.
*   **`vw_gb_open_superfecta_next60`**: Lists open GB Superfecta products starting in the next 60 minutes.
*   **`vw_gb_open_superfecta_next60_be`**: Calculates breakeven metrics for open GB Superfecta products starting in the next 60 minutes.
*   **`vw_superfecta_products`**: A convenient filter of the `tote_products` table for Superfecta bets.
*   **`vw_superfecta_dividends_latest`**: Shows the latest dividend for each selection for each Superfecta product.
*   **`vw_runner_features`**: Joins runner features with horse names.
*   **`vw_superfecta_runner_training_features`**: Extends `vw_superfecta_training` with per-runner feature vectors for model fitting.
*   **`vw_superfecta_runner_live_features`**: Provides the latest runner feature snapshot paired with Superfecta products for scoring upcoming races.
*   **`vw_superfecta_training`**: Combines product and event context with runner results for model training.
