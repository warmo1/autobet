from __future__ import annotations

import os
from typing import Iterable, Mapping, Any

from .config import cfg


class BigQuerySink:
    def __init__(self, project: str, dataset: str, location: str = "EU"):
        self.project = project
        self.dataset = dataset
        self.location = location
        self._client = None

    @property
    def enabled(self) -> bool:
        return bool(self.project and self.dataset)

    def _client_obj(self):
        if self._client is None:
            try:
                from google.cloud import bigquery  # type: ignore
            except Exception as e:
                raise RuntimeError("google-cloud-bigquery not installed") from e
            self._bq = bigquery
            self._client = bigquery.Client(project=self.project, location=self.location)
        return self._client

    def query(self, sql: str, **kwargs):
        """Run a query and return the results.

        If no QueryJobConfig is provided, set a default dataset so
        unqualified table names resolve to `<project>.<dataset>`.
        """
        client = self._client_obj()
        job_config = kwargs.pop("job_config", None)
        if job_config is None:
            job_config = self._bq.QueryJobConfig(
                default_dataset=f"{self.project}.{self.dataset}"
            )
        elif getattr(job_config, "default_dataset", None) in (None, ""):
            # Preserve provided config but add default dataset for convenience
            job_config.default_dataset = f"{self.project}.{self.dataset}"
        return client.query(sql, job_config=job_config, **kwargs).result()

    def _table_exists(self, table: str) -> bool:
        client = self._client_obj()
        try:
            client.get_table(f"{self.project}.{self.dataset}.{table}")
            return True
        except Exception:
            return False

    def _ensure_columns(self, table: str, schema_hint: dict[str, str]):
        """Ensure destination table has given columns with types. Adds missing columns.

        schema_hint: mapping of column name -> BigQuery type (e.g., STRING, INT64, FLOAT64).
        """
        client = self._client_obj(); self._ensure_dataset(); bq = self._bq
        dest_fq = f"{self.project}.{self.dataset}.{table}"
        try:
            tbl = client.get_table(dest_fq)
        except Exception:
            # Will be created by _merge's CREATE TABLE IF NOT EXISTS
            return
        existing = {f.name.lower(): f for f in tbl.schema}
        to_add = [(k, v) for k, v in (schema_hint or {}).items() if k.lower() not in existing]
        if not to_add:
            return
        # Build ALTER TABLE statement(s)
        alters = []
        for name, typ in to_add:
            safe_name = name.replace('`','')
            alters.append(f"ADD COLUMN IF NOT EXISTS `{safe_name}` {typ}")
        sql = f"ALTER TABLE `{dest_fq}`\n" + ",\n".join(alters)
        job = client.query(sql)
        job.result()

    # --- generic helpers ---
    def _ensure_dataset(self):
        bq = self._bq; client = self._client_obj()
        ds_ref = f"{self.project}.{self.dataset}"
        try:
            client.get_dataset(ds_ref)
        except Exception:
            ds = bq.Dataset(ds_ref)
            ds.location = self.location
            client.create_dataset(ds, exists_ok=True)

    def _load_to_temp(self, table: str, rows: Iterable[Mapping[str, Any]], schema_hint: dict[str, str] | None = None):
        client = self._client_obj(); self._ensure_dataset(); bq = self._bq
        import uuid
        temp_table = f"{self.project}.{self.dataset}._tmp_{table}_{uuid.uuid4().hex[:8]}"
        job_config = bq.LoadJobConfig(write_disposition=bq.WriteDisposition.WRITE_TRUNCATE)
        # Infer schema from first row
        rows = list(rows)
        if not rows:
            return None
        # Build types per column considering all rows and optional hints.
        # Include any hinted columns even if not present in the JSON rows so we
        # can reference them in downstream MERGEs (NULL values are fine).
        schema = []
        key_set = set(rows[0].keys())
        if schema_hint:
            key_set |= set(schema_hint.keys())
        for k in sorted(key_set):
            ftype = None
            if schema_hint and k in schema_hint:
                ftype = schema_hint[k]
            else:
                has_float = False; has_int = False; has_str = False
                for r in rows:
                    v = r.get(k)
                    if v is None:
                        continue
                    if isinstance(v, bool):
                        has_int = True
                    elif isinstance(v, int):
                        has_int = True
                    elif isinstance(v, float):
                        has_float = True
                    else:
                        has_str = True
                if has_float or (has_int and not has_str):
                    ftype = "FLOAT64" if has_float else "INT64"
                elif has_int and has_str:
                    # Mixed types, prefer STRING to be safe
                    ftype = "STRING"
                else:
                    # All None or strings
                    ftype = "STRING"
            schema.append(bq.SchemaField(k, ftype))
        job_config.schema = schema
        tbl = bq.Table(temp_table, schema=schema)
        client.create_table(tbl, exists_ok=True)
        job = client.load_table_from_json(rows, temp_table, job_config=job_config)
        job.result()
        return temp_table

    def _merge(self, dest: str, temp: str, key_expr: str, update_set: str):
        client = self._client_obj(); self._ensure_dataset()
        dest_fq = f"{self.project}.{self.dataset}.{dest}"
        # Ensure destination table exists first with temp schema (no rows)
        # Avoid issuing DDL on every upsert to reduce BigQuery table update rate limits.
        try:
            client.get_table(dest_fq)
        except Exception:
            sql_ctas = f"CREATE TABLE IF NOT EXISTS `{dest_fq}` AS SELECT * FROM `{temp}` WHERE 1=0;"
            client.query(sql_ctas).result()

        # Fetch schemas to build robust INSERT column list
        dest_tbl = client.get_table(dest_fq)
        temp_tbl = client.get_table(temp)
        dest_cols = [f.name for f in dest_tbl.schema]
        temp_cols = {f.name for f in temp_tbl.schema}
        insert_cols = ", ".join(f"`{c}`" for c in dest_cols)
        insert_vals_parts = []
        for c in dest_cols:
            if c in temp_cols:
                insert_vals_parts.append(f"S.`{c}`")
            else:
                insert_vals_parts.append("NULL")
        insert_vals = ", ".join(insert_vals_parts)

        sql_merge = f"""
        MERGE `{dest_fq}` T
        USING `{temp}` S
        ON {key_expr}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        client.query(sql_merge).result()
        # Best-effort cleanup of staging table
        try:
            client.delete_table(temp, not_found_ok=True)
        except Exception:
            # Ignore cleanup errors
            pass

    # --- table-specific upserts ---
    def upsert_tote_products(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp(
            "tote_products",
            rows,
            schema_hint={
                "total_gross": "FLOAT64",
                "total_net": "FLOAT64",
                "rollover": "FLOAT64",
                "deduction_rate": "FLOAT64",
            },
        )
        if not temp:
            return
        self._merge(
            "tote_products",
            temp,
            key_expr="T.product_id = S.product_id",
            update_set=",".join([
                "status=S.status",
                "currency=S.currency",
                "total_gross=S.total_gross",
                "total_net=S.total_net",
                "rollover=S.rollover",
                "deduction_rate=S.deduction_rate",
                "event_id=S.event_id",
                "event_name=S.event_name",
                "venue=S.venue",
                "start_iso=S.start_iso",
                "source=S.source",
            ]),
        )

    def upsert_tote_product_dividends(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_product_dividends", rows, schema_hint={
            "dividend": "FLOAT64",
        })
        if not temp:
            return
        self._merge(
            "tote_product_dividends",
            temp,
            key_expr="T.product_id=S.product_id AND T.selection=S.selection AND T.ts=S.ts",
            update_set="dividend=S.dividend",
        )

    def upsert_tote_events(self, rows: Iterable[Mapping[str, Any]]):
        # Ensure optional columns exist on the staging side so MERGE can reference them safely
        temp = self._load_to_temp(
            "tote_events",
            rows,
            schema_hint={
                "result_status": "STRING",
                "comp": "STRING",
                "home": "STRING",
                "away": "STRING",
                "competitors_json": "STRING",
            },
        )
        if not temp:
            return
        # Backfill new columns if dest exists without them
        self._ensure_columns("tote_events", {"result_status": "STRING", "comp": "STRING", "home": "STRING", "away": "STRING", "competitors_json": "STRING"})
        self._merge(
            "tote_events",
            temp,
            key_expr="T.event_id=S.event_id",
            update_set=",".join([
                "name=S.name",
                "sport=S.sport",
                "venue=S.venue",
                "country=S.country",
                "start_iso=S.start_iso",
                "status=S.status",
                "result_status=S.result_status",
                "comp=S.comp",
                "home=S.home",
                "away=S.away",
                "competitors_json=S.competitors_json",
                "source=S.source",
            ]),
        )

    def upsert_tote_event_competitors_log(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_event_competitors_log", rows)
        if not temp:
            return
        self._merge(
            "tote_event_competitors_log",
            temp,
            key_expr="T.event_id=S.event_id AND T.ts_ms=S.ts_ms",
            update_set="competitors_json=S.competitors_json",
        )

    def upsert_raw_tote(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("raw_tote", rows)
        if not temp:
            return
        self._merge(
            "raw_tote",
            temp,
            key_expr="T.raw_id=S.raw_id",
            update_set=",".join([
                "endpoint=S.endpoint",
                "entity_id=S.entity_id",
                "sport=S.sport",
                "fetched_ts=S.fetched_ts",
                "payload=S.payload",
            ]),
        )

    def upsert_tote_product_selections(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_product_selections", rows, schema_hint={
            "leg_index": "INT64",
            "number": "INT64",
        })
        if not temp:
            return
        self._ensure_columns("tote_product_selections", {"product_leg_id": "STRING"})
        self._merge(
            "tote_product_selections",
            temp,
            key_expr="T.product_id=S.product_id AND T.leg_index=S.leg_index AND T.selection_id=S.selection_id",
            update_set=",".join([
                "product_leg_id=S.product_leg_id",
                "competitor=S.competitor",
                "number=S.number",
                "leg_event_id=S.leg_event_id",
                "leg_event_name=S.leg_event_name",
                "leg_venue=S.leg_venue",
                "leg_start_iso=S.leg_start_iso",
            ]),
        )

    def upsert_tote_bet_rules(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_bet_rules", rows, schema_hint={
            "min_bet": "FLOAT64",
            "max_bet": "FLOAT64",
            "min_line": "FLOAT64",
            "max_line": "FLOAT64",
            "line_increment": "FLOAT64",
        })
        if not temp:
            return
        self._merge(
            "tote_bet_rules",
            temp,
            key_expr="T.product_id=S.product_id",
            update_set=",".join([
                "bet_type=S.bet_type",
                "currency=S.currency",
                "min_bet=S.min_bet",
                "max_bet=S.max_bet",
                "min_line=S.min_line",
                "max_line=S.max_line",
                "line_increment=S.line_increment",
            ]),
        )

    def upsert_tote_pool_snapshots(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_pool_snapshots", rows, schema_hint={
            "ts_ms": "INT64",
            "total_gross": "FLOAT64",
            "total_net": "FLOAT64",
        })
        if not temp:
            return
        self._merge(
            "tote_pool_snapshots",
            temp,
            key_expr="T.product_id=S.product_id AND T.ts_ms=S.ts_ms",
            update_set=",".join([
                "event_id=S.event_id",
                "bet_type=S.bet_type",
                "status=S.status",
                "currency=S.currency",
                "start_iso=S.start_iso",
                "total_gross=S.total_gross",
                "total_net=S.total_net",
            ]),
        )

    def upsert_tote_dividend_updates(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_dividend_updates", rows, schema_hint={
            "ts_ms": "INT64",
            "dividend_type": "INT64",
            "dividend_status": "INT64",
            "dividend_amount": "FLOAT64",
            "finishing_position": "INT64",
        })
        if not temp:
            return
        self._merge(
            "tote_dividend_updates",
            temp,
            key_expr="T.product_id=S.product_id AND T.ts_ms=S.ts_ms AND T.leg_id=S.leg_id AND T.selection_id=S.selection_id",
            update_set="dividend_name=S.dividend_name, dividend_type=S.dividend_type, dividend_status=S.dividend_status, dividend_amount=S.dividend_amount, dividend_currency=S.dividend_currency, finishing_position=S.finishing_position",
        )

    def upsert_tote_event_results_log(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_event_results_log", rows, schema_hint={
            "ts_ms": "INT64",
            "finishing_position": "INT64",
        })
        if not temp:
            return
        self._merge(
            "tote_event_results_log",
            temp,
            key_expr="T.event_id=S.event_id AND T.ts_ms=S.ts_ms AND T.competitor_id=S.competitor_id",
            update_set="finishing_position=S.finishing_position, status=S.status",
        )

    def upsert_tote_event_status_log(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_event_status_log", rows, schema_hint={"ts_ms": "INT64"})
        if not temp:
            return
        self._merge(
            "tote_event_status_log",
            temp,
            key_expr="T.event_id=S.event_id AND T.ts_ms=S.ts_ms",
            update_set="status=S.status",
        )

    def upsert_tote_product_status_log(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_product_status_log", rows, schema_hint={"ts_ms": "INT64"})
        if not temp:
            return
        self._merge(
            "tote_product_status_log",
            temp,
            key_expr="T.product_id=S.product_id AND T.ts_ms=S.ts_ms",
            update_set="status=S.status",
        )

    def upsert_tote_selection_status_log(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_selection_status_log", rows, schema_hint={"ts_ms": "INT64"})
        if not temp:
            return
        self._merge(
            "tote_selection_status_log",
            temp,
            key_expr="T.product_id=S.product_id AND T.selection_id=S.selection_id AND T.ts_ms=S.ts_ms",
            update_set="status=S.status",
        )

    def upsert_tote_event_updated_log(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_event_updated_log", rows, schema_hint={"ts_ms": "INT64"})
        if not temp:
            return
        self._merge(
            "tote_event_updated_log",
            temp,
            key_expr="T.event_id=S.event_id AND T.ts_ms=S.ts_ms",
            update_set="payload=S.payload",
        )

    def upsert_tote_lines_changed_log(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_lines_changed_log", rows, schema_hint={"ts_ms": "INT64"})
        if not temp:
            return
        self._merge(
            "tote_lines_changed_log",
            temp,
            key_expr="T.product_id=S.product_id AND T.ts_ms=S.ts_ms",
            update_set="payload=S.payload",
        )

    def upsert_tote_competitor_status_log(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("tote_competitor_status_log", rows, schema_hint={"ts_ms": "INT64"})
        if not temp:
            return
        self._merge(
            "tote_competitor_status_log",
            temp,
            key_expr="T.event_id=S.event_id AND T.competitor_id=S.competitor_id AND T.ts_ms=S.ts_ms",
            update_set="status=S.status",
        )

    def upsert_hr_horse_runs(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("hr_horse_runs", rows, schema_hint={
            "finish_pos": "INT64",
            "cloth_number": "INT64",
        })
        if not temp:
            return
        self._merge(
            "hr_horse_runs",
            temp,
            key_expr="T.horse_id=S.horse_id AND T.event_id=S.event_id",
            update_set=",".join([
                "race_id=S.race_id",
                "finish_pos=S.finish_pos",
                "status=S.status",
                "cloth_number=S.cloth_number",
                "jockey=S.jockey",
                "trainer=S.trainer",
                "recorded_ts=S.recorded_ts",
            ]),
        )

    def upsert_hr_horses(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("hr_horses", rows)
        if not temp:
            return
        self._merge(
            "hr_horses",
            temp,
            key_expr="T.horse_id=S.horse_id",
            update_set=",".join([
                "name=S.name",
                "country=S.country",
            ]),
        )

    def upsert_race_conditions(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("race_conditions", rows, schema_hint={
            "weather_temp_c": "FLOAT64",
            "weather_wind_kph": "FLOAT64",
            "weather_precip_mm": "FLOAT64",
        })
        if not temp:
            return
        self._merge(
            "race_conditions",
            temp,
            key_expr="T.event_id=S.event_id",
            update_set=",".join([
                "venue=S.venue",
                "country=S.country",
                "start_iso=S.start_iso",
                "going=S.going",
                "surface=S.surface",
                "distance=S.distance",
                "weather_desc=S.weather_desc",
                "weather_temp_c=S.weather_temp_c",
                "weather_wind_kph=S.weather_wind_kph",
                "weather_precip_mm=S.weather_precip_mm",
                "source=S.source",
                "fetched_ts=S.fetched_ts",
            ]),
        )

    def upsert_models(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("models", rows)
        if not temp:
            return
        self._merge(
            "models",
            temp,
            key_expr="T.model_id=S.model_id",
            update_set=",".join([
                "created_ts=S.created_ts",
                "market=S.market",
                "algo=S.algo",
                "params_json=S.params_json",
                "metrics_json=S.metrics_json",
                "path=S.path",
            ]),
        )

    def upsert_predictions(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("predictions", rows, schema_hint={
            "ts_ms": "INT64",
            "proba": "FLOAT64",
            "rank": "INT64",
        })
        if not temp:
            return
        self._merge(
            "predictions",
            temp,
            key_expr="T.model_id=S.model_id AND T.ts_ms=S.ts_ms AND T.event_id=S.event_id AND T.horse_id=S.horse_id",
            update_set=",".join([
                "market=S.market",
                "proba=S.proba",
                "rank=S.rank",
            ]),
        )

    def upsert_features_runner_event(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp("features_runner_event", rows, schema_hint={
            "cloth_number": "INT64",
            "total_net": "FLOAT64",
            "weather_temp_c": "FLOAT64",
            "weather_wind_kph": "FLOAT64",
            "weather_precip_mm": "FLOAT64",
            "weight_kg": "FLOAT64",
            "weight_lbs": "FLOAT64",
            "recent_runs": "INT64",
            "avg_finish": "FLOAT64",
            "wins_last5": "INT64",
            "places_last5": "INT64",
            "days_since_last_run": "INT64",
        })
        if not temp:
            return
        self._merge(
            "features_runner_event",
            temp,
            key_expr="T.event_id=S.event_id AND T.horse_id=S.horse_id",
            update_set=",".join([
                "event_date=S.event_date",
                "cloth_number=S.cloth_number",
                "total_net=S.total_net",
                "weather_temp_c=S.weather_temp_c",
                "weather_wind_kph=S.weather_wind_kph",
                "weather_precip_mm=S.weather_precip_mm",
                "going=S.going",
                "recent_runs=S.recent_runs",
                "avg_finish=S.avg_finish",
                "wins_last5=S.wins_last5",
                "places_last5=S.places_last5",
                "days_since_last_run=S.days_since_last_run",
            ]),
        )

    def upsert_raw_tote_probable_odds(self, rows: Iterable[Mapping[str, Any]]):
        """Append/merge raw probable odds payloads.

        Expected row keys: raw_id (str), fetched_ts (INT64 or STRING ISO), payload (STRING)
        """
        temp = self._load_to_temp(
            "raw_tote_probable_odds",
            rows,
            schema_hint={
                "fetched_ts": "INT64",
            },
        )
        if not temp:
            return
        self._merge(
            "raw_tote_probable_odds",
            temp,
            key_expr="T.raw_id=S.raw_id",
            update_set=",".join([
                "fetched_ts=S.fetched_ts",
                "payload=S.payload",
            ]),
        )

    def upsert_ingest_job_runs(self, rows: Iterable[Mapping[str, Any]]):
        """Insert/merge job run records for status dashboard.

        Expected keys per row:
          - job_id STRING (unique id)
          - component STRING ('ingest'|'orchestrator'|'scheduler')
          - task STRING
          - status STRING ('OK'|'ERROR')
          - started_ts INT64 (ms)
          - ended_ts INT64 (ms)
          - duration_ms INT64
          - payload_json STRING (optional)
          - error STRING (optional)
          - metrics_json STRING (optional)
        """
        temp = self._load_to_temp(
            "ingest_job_runs",
            rows,
            schema_hint={
                "started_ts": "INT64",
                "ended_ts": "INT64",
                "duration_ms": "INT64",
            },
        )
        if not temp:
            return
        # Ensure destination has all expected columns if it already exists
        self._ensure_columns(
            "ingest_job_runs",
            {
                "job_id": "STRING",
                "component": "STRING",
                "task": "STRING",
                "status": "STRING",
                "started_ts": "INT64",
                "ended_ts": "INT64",
                "duration_ms": "INT64",
                "payload_json": "STRING",
                "error": "STRING",
                "metrics_json": "STRING",
            },
        )
        self._merge(
            "ingest_job_runs",
            temp,
            key_expr="T.job_id=S.job_id",
            update_set=",".join([
                "component=S.component",
                "task=S.task",
                "status=S.status",
                "started_ts=S.started_ts",
                "ended_ts=S.ended_ts",
                "duration_ms=S.duration_ms",
                "payload_json=S.payload_json",
                "error=S.error",
                "metrics_json=S.metrics_json",
            ]),
        )

    def ensure_views(self):
        """Create views required by the web app if missing (idempotent)."""
        client = self._client_obj(); self._ensure_dataset()
        ds = f"{self.project}.{self.dataset}"
        # Ensure base tables required by views and app exist
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{ds}.tote_pool_snapshots`(
          product_id STRING,
          event_id STRING,
          bet_type STRING,
          status STRING,
          currency STRING,
          start_iso STRING,
          ts_ms INT64,
          total_gross FLOAT64,
          total_net FLOAT64
        );
        CREATE TABLE IF NOT EXISTS `{ds}.raw_tote_probable_odds`(
          raw_id STRING,
          fetched_ts INT64,
          payload STRING
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_event_results_log`(
          event_id STRING,
          ts_ms INT64,
          competitor_id STRING,
          finishing_position INT64,
          status STRING
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_event_status_log`(
          event_id STRING,
          ts_ms INT64,
          status STRING
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_product_status_log`(
          product_id STRING,
          ts_ms INT64,
          status STRING
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_selection_status_log`(
          product_id STRING,
          selection_id STRING,
          ts_ms INT64,
          status STRING
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_product_dividends`(
          product_id STRING,
          selection STRING,
          dividend FLOAT64,
          ts STRING
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_bet_rules`(
          product_id STRING,
          bet_type STRING,
          currency STRING,
          min_bet FLOAT64,
          max_bet FLOAT64,
          min_line FLOAT64,
          max_line FLOAT64,
          line_increment FLOAT64
        );
        CREATE TABLE IF NOT EXISTS `{ds}.ingest_job_runs`(
          job_id STRING,
          component STRING,
          task STRING,
          status STRING,
          started_ts INT64,
          ended_ts INT64,
          duration_ms INT64,
          payload_json STRING,
          error STRING,
          metrics_json STRING
        );
        """
        job = client.query(sql); job.result()
        # vw_horse_runs_by_name
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_horse_runs_by_name` AS
        WITH ep AS (
          SELECT event_id,
                 ANY_VALUE(event_name) AS event_name,
                 ANY_VALUE(venue) AS venue,
                 ANY_VALUE(start_iso) AS start_iso
          FROM `{ds}.tote_products`
          GROUP BY event_id
        )
        SELECT
          h.name AS horse_name,
          r.horse_id AS horse_id,
          r.event_id AS event_id,
          DATE(COALESCE(SUBSTR(te.start_iso,1,10), SUBSTR(ep.start_iso,1,10))) AS event_date,
          COALESCE(te.venue, ep.venue) AS venue,
          te.country AS country,
          ep.event_name AS race_name,
          r.cloth_number AS cloth_number,
          r.finish_pos AS finish_pos,
          r.status AS status
        FROM `{ds}.hr_horse_runs` r
        JOIN `{ds}.hr_horses` h ON h.horse_id = r.horse_id
        LEFT JOIN `{ds}.tote_events` te ON te.event_id = r.event_id
        LEFT JOIN ep ON ep.event_id = r.event_id;
        """
        job = client.query(sql)
        job.result()

        # Viability calculator table functions (simple + grid)
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_superfecta_viability_simple`(
          num_runners INT64,
          pool_gross_other NUMERIC,
          lines_covered INT64,
          stake_per_line NUMERIC,
          take_rate FLOAT64,
          net_rollover NUMERIC,
          include_self_in_pool BOOL,
          dividend_multiplier FLOAT64,
          f_share_override FLOAT64
        )
        RETURNS TABLE<
          total_lines INT64,
          lines_covered INT64,
          coverage_frac FLOAT64,
          stake_total NUMERIC,
          net_pool_if_bet NUMERIC,
          f_share_used FLOAT64,
          expected_return NUMERIC,
          expected_profit NUMERIC,
          is_positive_ev BOOL,
          cover_all_stake NUMERIC,
          cover_all_expected_profit NUMERIC,
          cover_all_is_positive BOOL,
          cover_all_o_min_break_even NUMERIC,
          coverage_frac_min_positive FLOAT64
        > AS (
          WITH base AS (
            SELECT
              CAST(num_runners AS INT64) AS N,
              GREATEST(num_runners * (num_runners-1) * (num_runners-2) * (num_runners-3), 0) AS C,
              CAST(pool_gross_other AS NUMERIC) AS O,
              CAST(lines_covered AS INT64) AS M,
              CAST(stake_per_line AS NUMERIC) AS stake_val,
              CAST(take_rate AS FLOAT64) AS t,
              CAST(net_rollover AS NUMERIC) AS R,
              include_self_in_pool AS inc_self,
              CAST(dividend_multiplier AS FLOAT64) AS mult,
              CAST(f_share_override AS FLOAT64) AS f_fix
          ),
          k AS (
            SELECT *, CASE WHEN C>0 THEN LEAST(GREATEST(M,0), C) ELSE 0 END AS M_adj FROM base
          ),
          cur AS (
            SELECT *, SAFE_DIVIDE(M_adj, C) AS alpha, (M_adj * stake_val) AS S,
              (CASE WHEN inc_self THEN (M_adj * stake_val) ELSE 0 END) AS S_inc
            FROM k
          ),
          fshare AS (
            SELECT *,
              CASE WHEN C=0 OR (C*stake_val + O)=0 THEN 0.0 ELSE CAST( (C*stake_val) / (C*stake_val + O) AS FLOAT64 ) END AS f_auto,
              CAST( mult * ( (1.0 - t) * ( (O + S_inc) ) + R ) AS NUMERIC ) AS NetPool_now
            FROM cur
          ),
          used AS (
            SELECT *, CAST(COALESCE(f_fix, f_auto) AS FLOAT64) AS f_used FROM fshare
          ),
          now_metrics AS (
            SELECT *, CAST(alpha * f_used * NetPool_now AS NUMERIC) AS ExpReturn,
              CAST(alpha * f_used * NetPool_now - S AS NUMERIC) AS ExpProfit,
              (alpha * f_used * NetPool_now - S) > 0 AS is_pos
            FROM used
          ),
          cover_all AS (
            SELECT *, CAST(C * stake_val AS NUMERIC) AS S_all,
              CAST(CASE WHEN inc_self THEN (C * stake_val) ELSE 0 END AS NUMERIC) AS S_all_inc,
              CAST( mult * ( (1.0 - t) * (O + (CASE WHEN inc_self THEN (C*stake_val) ELSE 0 END)) + R ) AS NUMERIC) AS NetPool_all,
              CAST( CAST(COALESCE(f_fix, f_auto) AS FLOAT64) * CAST( mult * ( (1.0 - t) * (O + (CASE WHEN inc_self THEN (C*stake_val) ELSE 0 END)) + R ) AS NUMERIC) - (C * stake_val) AS NUMERIC) AS ExpProfit_all
            FROM now_metrics
          ),
          cover_all_viab AS (
            SELECT *, (ExpProfit_all > 0) AS cover_all_pos,
              CAST( SAFE_DIVIDE( (C*stake_val) - (CAST(f_used AS FLOAT64) * mult * ( (1.0 - t) * (S_all_inc) + R ) ), (CAST(f_used AS FLOAT64) * mult * (1.0 - t)) ) AS NUMERIC ) AS O_min_cover_all
            FROM cover_all
          ),
          alpha_min AS (
            SELECT *,
              CASE
                WHEN C=0 OR stake_val=0 OR f_used<=0 OR (1.0 - t)<=0 THEN NULL
                WHEN inc_self THEN LEAST(1.0, GREATEST(0.0,
                  SAFE_DIVIDE(
                    CAST(C*stake_val AS FLOAT64) - (f_used * mult * ( (1.0 - t) * CAST(O AS FLOAT64) + CAST(R AS FLOAT64) )),
                    (f_used * mult * (1.0 - t) * CAST(C*stake_val AS FLOAT64))
                  )
                ))
                WHEN (f_used * mult * ( (1.0 - t) * CAST(O AS FLOAT64) + CAST(R AS FLOAT64) )) > CAST(C*stake_val AS FLOAT64)
                  THEN 0.0
                ELSE NULL
              END AS alpha_min_pos
            FROM cover_all_viab
          )
          SELECT C AS total_lines, M_adj AS lines_covered, alpha AS coverage_frac, S AS stake_total,
                 NetPool_now AS net_pool_if_bet, f_used AS f_share_used, ExpReturn AS expected_return,
                 ExpProfit AS expected_profit, is_pos AS is_positive_ev,
                 (C*stake_val) AS cover_all_stake, ExpProfit_all AS cover_all_expected_profit,
                 cover_all_pos AS cover_all_is_positive, O_min_cover_all AS cover_all_o_min_break_even,
                 alpha_min_pos AS coverage_frac_min_positive
          FROM alpha_min
        );
        """
        job = client.query(sql); job.result()

        # Combination-based viability (e.g., SWINGER/QUINELLA with k=2)
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_combo_viability_simple`(
          num_runners INT64,
          comb_k INT64,
          pool_gross_other NUMERIC,
          lines_covered INT64,
          stake_per_line NUMERIC,
          take_rate FLOAT64,
          net_rollover NUMERIC,
          include_self_in_pool BOOL,
          dividend_multiplier FLOAT64,
          f_share_override FLOAT64
        )
        RETURNS TABLE<
          total_lines INT64,
          lines_covered INT64,
          coverage_frac FLOAT64,
          stake_total NUMERIC,
          net_pool_if_bet NUMERIC,
          f_share_used FLOAT64,
          expected_return NUMERIC,
          expected_profit NUMERIC,
          is_positive_ev BOOL,
          cover_all_stake NUMERIC,
          cover_all_expected_profit NUMERIC,
          cover_all_is_positive BOOL,
          cover_all_o_min_break_even NUMERIC,
          coverage_frac_min_positive FLOAT64
        > AS (
          WITH base AS (
            SELECT CAST(num_runners AS INT64) AS N,
                   CAST(comb_k AS INT64) AS K,
                   CAST(pool_gross_other AS NUMERIC) AS O,
                   CAST(lines_covered AS INT64) AS M,
                   CAST(stake_per_line AS NUMERIC) AS stake_val,
                   CAST(take_rate AS FLOAT64) AS t,
                   CAST(net_rollover AS NUMERIC) AS R,
                   include_self_in_pool AS inc_self,
                   CAST(dividend_multiplier AS FLOAT64) AS mult,
                   CAST(f_share_override AS FLOAT64) AS f_fix
          ), comb AS (
            SELECT
              N, K, O, M, stake_val, t, R, inc_self, mult, f_fix,
              CASE WHEN N>=K AND K>0 THEN CAST(ROUND(EXP(SUM(LN(CAST(N - i + 1 AS FLOAT64)) - LN(CAST(i AS FLOAT64))))) AS INT64) ELSE 0 END AS C
            FROM base, UNNEST(GENERATE_ARRAY(1, LEAST(K, GREATEST(N,0)))) AS i
            GROUP BY N, K, O, M, stake_val, t, R, inc_self, mult, f_fix
          ), clamp AS (
            SELECT *, CASE WHEN C>0 THEN LEAST(GREATEST(M,0), C) ELSE 0 END AS M_adj FROM comb
          ), cur AS (
            SELECT *, SAFE_DIVIDE(M_adj, C) AS alpha,
                   (M_adj * stake_val) AS S,
                   (CASE WHEN inc_self THEN (M_adj * stake_val) ELSE 0 END) AS S_inc
            FROM clamp
          ), fshare AS (
            SELECT *,
              CASE WHEN C=0 OR (C*stake_val + O)=0 THEN 0.0 ELSE CAST( (C*stake_val) / (C*stake_val + O) AS FLOAT64 ) END AS f_auto,
              CAST( mult * ( (1.0 - t) * ( (O + S_inc) ) + R ) AS NUMERIC ) AS NetPool_now
            FROM cur
          ), used AS (
            SELECT *, CAST(COALESCE(f_fix, f_auto) AS FLOAT64) AS f_used FROM fshare
          ), now_metrics AS (
            SELECT *, CAST(alpha * f_used * NetPool_now AS NUMERIC) AS ExpReturn,
              CAST(alpha * f_used * NetPool_now - S AS NUMERIC) AS ExpProfit,
              (alpha * f_used * NetPool_now - S) > 0 AS is_pos
            FROM used
          ), cover_all AS (
            SELECT *, CAST(C * stake_val AS NUMERIC) AS S_all,
              CAST(CASE WHEN inc_self THEN (C * stake_val) ELSE 0 END AS NUMERIC) AS S_all_inc,
              CAST( mult * ( (1.0 - t) * (O + (CASE WHEN inc_self THEN (C*stake_val) ELSE 0 END)) + R ) AS NUMERIC) AS NetPool_all,
              CAST( CAST(COALESCE(f_fix, f_auto) AS FLOAT64) * CAST( mult * ( (1.0 - t) * (O + (CASE WHEN inc_self THEN (C*stake_val) ELSE 0 END)) + R ) AS NUMERIC) - (C * stake_val) AS NUMERIC) AS ExpProfit_all
            FROM now_metrics
          ), cover_all_viab AS (
            SELECT *, (ExpProfit_all > 0) AS cover_all_pos,
              CAST( SAFE_DIVIDE( (C*stake_val) - (CAST(f_used AS FLOAT64) * mult * ( (1.0 - t) * (S_all_inc) + R ) ), (CAST(f_used AS FLOAT64) * mult * (1.0 - t)) ) AS NUMERIC ) AS O_min_cover_all
            FROM cover_all
          ), alpha_min AS (
            SELECT *,
              CASE
                WHEN C=0 OR stake_val=0 OR f_used<=0 OR (1.0 - t)<=0 THEN NULL
                WHEN inc_self THEN LEAST(1.0, GREATEST(0.0,
                  SAFE_DIVIDE(
                    CAST(C*stake_val AS FLOAT64) - (f_used * mult * ( (1.0 - t) * CAST(O AS FLOAT64) + CAST(R AS FLOAT64) )),
                    (f_used * mult * (1.0 - t) * CAST(C*stake_val AS FLOAT64))
                  )
                ))
                WHEN (f_used * mult * ( (1.0 - t) * CAST(O AS FLOAT64) + CAST(R AS FLOAT64) )) > CAST(C*stake_val AS FLOAT64)
                  THEN 0.0
                ELSE NULL
              END AS alpha_min_pos
            FROM cover_all_viab
          )
          SELECT C AS total_lines, M_adj AS lines_covered, alpha AS coverage_frac, S AS stake_total,
                 NetPool_now AS net_pool_if_bet, f_used AS f_share_used, ExpReturn AS expected_return,
                 ExpProfit AS expected_profit, is_pos AS is_positive_ev,
                 (C*stake_val) AS cover_all_stake, ExpProfit_all AS cover_all_expected_profit,
                 cover_all_pos AS cover_all_is_positive, O_min_cover_all AS cover_all_o_min_break_even,
                 alpha_min_pos AS coverage_frac_min_positive
          FROM alpha_min
        );
        """
        job = client.query(sql); job.result()

        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_multileg_viability_simple`(
          leg_lines ARRAY<INT64>,
          pool_gross_other NUMERIC,
          lines_covered INT64,
          stake_per_line NUMERIC,
          take_rate FLOAT64,
          net_rollover NUMERIC,
          include_self_in_pool BOOL,
          dividend_multiplier FLOAT64,
          f_share_override FLOAT64
        )
        RETURNS TABLE<
          total_lines INT64,
          lines_covered INT64,
          coverage_frac FLOAT64,
          stake_total NUMERIC,
          net_pool_if_bet NUMERIC,
          f_share_used FLOAT64,
          expected_return NUMERIC,
          expected_profit NUMERIC,
          is_positive_ev BOOL
        > AS (
          WITH base AS (
            SELECT CAST(pool_gross_other AS NUMERIC) AS O,
                   CAST(lines_covered AS INT64) AS M,
                   CAST(stake_per_line AS NUMERIC) AS stake_val,
                   CAST(take_rate AS FLOAT64) AS t,
                   CAST(net_rollover AS NUMERIC) AS R,
                   include_self_in_pool AS inc_self,
                   CAST(dividend_multiplier AS FLOAT64) AS mult,
                   CAST(f_share_override AS FLOAT64) AS f_fix
          ), tot AS (
            SELECT CAST(ROUND(EXP(SUM(LN(CAST(x AS FLOAT64))))) AS INT64) AS C FROM UNNEST(leg_lines) AS x
          ), cur AS (
            SELECT *,
                   (SELECT C FROM tot) AS C,
                   CASE WHEN (SELECT C FROM tot)>0 THEN LEAST(GREATEST(M,0), (SELECT C FROM tot)) ELSE 0 END AS M_adj
            FROM base
          ), k AS (
            SELECT *, SAFE_DIVIDE(M_adj, C) AS alpha,
                   (M_adj * stake_val) AS S,
                   (CASE WHEN inc_self THEN (M_adj * stake_val) ELSE 0 END) AS S_inc
            FROM cur
          ), fshare AS (
            SELECT *,
              CASE WHEN C=0 OR (C*stake_val + O)=0 THEN 0.0 ELSE CAST( (C*stake_val) / (C*stake_val + O) AS FLOAT64 ) END AS f_auto,
              CAST( mult * ( (1.0 - t) * ( (O + S_inc) ) + R ) AS NUMERIC ) AS NetPool_now
            FROM k
          )
          SELECT
            C AS total_lines,
            M_adj AS lines_covered,
            SAFE_DIVIDE(M_adj, C) AS coverage_frac,
            (M_adj * stake_val) AS stake_total,
            NetPool_now AS net_pool_if_bet,
            CAST(COALESCE(f_fix, f_auto) AS FLOAT64) AS f_share_used,
            CAST(SAFE_DIVIDE(M_adj, C) * COALESCE(f_fix, f_auto) * NetPool_now AS NUMERIC) AS expected_return,
            CAST(SAFE_DIVIDE(M_adj, C) * COALESCE(f_fix, f_auto) * NetPool_now - (M_adj * stake_val) AS NUMERIC) AS expected_profit,
            (SAFE_DIVIDE(M_adj, C) * COALESCE(f_fix, f_auto) * NetPool_now - (M_adj * stake_val)) > 0 AS is_positive_ev
          FROM fshare
        );
        """
        job = client.query(sql); job.result()

        # Multi‑leg coverage grid (vary coverage in steps)
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_multileg_viability_grid`(
          leg_lines ARRAY<INT64>,
          pool_gross_other NUMERIC,
          stake_per_line NUMERIC,
          take_rate FLOAT64,
          net_rollover NUMERIC,
          include_self_in_pool BOOL,
          dividend_multiplier FLOAT64,
          f_share_override FLOAT64,
          steps INT64
        )
        RETURNS TABLE<
          coverage_frac FLOAT64,
          lines_covered INT64,
          stake_total NUMERIC,
          net_pool_if_bet NUMERIC,
          f_share_used FLOAT64,
          expected_return NUMERIC,
          expected_profit NUMERIC,
          is_positive_ev BOOL
        > AS (
          WITH cfg AS (
            SELECT
              CAST(pool_gross_other AS NUMERIC) AS O,
              CAST(stake_per_line AS NUMERIC) AS l,
              CAST(take_rate AS FLOAT64) AS t,
              CAST(net_rollover AS NUMERIC) AS R,
              include_self_in_pool AS inc_self,
              CAST(dividend_multiplier AS FLOAT64) AS m,
              CAST(f_share_override AS FLOAT64) AS f_fix,
              CAST(GREATEST(steps,1) AS INT64) AS S
          ), tot AS (
            SELECT CAST(ROUND(EXP(SUM(LN(CAST(x AS FLOAT64))))) AS INT64) AS C FROM UNNEST(leg_lines) AS x
          ), grid AS (
            SELECT c.O, c.l, c.t, c.R, c.inc_self, c.m, c.f_fix, c.S, (SELECT C FROM tot) AS C,
                   GENERATE_ARRAY(1, c.S) AS arr
            FROM cfg c
          ), p_rows AS (
            SELECT O, l, t, R, inc_self, m, f_fix, S, C,
                   SAFE_DIVIDE(i, S) AS alpha,
                   CAST(ROUND(SAFE_DIVIDE(i, S) * C) AS INT64) AS lines_cov
            FROM grid, UNNEST(arr) AS i
          )
          SELECT
            alpha AS coverage_frac,
            lines_cov AS lines_covered,
            CAST(lines_cov * l AS NUMERIC) AS stake_total,
            CAST(m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R) AS NUMERIC) AS net_pool_if_bet,
            CAST(COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) AS FLOAT64) AS f_share_used,
            CAST(alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) AS NUMERIC) AS expected_return,
            CAST(alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) - (lines_cov * l) AS NUMERIC) AS expected_profit,
            (alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) - (lines_cov * l)) > 0 AS is_positive_ev
          FROM p_rows
        );
        """
        job = client.query(sql); job.result()

        # Generic permutation-based viability (supports WIN/EXACTA/TRIFECTA/SUPERFECTA via k)
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_perm_viability_simple`(
          num_runners INT64,
          perm_k INT64,
          pool_gross_other NUMERIC,
          lines_covered INT64,
          stake_per_line NUMERIC,
          take_rate FLOAT64,
          net_rollover NUMERIC,
          include_self_in_pool BOOL,
          dividend_multiplier FLOAT64,
          f_share_override FLOAT64
        )
        RETURNS TABLE<
          total_lines INT64,
          lines_covered INT64,
          coverage_frac FLOAT64,
          stake_total NUMERIC,
          net_pool_if_bet NUMERIC,
          f_share_used FLOAT64,
          expected_return NUMERIC,
          expected_profit NUMERIC,
          is_positive_ev BOOL,
          cover_all_stake NUMERIC,
          cover_all_expected_profit NUMERIC,
          cover_all_is_positive BOOL,
          cover_all_o_min_break_even NUMERIC,
          coverage_frac_min_positive FLOAT64
        > AS (
          WITH base AS (
            SELECT CAST(num_runners AS INT64) AS N,
                   CAST(perm_k AS INT64) AS K,
                   CAST(pool_gross_other AS NUMERIC) AS O,
                   CAST(lines_covered AS INT64) AS M,
                   CAST(stake_per_line AS NUMERIC) AS stake_val,
                   CAST(take_rate AS FLOAT64) AS t,
                   CAST(net_rollover AS NUMERIC) AS R,
                   include_self_in_pool AS inc_self,
                   CAST(dividend_multiplier AS FLOAT64) AS mult,
                   CAST(f_share_override AS FLOAT64) AS f_fix
          ), perms AS (
            SELECT
              N, K, O, M, stake_val, t, R, inc_self, mult, f_fix,
              CASE WHEN N>=K AND K>0 THEN CAST(ROUND(EXP(SUM(LN(CAST(N - i AS FLOAT64))))) AS INT64) ELSE 0 END AS C
            FROM base, UNNEST(GENERATE_ARRAY(0, LEAST(K, GREATEST(N,0)) - 1)) AS i
            GROUP BY N, K, O, M, stake_val, t, R, inc_self, mult, f_fix
          ), clamp AS (
            SELECT *, CASE WHEN C>0 THEN LEAST(GREATEST(M,0), C) ELSE 0 END AS M_adj FROM perms
          ), cur AS (
            SELECT *, SAFE_DIVIDE(M_adj, C) AS alpha,
                   (M_adj * stake_val) AS S,
                   (CASE WHEN inc_self THEN (M_adj * stake_val) ELSE 0 END) AS S_inc
            FROM clamp
          ), fshare AS (
            SELECT *,
              CASE WHEN C=0 OR (C*stake_val + O)=0 THEN 0.0 ELSE CAST( (C*stake_val) / (C*stake_val + O) AS FLOAT64 ) END AS f_auto,
              CAST( mult * ( (1.0 - t) * ( (O + S_inc) ) + R ) AS NUMERIC ) AS NetPool_now
            FROM cur
          ), used AS (
            SELECT *, CAST(COALESCE(f_fix, f_auto) AS FLOAT64) AS f_used FROM fshare
          ), now_metrics AS (
            SELECT *, CAST(alpha * f_used * NetPool_now AS NUMERIC) AS ExpReturn,
              CAST(alpha * f_used * NetPool_now - S AS NUMERIC) AS ExpProfit,
              (alpha * f_used * NetPool_now - S) > 0 AS is_pos
            FROM used
          ), cover_all AS (
            SELECT *, CAST(C * stake_val AS NUMERIC) AS S_all,
              CAST(CASE WHEN inc_self THEN (C * stake_val) ELSE 0 END AS NUMERIC) AS S_all_inc,
              CAST( mult * ( (1.0 - t) * (O + (CASE WHEN inc_self THEN (C*stake_val) ELSE 0 END)) + R ) AS NUMERIC) AS NetPool_all,
              CAST( CAST(COALESCE(f_fix, f_auto) AS FLOAT64) * CAST( mult * ( (1.0 - t) * (O + (CASE WHEN inc_self THEN (C*stake_val) ELSE 0 END)) + R ) AS NUMERIC) - (C * stake_val) AS NUMERIC) AS ExpProfit_all
            FROM now_metrics
          ), cover_all_viab AS (
            SELECT *, (ExpProfit_all > 0) AS cover_all_pos,
              CAST( SAFE_DIVIDE( (C*stake_val) - (CAST(f_used AS FLOAT64) * mult * ( (1.0 - t) * (S_all_inc) + R ) ), (CAST(f_used AS FLOAT64) * mult * (1.0 - t)) ) AS NUMERIC ) AS O_min_cover_all
            FROM cover_all
          ), alpha_min AS (
            SELECT *,
              CASE
                WHEN C=0 OR stake_val=0 OR f_used<=0 OR (1.0 - t)<=0 THEN NULL
                WHEN inc_self THEN LEAST(1.0, GREATEST(0.0,
                  SAFE_DIVIDE(
                    CAST(C*stake_val AS FLOAT64) - (f_used * mult * ( (1.0 - t) * CAST(O AS FLOAT64) + CAST(R AS FLOAT64) )),
                    (f_used * mult * (1.0 - t) * CAST(C*stake_val AS FLOAT64))
                  )
                ))
                WHEN (f_used * mult * ( (1.0 - t) * CAST(O AS FLOAT64) + CAST(R AS FLOAT64) )) > CAST(C*stake_val AS FLOAT64)
                  THEN 0.0
                ELSE NULL
              END AS alpha_min_pos
            FROM cover_all_viab
          )
          SELECT C AS total_lines, M_adj AS lines_covered, alpha AS coverage_frac, S AS stake_total,
                 NetPool_now AS net_pool_if_bet, f_used AS f_share_used, ExpReturn AS expected_return,
                 ExpProfit AS expected_profit, is_pos AS is_positive_ev,
                 (C*stake_val) AS cover_all_stake, ExpProfit_all AS cover_all_expected_profit,
                 cover_all_pos AS cover_all_is_positive, O_min_cover_all AS cover_all_o_min_break_even,
                 alpha_min_pos AS coverage_frac_min_positive
          FROM alpha_min
        );
        """
        job = client.query(sql); job.result()

        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_perm_viability_grid`(
          num_runners INT64,
          perm_k INT64,
          pool_gross_other NUMERIC,
          stake_per_line NUMERIC,
          take_rate FLOAT64,
          net_rollover NUMERIC,
          include_self_in_pool BOOL,
          dividend_multiplier FLOAT64,
          f_share_override FLOAT64,
          steps INT64
        )
        RETURNS TABLE<
          coverage_frac FLOAT64,
          lines_covered INT64,
          stake_total NUMERIC,
          net_pool_if_bet NUMERIC,
          f_share_used FLOAT64,
          expected_return NUMERIC,
          expected_profit NUMERIC,
          is_positive_ev BOOL
        > AS (
          WITH cfg AS (
            SELECT CAST(num_runners AS INT64) AS N,
                   CAST(perm_k AS INT64) AS K,
                   CAST(pool_gross_other AS NUMERIC) AS O,
                   CAST(stake_per_line AS NUMERIC) AS l,
                   CAST(take_rate AS FLOAT64) AS t,
                   CAST(net_rollover AS NUMERIC) AS R,
                   include_self_in_pool AS inc_self,
                   CAST(dividend_multiplier AS FLOAT64) AS m,
                   CAST(f_share_override AS FLOAT64) AS f_fix,
                   CAST(GREATEST(steps,1) AS INT64) AS S
          ), perms AS (
            SELECT
              N, K, O, l, t, R, inc_self, m, f_fix, S,
              CASE WHEN N>=K AND K>0 THEN CAST(ROUND(EXP(SUM(LN(CAST(N - i AS FLOAT64))))) AS INT64) ELSE 0 END AS C
            FROM cfg, UNNEST(GENERATE_ARRAY(0, LEAST(K, GREATEST(N,0)) - 1)) AS i
            GROUP BY N, K, O, l, t, R, inc_self, m, f_fix, S
          ), grid AS (
            SELECT N, K, O, l, t, R, inc_self, m, f_fix, S, C, GENERATE_ARRAY(1, S) AS arr FROM perms
          ), p_rows AS (
            SELECT N, K, O, l, t, R, inc_self, m, f_fix, S, C,
                   SAFE_DIVIDE(i, S) AS alpha,
                   CAST(ROUND(SAFE_DIVIDE(i, S) * C) AS INT64) AS lines_cov
            FROM grid, UNNEST(arr) AS i
          )
          SELECT alpha AS coverage_frac,
                 lines_cov AS lines_covered,
                 CAST(lines_cov * l AS NUMERIC) AS stake_total,
                 CAST(m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R) AS NUMERIC) AS net_pool_if_bet,
                 CAST(COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) AS FLOAT64) AS f_share_used,
                 CAST(alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) AS NUMERIC) AS expected_return,
                 CAST(alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) - (lines_cov * l) AS NUMERIC) AS expected_profit,
                 (alpha * COALESCE(f_fix, (CASE WHEN C=0 OR (C*l + O)=0 THEN 0.0 ELSE CAST( (C*l) / (C*l + O) AS FLOAT64 ) END)) * (m * ((1.0 - t) * (O + (CASE WHEN inc_self THEN (lines_cov * l) ELSE 0 END)) + R)) - (lines_cov * l)) > 0 AS is_positive_ev
          FROM p_rows
        );
        """
        job = client.query(sql); job.result()

        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_superfecta_viability_grid`(
          num_runners INT64,
          pool_gross_other NUMERIC,
          stake_per_line NUMERIC,
          take_rate FLOAT64,
          net_rollover NUMERIC,
          include_self_in_pool BOOL,
          dividend_multiplier FLOAT64,
          f_share_override FLOAT64,
          steps INT64
        )
        RETURNS TABLE<
          coverage_frac FLOAT64,
          lines_covered INT64,
          stake_total NUMERIC,
          net_pool_if_bet NUMERIC,
          f_share_used FLOAT64,
          expected_return NUMERIC,
          expected_profit NUMERIC,
          is_positive_ev BOOL
        > AS (
          WITH cfg AS (
            SELECT
              CAST(num_runners AS INT64) AS N,
              GREATEST(num_runners * (num_runners-1) * (num_runners-2) * (num_runners-3), 0) AS C,
              CAST(pool_gross_other AS NUMERIC) AS O,
              CAST(stake_per_line AS NUMERIC) AS stake_val,
              CAST(take_rate AS FLOAT64) AS t,
              CAST(net_rollover AS NUMERIC) AS R,
              include_self_in_pool AS inc_self,
              CAST(dividend_multiplier AS FLOAT64) AS mult,
              CAST(f_share_override AS FLOAT64) AS f_fix,
              CAST(GREATEST(steps,1) AS INT64) AS K
          ), grid AS (
            SELECT N,C,O,stake_val,t,R,inc_self,mult,f_fix,K, GENERATE_ARRAY(1, K) AS arr FROM cfg
          ), grid_rows AS (
            SELECT
              N,C,O,stake_val,t,R,inc_self,mult,f_fix,K,
              SAFE_DIVIDE(i, K) AS alpha,
              CAST(ROUND(SAFE_DIVIDE(i, K) * C) AS INT64) AS L
            FROM grid, UNNEST(arr) AS i
          ), calc AS (
            SELECT
              N,C,O,stake_val,t,R,inc_self,mult,f_fix,
              alpha, L,
              (L * stake_val) AS S,
              (CASE WHEN inc_self THEN (L * stake_val) ELSE 0 END) AS S_inc,
              CASE WHEN C=0 OR (C*stake_val + O)=0 THEN 0.0 ELSE CAST( (C*stake_val) / (C*stake_val + O) AS FLOAT64 ) END AS f_auto
            FROM grid_rows
          )
          SELECT
            alpha AS coverage_frac,
            L AS lines_covered,
            S AS stake_total,
            CAST(mult * ( (1.0 - t) * (O + S_inc) + R ) AS NUMERIC) AS net_pool_if_bet,
            CAST(COALESCE(f_fix, f_auto) AS FLOAT64) AS f_share_used,
            CAST(alpha * COALESCE(f_fix, f_auto) * (mult * ( (1.0 - t) * (O + S_inc) + R )) AS NUMERIC) AS expected_return,
            CAST(alpha * COALESCE(f_fix, f_auto) * (mult * ( (1.0 - t) * (O + S_inc) + R )) - S AS NUMERIC) AS expected_profit,
            (alpha * COALESCE(f_fix, f_auto) * (mult * ( (1.0 - t) * (O + S_inc) + R )) - S) > 0 AS is_positive_ev
          FROM calc
        );
        """
        job = client.query(sql); job.result()

        

        # vw_products_coverage: per-product coverage and missing-data flags
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_products_coverage` AS
        SELECT
          p.product_id,
          p.event_id,
          UPPER(p.bet_type) AS bet_type,
          COALESCE(p.status, '') AS status,
          p.currency,
          p.start_iso,
          COALESCE(te.name, p.event_name) AS event_name,
          COALESCE(te.venue, p.venue) AS venue,
          COUNT(s.selection_id) AS n_selections,
          te.event_id IS NOT NULL AS event_present,
          (COUNT(s.selection_id) = 0) AS no_selections,
          (te.event_id IS NULL) AS missing_event,
          (p.start_iso IS NULL OR p.start_iso = '') AS missing_start_iso
        FROM `{ds}.tote_products` p
        LEFT JOIN `{ds}.tote_events` te ON te.event_id = p.event_id
        LEFT JOIN `{ds}.tote_product_selections` s ON s.product_id = p.product_id
        GROUP BY p.product_id, p.event_id, bet_type, status, p.currency, p.start_iso, event_name, venue, event_present, missing_event, missing_start_iso;
        """
        job = client.query(sql)
        job.result()

        # vw_products_coverage_issues: only products with missing data
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_products_coverage_issues` AS
        SELECT *
        FROM `{ds}.vw_products_coverage`
        WHERE no_selections OR missing_event OR missing_start_iso;
        """
        job = client.query(sql)
        job.result()

        # vw_today_gb_events: today's GB events with competitor count
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_today_gb_events` AS
        SELECT
          e.event_id,
          e.name,
          e.status,
          e.start_iso,
          e.venue,
          e.country,
          ARRAY_LENGTH(JSON_EXTRACT_ARRAY(e.competitors_json, '$')) AS n_competitors
        FROM `{ds}.tote_events` e
        WHERE e.country = 'GB' AND DATE(SUBSTR(e.start_iso,1,10)) = CURRENT_DATE();
        """
        job = client.query(sql)
        job.result()

        # vw_today_gb_superfecta: today's GB superfecta products with pool totals and competitor count
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_today_gb_superfecta` AS
        SELECT
          p.product_id,
          p.event_id,
          p.event_name,
          COALESCE(te.venue, p.venue) AS venue,
          te.country,
          p.start_iso,
          p.status,
          p.currency,
          p.total_gross,
          p.total_net,
          ARRAY_LENGTH(JSON_EXTRACT_ARRAY(te.competitors_json, '$')) AS n_competitors
        FROM `{ds}.tote_products` p
        LEFT JOIN `{ds}.tote_events` te USING(event_id)
        WHERE UPPER(p.bet_type) = 'SUPERFECTA' AND te.country = 'GB' AND DATE(SUBSTR(p.start_iso,1,10)) = CURRENT_DATE();
        """
        job = client.query(sql)
        job.result()

        # Ensure a tote_params table exists and has at least one row for defaults
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{ds}.tote_params`(
          t FLOAT64,                -- takeout fraction
          f FLOAT64,                -- your share on winner
          stake_per_line FLOAT64,   -- £ per combination
          R FLOAT64,                -- net rollover/seed
          updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        job = client.query(sql); job.result()
        # Add optional tuning columns if missing (idempotent)
        for name, typ in (
            ("model_id", "STRING"),
            ("ts_ms", "INT64"),
            ("default_top_n", "INT64"),
            ("target_coverage", "FLOAT64"),
        ):
            try:
                client.query(f"ALTER TABLE `{ds}.tote_params` ADD COLUMN IF NOT EXISTS `{name}` {typ}").result()
            except Exception:
                pass
        # Insert default params if table is empty
        sql = f"""
        INSERT INTO `{ds}.tote_params`(t,f,stake_per_line,R)
        SELECT 0.30, 1.0, 0.10, 0.0
        FROM (SELECT 1) 
        WHERE (SELECT COUNT(1) FROM `{ds}.tote_params`) = 0;
        """
        job = client.query(sql); job.result()

        # Runner strength view (predictions-based): choose latest ts for configured model_id (from tote_params)
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_superfecta_runner_strength` AS
        WITH prm AS (
          SELECT model_id, ts_ms, updated_ts
          FROM `{ds}.tote_params`
          WHERE model_id IS NOT NULL
          ORDER BY updated_ts DESC LIMIT 1
        ), latest AS (
          SELECT p.model_id, IFNULL((SELECT ts_ms FROM prm LIMIT 1), MAX(p.ts_ms)) AS ts_ms
          FROM `{ds}.predictions` p
          WHERE p.model_id = (SELECT model_id FROM prm LIMIT 1)
          GROUP BY p.model_id
        )
        SELECT
          tp.product_id,
          p.event_id,
          p.horse_id AS runner_id,
          GREATEST(p.proba, 1e-9) AS strength
        FROM `{ds}.predictions` p
        JOIN latest l ON l.model_id = p.model_id AND l.ts_ms = p.ts_ms
        JOIN `{ds}.tote_products` tp ON tp.event_id = p.event_id
        WHERE UPPER(tp.bet_type) = 'SUPERFECTA';
        """
        job = client.query(sql); job.result()

        # Table function: enumerate & score permutations (Plackett–Luce style)
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_superfecta_perms`(
          in_product_id STRING,
          top_n INT64
        )
        RETURNS TABLE<
          product_id STRING,
          h1 STRING, h2 STRING, h3 STRING, h4 STRING,
          p FLOAT64,
          line_cost_cents INT64
        > AS (
          WITH runners AS (
            SELECT product_id, runner_id, strength,
                   ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY strength DESC) AS rnk
            FROM `{ds}.vw_superfecta_runner_strength`
            WHERE product_id = in_product_id
          ), top_r AS (
            SELECT * FROM runners WHERE rnk <= top_n
          ), tot AS (
            SELECT product_id, SUM(strength) AS sum_s FROM top_r GROUP BY product_id
          )
          SELECT
            tr1.product_id,
            tr1.runner_id AS h1,
            tr2.runner_id AS h2,
            tr3.runner_id AS h3,
            tr4.runner_id AS h4,
            (tr1.strength / t.sum_s) *
            (tr2.strength / (t.sum_s - tr1.strength)) *
            (tr3.strength / (t.sum_s - tr1.strength - tr2.strength)) *
            (tr4.strength / (t.sum_s - tr1.strength - tr2.strength - tr3.strength)) AS p,
            1 AS line_cost_cents
          FROM top_r tr1
          JOIN top_r tr2 ON tr2.runner_id != tr1.runner_id
          JOIN top_r tr3 ON tr3.runner_id NOT IN (tr1.runner_id, tr2.runner_id)
          JOIN top_r tr4 ON tr4.runner_id NOT IN (tr1.runner_id, tr2.runner_id, tr3.runner_id)
          JOIN tot t ON t.product_id = tr1.product_id
        );
        """
        job = client.query(sql); job.result()

        # Table function: coverage curve and efficiency
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_superfecta_coverage`(
          in_product_id STRING,
          top_n INT64
        )
        RETURNS TABLE<
          product_id STRING,
          total_lines INT64,
          line_index INT64,
          h1 STRING, h2 STRING, h3 STRING, h4 STRING,
          p FLOAT64,
          cum_p FLOAT64,
          lines_frac FLOAT64,
          random_cov FLOAT64,
          efficiency FLOAT64
        > AS (
          WITH perms AS (
            SELECT * FROM `{ds}.tf_superfecta_perms`(in_product_id, top_n)
          ), ordered AS (
            SELECT product_id, h1,h2,h3,h4, p,
                   ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY p DESC) AS rn,
                   COUNT(*)    OVER (PARTITION BY product_id) AS total_lines
            FROM perms
          )
          SELECT
            product_id,
            total_lines,
            rn AS line_index,
            h1,h2,h3,h4,
            p,
            SUM(p) OVER (PARTITION BY product_id ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_p,
            rn / total_lines AS lines_frac,
            rn / total_lines AS random_cov,
            SAFE_DIVIDE(
              SUM(p) OVER (PARTITION BY product_id ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
              rn / total_lines
            ) AS efficiency
          FROM ordered
        );
        """
        job = client.query(sql); job.result()

        # Table function: breakeven metrics for any coverage point
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_superfecta_breakeven`(
          in_product_id STRING,
          top_n INT64,
          stake_per_line NUMERIC,
          f_share FLOAT64,
          net_rollover NUMERIC
        )
        RETURNS TABLE<
          product_id STRING,
          line_index INT64,
          lines INT64,
          lines_frac FLOAT64,
          cum_p FLOAT64,
          stake_total NUMERIC,
          o_min_break_even NUMERIC
        > AS (
          WITH cov AS (
            SELECT * FROM `{ds}.tf_superfecta_coverage`(in_product_id, top_n)
          )
          SELECT
            cov.product_id,
            cov.line_index,
            cov.line_index AS lines,
            cov.lines_frac,
            cov.cum_p,
            cov.line_index * stake_per_line AS stake_total,
            SAFE_DIVIDE(
              (cov.line_index * stake_per_line)
              - CAST(f_share AS NUMERIC) * (CAST(0.70 AS NUMERIC) * (cov.line_index * stake_per_line) + net_rollover),
              CAST(0.70 AS NUMERIC) * CAST(f_share AS NUMERIC)
            ) AS o_min_break_even
          FROM cov
        );
        """
        job = client.query(sql); job.result()

        # vw_today_gb_superfecta_latest: latest pool snapshot per product (GB only)
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_today_gb_superfecta_latest` AS
        WITH latest AS (
          SELECT product_id, MAX(ts_ms) AS ts_ms
          FROM `{ds}.tote_pool_snapshots`
          GROUP BY product_id
        )
        SELECT s.*
        FROM `{ds}.tote_pool_snapshots` s
        JOIN latest l USING(product_id, ts_ms)
        JOIN `{ds}.tote_products` p USING(product_id)
        LEFT JOIN `{ds}.tote_events` e ON e.event_id = p.event_id
        WHERE e.country = 'GB' AND DATE(SUBSTR(p.start_iso,1,10)) = CURRENT_DATE()
          AND UPPER(p.bet_type)='SUPERFECTA';
        """
        job = client.query(sql); job.result()

        # vw_today_gb_superfecta_be: add breakeven metrics using latest snapshots and tote_params (latest row)
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_today_gb_superfecta_be` AS
        WITH params AS (
          SELECT t, f, stake_per_line, R FROM `{ds}.tote_params`
          ORDER BY updated_ts DESC LIMIT 1
        )
        SELECT
          v.product_id,
          v.event_id,
          v.event_name,
          v.venue,
          v.country,
          v.start_iso,
          v.status,
          v.currency,
          v.total_gross,
          v.total_net,
          v.n_competitors,
          -- permutations C = N*(N-1)*(N-2)*(N-3)
          SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3) AS combos,
          -- S = C * stake_per_line
          SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line) AS S,
          prm.t,
          prm.f,
          prm.R,
          -- O_min = S/(f*(1-t)) - S - R/(1-t)
          CASE
            WHEN v.n_competitors >= 4 AND prm.f > 0 AND (1-prm.t) > 0 THEN (
              (SAFE_DIVIDE(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line), (prm.f * (1-prm.t))))
              - (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line))
              - (SAFE_DIVIDE(prm.R, (1-prm.t)))
            ) ELSE NULL END AS O_min,
          -- ROI at current pool using latest snapshot gross as O
          CASE
            WHEN v.n_competitors >= 4 AND prm.f > 0 AND (1-prm.t) > 0 AND s.latest_gross IS NOT NULL THEN (
              (prm.f * (((1-prm.t) * ( (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)) + s.latest_gross)) + prm.R))
              - (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line))
            ) / NULLIF(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line), 0)
            ELSE NULL END AS roi_current,
          CASE
            WHEN v.n_competitors >= 4 AND prm.f > 0 AND (1-prm.t) > 0 AND s.latest_gross IS NOT NULL THEN (
              (prm.f * (((1-prm.t) * ( (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)) + s.latest_gross)) + prm.R))
              > (SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line))
            ) ELSE NULL END AS viable_now
        FROM `{ds}.vw_today_gb_superfecta` v
        LEFT JOIN (
          SELECT product_id,
                 ANY_VALUE(total_gross) AS latest_gross,
                 ANY_VALUE(total_net) AS latest_net,
                 MAX(ts_ms) AS ts_ms
          FROM `{ds}.tote_pool_snapshots`
          GROUP BY product_id
        ) s ON s.product_id = v.product_id
        CROSS JOIN params prm;
        """
        job = client.query(sql)
        job.result()

        # vw_gb_open_superfecta_next60: GB Superfecta products starting in next 60 minutes
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_gb_open_superfecta_next60` AS
        SELECT
          p.product_id,
          p.event_id,
          p.event_name,
          COALESCE(e.venue, p.venue) AS venue,
          e.country,
          p.start_iso,
          p.status,
          p.currency,
          p.total_gross,
          p.total_net,
          ARRAY_LENGTH(JSON_EXTRACT_ARRAY(e.competitors_json, '$')) AS n_competitors
        FROM `{ds}.tote_products` p
        LEFT JOIN `{ds}.tote_events` e ON e.event_id = p.event_id
        WHERE UPPER(p.bet_type)='SUPERFECTA'
          AND e.country='GB'
          AND p.status='OPEN'
          AND TIMESTAMP(p.start_iso) BETWEEN CURRENT_TIMESTAMP() AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE);
        """
        job = client.query(sql); job.result()

        # vw_gb_open_superfecta_next60_be: with breakeven using latest snapshots and tote_params
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_gb_open_superfecta_next60_be` AS
        WITH params AS (
          SELECT t, f, stake_per_line, R FROM `{ds}.tote_params` ORDER BY updated_ts DESC LIMIT 1
        ), latest AS (
          SELECT product_id, ANY_VALUE(total_gross) AS latest_gross, ANY_VALUE(total_net) AS latest_net, MAX(ts_ms) AS ts_ms
          FROM `{ds}.tote_pool_snapshots`
          GROUP BY product_id
        )
        SELECT
          v.*, prm.t, prm.f, prm.stake_per_line, prm.R,
          SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3) AS combos,
          SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line) AS S,
          CASE WHEN v.n_competitors>=4 AND prm.f>0 AND (1-prm.t)>0 THEN (
            SAFE_DIVIDE(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line), prm.f*(1-prm.t))
            - SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)
            - SAFE_DIVIDE(prm.R, (1-prm.t))
          ) END AS O_min,
          CASE WHEN v.n_competitors>=4 AND prm.f>0 AND (1-prm.t)>0 THEN (
            (prm.f * (((1-prm.t) * ( SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line) + COALESCE(l.latest_gross, v.total_gross))) + prm.R))
            - SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)
          ) / NULLIF(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line),0) END AS roi_current,
          CASE WHEN v.n_competitors>=4 AND prm.f>0 AND (1-prm.t)>0 THEN (
            (prm.f * (((1-prm.t) * ( SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line) + COALESCE(l.latest_gross, v.total_gross))) + prm.R))
            > SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(SAFE_MULTIPLY(v.n_competitors, v.n_competitors-1), v.n_competitors-2), v.n_competitors-3), prm.stake_per_line)
          ) END AS viable_now
        FROM `{ds}.vw_gb_open_superfecta_next60` v
        CROSS JOIN params prm
        LEFT JOIN latest l USING(product_id);
        """
        job = client.query(sql); job.result()

        # vw_superfecta_products: convenient filter of products table
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_superfecta_products` AS
        SELECT
          p.product_id,
          p.event_id,
          p.event_name,
          COALESCE(te.venue, p.venue) AS venue,
          te.country,
          p.start_iso,
          p.status,
          p.currency,
          p.total_gross,
          p.total_net
        FROM `{ds}.tote_products` p
        LEFT JOIN `{ds}.tote_events` te USING(event_id)
        WHERE UPPER(p.bet_type) = 'SUPERFECTA';
        """
        job = client.query(sql)
        job.result()

        # vw_superfecta_dividends_latest: latest dividend per selection for each product
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_superfecta_dividends_latest` AS
        SELECT
          product_id,
          selection,
          ARRAY_AGG(dividend ORDER BY ts DESC LIMIT 1)[OFFSET(0)] AS dividend,
          MAX(ts) AS ts
        FROM `{ds}.tote_product_dividends`
        GROUP BY product_id, selection;
        """
        job = client.query(sql)
        job.result()

        # vw_tote_probable_odds: parse latest probable odds per selection from raw payloads
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_tote_probable_odds` AS
        WITH exploded AS (
          SELECT
            r.fetched_ts,
            JSON_EXTRACT_SCALAR(sel, '$.selectionId') AS selection_id,
            SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds[0].decimal') AS FLOAT64) AS decimal_odds
          FROM `{ds}.raw_tote_probable_odds` r,
          UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
          UNNEST(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes')) AS line,
          UNNEST(JSON_EXTRACT_ARRAY(line, '$.legs')) AS leg,
          UNNEST(JSON_EXTRACT_ARRAY(leg, '$.lineSelections')) AS sel
        ), latest AS (
          SELECT selection_id,
                 ANY_VALUE(decimal_odds) AS decimal_odds,
                 MAX(fetched_ts) AS ts_ms
          FROM exploded
          WHERE selection_id IS NOT NULL AND decimal_odds IS NOT NULL
          GROUP BY selection_id
        )
        SELECT
          s.product_id,
          s.selection_id,
          s.number AS cloth_number,
          l.decimal_odds,
          l.ts_ms
        FROM latest l
        JOIN `{ds}.tote_product_selections` s
          ON s.selection_id = l.selection_id;
        """
        job = client.query(sql)
        job.result()

        # vw_tote_probable_history: parsed stream of probable odds with timestamps (no aggregation)
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_tote_probable_history` AS
        WITH exploded AS (
          SELECT
            r.fetched_ts AS ts_ms,
            JSON_EXTRACT_SCALAR(sel, '$.selectionId') AS selection_id,
            SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds[0].decimal') AS FLOAT64) AS decimal_odds
          FROM `{ds}.raw_tote_probable_odds` r,
          UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
          UNNEST(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes')) AS line,
          UNNEST(JSON_EXTRACT_ARRAY(line, '$.legs')) AS leg,
          UNNEST(JSON_EXTRACT_ARRAY(leg, '$.lineSelections')) AS sel
        )
        SELECT s.product_id,
               s.selection_id,
               s.number AS cloth_number,
               e.decimal_odds,
               e.ts_ms
        FROM exploded e
        JOIN `{ds}.tote_product_selections` s ON s.selection_id = e.selection_id
        WHERE e.selection_id IS NOT NULL AND e.decimal_odds IS NOT NULL;
        """
        job = client.query(sql)
        job.result()

        # vw_runner_features: join runner features with horse name (useful for UI/ML)
        if self._table_exists("features_runner_event"):
            sql = f"""
            CREATE VIEW IF NOT EXISTS `{ds}.vw_runner_features` AS
            SELECT
              f.event_id,
              f.horse_id,
              h.name AS horse_name,
              f.event_date,
              f.cloth_number,
              f.total_net,
              f.weather_temp_c,
              f.weather_wind_kph,
              f.weather_precip_mm,
              f.going,
              f.recent_runs,
              f.avg_finish,
              f.wins_last5,
              f.places_last5,
              f.days_since_last_run
            FROM `{ds}.features_runner_event` f
            LEFT JOIN `{ds}.hr_horses` h ON h.horse_id = f.horse_id;
            """
            job = client.query(sql)
            job.result()

        # vw_superfecta_training: combine product/event context with runner results for modeling
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_superfecta_training` AS
        SELECT
          p.event_id,
          p.product_id,
          DATE(SUBSTR(COALESCE(te.start_iso, p.start_iso),1,10)) AS event_date,
          COALESCE(te.venue, p.venue) AS venue,
          te.country,
          p.total_net,
          rc.going,
          rc.weather_temp_c,
          rc.weather_wind_kph,
          rc.weather_precip_mm,
          r.horse_id,
          r.cloth_number,
          r.finish_pos,
          r.status
        FROM `{ds}.tote_products` p
        JOIN `{ds}.hr_horse_runs` r ON r.event_id = p.event_id
        LEFT JOIN `{ds}.race_conditions` rc ON rc.event_id = p.event_id
        LEFT JOIN `{ds}.tote_events` te ON te.event_id = p.event_id
        WHERE UPPER(p.bet_type) = 'SUPERFECTA' AND r.finish_pos IS NOT NULL;
        """
        job = client.query(sql)
        job.result()

    def cleanup_temp_tables(self, prefix: str = "_tmp_", older_than_days: int | None = None) -> int:
        """Delete leftover staging tables with the given prefix. Returns count deleted.

        If older_than_days is provided, only delete tables older than that many days.
        """
        client = self._client_obj(); self._ensure_dataset()
        from datetime import datetime, timezone, timedelta
        ds_ref = self._bq.DatasetReference(self.project, self.dataset)
        deleted = 0
        cutoff = None
        if older_than_days is not None and older_than_days > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        for tbl in client.list_tables(ds_ref):
            name = tbl.table_id
            if not name.startswith(prefix):
                continue
            if cutoff is not None and getattr(tbl, "created", None):
                try:
                    if tbl.created and tbl.created.replace(tzinfo=timezone.utc) > cutoff:
                        continue
                except Exception:
                    pass
            try:
                client.delete_table(tbl, not_found_ok=True)
                deleted += 1
            except Exception:
                pass
        return deleted


def get_bq_sink() -> BigQuerySink | None:
    if not cfg.bq_write_enabled:
        return None
    if not cfg.bq_project or not cfg.bq_dataset:
        return None
    try:
        return BigQuerySink(cfg.bq_project, cfg.bq_dataset, cfg.bq_location)
    except Exception:
        return None
