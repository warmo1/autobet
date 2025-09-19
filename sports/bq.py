from __future__ import annotations

import os
import json
import threading
from typing import Iterable, Mapping, Any, Optional

from .config import cfg


class BigQuerySink:
    def __init__(self, project: str, dataset: str, location: str = "EU"):
        self.project = project
        self.dataset = dataset
        self.location = location
        self._client = None
        self._bq = None
        self._client_lock = threading.Lock()
        self._default_dataset = f"{self.project}.{self.dataset}"

    @property
    def enabled(self) -> bool:
        return bool(self.project and self.dataset)

    def _client_obj(self):
        if self._client is not None:
            return self._client

        with self._client_lock:
            if self._client is not None:
                return self._client

            try:
                from google.cloud import bigquery  # type: ignore
            except Exception as e:
                raise RuntimeError("google-cloud-bigquery not installed") from e
            self._bq = bigquery
            # Prefer explicit service account credentials if provided to avoid
            # accidental reliance on user ADC tokens that may require reauth.
            creds = None
            try:
                # 1) JSON content in env
                sa_json_env = os.getenv("GCP_SERVICE_ACCOUNT_JSON") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
                if sa_json_env:
                    from google.oauth2 import service_account  # type: ignore
                    info = json.loads(sa_json_env)
                    creds = service_account.Credentials.from_service_account_info(info)
                else:
                    # 2) File path in env
                    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                    if sa_path and os.path.exists(sa_path):
                        from google.oauth2 import service_account  # type: ignore
                        creds = service_account.Credentials.from_service_account_file(sa_path)
            except Exception:
                # Fall back to ADC below
                creds = None

            client_kwargs = {"project": self.project, "location": self.location}
            if creds is not None:
                client_kwargs["credentials"] = creds

            # Fall back to Application Default Credentials (gcloud auth, metadata, etc.)
            self._client = bigquery.Client(**client_kwargs)
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
                default_dataset=self._default_dataset,
                use_query_cache=True,
            )
        elif getattr(job_config, "default_dataset", None) in (None, ""):
            # Preserve provided config but add default dataset for convenience
            job_config.default_dataset = self._default_dataset
        if getattr(job_config, "use_query_cache", None) is None:
            job_config.use_query_cache = True
        if getattr(job_config, "location", None) in (None, ""):
            job_config.location = self.location
        return client.query(sql, job_config=job_config, **kwargs).result()

    def query_dataframe(self, sql: str, **kwargs):
        """Run a query and return the result as a pandas DataFrame."""

        result = self.query(sql, **kwargs)
        try:
            return result.to_dataframe()
        except AttributeError as exc:  # pragma: no cover - depends on extras
            raise RuntimeError(
                "BigQuery result does not expose to_dataframe(); ensure pandas/pyarrow extras are installed."
            ) from exc

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

    def set_active_model(self, model_id: str, ts_ms: int) -> None:
        """Update tote_params to point to the specified model snapshot."""

        client = self._client_obj(); self._ensure_dataset()
        job_config = self._bq.QueryJobConfig(
            query_parameters=[
                self._bq.ScalarQueryParameter("model_id", "STRING", model_id),
                self._bq.ScalarQueryParameter("ts_ms", "INT64", int(ts_ms)),
            ]
        )
        sql = f"""
        UPDATE `{self.project}.{self.dataset}.tote_params`
        SET model_id = @model_id,
            ts_ms = @ts_ms,
            updated_ts = CURRENT_TIMESTAMP()
        """
        client.query(sql, job_config=job_config).result()

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
                "total_gross=COALESCE(S.total_gross, T.total_gross)",
                "total_net=COALESCE(S.total_net, T.total_net)",
                "rollover=COALESCE(S.rollover, T.rollover)",
                "deduction_rate=COALESCE(S.deduction_rate, T.deduction_rate)",
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
                "competitors_json=COALESCE(S.competitors_json, T.competitors_json)",
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
            "rollover": "FLOAT64",
            "deduction_rate": "FLOAT64",
        })
        if not temp:
            return
        self._ensure_columns("tote_pool_snapshots", {"rollover": "FLOAT64", "deduction_rate": "FLOAT64"})
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
                "rollover=S.rollover",
                "deduction_rate=S.deduction_rate",
            ]),
        )

    def stream_tote_pool_snapshots(self, rows: Iterable[Mapping[str, Any]]):
        """Stream rows into the tote_pool_snapshots table using the BQ Streaming API.
        
        This is preferred for high-frequency appends to avoid DML quota issues.
        """
        client = self._client_obj()
        self._ensure_dataset() # Make sure dataset exists
        table_id = f"{self.project}.{self.dataset}.tote_pool_snapshots"
        
        rows_to_insert = list(rows)
        if not rows_to_insert:
            return

        # The streaming API is less strict about schema on insert, but the table must exist.
        # We can rely on ensure_views() to have created it.
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            # For production, consider more robust error handling like logging to another service.
            print(f"BigQuery streaming insert errors for table {table_id}: {errors}")
            raise RuntimeError(f"BigQuery streaming insert failed: {errors}")

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
        # Also update tote_events.status to latest status from this batch
        client = self._client_obj(); self._ensure_dataset()
        ds = f"{self.project}.{self.dataset}"
        sql = f"""
        MERGE `{ds}.tote_events` T
        USING (
          SELECT event_id, status
          FROM `{temp}`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts_ms DESC) = 1
        ) S
        ON T.event_id = S.event_id
        WHEN MATCHED THEN UPDATE SET status = S.status
        """
        try:
            client.query(sql).result()
        except Exception:
            pass

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
        # Mirror latest product status onto tote_products.status for UI queries
        client = self._client_obj(); self._ensure_dataset()
        ds = f"{self.project}.{self.dataset}"
        sql = f"""
        MERGE `{ds}.tote_products` T
        USING (
          SELECT product_id, status
          FROM `{temp}`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ts_ms DESC) = 1
        ) S
        ON T.product_id = S.product_id
        WHEN MATCHED THEN UPDATE SET status = S.status
        """
        try:
            client.query(sql).result()
        except Exception:
            pass

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
                "finish_pos=S.finish_pos",
                "status=S.status",
                "cloth_number=S.cloth_number",
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
        temp = self._load_to_temp("models", rows, schema_hint={
            "created_ts": "INT64",
        })
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

    def load_superfecta_predictions(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        model_dataset: str | None = None,
    ) -> None:
        payload = list(rows)
        if not payload:
            return
        client = self._client_obj()
        dataset_id = model_dataset or os.getenv("BQ_MODEL_DATASET") or os.getenv(
            "ML_BQ_MODEL_DATASET", f"{self.dataset}_model"
        )
        table_id = f"{self.project}.{dataset_id}.superfecta_runner_predictions"
        job_config = self._bq.LoadJobConfig(
            write_disposition=self._bq.WriteDisposition.WRITE_APPEND
        )
        job = client.load_table_from_json(payload, table_id, job_config=job_config)
        job.result()

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

        Expected row keys: raw_id (str), fetched_ts (INT64 or STRING ISO), payload (STRING), product_id (STRING)
        """
        temp = self._load_to_temp(
            "raw_tote_probable_odds",
            rows,
            schema_hint={
                "fetched_ts": "INT64",
                "product_id": "STRING",
                "raw_id": "STRING",
            },
        )
        if not temp:
            return
        # Ensure required columns exist on destination
        self._ensure_columns("raw_tote_probable_odds", {"raw_id": "STRING", "product_id": "STRING"})
        self._merge(
            "raw_tote_probable_odds",
            temp,
            key_expr="T.raw_id=S.raw_id",
            update_set=",".join([
                "fetched_ts=S.fetched_ts",
                "payload=S.payload",
                "product_id=S.product_id",
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

    def upsert_tote_audit_bets(self, rows: Iterable[Mapping[str, Any]]):
        """Insert/merge audit bet records."""
        temp = self._load_to_temp(
            "tote_audit_bets",
            rows,
            schema_hint={
                "ts_ms": "INT64",
                "stake": "FLOAT64",
            },
        )
        if not temp:
            return
        self._merge(
            "tote_audit_bets",
            temp,
            key_expr="T.bet_id = S.bet_id",
            update_set=",".join([
                "ts_ms=S.ts_ms",
                "mode=S.mode",
                "product_id=S.product_id",
                "selection=S.selection",
                "stake=S.stake",
                "currency=S.currency",
                "status=S.status",
                "response_json=S.response_json",
                "error=S.error",
            ]),
        )


    def upsert_superfecta_morning(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp(
            "tote_superfecta_morning",
            rows,
            schema_hint={
                "run_date": "DATE",
                "run_ts": "INT64",
                "n_competitors": "INT64",
                "total_net": "FLOAT64",
                "total_gross": "FLOAT64",
                "rollover": "FLOAT64",
                "roi_current": "FLOAT64",
                "bankroll_allocated": "FLOAT64",
                "total_stake": "FLOAT64",
                "expected_profit": "FLOAT64",
                "expected_return": "FLOAT64",
                "hit_rate": "FLOAT64",
                "minutes_to_post": "FLOAT64",
                "created_ts": "INT64",
            },
        )
        if not temp:
            return
        self._merge(
            "tote_superfecta_morning",
            temp,
            key_expr="T.run_id = S.run_id",
            update_set=",".join([
                "run_date=COALESCE(S.run_date, T.run_date)",
                "run_ts=COALESCE(S.run_ts, T.run_ts)",
                "product_id=COALESCE(S.product_id, T.product_id)",
                "event_id=COALESCE(S.event_id, T.event_id)",
                "event_name=COALESCE(S.event_name, T.event_name)",
                "venue=COALESCE(S.venue, T.venue)",
                "country=COALESCE(S.country, T.country)",
                "start_iso=COALESCE(S.start_iso, T.start_iso)",
                "currency=COALESCE(S.currency, T.currency)",
                "n_competitors=COALESCE(S.n_competitors, T.n_competitors)",
                "total_net=COALESCE(S.total_net, T.total_net)",
                "total_gross=COALESCE(S.total_gross, T.total_gross)",
                "rollover=COALESCE(S.rollover, T.rollover)",
                "roi_current=COALESCE(S.roi_current, T.roi_current)",
                "preset_key=COALESCE(S.preset_key, T.preset_key)",
                "bankroll_allocated=COALESCE(S.bankroll_allocated, T.bankroll_allocated)",
                "total_stake=COALESCE(S.total_stake, T.total_stake)",
                "expected_profit=COALESCE(S.expected_profit, T.expected_profit)",
                "expected_return=COALESCE(S.expected_return, T.expected_return)",
                "hit_rate=COALESCE(S.hit_rate, T.hit_rate)",
                "plan_json=COALESCE(S.plan_json, T.plan_json)",
                "selections_text=COALESCE(S.selections_text, T.selections_text)",
                "status=COALESCE(S.status, T.status)",
                "filter_reason=COALESCE(S.filter_reason, T.filter_reason)",
                "notes=COALESCE(S.notes, T.notes)",
                "created_by=COALESCE(S.created_by, T.created_by)",
                "minutes_to_post=COALESCE(S.minutes_to_post, T.minutes_to_post)",
                "created_ts=COALESCE(S.created_ts, T.created_ts)",
            ]),
        )

    def upsert_superfecta_live_checks(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp(
            "tote_superfecta_live_checks",
            rows,
            schema_hint={
                "check_ts": "INT64",
                "minutes_to_post": "FLOAT64",
                "pool_total_net": "FLOAT64",
                "pool_total_gross": "FLOAT64",
                "rollover": "FLOAT64",
                "roi_current": "FLOAT64",
                "expected_profit": "FLOAT64",
                "expected_return": "FLOAT64",
                "hit_rate": "FLOAT64",
                "plan_total_stake": "FLOAT64",
                "decision_threshold_met": "BOOL",
            },
        )
        if not temp:
            return
        self._merge(
            "tote_superfecta_live_checks",
            temp,
            key_expr="T.check_id = S.check_id",
            update_set=",".join([
                "recommendation_id=COALESCE(S.recommendation_id, T.recommendation_id)",
                "run_id=COALESCE(S.run_id, T.run_id)",
                "product_id=COALESCE(S.product_id, T.product_id)",
                "check_ts=COALESCE(S.check_ts, T.check_ts)",
                "minutes_to_post=COALESCE(S.minutes_to_post, T.minutes_to_post)",
                "pool_total_net=COALESCE(S.pool_total_net, T.pool_total_net)",
                "pool_total_gross=COALESCE(S.pool_total_gross, T.pool_total_gross)",
                "rollover=COALESCE(S.rollover, T.rollover)",
                "roi_current=COALESCE(S.roi_current, T.roi_current)",
                "expected_profit=COALESCE(S.expected_profit, T.expected_profit)",
                "expected_return=COALESCE(S.expected_return, T.expected_return)",
                "hit_rate=COALESCE(S.hit_rate, T.hit_rate)",
                "plan_total_stake=COALESCE(S.plan_total_stake, T.plan_total_stake)",
                "status=COALESCE(S.status, T.status)",
                "notes=COALESCE(S.notes, T.notes)",
                "plan_json=COALESCE(S.plan_json, T.plan_json)",
                "decision_threshold_met=COALESCE(S.decision_threshold_met, T.decision_threshold_met)",
            ]),
        )

    def upsert_superfecta_recommendations(self, rows: Iterable[Mapping[str, Any]]):
        temp = self._load_to_temp(
            "tote_superfecta_recommendations",
            rows,
            schema_hint={
                "run_date": "DATE",
                "created_ts": "INT64",
                "updated_ts": "INT64",
                "bankroll_allocated": "FLOAT64",
                "total_stake": "FLOAT64",
                "expected_profit": "FLOAT64",
                "expected_return": "FLOAT64",
                "hit_rate": "FLOAT64",
                "auto_place": "BOOL",
                "ready_ts": "INT64",
                "final_ts": "INT64",
                "minutes_to_post": "FLOAT64",
                "last_check_ts": "INT64",
            },
        )
        if not temp:
            return
        self._merge(
            "tote_superfecta_recommendations",
            temp,
            key_expr="T.recommendation_id = S.recommendation_id",
            update_set=",".join([
                "run_id=COALESCE(S.run_id, T.run_id)",
                "product_id=COALESCE(S.product_id, T.product_id)",
                "event_id=COALESCE(S.event_id, T.event_id)",
                "run_date=COALESCE(S.run_date, T.run_date)",
                "created_ts=COALESCE(T.created_ts, S.created_ts)",
                "updated_ts=COALESCE(S.updated_ts, T.updated_ts)",
                "status=COALESCE(S.status, T.status)",
                "preset_key=COALESCE(S.preset_key, T.preset_key)",
                "bankroll_allocated=COALESCE(S.bankroll_allocated, T.bankroll_allocated)",
                "total_stake=COALESCE(S.total_stake, T.total_stake)",
                "expected_profit=COALESCE(S.expected_profit, T.expected_profit)",
                "expected_return=COALESCE(S.expected_return, T.expected_return)",
                "hit_rate=COALESCE(S.hit_rate, T.hit_rate)",
                "currency=COALESCE(S.currency, T.currency)",
                "plan_json=COALESCE(S.plan_json, T.plan_json)",
                "selections_text=COALESCE(S.selections_text, T.selections_text)",
                "auto_place=COALESCE(S.auto_place, T.auto_place)",
                "decision_reason=COALESCE(S.decision_reason, T.decision_reason)",
                "ready_ts=COALESCE(S.ready_ts, T.ready_ts)",
                "final_ts=COALESCE(S.final_ts, T.final_ts)",
                "bet_id=COALESCE(S.bet_id, T.bet_id)",
                "live_metrics_json=COALESCE(S.live_metrics_json, T.live_metrics_json)",
                "minutes_to_post=COALESCE(S.minutes_to_post, T.minutes_to_post)",
                "last_check_ts=COALESCE(S.last_check_ts, T.last_check_ts)",
                "source=COALESCE(S.source, T.source)",
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
          total_net FLOAT64,
          rollover FLOAT64,
          deduction_rate FLOAT64
        );
        CREATE TABLE IF NOT EXISTS `{ds}.raw_tote_probable_odds`(
          fetched_ts INT64,
          payload STRING,
          product_id STRING
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
        CREATE TABLE IF NOT EXISTS `{ds}.tote_audit_bets`(
          bet_id STRING,
          ts_ms INT64,
          mode STRING,
          product_id STRING,
          selection STRING,
          stake FLOAT64,
          currency STRING,
          status STRING,
          response_json STRING,
          error STRING
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_superfecta_morning`(
          run_id STRING,
          run_date DATE,
          run_ts INT64,
          product_id STRING,
          event_id STRING,
          event_name STRING,
          venue STRING,
          country STRING,
          start_iso STRING,
          currency STRING,
          n_competitors INT64,
          total_net FLOAT64,
          total_gross FLOAT64,
          rollover FLOAT64,
          roi_current FLOAT64,
          preset_key STRING,
          bankroll_allocated FLOAT64,
          total_stake FLOAT64,
          expected_profit FLOAT64,
          expected_return FLOAT64,
          hit_rate FLOAT64,
          plan_json STRING,
          selections_text STRING,
          status STRING,
          filter_reason STRING,
          notes STRING,
          created_by STRING,
          minutes_to_post FLOAT64,
          created_ts INT64
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_superfecta_live_checks`(
          check_id STRING,
          recommendation_id STRING,
          run_id STRING,
          product_id STRING,
          check_ts INT64,
          minutes_to_post FLOAT64,
          pool_total_net FLOAT64,
          pool_total_gross FLOAT64,
          rollover FLOAT64,
          roi_current FLOAT64,
          expected_profit FLOAT64,
          expected_return FLOAT64,
          hit_rate FLOAT64,
          plan_total_stake FLOAT64,
          status STRING,
          notes STRING,
          plan_json STRING,
          decision_threshold_met BOOL
        );
        CREATE TABLE IF NOT EXISTS `{ds}.tote_superfecta_recommendations`(
          recommendation_id STRING,
          run_id STRING,
          product_id STRING,
          event_id STRING,
          run_date DATE,
          created_ts INT64,
          updated_ts INT64,
          status STRING,
          preset_key STRING,
          bankroll_allocated FLOAT64,
          total_stake FLOAT64,
          expected_profit FLOAT64,
          expected_return FLOAT64,
          hit_rate FLOAT64,
          currency STRING,
          plan_json STRING,
          selections_text STRING,
          auto_place BOOL,
          decision_reason STRING,
          ready_ts INT64,
          final_ts INT64,
          bet_id STRING,
          live_metrics_json STRING,
          minutes_to_post FLOAT64,
          last_check_ts INT64,
          source STRING
        );
        """
        job = client.query(sql); job.result()

        # vw_products_latest_totals: tote_products with the latest snapshot metrics/status overlays
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_products_latest_totals` AS
        WITH latest AS (
          SELECT
            product_id,
            ARRAY_AGG(total_gross ORDER BY ts_ms DESC LIMIT 1)[OFFSET(0)] AS latest_gross,
            ARRAY_AGG(total_net ORDER BY ts_ms DESC LIMIT 1)[OFFSET(0)] AS latest_net,
            MAX(ts_ms) AS ts_ms
          FROM `{ds}.tote_pool_snapshots`
          GROUP BY product_id
        ), pstat AS (
          SELECT
            product_id,
            ARRAY_AGG(status ORDER BY ts_ms DESC LIMIT 1)[OFFSET(0)] AS latest_status
          FROM `{ds}.tote_product_status_log`
          GROUP BY product_id
        )
        SELECT
          p.product_id,
          p.event_id,
          p.event_name,
          p.venue,
          p.start_iso,
          COALESCE(pstat.latest_status, p.status) AS status,
          p.currency,
          COALESCE(l.latest_gross, p.total_gross) AS total_gross,
          COALESCE(l.latest_net, p.total_net) AS total_net,
          p.rollover,
          p.deduction_rate,
          p.bet_type
        FROM `{ds}.tote_products` p
        LEFT JOIN latest l USING(product_id)
        LEFT JOIN pstat USING(product_id);
        """
        client.query(sql).result()
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

        # Runner strength view (predictions-based): latest row per runner/product from model dataset
        model_dataset = os.getenv("BQ_MODEL_DATASET", f"{self.dataset}_model")
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_superfecta_runner_strength` AS
        SELECT
          product_id,
          event_id,
          horse_id AS runner_id,
          GREATEST(p_place1, 1e-9) AS strength
        FROM (
          SELECT
            product_id,
            event_id,
            horse_id,
            p_place1,
            ROW_NUMBER() OVER (
              PARTITION BY product_id, horse_id
              ORDER BY scored_at DESC
            ) AS rn
          FROM `{self.project}.{model_dataset}.superfecta_runner_predictions`
        )
        WHERE rn = 1;
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
          p.rollover,
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

        # --- QC Views ---
        # QC: products for today with missing runner numbers (cloth/trap)
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_qc_today_missing_runner_numbers` AS
        SELECT DISTINCT p.product_id, p.event_id, p.event_name, p.venue, p.start_iso
        FROM `{ds}.tote_products` p
        LEFT JOIN `{ds}.tote_product_selections` s ON s.product_id = p.product_id
        WHERE DATE(SUBSTR(p.start_iso,1,10)) = CURRENT_DATE()
          AND s.number IS NULL;
        """
        client.query(sql).result()

        # QC: products missing bet rules (min/max/increment)
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_qc_missing_bet_rules` AS
        SELECT p.product_id, p.event_id, p.bet_type, p.start_iso
        FROM `{ds}.tote_products` p
        LEFT JOIN `{ds}.tote_bet_rules` r ON r.product_id = p.product_id
        WHERE r.product_id IS NULL
           OR (r.min_line IS NULL AND r.line_increment IS NULL AND r.min_bet IS NULL);
        """
        client.query(sql).result()

        # QC: probable odds coverage by product (share of selections with odds)
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_qc_probable_odds_coverage` AS
        WITH sel AS (
          SELECT product_id, COUNT(1) AS sel_count
          FROM `{ds}.tote_product_selections`
          GROUP BY product_id
        ), odded AS (
          SELECT product_id, COUNT(DISTINCT selection_id) AS with_odds
          FROM `{ds}.vw_tote_probable_odds`
          GROUP BY product_id
        )
        SELECT p.product_id, p.event_id, COALESCE(o.with_odds,0) AS with_odds, COALESCE(s.sel_count,0) AS selections,
               SAFE_DIVIDE(COALESCE(o.with_odds,0), NULLIF(s.sel_count,0)) AS coverage
        FROM `{ds}.tote_products` p
        LEFT JOIN sel s USING(product_id)
        LEFT JOIN odded o USING(product_id);
        """
        client.query(sql).result()

        # QC: today's GB Superfecta products with no pool snapshots yet
        sql = f"""
        CREATE VIEW IF NOT EXISTS `{ds}.vw_qc_today_gb_sf_missing_snapshots` AS
        SELECT v.product_id, v.event_id, v.event_name, v.venue, v.start_iso
        FROM `{ds}.vw_today_gb_superfecta` v
        LEFT JOIN (
          SELECT DISTINCT product_id FROM `{ds}.tote_pool_snapshots`
        ) s USING(product_id)
        WHERE s.product_id IS NULL;
        """
        client.query(sql).result()

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

        # vw_tote_probable_odds: robust parse of latest probable odds per selection from raw payloads
        # Handles both shapes for lines.legs (array vs object with lineSelections)
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_tote_probable_odds` AS
        WITH exploded AS (
          SELECT
            JSON_EXTRACT_SCALAR(prod, '$.id') AS product_id,
            r.fetched_ts,
            COALESCE(
              JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId'),
              JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId')
            ) AS selection_id,
            SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds.decimal') AS FLOAT64) AS decimal_odds
          FROM `{ds}.raw_tote_probable_odds` r,
          UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
          UNNEST(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes')) AS line
        ), latest AS (
          SELECT product_id,
                 selection_id,
                 ARRAY_AGG(decimal_odds ORDER BY fetched_ts DESC LIMIT 1)[OFFSET(0)] AS decimal_odds,
                 MAX(fetched_ts) AS ts_ms
          FROM exploded
          WHERE selection_id IS NOT NULL AND decimal_odds IS NOT NULL AND product_id IS NOT NULL
          GROUP BY product_id, selection_id
        )
        SELECT
          l.product_id,
          COALESCE(s.selection_id, l.selection_id) AS selection_id,
          s.number AS cloth_number,
          l.decimal_odds,
          l.ts_ms
        FROM latest l
        LEFT JOIN `{ds}.tote_product_selections` s
          ON s.selection_id = l.selection_id AND s.product_id = l.product_id;
        """
        job = client.query(sql)
        job.result()

        # vw_tote_probable_history: parsed stream of probable odds with timestamps (robust legs parsing)
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_tote_probable_history` AS
        WITH exploded AS (
          SELECT
            JSON_EXTRACT_SCALAR(prod, '$.id') AS product_id,
            r.fetched_ts AS ts_ms,
            COALESCE(
              JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId'),
              JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId')
            ) AS selection_id,
            SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds.decimal') AS FLOAT64) AS decimal_odds
          FROM `{ds}.raw_tote_probable_odds` r,
          UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
          UNNEST(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes')) AS line
        )
        SELECT e.product_id,
               COALESCE(s.selection_id, e.selection_id) AS selection_id,
               s.number AS cloth_number,
               e.decimal_odds,
               e.ts_ms
        FROM exploded e
        LEFT JOIN `{ds}.tote_product_selections` s ON s.selection_id = e.selection_id AND s.product_id = e.product_id
        WHERE e.selection_id IS NOT NULL AND e.decimal_odds IS NOT NULL;
        """
        job = client.query(sql)
        job.result()

        # vw_sf_strengths_from_win_horse: derive fallback runner strengths from WIN probable odds
        # Drop the legacy view so we can install a materialized view under the same name.
        try:
            client.query(f"DROP VIEW IF EXISTS `{ds}.vw_sf_strengths_from_win_horse`").result()
        except Exception:
            pass

        sql = f"""
        CREATE OR REPLACE MATERIALIZED VIEW `{ds}.mv_sf_strengths_from_win_horse` AS
        WITH sf AS (
          SELECT product_id, event_id
          FROM `{ds}.tote_products`
          WHERE UPPER(bet_type) = 'SUPERFECTA'
        ),
        win AS (
          SELECT event_id, product_id AS win_product_id
          FROM `{ds}.tote_products`
          WHERE UPPER(bet_type) = 'WIN'
        ),
        paired AS (
          SELECT sf.product_id, sf.event_id, win.win_product_id
          FROM sf
          JOIN win USING(event_id)
        ),
        parsed_odds AS (
          SELECT
            SAFE_CAST(JSON_EXTRACT_SCALAR(prod, '$.id') AS STRING) AS product_id,
            COALESCE(
              JSON_EXTRACT_SCALAR(line, '$.legs.lineSelections[0].selectionId'),
              JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(line, '$.legs')[SAFE_OFFSET(0)], '$.lineSelections[0].selectionId')
            ) AS selection_id,
            SAFE_CAST(JSON_EXTRACT_SCALAR(line, '$.odds.decimal') AS FLOAT64) AS decimal_odds,
            r.fetched_ts
          FROM `{ds}.raw_tote_probable_odds` r,
          UNNEST(JSON_EXTRACT_ARRAY(r.payload, '$.products.nodes')) AS prod,
          UNNEST(JSON_EXTRACT_ARRAY(prod, '$.lines.nodes')) AS line
          WHERE JSON_EXTRACT_SCALAR(line, '$.odds.decimal') IS NOT NULL
        ),
        latest_odds AS (
          SELECT product_id, selection_id, decimal_odds
          FROM (
            SELECT
              product_id,
              selection_id,
              decimal_odds,
              ROW_NUMBER() OVER (PARTITION BY product_id, selection_id ORDER BY fetched_ts DESC) AS rn
            FROM parsed_odds
            WHERE selection_id IS NOT NULL AND decimal_odds IS NOT NULL AND decimal_odds > 0
              AND product_id IS NOT NULL
          )
          WHERE rn = 1
        ),
        selection_map AS (
          SELECT product_id, selection_id, number AS cloth_number
          FROM `{ds}.tote_product_selections`
        ),
        selection_status AS (
          SELECT product_id, selection_id, status
          FROM `{ds}.vw_selection_status_current`
        ),
        competitors AS (
          SELECT
            e.event_id,
            SAFE_CAST(JSON_VALUE(comp, '$.details.clothNumber') AS INT64) AS cloth_number,
            JSON_VALUE(comp, '$.id') AS horse_id
          FROM `{ds}.tote_events` e,
          UNNEST(IFNULL(JSON_EXTRACT_ARRAY(e.competitors_json, '$'), [])) AS comp
        ),
        weights AS (
          SELECT
            p.product_id,
            p.event_id,
            c.horse_id AS runner_id,
            SAFE_DIVIDE(1.0, lo.decimal_odds) AS weight
          FROM paired p
          JOIN selection_map sel
            ON sel.product_id = p.win_product_id
          JOIN latest_odds lo
            ON lo.product_id = p.win_product_id AND lo.selection_id = sel.selection_id
          LEFT JOIN selection_status st
            ON st.product_id = sel.product_id AND st.selection_id = sel.selection_id
          JOIN competitors c
            ON c.event_id = p.event_id
           AND c.cloth_number IS NOT NULL
           AND sel.cloth_number IS NOT NULL
           AND c.cloth_number = sel.cloth_number
          WHERE c.horse_id IS NOT NULL
            AND (st.status IS NULL OR st.status NOT IN ('NON_RUNNER','NR','WITHDRAWN','SCRATCHED','RESERVE','NONRUNNER'))
        ),
        sums AS (
          SELECT product_id, SUM(weight) AS total_weight
          FROM weights
          GROUP BY product_id
        )
        SELECT
          w.product_id,
          w.event_id,
          w.runner_id,
          SAFE_DIVIDE(w.weight, NULLIF(s.total_weight, 0)) AS strength
        FROM weights w
        JOIN sums s USING(product_id)
        WHERE SAFE_DIVIDE(w.weight, NULLIF(s.total_weight, 0)) IS NOT NULL;
        """
        client.query(sql).result()

        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_sf_strengths_from_win_horse` AS
        SELECT * FROM `{ds}.mv_sf_strengths_from_win_horse`;
        """
        client.query(sql).result()

        # vw_superfecta_runner_strength_any: prefer model strengths, fall back to WIN odds-derived weights
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_superfecta_runner_strength_any` AS
        WITH pred AS (
          SELECT product_id, event_id, runner_id, strength
          FROM `{ds}.vw_superfecta_runner_strength`
        ),
        fallback AS (
          SELECT product_id, event_id, runner_id, strength
          FROM `{ds}.mv_sf_strengths_from_win_horse`
        ),
        pred_products AS (
          SELECT DISTINCT product_id FROM pred
        )
        SELECT p.product_id, p.event_id, p.runner_id, p.strength
        FROM pred p
        UNION ALL
        SELECT f.product_id, f.event_id, f.runner_id, f.strength
        FROM fallback f
        LEFT JOIN pred_products pp USING(product_id)
        WHERE pp.product_id IS NULL;
        """
        client.query(sql).result()

        # Table function: enumerate permutations using any available strengths (model or WIN odds fallback)
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_superfecta_perms_any`(
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
            FROM `{ds}.vw_superfecta_runner_strength_any`
            WHERE product_id = in_product_id
          ), top_r AS (
            SELECT * FROM runners WHERE rnk <= top_n
          ), tot AS (
            SELECT product_id, SUM(strength) AS sum_s
            FROM top_r
            GROUP BY product_id
          )
          SELECT
            tr1.product_id,
            tr1.runner_id AS h1,
            tr2.runner_id AS h2,
            tr3.runner_id AS h3,
            tr4.runner_id AS h4,
            SAFE_DIVIDE(tr1.strength, t.sum_s)
            * SAFE_DIVIDE(tr2.strength, t.sum_s - tr1.strength)
            * SAFE_DIVIDE(tr3.strength, t.sum_s - tr1.strength - tr2.strength)
            * SAFE_DIVIDE(tr4.strength, t.sum_s - tr1.strength - tr2.strength - tr3.strength) AS p,
            1 AS line_cost_cents
          FROM top_r tr1
          JOIN top_r tr2 ON tr2.runner_id != tr1.runner_id
          JOIN top_r tr3 ON tr3.runner_id NOT IN (tr1.runner_id, tr2.runner_id)
          JOIN top_r tr4 ON tr4.runner_id NOT IN (tr1.runner_id, tr2.runner_id, tr3.runner_id)
          JOIN tot t ON t.product_id = tr1.product_id
        );
        """
        job = client.query(sql)
        job.result()

        # Table function: legacy alias returning the same permutations as tf_superfecta_perms_any
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_superfecta_perms_horse_any`(
          in_product_id STRING,
          top_n INT64
        )
        RETURNS TABLE<
          product_id STRING,
          h1 STRING, h2 STRING, h3 STRING, h4 STRING,
          p FLOAT64,
          line_cost_cents INT64
        > AS (
          SELECT * FROM `{ds}.tf_superfecta_perms_any`(in_product_id, top_n)
        );
        """
        job = client.query(sql)
        job.result()

        # Table function: Superfecta backtest using model or fallback strengths
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_sf_backtest_horse_any`(
          start_date DATE,
          end_date DATE,
          top_n INT64,
          coverage FLOAT64
        )
        RETURNS TABLE<
          product_id STRING,
          event_id STRING,
          event_name STRING,
          venue STRING,
          country STRING,
          start_iso STRING,
          total_net FLOAT64,
          total_lines INT64,
          winner_rank INT64,
          winner_p FLOAT64,
          cover_lines INT64,
          hit_at_coverage BOOL
        > AS (
          WITH params AS (
            SELECT
              IFNULL(top_n, 10) AS top_n,
              GREATEST(0.0, LEAST(IFNULL(coverage, 0.0), 1.0)) AS coverage
          ),
          prods AS (
            SELECT
              p.product_id,
              p.event_id,
              p.event_name,
              COALESCE(e.venue, p.venue) AS venue,
              e.country,
              p.start_iso,
              p.total_net
            FROM `{ds}.tote_products` p
            LEFT JOIN `{ds}.tote_events` e USING(event_id)
            WHERE UPPER(p.bet_type) = 'SUPERFECTA'
              AND DATE(SUBSTR(p.start_iso,1,10)) BETWEEN start_date AND end_date
              AND UPPER(COALESCE(p.status, '')) IN ('CLOSED','SETTLED','RESULTED')
          ),
          winners AS (
            SELECT event_id,
                   MAX(IF(finish_pos = 1, horse_id, NULL)) AS h1,
                   MAX(IF(finish_pos = 2, horse_id, NULL)) AS h2,
                   MAX(IF(finish_pos = 3, horse_id, NULL)) AS h3,
                   MAX(IF(finish_pos = 4, horse_id, NULL)) AS h4
            FROM `{ds}.hr_horse_runs`
            WHERE finish_pos IS NOT NULL AND finish_pos BETWEEN 1 AND 4
            GROUP BY event_id
          ),
          perms AS (
            SELECT
              pr.product_id,
              t.h1, t.h2, t.h3, t.h4,
              t.p
            FROM prods pr,
                 `{ds}.tf_superfecta_perms_any`(pr.product_id, (SELECT top_n FROM params)) AS t
          ),
          ranked AS (
            SELECT
              product_id,
              h1, h2, h3, h4,
              p,
              ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY p DESC) AS rn,
              COUNT(*) OVER (PARTITION BY product_id) AS total_lines
            FROM perms
          ),
          answers AS (
            SELECT pr.product_id, pr.event_id, w.h1, w.h2, w.h3, w.h4
            FROM prods pr
            JOIN winners w USING(event_id)
            WHERE w.h1 IS NOT NULL AND w.h2 IS NOT NULL AND w.h3 IS NOT NULL AND w.h4 IS NOT NULL
          ),
          hits AS (
            SELECT
              a.product_id,
              a.event_id,
              r.total_lines,
              r.rn AS winner_rank,
              r.p AS winner_p
            FROM answers a
            JOIN ranked r
              ON r.product_id = a.product_id
             AND r.h1 = a.h1 AND r.h2 = a.h2 AND r.h3 = a.h3 AND r.h4 = a.h4
          ),
          coverage_calc AS (
            SELECT
              product_id,
              MAX(total_lines) AS total_lines,
              GREATEST(1, CAST(ROUND((SELECT coverage FROM params) * MAX(total_lines)) AS INT64)) AS cover_lines
            FROM ranked
            GROUP BY product_id
          )
          SELECT
            pr.product_id,
            pr.event_id,
            pr.event_name,
            pr.venue,
            pr.country,
            pr.start_iso,
            pr.total_net,
            c.total_lines,
            h.winner_rank,
            h.winner_p,
            c.cover_lines,
            (h.winner_rank IS NOT NULL AND h.winner_rank <= c.cover_lines) AS hit_at_coverage
          FROM prods pr
          LEFT JOIN coverage_calc c USING(product_id)
          LEFT JOIN hits h USING(product_id, event_id)
        );
        """
        job = client.query(sql)
        job.result()

        # Table function: Expected Value grid over coverage using permutations and guardrail params
        sql = f"""
        CREATE OR REPLACE TABLE FUNCTION `{ds}.tf_perm_ev_grid`(
          in_product_id STRING,
          in_top_n INT64,
          O NUMERIC,
          S NUMERIC,
          t FLOAT64,
          R NUMERIC,
          inc BOOL,
          mult FLOAT64,
          f FLOAT64,
          conc FLOAT64,
          mi FLOAT64
        )
        RETURNS TABLE<
          lines_covered INT64,
          hit_rate FLOAT64,
          expected_return FLOAT64,
          expected_profit FLOAT64,
          f_share_used FLOAT64
        > AS (
          WITH perms AS (
            SELECT ROW_NUMBER() OVER (ORDER BY p DESC) AS rn, p, h1, h2, h3, h4
            FROM `{ds}.tf_superfecta_perms_any`(in_product_id, in_top_n)
          ), odds AS (
            SELECT CAST(cloth_number AS STRING) AS sel_id, CAST(decimal_odds AS FLOAT64) AS odds
            FROM `{ds}.vw_tote_probable_odds`
            WHERE product_id = in_product_id
          ), parms AS (
            SELECT
              GREATEST(0.1, 1.0 - 0.6 * mi) AS beta,
              1.0 + 2.0 * conc AS gamma,
              CAST(O AS FLOAT64) AS O_f,
              CAST(S AS FLOAT64) AS S_f
          ), scored AS (
            SELECT
              p.rn,
              p.p,
              POW(p.p, parms.gamma) AS w_raw,
              POW(1.0 / NULLIF(o1.odds, 0.0), parms.beta)
              * POW(1.0 / NULLIF(o2.odds, 0.0), parms.beta)
              * POW(1.0 / NULLIF(o3.odds, 0.0), parms.beta)
              * POW(1.0 / NULLIF(o4.odds, 0.0), parms.beta) AS q_raw
            FROM perms p
            CROSS JOIN parms
            LEFT JOIN odds o1 ON o1.sel_id = CAST(p.h1 AS STRING)
            LEFT JOIN odds o2 ON o2.sel_id = CAST(p.h2 AS STRING)
            LEFT JOIN odds o3 ON o3.sel_id = CAST(p.h3 AS STRING)
            LEFT JOIN odds o4 ON o4.sel_id = CAST(p.h4 AS STRING)
          ), metrics AS (
            SELECT
              s.rn,
              s.p,
              s.w_raw,
              s.q_raw,
              SUM(s.p) OVER (ORDER BY s.rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_prob,
              SUM(s.w_raw) OVER (ORDER BY s.rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_w,
              SUM(s.q_raw) OVER (ORDER BY s.rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_q
            FROM scored s
          ), enriched AS (
            SELECT
              m.*,
              SAFE_DIVIDE((SELECT S_f FROM parms) * m.w_raw, NULLIF(m.cum_w, 0)) AS stake_i,
              SAFE_DIVIDE((SELECT O_f FROM parms) * (1.0 - mi) * m.q_raw, NULLIF(m.cum_q, 0)) AS others_i
            FROM metrics m
          ), with_f AS (
            SELECT
              e.*,
              SAFE_DIVIDE(e.stake_i, e.stake_i + e.others_i) AS f_i
            FROM enriched e
          ), agg AS (
            SELECT
              rn AS lines_covered,
              cum_prob AS hit_rate,
              SUM(p * f_i) OVER (ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_pf,
              SUM(p) OVER (ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_p
            FROM with_f
          )
          SELECT
            lines_covered,
            hit_rate,
            sum_pf * (mult * (((1.0 - t) * ((SELECT O_f FROM parms) + IF(inc, (SELECT S_f FROM parms), 0.0))) + R)) AS expected_return,
            sum_pf * (mult * (((1.0 - t) * ((SELECT O_f FROM parms) + IF(inc, (SELECT S_f FROM parms), 0.0))) + R)) - CAST(S AS FLOAT64) AS expected_profit,
            SAFE_DIVIDE(sum_pf, NULLIF(sum_p, 0)) AS f_share_used
          FROM agg
        );
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

            sql = f"""
            CREATE OR REPLACE VIEW `{ds}.vw_superfecta_runner_training_features` AS
            SELECT
              tr.event_id,
              tr.product_id,
              tr.event_date,
              tr.venue,
              tr.country,
              tr.total_net,
              tr.going,
              tr.weather_temp_c,
              tr.weather_wind_kph,
              tr.weather_precip_mm,
              tr.horse_id,
              tr.cloth_number,
              tr.finish_pos,
              tr.status,
              f.recent_runs,
              f.avg_finish,
              f.wins_last5,
              f.places_last5,
              f.days_since_last_run,
              f.weight_kg,
              f.weight_lbs
            FROM `{ds}.vw_superfecta_training` tr
            LEFT JOIN `{ds}.features_runner_event` f
              ON tr.event_id = f.event_id AND tr.horse_id = f.horse_id;
            """
            job = client.query(sql)
            job.result()

        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_selection_status_current` AS
        SELECT
          product_id,
          selection_id,
          UPPER(status) AS status
        FROM `{ds}.tote_selection_status_log`
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY product_id, selection_id
          ORDER BY ts_ms DESC
        ) = 1;
        """
        job = client.query(sql); job.result()

        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_superfecta_runner_live_features` AS
        WITH selection_map AS (
          SELECT product_id, selection_id, SAFE_CAST(number AS INT64) AS cloth_number
          FROM `{ds}.tote_product_selections`
        ), selection_status AS (
          SELECT product_id, selection_id, status
          FROM `{ds}.vw_selection_status_current`
        )
        SELECT
          p.product_id,
          f.event_id,
          DATE(SUBSTR(COALESCE(te.start_iso, p.start_iso), 1, 10)) AS event_date,
          p.event_name,
          COALESCE(te.venue, p.venue) AS venue,
          te.country,
          p.start_iso,
          COALESCE(p.status, te.status) AS status,
          p.total_net,
          COALESCE(rc.going, f.going) AS going,
          COALESCE(rc.weather_temp_c, f.weather_temp_c) AS weather_temp_c,
          COALESCE(rc.weather_wind_kph, f.weather_wind_kph) AS weather_wind_kph,
          COALESCE(rc.weather_precip_mm, f.weather_precip_mm) AS weather_precip_mm,
          f.horse_id,
          f.cloth_number,
          f.recent_runs,
          f.avg_finish,
          f.wins_last5,
          f.places_last5,
          f.days_since_last_run,
          f.weight_kg,
          f.weight_lbs
        FROM `{ds}.tote_products` p
        JOIN `{ds}.features_runner_event` f ON f.event_id = p.event_id AND f.horse_id IS NOT NULL
        LEFT JOIN selection_map sm
          ON sm.product_id = p.product_id AND sm.cloth_number = f.cloth_number
        LEFT JOIN selection_status ss
          ON ss.product_id = sm.product_id AND ss.selection_id = sm.selection_id
        LEFT JOIN `{ds}.race_conditions` rc ON rc.event_id = p.event_id
        LEFT JOIN `{ds}.tote_events` te ON te.event_id = p.event_id
        WHERE UPPER(p.bet_type) = 'SUPERFECTA'
          AND (ss.status IS NULL OR ss.status NOT IN ('NON_RUNNER','NR','WITHDRAWN','SCRATCHED','RESERVE','NONRUNNER'));
        """
        job = client.query(sql); job.result()

        # Ensure features table carries optional columns referenced by ML views
        if self._table_exists("features_runner_event"):
            self._ensure_columns(
                "features_runner_event",
                {
                    "recent_runs": "INT64",
                    "avg_finish": "FLOAT64",
                    "wins_last5": "INT64",
                    "places_last5": "INT64",
                    "days_since_last_run": "INT64",
                    "weight_kg": "FLOAT64",
                    "weight_lbs": "FLOAT64",
                },
            )

        # vw_superfecta_training_base: historical runners with clean horse_ids for modeling
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_superfecta_training_base` AS
        SELECT
          p.event_id,
          p.product_id,
          DATE(SUBSTR(COALESCE(te.start_iso, p.start_iso), 1, 10)) AS event_date,
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
        WHERE UPPER(p.bet_type) = 'SUPERFECTA'
          AND r.finish_pos IS NOT NULL
          AND r.horse_id IS NOT NULL;
        """
        job = client.query(sql)
        job.result()

        # Latest model probabilities enriched with product context
        model_dataset = os.getenv("BQ_MODEL_DATASET", f"{self.dataset}_model")
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_superfecta_predictions_latest` AS
        WITH ranked AS (
          SELECT
            product_id,
            event_id,
            horse_id,
            runner_key,
            p_place1,
            model_id,
            model_version,
            scored_at,
            ROW_NUMBER() OVER (
              PARTITION BY product_id, horse_id
              ORDER BY scored_at DESC
            ) AS rn
          FROM `{self.project}.{model_dataset}.superfecta_runner_predictions`
        ), selection_map AS (
          SELECT product_id, selection_id, SAFE_CAST(number AS INT64) AS cloth_number
          FROM `{ds}.tote_product_selections`
        ), selection_status AS (
          SELECT product_id, selection_id, status
          FROM `{ds}.vw_selection_status_current`
        )
        SELECT
          r.product_id,
          r.event_id,
          DATE(SUBSTR(COALESCE(tp.start_iso, te.start_iso), 1, 10)) AS event_date,
          COALESCE(tp.event_name, te.name) AS event_name,
          COALESCE(te.venue, tp.venue) AS venue,
          te.country,
          tp.start_iso,
          COALESCE(tp.status, te.status) AS status,
          tp.currency,
          SAFE_CAST(tp.total_gross AS FLOAT64) AS total_gross,
          SAFE_CAST(tp.total_net AS FLOAT64) AS total_net,
          SAFE_CAST(tp.rollover AS FLOAT64) AS rollover,
          SAFE_CAST(tp.deduction_rate AS FLOAT64) AS deduction_rate,
          r.horse_id,
          h.name AS horse_name,
          fe.cloth_number,
          fe.recent_runs,
          fe.avg_finish,
          fe.wins_last5,
          fe.places_last5,
          fe.days_since_last_run,
          fe.weight_kg,
          fe.weight_lbs,
          fe.going,
          r.p_place1,
          r.model_id,
          r.model_version,
          r.scored_at
        FROM ranked r
        JOIN `{ds}.tote_products` tp
          ON tp.product_id = r.product_id AND tp.event_id = r.event_id
        LEFT JOIN `{ds}.tote_events` te ON te.event_id = r.event_id
        LEFT JOIN `{ds}.hr_horses` h ON h.horse_id = r.horse_id
        LEFT JOIN `{ds}.features_runner_event` fe
          ON fe.event_id = r.event_id AND fe.horse_id = r.horse_id
        LEFT JOIN selection_map sm
          ON sm.product_id = r.product_id AND sm.cloth_number = fe.cloth_number
        LEFT JOIN selection_status ss
          ON ss.product_id = sm.product_id AND ss.selection_id = sm.selection_id
        WHERE r.rn = 1
          AND (ss.status IS NULL OR ss.status NOT IN ('NON_RUNNER','NR','WITHDRAWN','SCRATCHED','RESERVE','NONRUNNER'));
        """
        job = client.query(sql); job.result()

        # Maintain backward-compatible name for downstream consumers
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_superfecta_training` AS
        SELECT * FROM `{ds}.vw_superfecta_training_base`;
        """
        job = client.query(sql)
        job.result()

        # vw_superfecta_runner_training_features: labeled runners joined with feature table
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_superfecta_runner_training_features` AS
        SELECT
          base.event_id,
          base.product_id,
          base.event_date,
          base.venue,
          base.country,
          base.total_net,
          base.going,
          base.weather_temp_c,
          base.weather_wind_kph,
          base.weather_precip_mm,
          base.horse_id,
          base.cloth_number,
          base.finish_pos,
          base.status,
          feat.recent_runs,
          feat.avg_finish,
          feat.wins_last5,
          feat.places_last5,
          feat.days_since_last_run,
          feat.weight_kg,
          feat.weight_lbs
        FROM `{ds}.vw_superfecta_training_base` base
        LEFT JOIN `{ds}.features_runner_event` feat
          ON base.event_id = feat.event_id AND base.horse_id = feat.horse_id;
        """
        job = client.query(sql)
        job.result()

        # vw_superfecta_runner_live_features: live products enriched with runner features
        sql = f"""
        CREATE OR REPLACE VIEW `{ds}.vw_superfecta_runner_live_features` AS
        SELECT
          p.product_id,
          feat.event_id,
          DATE(SUBSTR(COALESCE(te.start_iso, p.start_iso), 1, 10)) AS event_date,
          p.event_name,
          COALESCE(te.venue, p.venue) AS venue,
          te.country,
          p.start_iso,
          COALESCE(p.status, te.status) AS status,
          p.total_net,
          COALESCE(rc.going, feat.going) AS going,
          COALESCE(rc.weather_temp_c, feat.weather_temp_c) AS weather_temp_c,
          COALESCE(rc.weather_wind_kph, feat.weather_wind_kph) AS weather_wind_kph,
          COALESCE(rc.weather_precip_mm, feat.weather_precip_mm) AS weather_precip_mm,
          feat.horse_id,
          feat.cloth_number,
          feat.recent_runs,
          feat.avg_finish,
          feat.wins_last5,
          feat.places_last5,
          feat.days_since_last_run,
          feat.weight_kg,
          feat.weight_lbs
        FROM `{ds}.tote_products` p
        JOIN `{ds}.features_runner_event` feat
          ON feat.event_id = p.event_id AND feat.horse_id IS NOT NULL
        LEFT JOIN `{ds}.race_conditions` rc ON rc.event_id = p.event_id
        LEFT JOIN `{ds}.tote_events` te ON te.event_id = p.event_id
        WHERE UPPER(p.bet_type) = 'SUPERFECTA';
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


_BQ_SINK_SINGLETON: Optional[BigQuerySink] = None
_BQ_SINK_LOCK = threading.Lock()


def get_bq_sink() -> BigQuerySink | None:
    if not cfg.bq_write_enabled:
        return None
    if not cfg.bq_project or not cfg.bq_dataset:
        return None

    global _BQ_SINK_SINGLETON
    if _BQ_SINK_SINGLETON is not None:
        return _BQ_SINK_SINGLETON

    with _BQ_SINK_LOCK:
        if _BQ_SINK_SINGLETON is None:
            try:
                _BQ_SINK_SINGLETON = BigQuerySink(cfg.bq_project, cfg.bq_dataset, cfg.bq_location)
                if cfg.bq_ensure_on_boot:
                    try:
                        _BQ_SINK_SINGLETON.ensure_views()
                    except Exception as exc:
                        print(f"Failed to ensure BigQuery views: {exc}")
                else:
                    print("Skipping BigQuery ensure_views on boot (BQ_ENSURE_ON_BOOT=false).")
            except Exception:
                _BQ_SINK_SINGLETON = None
        return _BQ_SINK_SINGLETON
