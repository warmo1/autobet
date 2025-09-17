import csv
import json
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from .bq import get_bq_sink

def export_bq_schema_to_csv():
    load_dotenv()
    sink = get_bq_sink()
    if not sink:
        print("BigQuery sink not enabled/configured. Make sure .env is set up.")
        return

    client = sink._client_obj()
    dataset_ref = sink._bq.DatasetReference(sink.project, sink.dataset)
    
    with open('bq_schema.csv', 'w', newline='') as csvfile:
        fieldnames = ['table_name', 'table_type', 'column_name', 'data_type', 'description', 'mode']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for table in client.list_tables(dataset_ref):
            # Skip temporary staging tables
            if table.table_id.startswith('_tmp_'):
                continue
            table_details = client.get_table(table.reference)
            schema = table_details.schema or []
            # If a view has no explicit schema, skip columns but still emit a header row
            if not schema and table.table_type == 'VIEW':
                writer.writerow({
                    'table_name': table.table_id,
                    'table_type': table.table_type,
                    'column_name': '',
                    'data_type': '',
                    'description': '',
                    'mode': ''
                })
                continue
            for col in schema:
                writer.writerow({
                    'table_name': table.table_id,
                    'table_type': table.table_type,
                    'column_name': col.name,
                    'data_type': col.field_type,
                    'description': col.description,
                    'mode': col.mode
                })

    print("Successfully exported BigQuery schema to bq_schema.csv")


def export_bq_definitions(output_dir: str | Path = "bq_exports") -> Path | None:
    """
    Export DDL for tables, views, materialized views, and routines to local files.

    Returns the directory containing the export, or None if BigQuery is not configured.
    """

    load_dotenv()
    sink = get_bq_sink()
    if not sink:
        print("BigQuery sink not enabled/configured. Make sure .env is set up.")
        return None

    client = sink._client_obj()
    dataset_fq = f"{sink.project}.{sink.dataset}"

    base_dir = Path(output_dir)
    dataset_dir = base_dir / f"{sink.project}.{sink.dataset}"
    tables_dir = dataset_dir / "tables"
    views_dir = dataset_dir / "views"
    mviews_dir = dataset_dir / "materialized_views"
    routines_dir = dataset_dir / "routines"
    for folder in (tables_dir, views_dir, mviews_dir, routines_dir):
        folder.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, object] = {
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "project": sink.project,
        "dataset": sink.dataset,
        "paths": {
            "base": str(dataset_dir.resolve()),
            "tables": str(tables_dir.resolve()),
            "views": str(views_dir.resolve()),
            "materialized_views": str(mviews_dir.resolve()),
            "routines": str(routines_dir.resolve()),
        },
        "counts": {
            "tables": 0,
            "views": 0,
            "materialized_views": 0,
            "routines": 0,
            "other": 0,
        },
    }

    tables_sql = f"""
        SELECT table_name, table_type, ddl
        FROM `{dataset_fq}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
          AND NOT STARTS_WITH(table_name, '_tmp_')
        ORDER BY table_name
    """

    for row in client.query(tables_sql).result():
        ddl_text = (row.ddl or "").rstrip() + "\n"
        table_type = row.table_type.upper()
        if table_type == "BASE TABLE":
            out_path = tables_dir / f"{row.table_name}.sql"
            manifest["counts"]["tables"] += 1
        elif table_type == "VIEW":
            out_path = views_dir / f"{row.table_name}.sql"
            manifest["counts"]["views"] += 1
        elif table_type == "MATERIALIZED VIEW":
            out_path = mviews_dir / f"{row.table_name}.sql"
            manifest["counts"]["materialized_views"] += 1
        else:
            out_path = dataset_dir / f"{table_type.lower()}_{row.table_name}.sql"
            manifest["counts"]["other"] += 1
        out_path.write_text(ddl_text, encoding="utf-8")

    routines_sql = f"""
        SELECT routine_name,
               routine_type,
               routine_definition,
               specific_name,
               external_language
        FROM `{dataset_fq}.INFORMATION_SCHEMA.ROUTINES`
        WHERE routine_type IS NOT NULL
        ORDER BY routine_name
    """
    routine_rows = list(client.query(routines_sql).result())

    params_sql = f"""
        SELECT specific_name,
               ordinal_position,
               parameter_mode,
               parameter_name,
               data_type
        FROM `{dataset_fq}.INFORMATION_SCHEMA.PARAMETERS`
        ORDER BY specific_name, ordinal_position
    """
    params_map: dict[str, list] = {}
    for param in client.query(params_sql).result():
        params_map.setdefault(param.specific_name, []).append(param)

    for row in routine_rows:
        routine_type = (row.routine_type or "").upper()
        param_rows = params_map.get(row.specific_name, [])
        param_lines = []
        for p in param_rows:
            mode = (p.parameter_mode or "").upper()
            name = p.parameter_name or f"param_{p.ordinal_position}"
            dtype = p.data_type or "ANY TYPE"
            fragment = f"{name} {dtype}"
            if mode and mode not in ("IN",):
                fragment = f"{mode} " + fragment
            param_lines.append(f"  {fragment}")

        header = f"CREATE OR REPLACE {routine_type} `{dataset_fq}.{row.routine_name}`"
        if param_lines:
            header += "(\n" + ",\n".join(param_lines) + "\n)"

        body = (row.routine_definition or "-- Definition not available").strip()
        if body and not body.endswith("\n"):
            body += "\n"

        comment_lines = [f"-- Routine type: {routine_type}"]
        language = getattr(row, "external_language", None)
        if language:
            comment_lines.append(f"-- Language: {language}")

        routine_sql = "\n".join(comment_lines + [header, "AS", body])

        out_path = routines_dir / f"{row.routine_name}.sql"
        out_path.write_text(routine_sql, encoding="utf-8")
        manifest["counts"]["routines"] += 1

    manifest_path = dataset_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Exported BigQuery definitions to {dataset_dir}")
    return dataset_dir
