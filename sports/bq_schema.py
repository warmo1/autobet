import os
import csv
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
            table_details = client.get_table(table.reference)
            for col in table_details.schema:
                writer.writerow({
                    'table_name': table.table_id,
                    'table_type': table.table_type,
                    'column_name': col.name,
                    'data_type': col.field_type,
                    'description': col.description,
                    'mode': col.mode
                })

    print("Successfully exported BigQuery schema to bq_schema.csv")
