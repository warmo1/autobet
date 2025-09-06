from __future__ import annotations
from typing import TYPE_CHECKING

from .bq import BigQuerySink
from .config import cfg

if TYPE_CHECKING:
    from .bq import BigQuerySink

_db: BigQuerySink | None = None

def get_db() -> BigQuerySink:
    """Returns a singleton BigQuerySink instance for read/write queries.

    Note: Unlike the writer helper, this does not require `BQ_WRITE_ENABLED`.
    It only requires `BQ_PROJECT` and `BQ_DATASET` to be set so the web app
    can read from BigQuery even in read-only scenarios.
    """
    global _db
    if _db is None:
        if not (cfg.bq_project and cfg.bq_dataset):
            raise RuntimeError(
                "BigQuery is not configured. Please set BQ_PROJECT and BQ_DATASET."
            )
        _db = BigQuerySink(cfg.bq_project, cfg.bq_dataset, cfg.bq_location)
    return _db

def init_db():
    """Initializes the database by ensuring all required views are created."""
    print("Initializing BigQuery database...")
    db = get_db()
    db.ensure_views()
    print("Database initialization complete.")
