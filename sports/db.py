from __future__ import annotations
from typing import TYPE_CHECKING

from .bq import get_bq_sink, BigQuerySink

if TYPE_CHECKING:
    from .bq import BigQuerySink

_db: BigQuerySink | None = None

def get_db() -> BigQuerySink:
    """Returns a singleton BigQuerySink instance."""
    global _db
    if _db is None:
        _db = get_bq_sink()
        if not _db:
            raise RuntimeError(
                "BigQuery is not configured. Please set BQ_PROJECT and BQ_DATASET."
            )
    return _db

def init_db():
    """Initializes the database by ensuring all required views are created."""
    print("Initializing BigQuery database...")
    db = get_db()
    db.ensure_views()
    print("Database initialization complete.")