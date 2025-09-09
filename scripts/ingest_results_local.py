from __future__ import annotations

"""Ingest event results for a given date directly into BigQuery.

Usage:
  python autobet/scripts/ingest_results_local.py --date YYYY-MM-DD

Reads the `results(date:, first:, after:)` connection and writes rows to
`tote_event_results_log` via BigQuerySink. If available, it also writes
event status changes to `tote_event_status_log`.
"""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.bq import get_bq_sink
from sports.providers.tote_api import ToteClient

RESULTS_QUERY = (
    Path(ROOT / "sql" / "tote_results.graphql").read_text(encoding="utf-8")
    if (ROOT / "sql" / "tote_results.graphql").exists()
    else """
query ResultsByDate($date: String!, $first: Int!, $after: String) {
  results(date: $date, first: $first, after: $after) {
    edges {
      cursor
      node {
        eventId
        competitorId
        finishingPosition
        status
        ts
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""
)


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Tote results by date into BigQuery")
    ap.add_argument("--date", required=True)
    ap.add_argument("--first", type=int, default=500)
    args = ap.parse_args()

    sink = get_bq_sink()
    if not sink:
        raise SystemExit("BigQuery not configured (set BQ_PROJECT/BQ_DATASET and enable writes)")
    client = ToteClient()

    after = None
    total = 0
    while True:
        vars = {"date": args.date, "first": args.first}
        if after:
            vars["after"] = after
        data = client.graphql(RESULTS_QUERY, vars)
        res = (data.get("results") or {})
        edges = res.get("edges") or []
        rows = []
        for e in edges:
            n = (e or {}).get("node") or {}
            eid = n.get("eventId")
            cid = n.get("competitorId")
            fp = n.get("finishingPosition")
            st = n.get("status")
            ts = n.get("ts")
            if not eid or not ts:
                continue
            try:
                ts_ms = int(ts)
            except Exception:
                # Attempt ISO -> ms if needed
                import datetime
                try:
                    dt = datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    ts_ms = int(dt.timestamp() * 1000)
                except Exception:
                    continue
            rows.append({
                "event_id": str(eid),
                "ts_ms": ts_ms,
                "competitor_id": str(cid) if cid is not None else "",
                "finishing_position": int(fp) if fp is not None else None,
                "status": str(st) if st is not None else None,
            })
        if rows:
            sink.upsert_tote_event_results_log(rows)
            total += len(rows)
        page = res.get("pageInfo") or {}
        has_next = bool(page.get("hasNextPage"))
        after = page.get("endCursor")
        if not has_next or not after:
            break

    print(f"Ingested {total} result rows for {args.date}")


if __name__ == "__main__":
    main()

