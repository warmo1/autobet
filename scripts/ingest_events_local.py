from __future__ import annotations

"""Directly ingest Tote events (rich fields) into BigQuery.

Usage:
  python autobet/scripts/ingest_events_local.py --date YYYY-MM-DD [--first 500]

Fetches events via the gateway GraphQL `events` field for the given date,
and upserts into BigQuery `tote_events`, including status/resultStatus and
`competitors_json` for analysis.
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.bq import get_bq_sink
from sports.providers.tote_api import ToteClient


QUERY = """
query EventsByDate($since: DateTime, $until: DateTime, $first: Int, $after: String) {
  events(since: $since, until: $until, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      status
      resultStatus
      scheduledStartDateTime { iso8601 }
      venue { name }
      country { alpha2Code }
      competitors {
        nodes {
          id
          name
        }
      }
    }
  }
}
"""


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Tote events for a date (BigQuery)")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--first", type=int, default=500)
    args = ap.parse_args()

    sink = get_bq_sink()
    if not sink:
        raise SystemExit("BigQuery not configured. Set BQ_* envs and enable writes.")
    c = ToteClient()

    day = args.date
    since = f"{day}T00:00:00Z"; until = f"{day}T23:59:59Z"
    after = None
    total = 0
    while True:
        vars = {"since": since, "until": until, "first": args.first}
        if after:
            vars["after"] = after
        d = c.graphql(QUERY, vars)
        evs = ((d.get("events") or {}).get("nodes")) or []
        rows = []
        for ev in evs:
            try:
                comps = (((ev.get("competitors") or {}).get("nodes")) or [])
            except Exception:
                comps = []
            rows.append({
                "event_id": ev.get("id"),
                "name": ev.get("name"),
                "sport": "horse_racing",
                "venue": ((ev.get("venue") or {}).get("name")),
                "country": ((ev.get("country") or {}).get("alpha2Code")),
                "start_iso": ((ev.get("scheduledStartDateTime") or {}).get("iso8601")),
                "status": ev.get("status"),
                "result_status": ev.get("resultStatus"),
                "competitors_json": json.dumps(comps, ensure_ascii=False),
                "source": "tote_events",
                "comp": None, "home": None, "away": None,
            })
        if rows:
            sink.upsert_tote_events(rows)
            total += len(rows)
        pi = ((d.get("events") or {}).get("pageInfo")) or {}
        if not bool(pi.get("hasNextPage")):
            break
        after = pi.get("endCursor")
        if not after:
            break

    print(f"Ingested/updated {total} event rows for {day}")


if __name__ == "__main__":
    main()

