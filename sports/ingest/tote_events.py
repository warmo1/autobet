from __future__ import annotations
import json
from typing import Any, Dict, List
from ..providers.tote_api import ToteClient, ToteError
from ..bq import BigQuerySink
import time

EVENTS_QUERY = """
query GetEvents($since: DateTime, $until: DateTime, $first: Int, $after: String) {
  events(first: $first, after: $after, since: $since, until: $until) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      venue { name country { alpha2Code } }
      scheduledStartDateTime { iso8601 }
      status
      result { status }
      eventCompetitors {
        nodes {
          id
          name
          entryStatus
          details { __typename ... on HorseDetails { clothNumber } ... on GreyhoundDetails { trapNumber } }
        }
      }
    }
  }
}
"""

def ingest_tote_events(
    db: BigQuerySink,
    client: ToteClient,
    first: int = 100,
    since_iso: str | None = None,
    until_iso: str | None = None,
) -> int:
    """
    Ingests event data from the Tote GraphQL API into BigQuery.
    This includes event details, competitor lists, and results if available.
    """
    print(f"Ingesting events from Tote API (since: {since_iso}, until: {until_iso})")
    variables: Dict[str, Any] = {"first": first}
    if since_iso:
        variables["since"] = since_iso
    if until_iso:
        variables["until"] = until_iso

    all_nodes = []
    page = 1
    while True:
        print(f"Fetching page {page}...")
        try:
            data = client.graphql(EVENTS_QUERY, variables)
        except Exception as e:
            print(f"Failed to fetch events from Tote API: {e}")
            break

        nodes = (data.get("events", {}).get("nodes", []))
        if not nodes:
            print("No more events found.")
            break
        all_nodes.extend(nodes)

        page_info = data.get("events", {}).get("pageInfo", {})
        if page_info.get("hasNextPage") and page_info.get("endCursor"):
            variables["after"] = page_info["endCursor"]
            page += 1
        else:
            break

    if not all_nodes:
        return 0

    rows_events: List[Dict[str, Any]] = []
    rows_runs: List[Dict[str, Any]] = []
    rows_horses: List[Dict[str, Any]] = []
    rows_competitors_log: List[Dict[str, Any]] = []
    rows_status_log: List[Dict[str, Any]] = []
    ts_ms = int(time.time() * 1000)

    for ev in all_nodes:
        event_id = ev.get("id")
        if not event_id:
            continue

        competitors = (ev.get("eventCompetitors") or {}).get("nodes") or []
        # Build a quick map of competitor -> cloth/trap number for finishing order enrichment
        cloth_map = {}
        for c in competitors:
            try:
                cid = c.get("id")
                det = (c.get("details") or {})
                num = None
                if det.get("__typename") == "HorseDetails":
                    num = det.get("clothNumber")
                elif det.get("__typename") == "GreyhoundDetails":
                    num = det.get("trapNumber")
                if cid and num is not None:
                    cloth_map[str(cid)] = int(num)
            except Exception:
                pass
        # Basic sport detection from competitor details
        sport_val = "horse_racing"
        try:
            for c in competitors:
                if (c.get("details") or {}).get("__typename") == "GreyhoundDetails":
                    sport_val = "greyhound_racing"; break
        except Exception:
            pass

        rows_events.append({
            "event_id": event_id, "name": ev.get("name"), "sport": sport_val,
            "venue": (ev.get("venue") or {}).get("name"), "country": ((ev.get("venue") or {}).get("country") or {}).get("alpha2Code"),
            "start_iso": (ev.get("scheduledStartDateTime") or {}).get("iso8601"), "status": ev.get("status"),
            "result_status": (ev.get("result") or {}).get("status"), "competitors_json": json.dumps(competitors), "source": "tote_api",
        })
        rows_competitors_log.append({"event_id": event_id, "ts_ms": ts_ms, "competitors_json": json.dumps(competitors)})
        # Status log for visibility on event state transitions
        if ev.get("status"):
            rows_status_log.append({"event_id": event_id, "ts_ms": ts_ms, "status": ev.get("status")})

        for comp in competitors:
            horse_id = comp.get("id")
            if horse_id:
                rows_horses.append({"horse_id": horse_id, "name": comp.get("name"), "country": ((ev.get("venue") or {}).get("country") or {}).get("alpha2Code")})

        # Finishing order (when exposed on Event)
        try:
            res_nodes = (((ev.get("result") or {}).get("results") or {}).get("nodes")) or []
            for res in res_nodes:
                cid = res.get("id")
                pos = res.get("finishingPosition")
                if cid and pos is not None:
                    rows_runs.append({
                        "horse_id": str(cid),
                        "event_id": event_id,
                        "finish_pos": int(pos),
                        "status": "RESULTED",
                        "cloth_number": cloth_map.get(str(cid)),
                        "recorded_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    })
        except Exception:
            pass

    db.upsert_tote_events(rows_events)
    if rows_horses: db.upsert_hr_horses(list({h['horse_id']: h for h in rows_horses}.values()))
    if rows_runs: db.upsert_hr_horse_runs(rows_runs)
    if rows_competitors_log: db.upsert_tote_event_competitors_log(rows_competitors_log)
    if rows_status_log: db.upsert_tote_event_status_log(rows_status_log)
    print(f"Successfully ingested {len(rows_events)} events, {len(rows_runs)} results, and {len(rows_competitors_log)} competitor logs.")
    return len(rows_events)
