from __future__ import annotations

"""Probe the Tote GraphQL API to validate connectivity and discover usable fields
without relying on full introspection (many partners disable it).

Usage:
  python autobet/scripts/probe_tote_graphql.py
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.providers.tote_api import ToteClient, ToteError
from sports.config import cfg


def main() -> None:
    print("Configured endpoint:", cfg.tote_graphql_url)
    print("API key present:", bool(cfg.tote_api_key))
    c = ToteClient()
    print("Effective HTTP endpoint:", c.base_url)
    try:
        # Introspect Query type fields (may be disabled)
        introspect = '''
        query Q {
          __schema { queryType { name } }
          __type(name: "Query") { fields { name } }
        }
        '''
        data = c.graphql(introspect, {})
        fields = ((data.get("__type") or {}).get("fields") or [])
        print("Top-level Query fields:")
        for f in fields:
            print(f" - {f.get('name')}")
    except ToteError as e:
        print(f"Could not run introspection query, the endpoint may have it disabled: {e}")
        try:
            sdl = c.graphql_sdl()
            print(f"Fetched SDL via gateway (?sdl), length={len(sdl)} chars")
        except Exception as ee:
            print("SDL fetch via gateway failed:", ee)

    # Smoke test: __typename on Query
    try:
        t = c.graphql("query{ __typename }", {})
        print("Root typename:", t.get("__typename"))
    except Exception as e:
        print("__typename probe failed:", e)

    # Try common product/event listings
    probes = [
        ("products", "query($first:Int){ products(first:$first){ nodes { __typename id } pageInfo { hasNextPage endCursor } } }", {"first": 1}),
        ("bettingProducts", "query($first:Int){ bettingProducts(first:$first){ edges { node { __typename id } } pageInfo { hasNextPage endCursor } } }", {"first": 1}),
        ("discover", "query($sport: Sport!, $date: String!){ discover(filter:{sport:$sport, date:$date}){ events(first:1){ nodes { eventId eventName products { nodes { productId betType } } } } } }", {"sport": "HorseRacing", "date": "2025-01-01"}),
        ("events", "query($since: DateTime, $until: DateTime){ events(first:1, since:$since, until:$until){ nodes { id name venue { name } scheduledStartDateTime { iso8601 } } } }", {"since": "2025-01-01T00:00:00Z", "until": "2025-12-31T23:59:59Z"}),
    ]
    for name, q, vars in probes:
        try:
            d = c.graphql(q, vars)
            k = next(iter(d.keys())) if d else None
            print(f"Probe {name}: OK -> keys={list(d.keys())} first={d.get(k)}")
        except Exception as e:
            print(f"Probe {name}: FAIL -> {e}")


if __name__ == "__main__":
    main()
