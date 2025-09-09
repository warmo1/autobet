from __future__ import annotations

"""Download Tote GraphQL SDL using the authenticated gateway endpoint.

Usage:
  python autobet/scripts/fetch_tote_schema.py [--out autobet/docs/tote_schema.graphqls]

Requires:
  - TOTE_GRAPHQL_URL (should point at https://.../partner/gateway/graphql)
  - TOTE_API_KEY
"""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.providers.tote_api import ToteClient


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Tote GraphQL SDL (schema)")
    ap.add_argument("--out", default=str(ROOT / "docs" / "tote_schema.graphqls"))
    args = ap.parse_args()

    c = ToteClient()
    sdl = c.graphql_sdl()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(sdl, encoding="utf-8")
    print(f"Wrote SDL to {out}")


if __name__ == "__main__":
    main()

