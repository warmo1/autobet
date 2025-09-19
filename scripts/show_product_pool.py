from __future__ import annotations

"""Fetch a single product and print its pool totals (total, carryIn, funds).

Usage:
  python autobet/scripts/show_product_pool.py --id <PRODUCT_ID>
"""

import argparse
from pathlib import Path
import sys
import json

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sports.providers.tote_api import ToteClient

Q = """
query($id:String!){
  product(id:$id){
    id
    name
    type {
      ... on BettingProduct {
        pool {
          total { 
            grossAmount { decimalAmount }
            netAmount { decimalAmount }
          }
          carryIn { 
            grossAmount { decimalAmount }
            netAmount { decimalAmount }
          }
          takeout { 
            percentage
            amount { decimalAmount }
          }
        }
        betType { code }
        selling { status }
      }
    }
  }
}
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    args = ap.parse_args()
    c = ToteClient()
    d = c.graphql(Q, {"id": args.id})
    print(json.dumps(d, indent=2))


if __name__ == "__main__":
    main()

