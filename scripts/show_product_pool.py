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
    ... on BettingProduct {
      pool {
        total { 
          grossAmounts { decimalAmount currency { code } }
          netAmounts { decimalAmount currency { code } }
        }
        carryIn { 
          grossAmounts { decimalAmount currency { code } }
          netAmounts { decimalAmount currency { code } }
        }
        guarantee { 
          grossAmounts { decimalAmount currency { code } }
          netAmounts { decimalAmount currency { code } }
        }
        takeout { percentage }
        funds {
          total { 
            grossAmounts { decimalAmount currency { code } }
            netAmounts { decimalAmount currency { code } }
          }
          carryIn { 
            grossAmounts { decimalAmount currency { code } }
            netAmounts { decimalAmount currency { code } }
          }
        }
      }
      betType { code }
      selling { status }
    }
    type {
      ... on BettingProduct {
        pool {
          total { 
            grossAmounts { decimalAmount currency { code } }
            netAmounts { decimalAmount currency { code } }
          }
          carryIn { 
            grossAmounts { decimalAmount currency { code } }
            netAmounts { decimalAmount currency { code } }
          }
          guarantee { 
            grossAmounts { decimalAmount currency { code } }
            netAmounts { decimalAmount currency { code } }
          }
          takeout { percentage }
          funds {
            total { 
              grossAmounts { decimalAmount currency { code } }
              netAmounts { decimalAmount currency { code } }
            }
            carryIn { 
              grossAmounts { decimalAmount currency { code } }
              netAmounts { decimalAmount currency { code } }
            }
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

