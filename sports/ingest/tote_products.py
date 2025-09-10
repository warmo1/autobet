from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..providers.tote_api import ToteClient
from ..bq import BigQuerySink

# Full products query including legs, selections (runners), pool totals and result dividends.
# Mirrors autobet/sql/tote_products.graphql so the web refresh ingestor pulls runners as well.
PRODUCTS_QUERY = """
query GetProducts($date: Date, $status: BettingProductSellingStatus, $first: Int, $betTypes: [BetTypeCode!]) {
  products(date: $date, betTypes: $betTypes, sellingStatus: $status, first: $first) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      # Shape A: node is BettingProduct
      ... on BettingProduct {
        country { alpha2Code }
        betType { code rules { bet { min { decimalAmount currency { code } } max { decimalAmount currency { code } } } line { min { decimalAmount currency { code } } max { decimalAmount currency { code } } increment { decimalAmount currency { code } } } } }
        selling { status }
        pool {
          total { grossAmount { decimalAmount } netAmount { decimalAmount } }
          carryIn { grossAmount { decimalAmount } netAmount { decimalAmount } }
          takeout { percentage }
          funds {
            total { grossAmount { decimalAmount } netAmount { decimalAmount } }
            carryIn { grossAmount { decimalAmount } netAmount { decimalAmount } }
          }
        }
        legs {
          nodes {
            event { id name venue { name } scheduledStartDateTime { iso8601 } }
            selections { nodes { id competitor { name details { __typename ... on HorseDetails { clothNumber } ... on GreyhoundDetails { trapNumber } } } } }
          }
        }
        lines {
          nodes {
            legs { lineSelections { selectionId } } odds { decimal }
          }
        }
        result {
          status
          dividends {
            nodes {
              dividend { amount { decimalAmount } }
              dividendLegs { nodes { dividendSelections { nodes { id finishingPosition } } } }
            }
          }
        }
      }
      # Shape B: node has a type wrapper exposing BettingProduct
      type {
        ... on BettingProduct {
          country { alpha2Code }
          betType { code rules { bet { min { decimalAmount currency { code } } max { decimalAmount currency { code } } } line { min { decimalAmount currency { code } } max { decimalAmount currency { code } } increment { decimalAmount currency { code } } } } }
          selling { status }
          pool {
            total { grossAmount { decimalAmount } netAmount { decimalAmount } }
            carryIn { grossAmount { decimalAmount } netAmount { decimalAmount } }
            takeout { percentage }
            funds {
              total { grossAmount { decimalAmount } netAmount { decimalAmount } }
              carryIn { grossAmount { decimalAmount } netAmount { decimalAmount } }
            }
          }
          legs {
            nodes {
              event { id name venue { name } scheduledStartDateTime { iso8601 } }
              selections { nodes { id competitor { name details { __typename ... on HorseDetails { clothNumber } ... on GreyhoundDetails { trapNumber } } } } }
            }
          }
        lines {
          nodes {
            legs { lineSelections { selectionId } } odds { decimal }
          }
        }
          result {
            status
            dividends {
              nodes {
                dividend { amount { decimalAmount } }
                dividendLegs { nodes { dividendSelections { nodes { id finishingPosition } } } }
              }
            }
          }
        }
      }
    }
  }
}
"""

PRODUCT_BY_ID_QUERY = """
query GetProduct($id: String!) {
  product(id: $id) {
    id
    ... on BettingProduct {
      country { alpha2Code }
      betType { code rules { bet { min { decimalAmount currency { code } } max { decimalAmount currency { code } } } line { min { decimalAmount currency { code } } max { decimalAmount currency { code } } increment { decimalAmount currency { code } } } } }
      selling { status }
      pool {
        total { grossAmount { decimalAmount } netAmount { decimalAmount } }
        carryIn { grossAmount { decimalAmount } netAmount { decimalAmount } }
        takeout { percentage }
        funds {
          total { grossAmount { decimalAmount } netAmount { decimalAmount } }
          carryIn { grossAmount { decimalAmount } netAmount { decimalAmount } }
        }
      }
      legs { nodes { id event { id name venue { name } scheduledStartDateTime { iso8601 } } selections { nodes { id competitor { name details { __typename ... on HorseDetails { clothNumber } ... on GreyhoundDetails { trapNumber } } } } } } }
      result { status dividends { nodes { dividend { amount { decimalAmount } } dividendLegs { nodes { dividendSelections { nodes { id finishingPosition } } } } } } }
    }
    type { ... on BettingProduct { country { alpha2Code } betType { code } selling { status } legs { nodes { event { id name venue { name } scheduledStartDateTime { iso8601 } } selections { nodes { id competitor { name details { __typename ... on HorseDetails { clothNumber } ... on GreyhoundDetails { trapNumber } } } } } } } } }
  }
}
"""

def ingest_products(db: BigQuerySink, client: ToteClient, date_iso: str | None, status: str | None, first: int, bet_types: list[str] | None, product_ids: list[str] | None = None) -> int:
    """
    Ingests product data from the Tote API into BigQuery.
    """
    print(f"Ingesting products for date={date_iso}, status={status}, bet_types={bet_types}, product_ids={product_ids}")
    variables: Dict[str, Any] = {"first": first}
    if date_iso:
        variables["date"] = date_iso
    if status:
        variables["status"] = status
    if bet_types:
        variables["betTypes"] = bet_types
    products_nodes: List[Dict[str, Any]] = []
    if product_ids:
        # Fetch by ID one-by-one via product(id:)
        for pid in product_ids:
            try:
                data = client.graphql(PRODUCT_BY_ID_QUERY, {"id": pid})
                node = data.get("product")
                if node:
                    products_nodes.append(node)
            except Exception as e:
                print(f"Failed to fetch product {pid} from Tote API: {e}")
    else:
        try:
            page = 1
            while True:
                data = client.graphql(PRODUCTS_QUERY, variables)
                nodes = (data.get("products", {}).get("nodes", []))
                if nodes:
                    products_nodes.extend(nodes)
                page_info = (data.get("products", {}).get("pageInfo", {}) if isinstance(data, dict) else {})
                if page_info.get("hasNextPage") and page_info.get("endCursor"):
                    variables["after"] = page_info["endCursor"]
                    page += 1
                else:
                    break
            print(f"Successfully fetched {len(products_nodes)} products from Tote API for date: {date_iso}")
        except Exception as e:
            print(f"Failed to fetch product data from Tote API: {e}")
            return 0
    if not products_nodes:
        print("No products found.")
        return 0

    # Store the raw payload containing probable odds for this batch
    try:
        import time as _t
        ts_ms = int(_t.time() * 1000)
        payload_for_raw = {"products": {"nodes": products_nodes}}
        # Generate an ID based on the request params
        req_id_part = date_iso or "live"
        if bet_types:
            req_id_part += "_" + "_".join(bet_types)
        rid = f"probable:{req_id_part}:{ts_ms}"
        
        db.upsert_raw_tote_probable_odds([
            {
                "raw_id": rid,
                "fetched_ts": ts_ms,
                "payload": json.dumps(payload_for_raw),
                "product_id": None # This is a batch, not for a single product
            }
        ])
        print("Stored raw probable odds payload.")
    except Exception as e:
        print(f"Warning: failed to store raw probable odds payload: {e}")

    rows_products: List[Dict[str, Any]] = []
    rows_selections: List[Dict[str, Any]] = []
    rows_events: List[Dict[str, Any]] = []
    rows_rules: List[Dict[str, Any]] = []
    rows_dividends: List[Dict[str, Any]] = []
    rows_finishing_positions: List[Dict[str, Any]] = []
    for p in products_nodes:
        # Map current schema (BettingProduct fragment), supporting two shapes:
        # 1) fields directly on node
        # 2) fields under node.type (union wrapper)
        bp = p or {}
        src = (bp.get("type") or bp)
        legs = ((src.get("legs") or {}).get("nodes") or []) if "legs" in src else []
        event = None
        if legs:
            evt = (legs[0].get("event") or {})
            event = {
                "id": evt.get("id"),
                "name": evt.get("name"),
                "venue": ((evt.get("venue") or {}).get("name")),
                "start_iso": ((evt.get("scheduledStartDateTime") or {}).get("iso8601")),
            }
            # Collect event row (minimal) for tote_events
            rows_events.append({
                "event_id": event.get("id"),
                "name": event.get("name"),
                "sport": "horse_racing",
                "venue": event.get("venue"),
                "country": ((src.get("country") or {}).get("alpha2Code")),
                "start_iso": event.get("start_iso"),
                # Use product selling status as a proxy for event status when available
                "status": ((src.get("selling") or {}).get("status")),
                "result_status": (src.get("result") or {}).get("status"),
                "competitors_json": None,
                "comp": None,
                "home": None,
                "away": None,
                "source": "tote_api",
            })
        # Pool totals
        pool = (src.get("pool") or {})
        total = (pool.get("total") or {})
        gross = ((total.get("grossAmount") or {}).get("decimalAmount"))
        net = ((total.get("netAmount") or {}).get("decimalAmount"))
        carry_in = (pool.get("carryIn") or {})
        rollover = ((carry_in.get("grossAmount") or {}).get("decimalAmount"))
        takeout = (pool.get("takeout") or {})
        deduction_rate = takeout.get("percentage")
        # Fallback: sum pool.funds totals when aggregate totals are missing/zero
        try:
            funds = pool.get("funds") or []
            def _sum_funds(path_keys: list[str]) -> float | None:
                tot = 0.0; seen = False
                for f in funds:
                    cur = f
                    ok = True
                    for k in path_keys:
                        cur = (cur.get(k) if isinstance(cur, dict) else None)
                        if cur is None:
                            ok = False; break
                    if ok and cur is not None:
                        try:
                            tot += float(cur)
                            seen = True
                        except Exception:
                            pass
                return tot if seen else None
            # If gross missing or zero, try funds.total.grossAmount.decimalAmount
            if gross in (None, 0, 0.0):
                gross_f = _sum_funds(["total","grossAmount","decimalAmount"])
                if gross_f is not None:
                    gross = gross_f
            if net in (None, 0, 0.0):
                net_f = _sum_funds(["total","netAmount","decimalAmount"])
                if net_f is not None:
                    net = net_f
            if rollover in (None, 0, 0.0):
                ro_f = _sum_funds(["carryIn","grossAmount","decimalAmount"])
                if ro_f is not None:
                    rollover = ro_f
        except Exception:
            pass

        # Product fields
        bt_obj = (src.get("betType") or {})
        bet_type = bt_obj.get("code")
        status_v = ((src.get("selling") or {}).get("status"))
        country = ((src.get("country") or {}).get("alpha2Code"))

        rows_products.append({
            "product_id": (bp.get("id") or src.get("id")),
            "event_id": (event or {}).get("id"),
            "bet_type": bet_type,
            "status": status_v,
            # Using alpha2 country code in currency column to preserve existing filters
            "currency": country,
            "start_iso": (event or {}).get("start_iso"),
            "total_gross": float(gross) if gross is not None else None,
            "total_net": float(net) if net is not None else None,
            "rollover": float(rollover) if rollover is not None else None,
            "deduction_rate": float(deduction_rate) if deduction_rate is not None else None,
            "event_name": (event or {}).get("name"),
            "venue": (event or {}).get("venue"),
            "source": "tote_api",
        })

        # Collect selections for each leg
        for idx, leg in enumerate(legs, start=1):
            product_leg_id = leg.get("productLegId") or leg.get("id")
            levt = leg.get("event") or {}
            l_ev_id = levt.get("id")
            l_ev_name = levt.get("name")
            l_ev_venue = ((levt.get("venue") or {}).get("name"))
            l_ev_start = ((levt.get("scheduledStartDateTime") or {}).get("iso8601"))
            sels = ((leg.get("selections") or {}).get("nodes")) or []
            for sel in sels:
                sid = sel.get("id")
                comp = (sel.get("competitor") or {})
                det = (comp.get("details") or {})
                n = None
                try:
                    if det.get("__typename") == "HorseDetails":
                        n = det.get("clothNumber")
                    elif det.get("__typename") == "GreyhoundDetails":
                        n = det.get("trapNumber")
                    elif "clothNumber" in det:
                        n = det.get("clothNumber")
                except Exception:
                    n = None
                rows_selections.append({
                    "product_id": (bp.get("id") or src.get("id")),
                    "leg_index": idx,
                    "selection_id": sid,
                    "product_leg_id": product_leg_id,
                    "competitor": comp.get("name"),
                    "number": int(n) if n is not None else None,
                    "leg_event_id": l_ev_id,
                    "leg_event_name": l_ev_name,
                    "leg_venue": l_ev_venue,
                    "leg_start_iso": l_ev_start,
                })

        # Bet rules (if present)
        try:
            rules = bt_obj.get("rules") or {}
            bet = rules.get("bet") or {}
            line = rules.get("line") or {}
            def _amt(node):
                if not isinstance(node, dict):
                    return None, None
                try:
                    return float((node.get("decimalAmount") or 0)), (node.get("currency") or {}).get("code")
                except Exception:
                    return None, (node.get("currency") or {}).get("code")
            bet_min, bet_cur = _amt(bet.get("min"))
            bet_max, _ = _amt(bet.get("max"))
            line_min, line_cur = _amt(line.get("min"))
            line_max, _ = _amt(line.get("max"))
            inc, _ = _amt(line.get("increment"))
            if any(v is not None for v in (bet_min, bet_max, line_min, line_max, inc)):
                rows_rules.append({
                    "product_id": (bp.get("id") or src.get("id")),
                    "bet_type": bet_type,
                    "currency": bet_cur or line_cur,
                    "min_bet": bet_min,
                    "max_bet": bet_max,
                    "min_line": line_min,
                    "max_line": line_max,
                    "line_increment": inc,
                })
        except Exception:
            pass

        # Dividends (if present)
        res = (src.get("result") or {})
        dvs = ((res.get("dividends") or {}).get("nodes")) or []
        # Use event start time or current time as ts
        import time as _t
        ts_iso = (event or {}).get("start_iso") or _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime()) # noqa
        for d in dvs:
            amt = (((d.get("dividend") or {}).get("amount") or {}).get("decimalAmount"))
            dlegs = ((d.get("dividendLegs") or {}).get("nodes")) or []
            for dl in dlegs:
                dsel = ((dl.get("dividendSelections") or {}).get("nodes")) or []
                for ds in dsel:
                    sel_id = ds.get("id") # This is the selection_id
                    finish_pos = ds.get("finishingPosition")
                    
                    # Store dividend if present
                    if sel_id and amt is not None and float(amt) > 0:
                        rows_dividends.append({
                            "product_id": (bp.get("id") or src.get("id")),
                            "selection": sel_id,
                            "dividend": float(amt),
                            "ts": ts_iso,
                        })
                    # Store finishing position if present
                    if sel_id and finish_pos is not None:
                        rows_finishing_positions.append({
                            "product_id": (bp.get("id") or src.get("id")),
                            "event_id": (event or {}).get("id"),
                            "selection_id": sel_id,
                            "finish_pos": int(finish_pos),
                            "ts": ts_iso,
                        })
    
    try:
        if rows_products:
            # Deduplicate by product_id within this batch
            _pmap: Dict[str, Dict[str, Any]] = {}
            for r in rows_products:
                pid = r.get("product_id")
                if pid:
                    _pmap[pid] = r
            prod_rows = list(_pmap.values()) if _pmap else rows_products
            print(f"Inserting {len(prod_rows)} rows into tote_products")
            db.upsert_tote_products(prod_rows)
        if rows_selections:
            # Deduplicate by (product_id, leg_index, selection_id)
            seen_sel = set()
            sel_rows: List[Dict[str, Any]] = []
            for r in rows_selections:
                k = (r.get("product_id"), r.get("leg_index"), r.get("selection_id"))
                if k in seen_sel:
                    continue
                seen_sel.add(k)
                sel_rows.append(r)
            print(f"Inserting {len(sel_rows)} rows into tote_product_selections")
            db.upsert_tote_product_selections(sel_rows)
        if rows_dividends:
            # Deduplicate by (product_id, selection, ts)
            _dmap: Dict[tuple, Dict[str, Any]] = {}
            for r in rows_dividends:
                k = (r.get("product_id"), r.get("selection"), r.get("ts"))
                if k in _dmap:
                    try:
                        if float(r.get("dividend") or 0) > float(_dmap[k].get("dividend") or 0):
                            _dmap[k] = r
                    except Exception:
                        _dmap[k] = r
                else:
                    _dmap[k] = r
            div_rows = list(_dmap.values()) if _dmap else rows_dividends
            print(f"Inserting {len(div_rows)} rows into tote_product_dividends")
            db.upsert_tote_product_dividends(div_rows)
        if rows_events:
            # De-dup rows by event_id to reduce upsert work
            dedup = {}
            for r in rows_events:
                if r.get("event_id"):
                    dedup[r["event_id"]] = r
            ev_rows = list(dedup.values()) if dedup else rows_events
            print(f"Inserting {len(ev_rows)} rows into tote_events")
            db.upsert_tote_events(ev_rows)
        if rows_rules:
            # Deduplicate by product_id
            _rmap: Dict[str, Dict[str, Any]] = {}
            for r in rows_rules:
                pid = r.get("product_id")
                if pid:
                    _rmap[pid] = r
            rules_rows = list(_rmap.values())
            print(f"Inserting {len(rows_rules)} rows into tote_bet_rules")
            try:
                db.upsert_tote_bet_rules(rules_rows)
            except Exception as ee:
                print(f"Warning: bet rules upsert failed: {ee}")
        if rows_finishing_positions:
            # Deduplicate by (product_id, selection_id)
            _fmap: Dict[tuple, Dict[str, Any]] = {}
            for r in rows_finishing_positions:
                k = (r.get("product_id"), r.get("selection_id"))
                _fmap[k] = r
            finish_rows = list(_fmap.values())
            print(f"Inserting {len(finish_rows)} rows into tote_horse_finishing_positions")
            db.upsert_tote_horse_finishing_positions(finish_rows)
        print("Successfully ingested product data.")
        return len(rows_products)
    except Exception as e:
        print(f"Failed to insert product data into BigQuery: {e}")
        return 0
