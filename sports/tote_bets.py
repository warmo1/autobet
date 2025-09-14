import json
import time
from typing import Optional, Dict, Any, List
import uuid

from .tote_api import ToteClient


def _now_ms() -> int:
    """Returns the current time in milliseconds."""
    return int(time.time() * 1000)


def _record_audit(db, *, product_id: str, selection: str, stake: float, currency: Optional[str], payload: Dict[str, Any], response: Optional[Dict[str, Any]], error: Optional[str]):
    """Records an audit bet attempt to the `tote_audit_bets` table in BigQuery."""
    bet_id = f"audit:{product_id}:{_now_ms()}"
    status = 'audit-error' if error else 'audit-recorded'
    
    # This table name matches the webapp's /audit/bets page
    table_id = "tote_audit_bets"
    
    rows_to_insert = [{
        "bet_id": bet_id,
        "ts_ms": _now_ms(),
        "mode": "audit",
        "product_id": product_id,
        "selection": selection,
        "stake": float(stake),
        "currency": currency,
        "status": status,
        "response_json": json.dumps({"request": payload, "response": response}),
        "error": error,
        "settled_ts": None,
        "outcome": None,
        "result_json": None,
    }]
    
    try:
        # Assumes `db` is a BigQuery client instance.
        # The webapp may need to create the table on first run if it doesn't exist.
        errors = db.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"[AuditRecord] BQ insert errors for {table_id}: {errors}")
    except Exception as e:
        print(f"[AuditRecord] BQ insert failed for {table_id}: {e}")

    return bet_id


def place_audit_simple_bet(db, *, product_id: str, selection_id: str, stake: float, currency: str, post: bool = True, client: Optional[ToteClient] = None) -> Dict[str, Any]:
    """Places a single-line audit bet (e.g., WIN, PLACE) using the correct v2 GraphQL schema."""
    try:
        bet_type, event_id = product_id.split(":", 1)
        bet_type = bet_type.upper()
    except (ValueError, IndexError):
        err = f"Invalid product_id format for simple bet: {product_id}"
        bet_id = _record_audit(db, product_id=product_id, selection=selection_id, stake=stake, currency=currency, payload={}, response=None, error=err)
        return {"bet_id": bet_id, "error": err}

    if bet_type not in ("WIN", "PLACE"):
        err = f"Unsupported bet type for simple placement: {bet_type}"
        bet_id = _record_audit(db, product_id=product_id, selection=selection_id, stake=stake, currency=currency, payload={}, response=None, error=err)
        return {"bet_id": bet_id, "error": err}

    # Correct v2 payload for a WIN/PLACE bet
    bet_obj = {
        "productId": product_id,
        "stake": {"amount": {"decimalAmount": stake}, "currency": currency},
        "legs": [{"productLegId": event_id, "selections": [{"productLegSelectionID": selection_id}]}]
    }

    payload = {"bets": [{"bet": bet_obj}]}
    variables = {"input": payload}
    
    resp = None
    err = None
    if post:
        try:
            if client is None:
                client = ToteClient()
            mutation = """
            mutation PlaceBets($input: PlaceBetsInput!) {
              placeBets(input: $input) {
                results { toteBetId status failureReason }
              }
            }
            """
            # Use graphql_audit for audit, but regular graphql for live
            if "audit" in (client.base_url or ""):
                resp = client.graphql_audit(mutation, variables)
            else:
                resp = client.graphql(mutation, variables)
        except Exception as e:
            err = str(e)
    
    placement_status, failure_reason = None, None
    if resp and isinstance(resp, dict):
        results = (resp.get("placeBets") or {}).get("results")
        if isinstance(results, list) and results:
            placement_status = results[0].get("status")
            failure_reason = results[0].get("failureReason")

    bet_id = _record_audit(db, product_id=product_id, selection=selection_id, stake=stake, currency=currency, payload=variables, response=resp, error=err)
    
    out = {"bet_id": bet_id, "error": err, "response": resp, "placement_status": placement_status}
    if failure_reason:
        out["failure_reason"] = failure_reason
    return out


def place_audit_superfecta(
    db,
    *,
    product_id: str,
    selection: Optional[str] = None,
    selections: Optional[list[str]] = None,
    stake: float,
    currency: str = "GBP",
    post: bool = False,
    stake_type: str = "total",
    placement_product_id: Optional[str] = None,
    client: Optional[ToteClient] = None,
) -> Dict[str, Any]:
    """Audit-mode Superfecta bet using the correct v2 GraphQL schema."""
    bet_lines: list[str] = []
    if selections and isinstance(selections, list):
        bet_lines = [str(s).strip() for s in selections if s and str(s).strip()]
    elif selection:
        bet_lines = [selection.strip()]

    # Map selection numbers to GraphQL selection IDs
    # This part of your logic with DB/API fallback is good and has been preserved.
    by_number: Dict[int, str] = {}
    product_leg_id: Optional[str] = None
    try:
        from .db import sql_df # Use the project's sql_df for BQ
        rows_df = sql_df("SELECT product_leg_id, selection_id, number, leg_index FROM tote_product_selections WHERE product_id=@pid", params={"pid": product_id})
        if not rows_df.empty:
            for _, r in rows_df.iterrows():
                if product_leg_id is None and r.get("product_leg_id"):
                    product_leg_id = r["product_leg_id"]
                if r.get("number") is not None and int(r.get("leg_index", 1)) == 1:
                    by_number[int(r["number"])] = r["selection_id"]
    except Exception as e:
        print(f"[AuditBet] DB selection mapping failed: {e}")

    if not by_number:
        try:
            if client is None:
                client = ToteClient()
            query = """
            query ProductLegs($id: String){ product(id: $id){ ... on BettingProduct { legs{ nodes{ id selections{ nodes{ id competitor{ details{ ... on HorseDetails { clothNumber } ... on GreyhoundDetails { trapNumber } } } } } } } } } }
            """
            data = client.graphql(query, {"id": product_id})
            legs = (((data.get("product") or {}).get("legs") or {}).get("nodes")) or []
            if legs:
                product_leg_id = product_leg_id or legs[0].get("id")
                sels = ((legs[0].get("selections") or {}).get("nodes")) or []
                for sel in sels:
                    sid = sel.get("id")
                    det = ((sel.get("competitor") or {}).get("details") or {})
                    n = det.get("clothNumber") or det.get("trapNumber")
                    if n is not None: by_number[int(n)] = sid
        except Exception as e:
            print(f"[AuditBet] API selection mapping fallback failed: {e}")

    def _line_to_legs(line: str) -> Optional[Dict[str, Any]]:
        try:
            parts = [int(x.strip()) for x in line.split('-') if x.strip()]
            if len(parts) < 4: return None
            sels = [{"productLegSelectionID": by_number.get(int(num)), "position": pos} for pos, num in enumerate(parts[:4], start=1)]
            if any(s["productLegSelectionID"] is None for s in sels): return None
            return {"productLegId": product_leg_id, "selections": sels}
        except (ValueError, KeyError):
            return None

    legs_list = [_line_to_legs(s) for s in bet_lines]
    if not product_leg_id or any(lg is None for lg in legs_list):
        missing = {str(n) for s in bet_lines for n in s.split('-') if n.strip().isdigit() and int(n) not in by_number}
        err_msg = f"Selection mapping failed for product {product_id}"
        if missing: err_msg += f": unknown numbers {{{','.join(sorted(missing))}}}"
        bet_id = _record_audit(db, product_id=product_id, selection=",".join(bet_lines), stake=stake, currency=currency, payload={}, response=None, error=err_msg)
        return {"bet_id": bet_id, "error": err_msg}

    used_product_id = placement_product_id or product_id
    
    # Build the v2 payload directly
    bets_v2 = []
    for lg in legs_list:
        # For multi-line bets, stake_type determines if stake is per line or total
        line_stake = stake if (stake_type or "line").lower() == "line" else stake / len(legs_list)
        bet_obj = {
            "productId": used_product_id,
            "stake": {"amount": {"decimalAmount": float(line_stake)}, "currency": currency},
            "legs": [lg] if lg else [],
        }
        bets_v2.append({"bet": bet_obj})
    
    payload = {"bets": bets_v2}
    variables = {"input": payload}

    resp, err = None, None
    if post:
        try:
            if client is None:
                client = ToteClient()
            mutation = """
            mutation PlaceBets($input: PlaceBetsInput!) {
              placeBets(input: $input) {
                results { toteBetId status failureReason }
              }
            }
            """
            if "audit" in (client.base_url or ""):
                resp = client.graphql_audit(mutation, variables)
            else:
                resp = client.graphql(mutation, variables)
        except Exception as e:
            err = str(e)
            print(f"[AuditBet][ERROR] Superfecta placement failed: {err}")

    placement_status, failure_reason = None, None
    if resp and isinstance(resp, dict):
        results = (resp.get("placeBets") or {}).get("results")
        if isinstance(results, list) and results:
            # For multiple lines, report the status of the first one
            placement_status = results[0].get("status")
            failure_reason = results[0].get("failureReason")

    sel_str = ",".join(bet_lines)
    stored_req = {
        "original_product_id": product_id,
        "placement_product_id": used_product_id,
        "lines": len(bet_lines),
        "stake": stake,
        "currency": currency,
        "variables": variables,
    }
    bet_id = _record_audit(db, product_id=product_id, selection=sel_str, stake=stake, currency=currency, payload=stored_req, response=resp, error=err)
    
    out = {"bet_id": bet_id, "error": err, "response": resp, "placement_status": placement_status}
    if failure_reason:
        out["failure_reason"] = failure_reason
    return out

def refresh_bet_status(db, *, bet_id: str, post: bool = True, client: Optional[ToteClient] = None) -> Dict[str, Any]:
    """Refresh the status of a single bet by its Tote ID."""
    if client is None:
        client = ToteClient()
    query = "query Bet($id: String!){ bet(id: $id){ status settledTimeUTC result } }"
    variables = {"id": bet_id}
    resp, err = None, None
    if post:
        try:
            resp = client.graphql(query, variables)
        except Exception as e:
            err = str(e)
    return {"response": resp, "error": err}

def audit_list_bets(client: ToteClient, *, since_iso: Optional[str] = None, until_iso: Optional[str] = None, first: int = 20) -> Dict[str, Any]:
    """List bets from the audit/live gateway."""
    query = """
    query Bets($first: Int, $since: DateTime, $until: DateTime){
      bets(first: $first, since: $since, until: $until){
        nodes{
          toteId status settledTimeUTC result
          legs{ nodes{ selections{ nodes{ productLegSelectionID position } } } }
        }
      }
    }
    """
    variables = {"first": first}
    if since_iso: variables["since"] = since_iso
    if until_iso: variables["until"] = until_iso
    return client.graphql(query, variables)

def sync_bets_from_api(db, data: Dict[str, Any]) -> int:
    """Sync bet outcomes from API response to BigQuery."""
    # This function's implementation would go here, likely upserting to tote_audit_bets.
    return 0