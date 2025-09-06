import json
import time
from typing import Optional, Dict, Any, List
import uuid

from ..config import cfg
from .tote_api import ToteClient


def _now_ms() -> int:
    return int(time.time() * 1000)


def _record_audit(conn, *, product_id: str, selection: str, stake: float, currency: Optional[str], payload: Dict[str, Any], response: Optional[Dict[str, Any]], error: Optional[str]):
    bet_id = f"{product_id}:{_now_ms()}"
    conn.execute(
        """
        INSERT OR REPLACE INTO tote_bets(bet_id,ts,mode,product_id,selection,stake,currency,status,return,response_json,error)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            bet_id,
            _now_ms(),
            'audit',
            product_id,
            selection,
            float(stake),
            currency,
            'audit-recorded' if error is None else 'audit-error',
            None,
            json.dumps({"request": payload, "response": response} if error is None else {"request": payload}),
            error,
        ),
    )
    return bet_id


def place_audit_superfecta(
    conn,
    *,
    product_id: str,
    selection: Optional[str] = None,
    selections: Optional[list[str]] = None,
    stake: float,
    currency: str = "GBP",
    post: bool = False,
    stake_type: str = "total",  # 'total' or 'line' for v1 schema
    placement_product_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Audit-mode Superfecta bet. Supports single selection or a list (multiple bets).

    - selection: string like "3-7-1-5" (ordered 1st-4th)
    - selections: list of such strings to place multiple lines in one ticket
    """
    bet_lines: list[str] = []
    if selections and isinstance(selections, list):
        bet_lines = [s.strip() for s in selections if s and s.strip()]
    elif selection:
        bet_lines = [selection.strip()]
    else:
        bet_lines = []
    # Map selection strings to GraphQL legs structure using tote_product_selections
    rows = conn.execute(
        "SELECT product_leg_id, selection_id, number, leg_index FROM tote_product_selections WHERE product_id=?",
        (product_id,)
    ).fetchall()
    by_number: Dict[int, str] = {}
    product_leg_id: Optional[str] = None
    for r in rows:
        pli, sid, num, li = r[0], r[1], r[2], r[3]
        if product_leg_id is None and pli:
            product_leg_id = pli
        try:
            if num is not None and int(li) == 1:  # single leg assumed
                by_number[int(num)] = sid
        except Exception:
            pass
    print(f"[AuditBet] by_number from DB: {by_number}")
    # If mapping missing, fetch live from Tote GraphQL (handles WEB_USE_BIGQUERY case where SQLite lacks selections)
    if not by_number:
        try:
            client = ToteClient()
            query = """
            query ProductLegs($id: String){
              product(id: $id){
                ... on BettingProduct {
                  legs{ nodes{ id selections{ nodes{ id competitor{ name details{ __typename ... on HorseDetails { clothNumber } ... on GreyhoundDetails { trapNumber } } } } } } }
                }
              }
            }
            """
            data = client.graphql(query, {"id": product_id})
            prod = data.get("product") or {}
            legs = ((prod.get("legs") or {}).get("nodes")) or []
            # Use first leg
            if legs:
                product_leg_id = product_leg_id or legs[0].get("id")
                li = 1
                sels = ((legs[0].get("selections") or {}).get("nodes")) or []
                for sel in sels:
                    sid = sel.get("id")
                    comp = (sel.get("competitor") or {})
                    det = (comp.get("details") or {})
                    n = det.get("clothNumber") if det.get("__typename") == "HorseDetails" else det.get("trapNumber")
                    try:
                        if n is not None:
                            n_int = int(n)
                            by_number[n_int] = sid
                            # Cache locally for future requests
                            try:
                                conn.execute(
                                    """
                                    INSERT OR REPLACE INTO tote_product_selections(product_id,leg_index,product_leg_id,selection_id,competitor,number)
                                    VALUES(?,?,?,?,?,?)
                                    """,
                                    (product_id, li, product_leg_id, sid, comp.get("name"), n_int),
                                )
                                conn.commit()
                            except Exception:
                                pass
                    except Exception:
                        pass
        except Exception as e:
            print(f"[AuditBet] API fallback error: {e}")
            pass
    print(f"[AuditBet] by_number after API fallback: {by_number}")
    def _line_to_legs(line: str) -> Optional[Dict[str, Any]]:
        try:
            parts = [int(x.strip()) for x in line.split('-') if x.strip()]
            if len(parts) < 4:
                return None
            sels: List[Dict[str, Any]] = []
            for pos, num in enumerate(parts[:4], start=1):
                sid = by_number.get(int(num))
                if not sid:
                    return None
                sels.append({"productLegSelectionID": sid, "position": pos})
            return {
                "productLegId": product_leg_id,
                "selections": sels,
            }
        except Exception:
            return None
    legs = []
    for s in bet_lines:
        lg = _line_to_legs(s)
        if not lg:
            continue
        legs.append(lg)
    if bet_lines and (not product_leg_id or not legs or any(lg is None for lg in legs)):
        # Do not submit if mapping failed; surface a clear error with missing numbers
        missing: List[str] = []
        for s in bet_lines:
            try:
                parts = [int(x.strip()) for x in s.split('-') if x.strip()]
                for n in parts[:4]:
                    if n not in by_number:
                        missing.append(str(n))
            except Exception:
                pass
        err_msg = "selection mapping failed"
        if missing:
            err_msg += f": unknown numbers {{{','.join(sorted(set(missing)))}}} for product {product_id}"
        # Record audit row with error and return early
        bet_id = _record_audit(
            conn,
            product_id=product_id,
            selection=",".join(bet_lines),
            stake=stake,
            currency=currency,
            payload={"bets": [], "error": err_msg},
            response=None,
            error=err_msg,
        )
        return {"bet_id": bet_id, "error": err_msg}
    # Choose product id used for placement (may differ from the graph id)
    used_product_id = (placement_product_id or product_id)

    # Build bets arrays for both schema variants
    # v2 (docs/tote_queries.graphql): PlaceBetsInput with results[]
    bets_v2: List[Dict[str, Any]] = []
    for s in bet_lines:
        lg = _line_to_legs(s)
        bet_obj = {
            "productId": used_product_id,
            "stake": {
                "amount": {"decimalAmount": float(stake)},
                "currency": currency,
            },
            "legs": [lg] if lg else [],
        }
        bets_v2.append({"bet": bet_obj})
    payload_v2 = {"bets": bets_v2}

    # v1 (older audit schema): ticket + bets with currencyCode/totalAmount/lineAmount
    bets_v1: List[Dict[str, Any]] = []
    for s in bet_lines:
        lg = _line_to_legs(s)
        # Provide either totalAmount OR lineAmount, not both (per audit schema)
        if (stake_type or "total").lower() == "line":
            stake_obj = {"currencyCode": currency, "lineAmount": float(stake)}
        else:
            stake_obj = {"currencyCode": currency, "totalAmount": float(stake)}
        bets_v1.append({
            "betId": f"bet-superfecta-{uuid.uuid4()}",
            "productId": used_product_id,
            "stake": stake_obj,
            "legs": [lg] if lg else [],
        })
    resp = None
    err = None
    used_schema = None
    sent_variables = None
    if post:
        try:
            client = ToteClient()
            # Try v1 (ticket) first for compatibility with audit endpoint
            mutation_v1 = """
            mutation PlaceBets($input: PlaceBetsInput!) {
              placeBets(input:$input){
                ticket{
                  id
                  toteId
                  idempotent
                  bets{
                    nodes{
                      id
                      toteId
                      placement{ status rejectionReason }
                    }
                  }
                }
              }
            }
            """
            variables_v1 = {"input": {"ticketId": f"ticket-{uuid.uuid4()}", "bets": bets_v1}}
            try:
                resp = client.graphql_audit(mutation_v1, variables_v1)
                used_schema = "v1"
                sent_variables = variables_v1
            except Exception as e1:
                # Fallback to v2 (results[])
                mutation_v2 = """
                mutation PlaceBets($input: PlaceBetsInput!) {
                  placeBets(input: $input) {
                    results { toteBetId status failureReason }
                  }
                }
                """
                variables_v2 = {"input": payload_v2}
                try:
                    resp = client.graphql_audit(mutation_v2, variables_v2)
                    used_schema = "v2"
                    sent_variables = variables_v2
                except Exception as e2:
                    err = str(e2)
                    try:
                        print("[AuditBet][ERROR] v1 variables:", json.dumps(variables_v1)[:400])
                        print("[AuditBet][ERROR] v1 exception:", str(e1))
                        print("[AuditBet][ERROR] v2 variables:", json.dumps(variables_v2)[:400])
                        print("[AuditBet][ERROR] v2 exception:", err)
                    except Exception:
                        pass
        except Exception as e:
            err = str(e)
            try:
                # Log debug info to server output to aid troubleshooting
                print("[AuditBet][ERROR] unexpected exception")
                print("[AuditBet][ERROR] exception:", err)
            except Exception:
                pass
    # Parse placement status (if any); attach to response for visibility
    placement_status = None
    failure_reason = None
    try:
        if resp and isinstance(resp, dict):
            if used_schema == "v2":
                results = ((resp.get("placeBets") or {}).get("results"))
                if isinstance(results, list) and results:
                    placement_status = results[0].get("status")
                    failure_reason = results[0].get("failureReason")
            else:
                ticket = ((resp.get("placeBets") or {}).get("ticket")) or ((resp.get("ticket")) if "ticket" in resp else None)
                if ticket and isinstance(ticket, dict):
                    bets_node = ((ticket.get("bets") or {}).get("nodes"))
                    if isinstance(bets_node, list) and bets_node:
                        placement = (bets_node[0].get("placement") or {})
                        placement_status = placement.get("status")
                        # Rejection reason (if available on placement)
                        failure_reason = placement.get("rejectionReason") or failure_reason
    except Exception:
        placement_status = None
    # Record synthetic audit entry (for multiples, join selections)
    sel_str = ",".join(bet_lines) if bet_lines else (selection or "")
    # Persist the request variables we sent (or prepared if not posted)
    if not sent_variables:
        # prepare v1 variables even if not posted
        sent_variables = {"input": {"ticketId": f"ticket-{uuid.uuid4()}", "bets": bets_v1}}
    stored_req = {
        "schema": used_schema or "v1",
        "original_product_id": product_id,
        "placement_product_id": used_product_id,
        "lines": len(bet_lines),
        "stake": stake,
        "currency": currency,
        "variables": sent_variables,
    }
    bet_id = _record_audit(conn, product_id=product_id, selection=sel_str, stake=stake, currency=currency, payload=stored_req, response=resp, error=err)
    out = {"bet_id": bet_id, "error": err, "response": resp, "placement_status": placement_status}
    if failure_reason:
        out["failure_reason"] = failure_reason
    return out


def place_audit_win(conn, *, event_id: str, selection_id: str, stake: float, currency: str = "GBP", post: bool = False) -> Dict[str, Any]:
    payload = {
        "eventId": event_id,
        "selectionId": selection_id,
        "stake": f"{stake:.2f}",
        "currency": currency,
        "partnerId": None,
        "audit": True,
    }
    resp = None
    err = None
    if post:
        try:
            client = ToteClient()
            mutation = """
            mutation PlaceBets($input: PlaceBetsInput!) {
              placeBets(input:$input){
                ticket{ id toteId idempotent bets{ nodes{ id toteId placement{ status } } } }
              }
            }
            """
            # TODO: Align with official WIN PlaceBetInput when confirmed
            variables = {"input": {"ticketId": f"ticket-{uuid.uuid4()}", "bets": [{
                "betId": f"bet-win-{uuid.uuid4()}",
                "productId": f"WIN:{event_id}",
                "stake": {"currencyCode": currency, "totalAmount": float(stake), "lineAmount": float(stake)},
                "legs": [],
            }]}}
            resp = client.graphql_audit(mutation, variables)
        except Exception as e:
            err = str(e)
    # Use synthetic product id for WIN audit record (event scoped)
    bet_id = _record_audit(conn, product_id=f"WIN:{event_id}", selection=selection_id, stake=stake, currency=currency, payload=payload, response=resp, error=err)
    return {"bet_id": bet_id, "error": err, "response": resp}


def refresh_bet_status(conn, *, bet_id: str, post: bool = False) -> Dict[str, Any]:
    """Attempt to refresh bet status from Tote API if provider bet id is available in response_json.

    For audit-mode, uses test headers if post=True. If no provider id, marks as 'pending' and returns.
    """
    row = conn.execute("SELECT response_json, mode FROM tote_bets WHERE bet_id=?", (bet_id,)).fetchone()
    if not row:
        return {"error": "unknown bet_id"}
    resp_json = row[0]
    mode = row[1]
    provider_id = None
    try:
        if resp_json:
            d = json.loads(resp_json)
            # Try common keys
            provider_id = d.get('response',{}).get('betId') or d.get('response',{}).get('id') or d.get('betId')
    except Exception:
        provider_id = None
    status_payload = None; err = None
    if provider_id and post:
        try:
            client = ToteClient()
            # Use connections GraphQL for status, if available
            # Placeholder query; adjust to real schema
            query = """
            query Ticket($id: ID!){
              ticket(id:$id){ id toteId bets{ nodes{ id toteId placement{ status } } } }
            }
            """
            try:
                # Try audit endpoint first for audit bets
                if mode == 'audit':
                    status_payload = client.graphql_audit(query, {"id": provider_id})
                else:
                    status_payload = client.graphql(query, {"id": provider_id})
            except Exception:
                status_payload = None
        except Exception as e:
            err = str(e)
    # Update DB
    outcome = None
    settled_ts = None
    if status_payload:
        try:
            outcome = status_payload.get('status') or status_payload.get('outcome')
            settled_ts = _now_ms()
        except Exception:
            pass
    conn.execute(
        "UPDATE tote_bets SET result_json=COALESCE(result_json, ?), outcome=COALESCE(?, outcome), settled_ts=COALESCE(?, settled_ts) WHERE bet_id=?",
        (json.dumps(status_payload) if status_payload else None, outcome, settled_ts, bet_id)
    )
    return {"bet_id": bet_id, "provider_id": provider_id, "error": err, "status": status_payload, "outcome": outcome}


def audit_list_bets(*, since_iso: Optional[str] = None, until_iso: Optional[str] = None, first: int = 20) -> Dict[str, Any]:
    """Query audit GraphQL for recent bets in a window (best-effort)."""
    client = ToteClient()
    query = """
    query GetBets($since: DateTime, $until: DateTime, $first: Int){
      bets(since:$since, until:$until, first:$first){
        pageInfo{ startCursor endCursor hasNextPage hasPreviousPage }
        nodes{
          id
          toteId
          placement{ status stake{ currency{ code } decimalAmount } }
          ticket{ id toteId }
        }
      }
    }
    """
    vars: Dict[str, Any] = {"first": int(first)}
    if since_iso: vars["since"] = since_iso
    if until_iso: vars["until"] = until_iso
    try:
        data = client.graphql_audit(query, vars)
        return data or {}
    except Exception as e:
        return {"error": str(e)}


def sync_bets_from_api(conn, api_data: Dict[str, Any]) -> int:
    """Update tote_bets outcome/settled_ts by matching toteId from API data to response_json contents (best-effort)."""
    nodes = (((api_data or {}).get("bets") or {}).get("nodes")) if isinstance(api_data, dict) else None
    if nodes is None:
        return 0
    import time as _t
    updated = 0
    for n in nodes:
        try:
            bet_tid = n.get("toteId")
            status = ((n.get("placement") or {}).get("status"))
            if not bet_tid or not status:
                continue
            # Find rows whose response_json contains this toteId (very simple search)
            rows = conn.execute("SELECT bet_id, response_json FROM tote_bets WHERE response_json LIKE ?", (f"%{bet_tid}%",)).fetchall()
            for r in rows:
                bid = r[0]
                conn.execute(
                    "UPDATE tote_bets SET outcome=?, settled_ts=? WHERE bet_id=?",
                    (status, int(_t.time()*1000), bid)
                )
                updated += 1
        except Exception:
            continue
    conn.commit()
    return updated
