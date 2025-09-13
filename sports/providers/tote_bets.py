from __future__ import annotations
import json
import time
from typing import Optional, Dict, Any, List
import os
import uuid

from ..config import cfg
from .tote_api import ToteClient

# BigQuery-only providers for audit and placement helpers.
# All persistence is done via the BigQuerySink in sports.bq.


def _now_ms() -> int:
    return int(time.time() * 1000)


def _dbg(msg: str) -> None:
    try:
        if str(os.getenv("TOTE_DEBUG", "0")).lower() in ("1","true","yes","on"):
            print(f"[AuditMapDBG] {msg}")
    except Exception:
        pass


def _record_audit_bq(
    sink,
    *,
    bet_id: str,
    mode: str,
    product_id: str,
    selection: str,
    stake: float,
    currency: Optional[str],
    payload: Dict[str, Any],
    response: Optional[Dict[str, Any]],
    error: Optional[str],
    placement_status: Optional[str] = None,
):
    """Helper to write an audit bet record to BigQuery."""
    row = {
        "bet_id": bet_id,
        "ts_ms": _now_ms(),
        "mode": mode,
        "product_id": product_id,
        "selection": selection,
        "stake": float(stake),
        "currency": currency,
        "status": placement_status or (f'{mode}-recorded' if error is None else f'{mode}-error'),
        "response_json": json.dumps({"request": payload, "response": response}),
        "error": error,
    }
    sink.upsert_tote_audit_bets([row])

def _bq_query_rows(sink, sql: str, params: Dict[str, Any] | None = None) -> list[dict]:
    """Run a parameterized query on BigQuery and return a list of dict rows."""
    from google.cloud import bigquery  # lazy import to avoid hard dependency at import time
    qp = []
    if params:
        for k, v in params.items():
            if isinstance(v, bool):
                qp.append(bigquery.ScalarQueryParameter(k, "BOOL", v))
            elif isinstance(v, int):
                qp.append(bigquery.ScalarQueryParameter(k, "INT64", v))
            elif isinstance(v, float):
                qp.append(bigquery.ScalarQueryParameter(k, "FLOAT64", v))
            else:
                qp.append(bigquery.ScalarQueryParameter(k, "STRING", None if v is None else str(v)))
    job_cfg = bigquery.QueryJobConfig(query_parameters=qp)
    rs = sink.query(sql, job_config=job_cfg)
    try:
        df = rs.to_dataframe(create_bqstorage_client=False)
        return ([] if df.empty else df.to_dict("records"))
    except Exception:
        out = []
        for r in rs:
            try:
                out.append(dict(r))
            except Exception:
                out.append({})
        return out


def _resolve_audit_ids_for_simple(
    sink,
    *,
    live_product_id: str,
    live_selection_id: str,
    trace: Optional[List[str]] = None,
) -> Optional[dict]:
    """Translate live product + selection IDs to audit-side IDs using event+betType and cloth/trap number.

    Returns dict with keys: product_id, product_leg_id, selection_id; or None if not resolved.
    """
    # 1) From BQ, find event_id, bet_type and the runner number for the chosen selection on live
    _dbg(f"map_simple: live_product_id={live_product_id}, live_selection_id={live_selection_id}")
    if trace is not None:
        trace.append(f"live_product_id={live_product_id}; live_selection_id={live_selection_id}")
    prod = _bq_query_rows(
        sink,
        "SELECT event_id, bet_type FROM tote_products WHERE product_id=@pid LIMIT 1",
        {"pid": live_product_id},
    )
    if not prod:
        _dbg("no tote_products row for live product")
        if trace is not None:
            trace.append("tote_products lookup returned 0 rows for live product")
        return None
    event_id = (prod[0] or {}).get("event_id")
    bet_type = (prod[0] or {}).get("bet_type")
    if not event_id or not bet_type:
        _dbg(f"missing event_id/bet_type from BQ: event_id={event_id}, bet_type={bet_type}")
        if trace is not None:
            trace.append(f"missing event_id or bet_type from BQ: event_id={event_id}, bet_type={bet_type}")
        return None
    sel = _bq_query_rows(
        sink,
        "SELECT number FROM tote_product_selections WHERE product_id=@pid AND selection_id=@sid LIMIT 1",
        {"pid": live_product_id, "sid": live_selection_id},
    )
    runner_num = None
    runner_name: Optional[str] = None
    if sel:
        try:
            rn = (sel[0] or {}).get("number")
            runner_num = int(rn) if rn is not None else None
        except Exception:
            runner_num = None
    _dbg(f"runner_num from BQ: {runner_num}")
    if trace is not None:
        trace.append(f"runner_num from DB: {runner_num}")
    # Fallback: query live GraphQL to derive the runner number for this selection id
    if runner_num is None:
        try:
            live_client = ToteClient()  # live
            q = (
                "query Product($id: String){ product(id:$id){ ... on BettingProduct { legs{ nodes{ selections{ nodes{ id eventCompetitor{ __typename ... on HorseRacingEventCompetitor{ clothNumber } ... on GreyhoundRacingEventCompetitor{ trapNumber } } competitor{ details{ __typename ... on HorseDetails{ clothNumber } ... on GreyhoundDetails{ trapNumber } } } } } } } } }"
            )
            d = live_client.graphql(q, {"id": live_product_id})
            prod_node = d.get("product") or {}
            legs = ((prod_node.get("legs") or {}).get("nodes")) or []
            for lg in legs:
                sels = ((lg.get("selections") or {}).get("nodes")) or []
                for s in sels:
                    if (s.get("id") or "") == live_selection_id:
                        # Try modern then legacy fields
                        evc = s.get("eventCompetitor") or {}
                        n = None
                        try:
                            tnm = evc.get("__typename")
                            if tnm == "HorseRacingEventCompetitor":
                                n = evc.get("clothNumber")
                            elif tnm == "GreyhoundRacingEventCompetitor":
                                n = evc.get("trapNumber")
                        except Exception:
                            n = None
                        if n is None:
                            det = ((s.get("competitor") or {}).get("details") or {})
                            tnm2 = det.get("__typename")
                            if tnm2 == "HorseDetails":
                                n = det.get("clothNumber")
                            elif tnm2 == "GreyhoundDetails":
                                n = det.get("trapNumber")
                        # Capture a fallback name for matching if numbers are absent
                        try:
                            runner_name = (
                                (evc.get("name") if isinstance(evc, dict) else None)
                                or s.get("name")
                                or ((s.get("competitor") or {}).get("name"))
                            )
                        except Exception:
                            runner_name = runner_name or None
                        try:
                            if n is not None:
                                runner_num = int(n)
                        except Exception:
                            runner_num = None
                        break
                if runner_num is not None:
                    break
        except Exception:
            runner_num = None
    if runner_num is None and (runner_name is None or not str(runner_name).strip()):
        _dbg("could not derive runner number or name from live GraphQL; aborting")
        if trace is not None:
            trace.append("could not derive runner number or name from live GraphQL")
        return None
    # 2) Query audit endpoint for the equivalent product on that event/betType
    client = ToteClient()
    # Support both legacy competitor{details{...}} and newer eventCompetitor structures
    query = (
        "query GetEventProducts($id: String){\n"
        "  event(id: $id){\n"
        "    products{\n"
        "      nodes{\n"
        "        id\n"
        "        ... on BettingProduct {\n"
        "          betType { code }\n"
        "          selling { status }\n"
        "          legs{ nodes{ id selections{ nodes{ id\n"
        "            eventCompetitor{ __typename id name entryStatus ... on HorseRacingEventCompetitor { clothNumber } ... on GreyhoundRacingEventCompetitor { trapNumber } }\n"
        "            competitor{ name details{ __typename ... on HorseDetails { clothNumber } ... on GreyhoundDetails { trapNumber } } }\n"
        "          } } } }\n"
        "        }\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "}"
    )
    try:
        _dbg(f"query audit event products for event_id={event_id}")
        data = client.graphql_audit(query, {"id": event_id})
        ev = (data or {}).get("event") or {}
        nodes = ((ev.get("products") or {}).get("nodes")) or []
        _dbg(f"audit event products returned {len(nodes)} nodes")
        if trace is not None:
            trace.append(f"audit event(id) returned {len(nodes)} products")
        # pick matching betType; prefer OPEN
        cand = None
        for n in nodes:
            bt = ((n.get("betType") or {}) or {}).get("code")
            selling_status = ((n.get("selling") or {}) or {}).get("status")
            if (bt or "").upper() == (bet_type or "").upper():
                ss = (selling_status or "").upper()
                if ss in ("OPEN", "SELLING"):
                    cand = n; break
                cand = cand or n
        # If we couldn't find a candidate by event id, try a date/venue match via products() list
        if not cand:
            _dbg("no audit product via event(id); trying products(date, betTypes) match by venue/time")
            try:
                # First, fetch live product context to get venue/start when event_id mapping may differ across environments
                live_client = ToteClient()  # live
                q_live = (
                    "query Product($id: String){ product(id:$id){ ... on BettingProduct { betType{ code } legs{ nodes{ event{ venue{ name } scheduledStartDateTime{ iso8601 } } } } } } }"
                )
                d_live = live_client.graphql(q_live, {"id": live_product_id})
                prod_live = d_live.get("product") or {}
                bt_live = (((prod_live.get("betType") or {}).get("code")) or bet_type)
                legs_live = ((prod_live.get("legs") or {}).get("nodes")) or []
                v_name = None; start_iso = None
                if legs_live:
                    evl = (legs_live[0].get("event") or {})
                    v_name = ((evl.get("venue") or {}).get("name"))
                    start_iso = ((evl.get("scheduledStartDateTime") or {}).get("iso8601"))
                _dbg(f"live context: venue={v_name}, start_iso={start_iso}, bt={bt_live}")
                if trace is not None:
                    trace.append(f"live product context: venue={v_name}, start={start_iso}, bet_type={bt_live}")
                # Fallback: if missing, use existing inputs
                bt_code = (bt_live or bet_type or "").upper()
                date_part = (start_iso or "")[:10]
                # Query audit products for that date and bet type
                q_audit = (
                    "query GetProducts($date: Date, $betTypes: [BetTypeCode!], $first: Int){\n"
                    "  products(date:$date, betTypes:$betTypes, first:$first){ nodes{ id ... on BettingProduct { betType{ code } selling{ status } legs{ nodes{ id event{ venue{ name } scheduledStartDateTime{ iso8601 } } selections{ nodes{ id eventCompetitor{ __typename ... on HorseRacingEventCompetitor{ clothNumber } ... on GreyhoundRacingEventCompetitor{ trapNumber } } competitor{ details{ __typename ... on HorseDetails{ clothNumber } ... on GreyhoundDetails{ trapNumber } } } } } } } } }\n"
                    "}"
                )
                vars = {"date": date_part or None, "betTypes": [bt_code] if bt_code else None, "first": 200}
                _dbg(f"query audit products for date={date_part}, betTypes={[bt_code] if bt_code else None}")
                d_a = client.graphql_audit(q_audit, vars)
                nodes2 = ((d_a.get("products") or {}).get("nodes")) or []
                _dbg(f"audit products returned {len(nodes2)} candidates for betType {bt_code}")
                if trace is not None:
                    trace.append(f"audit products(date, betTypes) candidates: {len(nodes2)}")
                # Match by venue name (case-insensitive) and exact start time if available
                for n in nodes2:
                    bt2 = ((n.get("betType") or {}).get("code") or "").upper()
                    if bt2 != bt_code:
                        continue
                    legs2 = ((n.get("legs") or {}).get("nodes")) or []
                    if not legs2:
                        continue
                    ev2 = (legs2[0].get("event") or {})
                    v2 = ((ev2.get("venue") or {}).get("name") or "")
                    s2 = ((ev2.get("scheduledStartDateTime") or {}).get("iso8601") or "")
                    ok = True
                    if v_name and v2 and (v_name.strip().lower() != v2.strip().lower()):
                        ok = False
                    # Allow exact match, or minute-level match, or within ~2 hours if venue matches
                    if start_iso and s2:
                        if start_iso == s2:
                            pass
                        elif start_iso[:16] == s2[:16]:  # match to minute
                            pass
                        else:
                            # Last resort: accept if within ~120 minutes
                            try:
                                from datetime import datetime
                                fmt = "%Y-%m-%dT%H:%M:%S%z" if "+" in s2[-6:] else "%Y-%m-%dT%H:%M:%SZ"
                                t_live = datetime.strptime(start_iso.replace("Z","+0000"), "%Y-%m-%dT%H:%M:%S%z")
                                t_a = datetime.strptime(s2.replace("Z","+0000"), "%Y-%m-%dT%H:%M:%S%z")
                                delta_min = abs((t_live - t_a).total_seconds()) / 60.0
                                if delta_min > 120:
                                    ok = False
                            except Exception:
                                ok = False
                    if ok:
                        cand = n; break
                # If still nothing, relax venue match and use runner number presence near time
                if not cand and (runner_num is not None) and start_iso:
                    try:
                        from datetime import datetime
                        t_live = datetime.strptime(start_iso.replace("Z","+0000"), "%Y-%m-%dT%H:%M:%S%z")
                    except Exception:
                        t_live = None
                    for n in nodes2:
                        bt2 = ((n.get("betType") or {}).get("code") or "").upper()
                        if bt2 != bt_code:
                            continue
                        legs2 = ((n.get("legs") or {}).get("nodes")) or []
                        if not legs2:
                            continue
                        ev2 = (legs2[0].get("event") or {})
                        s2 = ((ev2.get("scheduledStartDateTime") or {}).get("iso8601") or "")
                        within = False
                        if t_live and s2:
                            try:
                                t_a = datetime.strptime(s2.replace("Z","+0000"), "%Y-%m-%dT%H:%M:%S%z")
                                within = abs((t_live - t_a).total_seconds())/60.0 <= 180
                            except Exception:
                                within = False
                        if not within and start_iso and s2 and (start_iso[:16] == s2[:16]):
                            within = True
                        if not within:
                            continue
                        # Check selections for matching runner number
                        sels2 = ((legs2[0].get("selections") or {}).get("nodes")) or []
                        for s in sels2:
                            # prefer modern eventCompetitor then legacy details
                            nnum = None
                            try:
                                evc2 = (s.get("eventCompetitor") or {})
                                tnm = evc2.get("__typename")
                                if tnm == "HorseRacingEventCompetitor": nnum = evc2.get("clothNumber")
                                elif tnm == "GreyhoundRacingEventCompetitor": nnum = evc2.get("trapNumber")
                            except Exception:
                                nnum = None
                            if nnum is None:
                                det2 = ((s.get("competitor") or {}).get("details") or {})
                                tnm2 = det2.get("__typename")
                                if tnm2 == "HorseDetails": nnum = det2.get("clothNumber")
                                elif tnm2 == "GreyhoundDetails": nnum = det2.get("trapNumber")
                            try:
                                if nnum is not None and int(nnum) == runner_num:
                                    cand = n; break
                            except Exception:
                                pass
                        if cand:
                            break
            except Exception:
                cand = None
        if not cand:
            _dbg("no audit product candidate matched by venue/time")
            if trace is not None:
                trace.append("no audit product matched by venue/time window")
            return None
        audit_pid = cand.get("id")
        legs = ((cand.get("legs") or {}).get("nodes")) or []
        if not legs:
            _dbg("audit product has no legs")
            if trace is not None:
                trace.append("audit product has no legs")
            return None
        # Assume first leg and map by runner number
        leg = legs[0]
        audit_plid = leg.get("id")
        _dbg(f"audit candidate product_id={audit_pid}, product_leg_id={audit_plid}")
        if trace is not None:
            trace.append(f"audit candidate: product_id={audit_pid}, product_leg_id={audit_plid}")
        sels = ((leg.get("selections") or {}).get("nodes")) or []
        audit_sid = None
        # First, try to match by runner number if available
        for s in sels:
            # Try modern eventCompetitor fields first
            evc = (s.get("eventCompetitor") or {})
            n = None
            try:
                tnm = evc.get("__typename")
                if tnm == "HorseRacingEventCompetitor":
                    n = evc.get("clothNumber")
                elif tnm == "GreyhoundRacingEventCompetitor":
                    n = evc.get("trapNumber")
            except Exception:
                n = None
            # Fallback to legacy competitor.details
            if n is None:
                det = ((s.get("competitor") or {}).get("details") or {})
                tnm2 = det.get("__typename")
                if tnm2 == "HorseDetails":
                    n = det.get("clothNumber")
                elif tnm2 == "GreyhoundDetails":
                    n = det.get("trapNumber")
            try:
                if (runner_num is not None) and (n is not None) and int(n) == runner_num:
                    audit_sid = s.get("id"); break
            except Exception:
                continue
        # If number-based matching failed, try name-based matching
        if not audit_sid and runner_name:
            _dbg(f"number match failed; trying name match for '{runner_name}'")
            rn = str(runner_name).strip().lower()
            for s in sels:
                name_candidates = []
                try:
                    name_candidates.append((s.get("name") or ""))
                except Exception:
                    pass
                try:
                    name_candidates.append(((s.get("eventCompetitor") or {}).get("name") or ""))
                except Exception:
                    pass
                try:
                    name_candidates.append(((s.get("competitor") or {}).get("name") or ""))
                except Exception:
                    pass
                for nm in name_candidates:
                    if nm and str(nm).strip().lower() == rn:
                        audit_sid = s.get("id"); break
                if audit_sid:
                    break
        if not (audit_pid and audit_plid and audit_sid):
            _dbg("could not resolve audit selection id")
            if trace is not None:
                trace.append("could not resolve audit selection id from candidates")
            return None
        _dbg(f"resolved audit mapping: product={audit_pid}, leg={audit_plid}, sel={audit_sid}")
        if trace is not None:
            trace.append("success: mapped to audit product/leg/selection")
        return {"product_id": audit_pid, "product_leg_id": audit_plid, "selection_id": audit_sid}
    except Exception as e:
        _dbg(f"audit mapping exception: {e}")
        if trace is not None:
            trace.append(f"exception during audit mapping: {e}")
        return None


def place_audit_simple_bet(
    sink,  # BigQuerySink
    *,
    product_id: str,
    selection_id: str,
    stake: float,
    currency: str = "GBP",
    post: bool = True,
    client: ToteClient | None = None,
    mode: str = "audit",
) -> Dict[str, Any]:
    """Audit-mode for a simple single-leg, single-selection bet (e.g., WIN). Writes to BigQuery."""
    product_leg_id = None
    try:
        from google.cloud import bigquery
        # Prefer product_leg_id by product+leg, independent of selection (more robust)
        pli_df = sink.query(
            "SELECT product_leg_id FROM tote_product_selections WHERE product_id=@pid AND leg_index=1 LIMIT 1",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pid", "STRING", product_id),
            ])
        ).to_dataframe()
        if not pli_df.empty:
            product_leg_id = pli_df.iloc[0]['product_leg_id']
    except Exception as e:
        print(f"[AuditBet] Could not get product_leg_id from DB: {e}")

    # Fallback: derive leg by the chosen selection_id
    if not product_leg_id:
        try:
            from google.cloud import bigquery
            pli_df2 = sink.query(
                "SELECT product_leg_id FROM tote_product_selections WHERE product_id=@pid AND selection_id=@sid LIMIT 1",
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("pid", "STRING", product_id),
                    bigquery.ScalarQueryParameter("sid", "STRING", selection_id),
                ])
            ).to_dataframe()
            if not pli_df2.empty:
                product_leg_id = pli_df2.iloc[0]['product_leg_id']
        except Exception as e:
            print(f"[AuditBet] DB lookup by selection failed: {e}")

    if not product_leg_id:
        # Try resolving via GraphQL on the live endpoint (discovery only)
        try:
            query_client = ToteClient()  # live
            query = (
                "query ProductLegs($id: String){ product(id: $id){ ... on BettingProduct { legs{ nodes{ id selections{ nodes{ id } } } } } } }"
            )
            data = query_client.graphql(query, {"id": product_id})
            legs = (((data.get("product") or {}).get("legs") or {}).get("nodes")) or []
            # Prefer the leg that contains our selection
            for lg in legs:
                try:
                    sels = ((lg.get("selections") or {}).get("nodes")) or []
                    if any((s or {}).get("id") == selection_id for s in sels):
                        product_leg_id = lg.get("id"); break
                except Exception:
                    continue
            # If not found by selection membership, use the first leg as a fallback
            if not product_leg_id and legs:
                product_leg_id = legs[0].get("id")
        except Exception as e:
            print(f"[AuditBet] API fallback for product_leg_id failed: {e}")

    bet_id = f"{product_id}:{selection_id}:{_now_ms()}"

    if not product_leg_id:
        err_msg = f"Could not determine productLegId for product {product_id}"
        _record_audit_bq(sink, bet_id=bet_id, mode=mode, product_id=product_id, selection=selection_id, stake=stake, currency=currency,
                         payload={"error": err_msg}, response=None, error=err_msg)
        return {"bet_id": bet_id, "error": err_msg}

    # Choose placement IDs; translate to audit-side if needed
    used_product_id = product_id
    used_product_leg_id = product_leg_id
    used_selection_id = selection_id
    if str(mode).lower() == 'audit':
        mapped = _resolve_audit_ids_for_simple(sink, live_product_id=product_id, live_selection_id=selection_id)
        if mapped:
            used_product_id = mapped["product_id"]
            used_product_leg_id = mapped["product_leg_id"]
            used_selection_id = mapped["selection_id"]
            try:
                print(f"[AuditBet] ID mapping -> product:{used_product_id} leg:{used_product_leg_id} sel:{used_selection_id}")
            except Exception:
                pass
        else:
            try:
                print("[AuditBet] ID mapping failed; using live IDs against audit endpoint (likely to be rejected)")
            except Exception:
                pass
    # When querying product legs for audit, prefer the audit endpoint to avoid ID mismatches
    # (product and leg IDs should be the same, but Tote recommends querying from audit when testing)

    # Build v1 (ticket) and v2 (results) variants
    bet_v1 = {"betId": f"bet-simple-{uuid.uuid4()}", "productId": used_product_id, "stake": {"currencyCode": currency, "lineAmount": float(stake)}, "legs": [{"productLegId": used_product_leg_id, "selections": [{"productLegSelectionID": used_selection_id}]}]}
    bet_v2 = {"bet": {"productId": used_product_id, "stake": {"amount": {"decimalAmount": float(stake)}, "currency": currency}, "legs": [{"productLegId": used_product_leg_id, "selections": [{"productLegSelectionID": used_selection_id}]}]}}

    resp = None; err = None; sent_variables = None; used_schema = None
    if post:
        try:
            if not client:
                client = ToteClient()
            # Executor respects live vs audit
            def _exec(query: str, variables: dict):
                if str(mode).lower() == 'live':
                    return client.graphql(query, variables)
                else:
                    return client.graphql_audit(query, variables)

            # Try v1 first for audit compatibility
            mutation_v1 = """
            mutation PlaceBets($input: PlaceBetsInput!) {
              placeBets(input:$input){
                ticket{ id toteId bets{ nodes{ id toteId placement{ status rejectionReason } } } }
              }
            }
            """
            variables_v1 = {"input": {"ticketId": f"ticket-{uuid.uuid4()}", "bets": [bet_v1]}}
            try:
                resp = _exec(mutation_v1, variables_v1)
                sent_variables = variables_v1; used_schema = "v1"
            except Exception as e1:
                # Fallback to v2
                mutation_v2 = """
                mutation PlaceBets($input: PlaceBetsInput!) {
                  placeBets(input: $input) {
                    results { toteBetId status failureReason }
                  }
                }
                """
                variables_v2 = {"input": {"bets": [bet_v2]}}
                try:
                    resp = _exec(mutation_v2, variables_v2)
                    sent_variables = variables_v2; used_schema = "v2"
                except Exception as e2:
                    err = str(e2)
                    try:
                        print("[AuditBet][ERROR] simple v1 variables:", json.dumps(variables_v1)[:400])
                        print("[AuditBet][ERROR] simple v1 exception:", str(e1))
                        print("[AuditBet][ERROR] simple v2 variables:", json.dumps(variables_v2)[:400])
                        print("[AuditBet][ERROR] simple v2 exception:", err)
                    except Exception:
                        pass
        except Exception as e:
            err = str(e)

    placement_status = None; failure_reason = None
    try:
        if resp and isinstance(resp, dict):
            if used_schema == "v2":
                results = ((resp.get("placeBets") or {}).get("results"))
                if isinstance(results, list) and results:
                    placement_status = results[0].get("status"); failure_reason = results[0].get("failureReason")
            else:
                ticket = ((resp.get("placeBets") or {}).get("ticket")) or ((resp.get("ticket")) if "ticket" in resp else None)
                if ticket and isinstance(ticket, dict):
                    bets_node = ((ticket.get("bets") or {}).get("nodes"))
                    if isinstance(bets_node, list) and bets_node:
                        placement = (bets_node[0].get("placement") or {})
                        placement_status = placement.get("status"); failure_reason = placement.get("rejectionReason")
    except Exception:
        pass

    stored_req = {"schema": used_schema or "v1", "product_id": product_id, "stake": stake, "currency": currency, "variables": sent_variables}
    _record_audit_bq(sink, bet_id=bet_id, mode=mode, product_id=product_id, selection=selection_id, stake=stake, currency=currency, payload=stored_req, response=resp, error=err, placement_status=placement_status)
    
    out = {"bet_id": bet_id, "error": err, "response": resp, "placement_status": placement_status}
    if failure_reason: out["failure_reason"] = failure_reason
    return out


def place_audit_superfecta(
    sink,
    *,
    product_id: str,
    selection: Optional[str] = None,
    selections: Optional[list[str]] = None,
    stake: float,
    currency: str = "GBP",
    post: bool = False,
    stake_type: str = "total",  # 'total' or 'line' for v1 schema
    placement_product_id: Optional[str] = None,
    mode: str = "audit",
    client: ToteClient | None = None,
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

    # Calculate per-line stake
    num_lines = len(bet_lines)
    line_stake = 0.0
    if (stake_type or "total").lower() == "line":
        line_stake = float(stake)
    else:  # 'total'
        line_stake = float(stake) / num_lines if num_lines > 0 else 0.0

    # Determine required positions based on product bet type (EXACTA=2, TRIFECTA=3, SUPERFECTA=4)
    pos_required = 4
    try:
        row_bt = _bq_query_rows(
            sink,
            "SELECT UPPER(bet_type) AS bt FROM tote_products WHERE product_id=@pid LIMIT 1",
            {"pid": product_id},
        )
        bt = ((row_bt[0] or {}).get("bt") if row_bt else None) or "SUPERFECTA"
        if bt == "EXACTA":
            pos_required = 2
        elif bt == "TRIFECTA":
            pos_required = 3
        else:
            pos_required = 4
    except Exception:
        pos_required = 4

    # Map selection strings to GraphQL legs structure using tote_product_selections
    by_number: Dict[int, str] = {}
    product_leg_id: Optional[str] = None
    try:
        rows = _bq_query_rows(
            sink,
            "SELECT product_leg_id, selection_id, number, leg_index FROM tote_product_selections WHERE product_id=@pid",
            {"pid": product_id},
        )
        for r in rows:
            pli = r.get("product_leg_id")
            sid = r.get("selection_id")
            num = r.get("number")
            li = r.get("leg_index")
            if product_leg_id is None and pli:
                product_leg_id = pli
            try:
                if num is not None and int(li) == 1:
                    by_number[int(num)] = sid
            except Exception:
                pass
    except Exception:
        by_number = {}
    print(f"[AuditBet] by_number from DB: {by_number}")
    # If we have selection mapping but are missing the leg id, fetch it from live GraphQL
    if by_number and not product_leg_id:
        try:
            query_client = ToteClient()  # live
            query = "query ProductLegs($id: String){ product(id: $id){ ... on BettingProduct { legs{ nodes{ id } } } } }"
            data = query_client.graphql(query, {"id": product_id})
            legs = (((data.get("product") or {}).get("legs") or {}).get("nodes")) or []
            if legs:
                product_leg_id = legs[0].get("id")
        except Exception as e:
            try:
                print(f"[AuditBet] live leg lookup failed: {e}")
            except Exception:
                pass
    # If mapping missing, fetch live from Tote GraphQL (handles cases where selections were not yet persisted)
    if not by_number:
        try:
            # Always use the live endpoint for discovery (products/legs/selections)
            query_client = ToteClient()  # live
            query = """
            query ProductLegs($id: String){
                product(id: $id) {
                    ... on BettingProduct {
                        legs {
                            nodes {
                                id
                                selections {
                                    nodes {
                                        id
                                        eventCompetitor {
                                            ... on HorseRacingEventCompetitor { clothNumber }
                                            ... on GreyhoundRacingEventCompetitor { trapNumber }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """
            data = query_client.graphql(query, {"id": product_id})
            prod = data.get("product") or {}
            legs = ((prod.get("legs") or {}).get("nodes")) or []
            # Use first leg
            if legs:
                product_leg_id = product_leg_id or legs[0].get("id")
                li = 1
                sels = ((legs[0].get("selections") or {}).get("nodes")) or []
                for sel in sels:
                    sid = sel.get("id")
                    # Use new `eventCompetitor` structure
                    event_comp = sel.get("eventCompetitor") or {}
                    n = event_comp.get("clothNumber") or event_comp.get("trapNumber")

                    try:
                        if n is not None:
                            n_int = int(n)
                            by_number[n_int] = sid
                            # Cache locally for future requests
                            try:
                                sink.upsert_tote_product_selections([
                                    {
                                        "product_id": product_id,
                                        "leg_index": li,
                                        "product_leg_id": product_leg_id,
                                        "selection_id": sid,
                                        "competitor": None, # Name is not needed for this mapping
                                        "number": n_int,
                                    }
                                ])
                            except Exception:
                                pass
                    except Exception:
                        pass
        except Exception as e:
            print(f"[AuditBet] API fallback error: {e}")
            pass
    print(f"[AuditBet] by_number after API fallback: {by_number}")
    # Determine placement IDs based on mode
    used_product_id = (placement_product_id or product_id)
    audit_product_leg_id = product_leg_id
    by_number_map: Dict[int, str] = dict(by_number)
    if str(mode).lower() == "audit":
        # Resolve the corresponding audit product and build a number->selectionID map from audit
        audit_pid: Optional[str] = None
        audit_plid: Optional[str] = None
        by_number_audit: Dict[int, str] = {}
        try:
            # Get event/bet_type context from BQ
            ctx = _bq_query_rows(
                sink,
                "SELECT event_id, bet_type FROM tote_products WHERE product_id=@pid LIMIT 1",
                {"pid": product_id},
            )
            ev_id = (ctx[0] or {}).get("event_id") if ctx else None
            bt_code = ((ctx[0] or {}).get("bet_type") or "").upper() if ctx else None
            # Get live product venue/start to help match on audit if event id differs
            v_name = None; start_iso = None
            try:
                live_client = ToteClient()  # live
                q_live = (
                    "query Product($id: String){ product(id:$id){ ... on BettingProduct { betType{ code } legs{ nodes{ event{ venue{ name } scheduledStartDateTime{ iso8601 } } } } } } }"
                )
                d_live = live_client.graphql(q_live, {"id": product_id})
                prod_live = d_live.get("product") or {}
                legs_live = ((prod_live.get("legs") or {}).get("nodes")) or []
                if legs_live:
                    evl = (legs_live[0].get("event") or {})
                    v_name = ((evl.get("venue") or {}).get("name"))
                    start_iso = ((evl.get("scheduledStartDateTime") or {}).get("iso8601"))
                bt_code = (bt_code or (((prod_live.get("betType") or {}).get("code")) or "")).upper()
            except Exception:
                pass

            audit_client = ToteClient()
            cand = None
            # Try event(id) on audit first
            if ev_id:
                q_ev = (
                    "query GetEvent($id: String){ event(id:$id){ products{ nodes{ id ... on BettingProduct { betType{ code } selling{ status } legs{ nodes{ id selections{ nodes{ id eventCompetitor{ __typename ... on HorseRacingEventCompetitor{ clothNumber } ... on GreyhoundRacingEventCompetitor{ trapNumber } } competitor{ details{ __typename ... on HorseDetails{ clothNumber } ... on GreyhoundDetails{ trapNumber } } } } } } } } } } }"
                )
                try:
                    d_ev = audit_client.graphql_audit(q_ev, {"id": ev_id})
                    nodes = (((d_ev.get("event") or {}).get("products") or {}).get("nodes")) or []
                    for n in nodes:
                        bt = ((n.get("betType") or {}).get("code") or "").upper()
                        ss = ((n.get("selling") or {}).get("status") or "").upper()
                        if bt == bt_code and ss in ("OPEN","SELLING"):
                            cand = n; break
                        if bt == bt_code and not cand:
                            cand = n
                except Exception:
                    cand = None
            # Fallback: audit products(date, betTypes) matched by venue/time
            if not cand:
                try:
                    date_part = (start_iso or "")[:10]
                    q_list = (
                        "query GetProducts($date: Date, $betTypes: [BetTypeCode!], $first: Int){ products(date:$date, betTypes:$betTypes, first:$first){ nodes{ id ... on BettingProduct { betType{ code } selling{ status } legs{ nodes{ id event{ venue{ name } scheduledStartDateTime{ iso8601 } } selections{ nodes{ id eventCompetitor{ __typename ... on HorseRacingEventCompetitor{ clothNumber } ... on GreyhoundRacingEventCompetitor{ trapNumber } } competitor{ details{ __typename ... on HorseDetails{ clothNumber } ... on GreyhoundDetails{ trapNumber } } } } } } } } } }"
                    )
                    vars = {"date": date_part or None, "betTypes": [bt_code] if bt_code else None, "first": 300}
                    d_list = audit_client.graphql_audit(q_list, vars)
                    nodes2 = ((d_list.get("products") or {}).get("nodes")) or []
                    for n in nodes2:
                        bt = ((n.get("betType") or {}).get("code") or "").upper()
                        if bt != bt_code:
                            continue
                        legs2 = ((n.get("legs") or {}).get("nodes")) or []
                        if not legs2:
                            continue
                        ev2 = (legs2[0].get("event") or {})
                        v2 = ((ev2.get("venue") or {}).get("name") or "")
                        s2 = ((ev2.get("scheduledStartDateTime") or {}).get("iso8601") or "")
                        ok = True
                        if v_name and v2 and (v_name.strip().lower() != v2.strip().lower()):
                            ok = False
                        if start_iso and s2:
                            if start_iso == s2 or start_iso[:16] == s2[:16]:
                                pass
                            else:
                                try:
                                    from datetime import datetime
                                    t_live = datetime.strptime(start_iso.replace("Z","+0000"), "%Y-%m-%dT%H:%M:%S%z")
                                    t_a = datetime.strptime(s2.replace("Z","+0000"), "%Y-%m-%dT%H:%M:%S%z")
                                    if abs((t_live - t_a).total_seconds())/60.0 > 120:
                                        ok = False
                                except Exception:
                                    ok = False
                        if ok:
                            cand = n; break
                except Exception:
                    cand = None

            if cand:
                audit_pid = cand.get("id")
                legs_a = ((cand.get("legs") or {}).get("nodes")) or []
                if legs_a:
                    audit_plid = legs_a[0].get("id")
                    sels_a = ((legs_a[0].get("selections") or {}).get("nodes")) or []
                    for s in sels_a:
                        # prefer modern eventCompetitor then legacy details
                        n = None
                        try:
                            evc = (s.get("eventCompetitor") or {})
                            tnm = evc.get("__typename")
                            if tnm == "HorseRacingEventCompetitor": n = evc.get("clothNumber")
                            elif tnm == "GreyhoundRacingEventCompetitor": n = evc.get("trapNumber")
                        except Exception:
                            n = None
                        if n is None:
                            det = ((s.get("competitor") or {}).get("details") or {})
                            tnm2 = det.get("__typename")
                            if tnm2 == "HorseDetails": n = det.get("clothNumber")
                            elif tnm2 == "GreyhoundDetails": n = det.get("trapNumber")
                        try:
                            if n is not None:
                                by_number_audit[int(n)] = s.get("id")
                        except Exception:
                            pass
        except Exception:
            audit_pid = None

        if audit_pid and audit_plid and by_number_audit:
            used_product_id = audit_pid
            audit_product_leg_id = audit_plid
            by_number_map = by_number_audit
            try:
                print(f"[AuditBet] Audit mapping -> product:{used_product_id} leg:{audit_product_leg_id} selections:{len(by_number_map)}")
            except Exception:
                pass
        else:
            try:
                print("[AuditBet] Audit mapping failed; using live IDs for audit (may be rejected)")
            except Exception:
                pass

    # --- Helper function to convert a line string to GraphQL leg structure ---
    def _line_to_legs(line: str) -> Optional[Dict[str, Any]]:
        try:
            parts = [int(x.strip()) for x in line.split('-') if x.strip()]
            if len(parts) < pos_required:
                return None
            sels: List[Dict[str, Any]] = []
            for pos, num in enumerate(parts[:pos_required], start=1):
                # Always use the mapped selection ID for both live and audit
                sid = by_number_map.get(int(num))
                if not sid:
                    return None
                sels.append({"productLegSelectionID": sid, "position": pos})
            
            return {
                "productLegId": audit_product_leg_id,
                "selections": sels,
            }
        except (ValueError, KeyError):
            return None

    # Process legs using the helper function
    legs = []
    for s in bet_lines:
        lg = _line_to_legs(s)
        if not lg:
            continue
        legs.append(lg)

    # Validate mapping before attempting placement
    if bet_lines and (not product_leg_id or not legs):
        # Surface a clear error with missing numbers for easier debugging
        missing: List[str] = []
        for s in bet_lines:
            try:
                parts = [int(x.strip()) for x in s.split('-') if x.strip()]
                for n in parts[:pos_required]:
                    if n not in by_number:
                        missing.append(str(n))
            except Exception:
                pass
        err_msg = "selection mapping failed"
        if missing:
            err_msg += f": unknown numbers {{{','.join(sorted(set(missing)))}}} for product {product_id}"
        # Record audit row with error and return early
        bet_id = f"{product_id}:{_now_ms()}"
        _record_audit_bq(
            sink,
            bet_id=bet_id,
            mode=mode,
            product_id=product_id,
            selection=",".join(bet_lines),
            stake=stake,
            currency=currency,
            payload={"bets": [], "error": err_msg},
            response=None,
            error=err_msg,
        )
        return {"bet_id": bet_id, "error": err_msg}

    # Build bets arrays for both schema variants
    # v2 (docs/tote_queries.graphql): PlaceBetsInput with results[]
    bets_v2: List[Dict[str, Any]] = []
    for lg in legs: # Use the processed legs
        bet_obj = {
            "productId": used_product_id,
            "stake": {
                "amount": {"decimalAmount": line_stake},
                "currency": currency,
            },
            "legs": [lg] if lg else [],
        }
        bets_v2.append({"bet": bet_obj})
    payload_v2 = {"bets": bets_v2}

    # v1 (ticket schema): for Superfecta use totalAmount per bet (not lineAmount)
    bets_v1: List[Dict[str, Any]] = []
    for lg in legs: # Use the processed legs
        stake_obj = {"currencyCode": currency, "totalAmount": line_stake}
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
            if not client:
                client = ToteClient()
            # Helper: choose live vs audit executor explicitly
            def _exec(query: str, variables: dict):
                if str(mode).lower() == 'live':
                    return client.graphql(query, variables)
                else:
                    return client.graphql_audit(query, variables)

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
                resp = _exec(mutation_v1, variables_v1)
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
                    resp = _exec(mutation_v2, variables_v2)
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
            else:
                # On success, optionally log the shape for diagnostics (without dumping full IDs unless in debug)
                try:
                    if str(os.getenv("TOTE_DEBUG", "0")).lower() in ("1","true","yes","on"):
                        print("[AuditBet][OK] v1 payload legs:", len(bets_v1[0].get("legs", [])), "selections:", len((bets_v1[0].get("legs") or [{}])[0].get("selections", [])))
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
    bet_id = f"{product_id}:{_now_ms()}"
    _record_audit_bq(
        sink,
        bet_id=bet_id,
        mode=mode,
        product_id=product_id,
        selection=sel_str,
        stake=stake,
        currency=currency,
        payload=stored_req,
        response=resp,
        error=err,
        placement_status=placement_status,
    )
    out = {"bet_id": bet_id, "error": err, "response": resp, "placement_status": placement_status}
    if failure_reason:
        out["failure_reason"] = failure_reason
    return out


def place_audit_win(
    sink,
    *,
    event_id: str,
    selection_id: str,
    stake: float,
    currency: str = "GBP",
    post: bool = False,
    client: ToteClient | None = None,
    mode: str = "audit",
) -> Dict[str, Any]:
    """
    Places a WIN bet for a given event. This is a helper for the CLI.
    It finds the WIN product for the event and then uses the simple bet placement logic.
    """
    # Find the OPEN WIN product_id for the event from BigQuery
    win_product_rows = _bq_query_rows(
        sink,
        "SELECT product_id FROM tote_products WHERE event_id=@eid AND bet_type='WIN' AND status='OPEN' LIMIT 1",
        {"eid": event_id}
    )
    
    actual_product_id = None
    if win_product_rows:
        actual_product_id = win_product_rows[0].get("product_id")

    bet_id = f"WIN:{event_id}:{_now_ms()}"

    if not actual_product_id:
        err_msg = f"Could not find an OPEN WIN product for event {event_id}"
        # Record failure using the synthetic product ID for traceability
        _record_audit_bq(
            sink,
            bet_id=bet_id,
            mode=mode,
            product_id=f"WIN:{event_id}",
            selection=selection_id,
            stake=stake,
            currency=currency,
            payload={"error": err_msg},
            response=None,
            error=err_msg,
        )
        return {"bet_id": bet_id, "error": err_msg}

    # We found a product, now place the bet using the common simple bet function
    # The client passed in will be configured by place_audit_simple_bet if it's None
    return place_audit_simple_bet(
        sink,
        product_id=actual_product_id,
        selection_id=selection_id,
        stake=stake,
        currency=currency,
        post=post,
        client=client,
        mode=mode
    )


def refresh_bet_status(sink, *, bet_id: str, post: bool = False) -> Dict[str, Any]:
    """Attempt to refresh audit bet status stored in BigQuery.

    Infers a provider bet id from the stored response_json and optionally queries the Tote API
    (audit endpoint by default) to update the stored status.
    """
    rows = _bq_query_rows(
        sink,
        "SELECT response_json, mode FROM tote_audit_bets WHERE bet_id=@bid LIMIT 1",
        {"bid": bet_id},
    )
    if not rows:
        return {"error": "unknown bet_id"}
    resp_json = (rows[0] or {}).get("response_json")
    mode = (rows[0] or {}).get("mode") or "audit"

    provider_id = None
    try:
        if resp_json:
            d = json.loads(resp_json)
            resp = d.get("response") or {}
            ticket = (resp.get("placeBets") or {}).get("ticket")
            if ticket and isinstance(ticket, dict):
                nodes = (ticket.get("bets") or {}).get("nodes") or []
                if nodes:
                    provider_id = nodes[0].get("toteId") or nodes[0].get("id")
            if not provider_id:
                results = (resp.get("placeBets") or {}).get("results") or []
                if results:
                    provider_id = results[0].get("toteBetId")
            provider_id = provider_id or d.get("betId") or d.get("id")
    except Exception:
        provider_id = None

    status_payload = None
    err = None
    if provider_id and post:
        try:
            client = ToteClient()
            query = """
            query Ticket($id: ID!){
              ticket(id:$id){ id toteId bets{ nodes{ id toteId placement{ status } } } }
            }
            """
            if str(mode).lower() == "audit":
                status_payload = client.graphql_audit(query, {"id": provider_id})
            else:
                status_payload = client.graphql(query, {"id": provider_id})
        except Exception as e:
            err = str(e)

    try:
        if status_payload and isinstance(status_payload, dict):
            st = None
            try:
                ticket = (status_payload.get("ticket") or {})
                nodes = (ticket.get("bets") or {}).get("nodes") or []
                if nodes:
                    st = (nodes[0].get("placement") or {}).get("status")
            except Exception:
                st = None
            if st:
                from google.cloud import bigquery
                job_cfg = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("st", "STRING", st),
                    bigquery.ScalarQueryParameter("bid", "STRING", bet_id),
                ])
                sink.query("UPDATE tote_audit_bets SET status=@st WHERE bet_id=@bid", job_config=job_cfg)
    except Exception:
        pass

    return {"bet_id": bet_id, "provider_id": provider_id, "error": err, "status": status_payload}


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


def sync_bets_from_api(sink, api_data: Dict[str, Any]) -> int:
    """Best-effort: update tote_audit_bets.status by matching toteId from API data to stored response_json."""
    nodes = (((api_data or {}).get("bets") or {}).get("nodes")) if isinstance(api_data, dict) else None
    if nodes is None:
        return 0
    updated = 0
    from google.cloud import bigquery
    for n in nodes:
        try:
            bet_tid = n.get("toteId")
            status = ((n.get("placement") or {}).get("status"))
            if not bet_tid or not status:
                continue
            job_cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("tid", "STRING", bet_tid),
                bigquery.ScalarQueryParameter("st", "STRING", status),
            ])
            sink.query(
                "UPDATE tote_audit_bets SET status=@st WHERE response_json LIKE CONCAT('%', @tid, '%')",
                job_config=job_cfg,
            )
            updated += 1
        except Exception:
            continue
    return updated
