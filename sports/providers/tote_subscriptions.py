import asyncio
import json
import ssl
import time
from typing import Optional

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None

from ..config import cfg


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_bq_sink(obj) -> bool:
    return hasattr(obj, "upsert_tote_pool_snapshots") and hasattr(obj, "upsert_tote_events")


async def _subscribe_pools(url: str, conn, *, duration: Optional[int] = None) -> None:
    """Subscribe to onPoolTotalChanged using graphql-ws and persist snapshots + raw messages.

    Persists into tote_pool_snapshots (when productId found) and tote_messages (raw).
    """
    assert websockets is not None, "websockets package not installed"

    # GraphQL WS handshake (try modern then legacy)
    headers = {"Authorization": f"Api-Key {cfg.tote_api_key}"}
    subprotocols = ["graphql-transport-ws", "graphql-ws"]
    query = (
        """
subscription {
  onPoolTotalChanged {
    isFinalized
    productId
    total {
      netAmounts {
        currency {
          code
        }
        decimalAmount
      }
      grossAmounts {
        currency {
          code
        }
        decimalAmount
      }
    }
  }
  onPoolDividendChanged {
    productId
    dividends {
      dividend {
        name
        type
        status
        amounts {
          decimalAmount
          currency {
            code
          }
        }
      }
      legs {
        legId
        selections {
          id
          finishingPosition
        }
      }
    }
  }
  onEventResultChanged {
    eventId
    competitorResults {
      competitorId
      finishingPosition
      status
    }
  }
  onEventStatusChanged {
    eventId
    status
  }
  onCompetitorStatusChanged {
    eventId
    competitorId
    status
  }
}
"""
    )
    started = time.time()
    backoff = 1.0
    while True:
        if duration and (time.time() - started) > duration:
            break
        try:
            async with websockets.connect(
                url,
                subprotocols=subprotocols,
                extra_headers=headers,
                ssl=ssl.create_default_context(),
                ping_interval=20,
            ) as ws:  # type: ignore
                proto = getattr(ws, "subprotocol", None) or ""
                # Send connection init based on negotiated protocol
                if proto == "graphql-transport-ws":
                    await ws.send(json.dumps({"type": "connection_init", "payload": {}}))
                    # wait for ack or keep-alive
                    while True:
                        m = json.loads(await ws.recv())
                        if m.get("type") in ("connection_ack", "ka"):
                            break
                    await ws.send(json.dumps({"id": "1", "type": "subscribe", "payload": {"query": query}}))
                else:
                    await ws.send(json.dumps({"type": "connection_init", "payload": {"authorization": f"Api-Key {cfg.tote_api_key}"}}))
                    await ws.send(json.dumps({"id": "1", "type": "start", "payload": {"query": query}}))

                backoff = 1.0
                while True:
                    if duration and (time.time() - started) > duration:
                        return
                    try:
                        raw = await ws.recv()
                    except Exception:
                        raise
                    ts = _now_ms()
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        msg = {"type": "raw", "raw": raw}
                    # Normalize to a common shape
                    if msg.get("type") in ("next", "data"):
                        payload = (msg.get("payload") or {}).get("data") or {}
                    elif msg.get("type") in ("ka", "connection_ack", "complete"):
                        continue
                    else:
                        payload = (msg.get("payload") or {}).get("data") or {}
            # Store raw (SQLite only; skip for BQ)
            if not _is_bq_sink(conn):
                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO tote_messages(ts_ms, channel, kind, entity_id, audit, payload) VALUES(?,?,?,?,?,?)",
                        (
                            ts,
                            str(payload.get("id") or ""),
                            str(payload.get("type") or ""),
                            str(payload.get("id") or ""),
                            0,
                            json.dumps(payload),
                        ),
                    )
                    conn.commit()
                except Exception:
                    pass
            # Parse message data
            try:
                if payload.get("type") == "data":
                    data = (payload.get("payload") or {}).get("data") or {}
                    
                    # onPoolTotalChanged → snapshots
                    if "onPoolTotalChanged" in data:
                        node = data["onPoolTotalChanged"]
                        if node and isinstance(node, dict):
                            pid = node.get("productId")
                            net_list = ((node.get("total") or {}).get("netAmounts") or [])
                            gross_list = ((node.get("total") or {}).get("grossAmounts") or [])
                            net = float(net_list[0].get("decimalAmount")) if net_list else None
                            gross = float(gross_list[0].get("decimalAmount")) if gross_list else None
                            if pid:
                                if _is_bq_sink(conn):
                                    try:
                                        # Minimal join context: rely on existing product context in BQ at query time
                                        conn.upsert_tote_pool_snapshots([
                                            {
                                                "product_id": pid,
                                                "event_id": None,
                                                "bet_type": None,
                                                "status": None,
                                                "currency": None,
                                                "start_iso": None,
                                                "ts_ms": ts,
                                                "total_gross": gross,
                                                "total_net": net,
                                            }
                                        ])
                                    except Exception:
                                        pass
                                else:
                                    row = conn.execute(
                                        "SELECT event_id, bet_type, status, currency, start_iso FROM tote_products WHERE product_id=?",
                                        (pid,),
                                    ).fetchone()
                                    ev, bt, st, curr, start_iso = (row or (None, None, None, None, None))
                                    conn.execute(
                                        """
                                        INSERT OR REPLACE INTO tote_pool_snapshots(product_id,event_id,bet_type,status,currency,start_iso,ts_ms,total_gross,total_net)
                                        VALUES(?,?,?,?,?,?,?,?,?)
                                        """,
                                        (pid, ev, bt, st, curr, start_iso, ts, gross, net),
                                    )
                                    conn.commit()

                    # onEventStatusChanged → update tote_events.status
                    if "onEventStatusChanged" in data:
                        node = data["onEventStatusChanged"]
                        if node and isinstance(node, dict):
                            eid = node.get("eventId") or node.get("EventId")
                            status = node.get("status") or node.get("Status")
                            if eid and status:
                                if _is_bq_sink(conn):
                                    try:
                                        conn.upsert_tote_event_status_log([
                                            {"event_id": str(eid), "ts_ms": ts, "status": str(status)}
                                        ])
                                    except Exception:
                                        pass
                                else:
                                    try:
                                        conn.execute("UPDATE tote_events SET status=? WHERE event_id=?", (status, eid))
                                        conn.commit()
                                    except Exception:
                                        pass

                    # onEventResultChanged → per-competitor finishing positions
                    if "onEventResultChanged" in data:
                        node = data["onEventResultChanged"]
                        if node and isinstance(node, dict):
                            eid = node.get("eventId") or node.get("EventId")
                            comps = (node.get("competitorResults") or [])
                            if eid:
                                if _is_bq_sink(conn):
                                    try:
                                        rows = []
                                        for c in comps:
                                            cid = str(c.get("competitorId") or "")
                                            fp = c.get("finishingPosition")
                                            st = c.get("status")
                                            rows.append({
                                                "event_id": str(eid),
                                                "ts_ms": ts,
                                                "competitor_id": cid,
                                                "finishing_position": int(fp) if fp is not None else None,
                                                "status": str(st) if st is not None else None,
                                            })
                                        if rows:
                                            conn.upsert_tote_event_results_log(rows)
                                    except Exception:
                                        pass
                                else:
                                    try:
                                        conn.execute("UPDATE tote_events SET result_status=COALESCE(result_status,'RESULTED') WHERE event_id=?", (eid,))
                                        conn.commit()
                                    except Exception:
                                        pass

                    # onPoolDividendChanged → append dividend updates per selection
                    if "onPoolDividendChanged" in data:
                        node = data["onPoolDividendChanged"]
                        if node and isinstance(node, dict):
                            pid = node.get("productId")
                            dvs = (node.get("dividends") or [])
                            if pid and dvs and _is_bq_sink(conn):
                                try:
                                    rows = []
                                    for d in dvs:
                                        legs = (d.get("legs") or [])
                                        amt_nodes = (((d.get("dividend") or {}).get("amounts")) or [])
                                        amount = None
                                        if amt_nodes:
                                            try:
                                                amount = float(amt_nodes[0].get("decimalAmount"))
                                            except Exception:
                                                amount = None
                                        for lg in legs:
                                            sels = (lg.get("selections") or [])
                                            for s in sels:
                                                sel_id = s.get("id")
                                                if sel_id and amount is not None:
                                                    rows.append({
                                                        "product_id": str(pid),
                                                        "selection": str(sel_id),
                                                        "dividend": float(amount),
                                                        "ts": str(ts),
                                                    })
                                    if rows:
                                        conn.upsert_tote_product_dividends(rows)
                                except Exception:
                                    pass

                    # Other updates: log raw payloads for future processing
                    for key in ("onEventUpdated","onLinesChanged","onProductStatusChanged","onSelectionStatusChanged"):
                        if key in data and _is_bq_sink(conn):
                            try:
                                conn.upsert_raw_tote([
                                    {
                                        "raw_id": f"sub:{key}:{ts}",
                                        "endpoint": key,
                                        "entity_id": str((data.get(key) or {}).get("productId") or (data.get(key) or {}).get("eventId") or ""),
                                        "sport": "horse_racing",
                                        "fetched_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts/1000.0)),
                                        "payload": json.dumps({key: data.get(key)}, ensure_ascii=False),
                                    }
                                ])
                            except Exception:
                                pass
        except Exception:
            # Reconnect with backoff
            try:
                await asyncio.sleep(min(30.0, backoff))
            except Exception:
                pass
            backoff = min(30.0, backoff * 2.0)


def run_subscriber(conn, *, audit: bool = False, duration: Optional[int] = None):
    url = cfg.tote_subscriptions_url or "wss://hub.production.racing.tote.co.uk/partner/connections/graphql/"
    if websockets is None:
        raise RuntimeError("websockets is not installed; pip install websockets")
    try:
        asyncio.run(_subscribe_pools(url, conn, duration=duration))
    except RuntimeError:
        # Fallback for environments with an existing loop policy
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_subscribe_pools(url, conn, duration=duration))
        finally:
            try:
                loop.close()
            except Exception:
                pass
