import asyncio
import json
import ssl
import time
from typing import Optional

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None

from sports.config import cfg


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _subscribe_pools(url: str, conn, *, duration: Optional[int] = None) -> None:
    """Subscribe to onPoolTotalChanged using graphql-ws and persist snapshots + raw messages.

    Persists into tote_pool_snapshots (when productId found) and tote_messages (raw).
    """
    assert websockets is not None, "websockets package not installed"

    # GraphQL WS handshake
    headers = {
        "Authorization": f"Api-Key {cfg.tote_api_key}",
    }
    subprotocols = ["graphql-ws"]
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
    start_msg = {
        "id": "1",
        "type": "start",
        "payload": {"query": query},
    }
    init_msg = {
        "type": "connection_init",
        "payload": {"useragent": "autobet-subscriber", "authorization": f"Api-Key {cfg.tote_api_key}"},
    }
    ctx = ssl.SSLContext()
    started = time.time()
    async with websockets.connect(url, subprotocols=subprotocols, extra_headers=headers, ssl=ssl.SSLContext(), ping_interval=20) as ws:  # type: ignore
        # init and start
        await ws.send(json.dumps(init_msg))
        await ws.send(json.dumps(start_msg))
        while True:
            if duration and (time.time() - started) > duration:
                break
            try:
                msg = await ws.recv()
            except Exception:
                await asyncio.sleep(1)
                continue
            ts = _now_ms()
            try:
                payload = json.loads(msg)
            except Exception:
                payload = {"raw": msg}
            # Store raw
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
                                try:
                                    conn.execute("UPDATE tote_events SET status=? WHERE event_id=?", (status, eid))
                                    conn.commit()
                                except Exception:
                                    pass

                    # onEventResultChanged → set result_status to indicate results available
                    if "onEventResultChanged" in data:
                        node = data["onEventResultChanged"]
                        if node and isinstance(node, dict):
                            eid = node.get("eventId") or node.get("EventId")
                            if eid:
                                try:
                                    conn.execute("UPDATE tote_events SET result_status=COALESCE(result_status,'RESULTED') WHERE event_id=?", (eid,))
                                    conn.commit()
                                except Exception:
                                    pass
            except Exception:
                pass


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
