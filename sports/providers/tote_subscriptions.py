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
        "subscription{ onPoolTotalChanged{ isFinalized productId total{ "
        " netAmounts{ currency{ code } decimalAmount } grossAmounts{ currency{ code } decimalAmount } } } }"
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
    async with websockets.connect(url, subprotocols=subprotocols, extra_headers=headers, ping_interval=20) as ws:  # type: ignore
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
            # Parse pool total change, write snapshot best-effort
            try:
                if payload.get("type") == "data":
                    node = (((payload.get("payload") or {}).get("data") or {}).get("onPoolTotalChanged"))
                    if node and isinstance(node, dict):
                        pid = node.get("productId")
                        # Choose first net/gross amounts
                        net_list = ((node.get("total") or {}).get("netAmounts") or [])
                        gross_list = ((node.get("total") or {}).get("grossAmounts") or [])
                        net = None
                        gross = None
                        if net_list:
                            try:
                                net = float(net_list[0].get("decimalAmount"))
                            except Exception:
                                net = None
                        if gross_list:
                            try:
                                gross = float(gross_list[0].get("decimalAmount"))
                            except Exception:
                                gross = None
                        if pid:
                            # Lookup product context
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
            except Exception:
                pass


def run_subscriber(conn, *, audit: bool = False, duration: Optional[int] = None):
    url = cfg.tote_subscriptions_url or "wss://hub.production.racing.tote.co.uk/partner/connections/graphql/"
    if websockets is None:
        raise RuntimeError("websockets is not installed; pip install websockets")
    asyncio.get_event_loop().run_until_complete(_subscribe_pools(url, conn, duration=duration))

