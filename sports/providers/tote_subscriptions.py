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
from .tote_api import ToteClient  # HTTP GraphQL fallback for totals
try:
    from ..realtime import bus as rt_bus  # optional; not required for ingest
except Exception:  # pragma: no cover
    rt_bus = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_bq_sink(obj) -> bool:
    return hasattr(obj, "upsert_tote_pool_snapshots") and hasattr(obj, "upsert_tote_events")


async def _subscribe_pools(url: str, conn, *, duration: Optional[int] = None, event_callback=None) -> None:
    """Subscribe to Tote WS channels and persist snapshots + logs.

    Updated to match official Tote API WebSocket format:
    - Uses standard WebSocket with JSON messages (not GraphQL subscriptions)
    - Handles x-message-type header for message routing
    - Matches official API message structure from documentation
    """
    assert websockets is not None, "websockets package not installed"

    def _money_to_float(m):
        """Safely convert a Tote Money object to a float."""
        if not isinstance(m, dict):
            return None
        # Prefer decimalAmount or stringAmount for precision.
        # Also check for 'amount' as a fallback for deprecated types.
        for k in ("decimalAmount", "stringAmount", "amount"):
            v = m.get(k)
            if v is not None:
                try:
                    return float(v)
                except (ValueError, TypeError):
                    continue
        # Fallback to minor units.
        mu = m.get("minorUnitsTotalAmount")
        if mu is not None:
            try:
                return float(mu) / 100.0
            except (ValueError, TypeError):
                pass
        return None

    # Official Tote API uses standard WebSocket with Authorization header
    headers = {"Authorization": f"{cfg.tote_auth_scheme} {cfg.tote_api_key}"}
    started = time.time()
    # Cache product context to enrich snapshots without repeated queries
    ctx_cache: dict[str, dict] = {}
    # Lazy HTTP client for fallback fetching of pool totals
    http_client: ToteClient | None = None
    last_fetch_ts: dict[str, float] = {}

    async def _fetch_totals_http(pid: str) -> tuple[float | None, float | None]:
        nonlocal http_client
        try:
            now = time.time()
            if now - last_fetch_ts.get(pid, 0) < 1.5:
                return (None, None)
            if http_client is None:
                try:
                    http_client = ToteClient()
                except Exception:
                    return (None, None)
            query = (
                """
                query GetTotals($id: String!) {
                  product(id: $id) {
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
                      }
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
                        } 
                      } 
                    }
                  }
                }
                """
            )
            # Run sync HTTP request in a thread so we don't block the event loop
            data = await asyncio.to_thread(http_client.graphql, query, {"id": pid})
            node = data.get("product") or {}
            src = (node.get("type") or node) or {}
            pool = (src.get("pool") or {})
            total = (pool.get("total") or {})
            
            # Handle the new array format from official API
            gross_amounts = total.get("grossAmounts", [])
            net_amounts = total.get("netAmounts", [])
            
            g_val = _money_to_float(gross_amounts[0] if gross_amounts else None)
            n_val = _money_to_float(net_amounts[0] if net_amounts else None)
            
            try:
                last_fetch_ts[pid] = now
            except Exception:
                pass
            return (n_val, g_val)
        except Exception:
            return (None, None)

    # Async queue and batching worker for BigQuery writes
    q: asyncio.Queue | None = None
    stop_event = asyncio.Event()

    async def _bq_worker():
        assert q is not None
        batch: dict[str, list[dict]] = {}
        last_flush = time.monotonic()
        flush_interval = 0.4  # seconds
        max_batch = 1000

        async def flush():
            nonlocal batch
            if not batch:
                return
            for method, rows in list(batch.items()):
                if not rows:
                    continue
                try:
                    fn = getattr(conn, method, None)
                    if callable(fn):
                        fn(rows)
                except Exception:
                    # best-effort; drop on error
                    pass
            batch = {}

        while not stop_event.is_set():
            timeout = max(0.0, flush_interval - (time.monotonic() - last_flush))
            try:
                item = await asyncio.wait_for(q.get(), timeout=timeout)
            except asyncio.TimeoutError:
                await flush()
                last_flush = time.monotonic()
                continue
            except Exception:
                # Unexpected wakeup; try flush and continue
                await flush()
                last_flush = time.monotonic()
                continue

            try:
                method, row = item  # type: ignore
                batch.setdefault(method, []).append(row)
                # Flush on size threshold
                if sum(len(v) for v in batch.values()) >= max_batch:
                    await flush()
                    last_flush = time.monotonic()
            except Exception:
                # Ignore malformed items
                pass

        # Final flush on stop
        try:
            await flush()
        except Exception:
            pass

    def _get_product_ctx(pid: str) -> dict | None:
        """Return product context from the in-memory cache.

        This is a cache-only lookup. The _refresh_ctx_cache_periodically task is
        responsible for populating the cache, which prevents N+1 query storms on BQ.
        """
        if not _is_bq_sink(conn):
            return None
        if not pid:
            return None
        return ctx_cache.get(pid)

    async def _refresh_ctx_cache_periodically():
        if not _is_bq_sink(conn):
            return
        # Initial warm load and periodic refresh
        while not stop_event.is_set():
            try:
                # Load products in the last ~36 hours from the view with the latest totals.
                sql = (
                    """
                    SELECT product_id, event_id, UPPER(bet_type) AS bet_type, currency, start_iso " +
                    "FROM `autobet-470818.autobet.vw_products_latest_totals` " +
                    WHERE SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_iso) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 36 HOUR)
                    """
                )
                rs = conn.query(sql)
                for row in rs:
                    try:
                        pid = str(getattr(row, "product_id", ""))
                        if pid:
                            ctx_cache[pid] = {
                                "event_id": str(getattr(row, "event_id", "")) or None,
                                "bet_type": str(getattr(row, "bet_type", "")) or None,
                                "currency": str(getattr(row, "currency", "")) or None,
                                "start_iso": str(getattr(row, "start_iso", "")) or None,
                            }
                    except Exception:
                        continue
            except Exception:
                # ignore refresh errors
                pass
            # Sleep 5 minutes between refreshes (fast first loop handled below)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=300.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue
    backoff = 1.0

    # Initialize worker + cache refresher once (outside reconnect loop)
    if _is_bq_sink(conn):
        q = asyncio.Queue(maxsize=5000)
        worker_task = asyncio.create_task(_bq_worker())
        refresher_task = asyncio.create_task(_refresh_ctx_cache_periodically())
    else:
        worker_task = None
        refresher_task = None
    while True:
        if duration and (time.time() - started) > duration:
            break
        try:
            # Connect to Tote WebSocket using standard WebSocket (not GraphQL)
            async with websockets.connect(
                url,
                extra_headers=headers,
                ssl=ssl.create_default_context(),
                ping_interval=20,
                ping_timeout=20,
                max_queue=None,
                max_size=10_000_000,
                read_limit=10_000_000,
            ) as ws:  # type: ignore
                try:
                    print(f"[PoolSub] Connected to {url} (standard WebSocket)")
                except Exception:
                    pass

                backoff = 1.0
                while True:
                    if duration and (time.time() - started) > duration:
                        break
                    try:
                        raw = await ws.recv()
                    except Exception:
                        raise
                    ts = _now_ms()
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        msg = {"type": "raw", "raw": raw}
                    
                    # Handle official Tote API message format
                    message_type = msg.get("MessageType") or msg.get("type", "")
                    
                    # Handle connection errors
                    if message_type in ("connection_error", "error"):
                        try:
                            print(f"[PoolSub] Connection/Subscription error: {msg}")
                        except Exception:
                            pass
                        if message_type == "connection_error":
                            try:
                                await ws.close(code=1011)
                            except Exception:
                                pass
                            break  # reconnect
                        continue
                    
                    # Skip keep-alive messages
                    if message_type in ("ka", "connection_ack", "complete", "ping", "pong"):
                        continue

                    # Parse message data based on official Tote API message types
                    try:
                        # PoolTotalChanged → snapshots
                        if message_type == "PoolTotalChanged":
                            pid = msg.get("ProductId")
                            total = msg.get("Total", {})
                            
                            # Handle the official API structure with arrays
                            net_amounts = total.get("NetAmounts", [])
                            gross_amounts = total.get("GrossAmounts", [])
                            
                            net = _money_to_float(net_amounts[0] if net_amounts else None)
                            gross = _money_to_float(gross_amounts[0] if gross_amounts else None)
                            
                            # If values are still missing, fetch via HTTP GraphQL as a fallback
                            if (net is None or gross is None) and pid:
                                try:
                                    n2, g2 = await _fetch_totals_http(str(pid))
                                    net = net if net is not None else n2
                                    gross = gross if gross is not None else g2
                                except Exception:
                                    pass
                            
                            # Get carryIn from the message if available
                            carry_in = msg.get("CarryIn", {})
                            carry_in_gross = carry_in.get("GrossAmounts", [])
                            rollover = _money_to_float(carry_in_gross[0] if carry_in_gross else None) or 0.0
                            
                            if pid:
                                # Publish realtime update for UI consumers
                                try:
                                    if rt_bus is not None:
                                        rt_bus.publish("pool_total_changed", {
                                            "product_id": str(pid),
                                            "total_net": net,
                                            "total_gross": gross,
                                            "rollover": rollover,
                                            "ts_ms": ts,
                                        })
                                except Exception:
                                    pass
                                if _is_bq_sink(conn):
                                    try:
                                        ctx = _get_product_ctx(str(pid)) or {}
                                        if q is not None:
                                            await q.put((
                                                "upsert_tote_pool_snapshots",
                                                {
                                                    "product_id": str(pid),
                                                    "event_id": ctx.get("event_id"),
                                                    "bet_type": ctx.get("bet_type"),
                                                    "status": None,
                                                    "currency": ctx.get("currency"),
                                                    "start_iso": ctx.get("start_iso"),
                                                    "ts_ms": ts,
                                                    "total_gross": gross,
                                                    "total_net": net,
                                                    "rollover": rollover,
                                                },
                                            ))
                                            if event_callback:
                                                event_callback("pool_total_changed", dict(
                                                    product_id=str(pid),
                                                    total_gross=gross,
                                                    total_net=net,
                                                    rollover=rollover,
                                                    currency=ctx.get("currency"),
                                                    ts_ms=ts,
                                                ))
                                    except asyncio.QueueFull:
                                        pass
                                    except Exception:
                                        pass

                        # EventStatusChanged → update tote_events.status
                        elif message_type == "EventStatusChanged":
                            eid = msg.get("EventId")
                            status = msg.get("Status")
                            if eid and status:
                                # Publish realtime update for UI consumers
                                try:
                                    if rt_bus is not None:
                                        rt_bus.publish("event_status_changed", {
                                            "event_id": str(eid),
                                            "status": str(status),
                                            "ts_ms": ts,
                                        })
                                except Exception:
                                    pass
                                if _is_bq_sink(conn):
                                    try:
                                        if q is not None:
                                            # Log the status change for historical record.
                                            await q.put((
                                                "upsert_tote_event_status_log",
                                                {"event_id": str(eid), "ts_ms": ts, "status": str(status)},
                                            ))
                                            # Also update the main events table for UI freshness.
                                            await q.put((
                                                "upsert_tote_events",
                                                {"event_id": str(eid), "status": str(status)},
                                            ))
                                            if event_callback:
                                                event_callback(
                                                    "event_status_changed",
                                                    {"event_id": str(eid), "status": str(status), "ts_ms": ts},
                                                )
                                    except asyncio.QueueFull:
                                        pass
                                    except Exception:
                                        pass

                        # EventResultChanged → per-competitor finishing positions
                        elif message_type == "EventResultChanged":
                            eid = msg.get("EventId")
                            comps = msg.get("CompetitorResults", [])
                            if eid:
                                if _is_bq_sink(conn):
                                    try:
                                        rows = []
                                        for c in comps:
                                            cid = str(c.get("CompetitorId") or "")
                                            fp = c.get("FinishingPosition")
                                            st = c.get("Status")
                                            rows.append({
                                                "event_id": str(eid),
                                                "ts_ms": ts,
                                                "competitor_id": cid,
                                                "finishing_position": int(fp) if fp is not None else None,
                                                "status": str(st) if st is not None else None,
                                            })
                                        if rows and q is not None:
                                            for r in rows:
                                                await q.put(("upsert_tote_event_results_log", r))
                                    except Exception:
                                        pass

                        # PoolDividendChanged → append dividend updates per selection
                        elif message_type == "PoolDividendChanged":
                            pid = msg.get("ProductId")
                            dvs = msg.get("Dividends", [])
                            if pid and dvs and _is_bq_sink(conn):
                                try:
                                    rows = []
                                    # Optional HTTP fallback for amounts if WS payload lacks them
                                    fetched_amounts: dict[str, float] | None = None
                                    for d in dvs:
                                        dividend = d.get("Dividend", {})
                                        amounts_list = dividend.get("Amounts", [])
                                        amount = _money_to_float(amounts_list[0] if amounts_list else None)
                                        legs = d.get("Legs", [])
                                        for lg in legs:
                                            sels = lg.get("Selections", [])
                                            for s in sels:
                                                sel_id = s.get("Id")
                                                if sel_id:
                                                    # If amount present in WS, use it; otherwise, fetch via HTTP once and map per selection
                                                    if amount is not None:
                                                        rows.append({
                                                            "product_id": str(pid),
                                                            "selection": str(sel_id),
                                                            "dividend": float(amount),
                                                            "ts": str(ts),
                                                        })
                                                    else:
                                                        if fetched_amounts is None:
                                                            # Debounced HTTP fetch
                                                            fetched_amounts = {}
                                                            try:
                                                                # Run HTTP fetch once per message if we need amounts
                                                                async def _fetch_map() -> dict[str, float]:
                                                                    nonlocal http_client
                                                                    if http_client is None:
                                                                        try:
                                                                            http_client = ToteClient()
                                                                        except Exception:
                                                                            return {}
                                                                    q = (
                                                                        """
                                                                        query GetDividends($id: String!) {
                                                                          product(id: $id) {
                                                                            ... on BettingProduct {
                                                                              result { dividends { nodes {
                                                                                dividend { amounts { decimalAmount stringAmount minorUnitsTotalAmount currency { code } } }
                                                                                dividendLegs { nodes { dividendSelections { nodes { id } } } }
                                                                              } } }
                                                                            }
                                                                            type { ... on BettingProduct { result { dividends { nodes {
                                                                              dividend { amounts { decimalAmount stringAmount minorUnitsTotalAmount currency { code } } }
                                                                              dividendLegs { nodes { dividendSelections { nodes { id } } } }
                                                                            } } } } }
                                                                          }
                                                                        }
                                                                        """
                                                                    )
                                                                    data2 = await asyncio.to_thread(http_client.graphql, q, {"id": str(pid)})
                                                                    node2 = (data2.get("product") or {})
                                                                    src2 = (node2.get("type") or node2) or {}
                                                                    res = ((src2.get("result") or {}).get("dividends") or {})
                                                                    nds = res.get("nodes") or []
                                                                    mp: dict[str, float] = {}
                                                                    for nd in nds:
                                                                        try:
                                                                            amt_list = (((nd.get("dividend") or {}).get("amounts")) or [])
                                                                            amt_val = _money_to_float(amt_list[0] if amt_list else None)
                                                                            dlegs = ((nd.get("dividendLegs") or {}).get("nodes") or [])
                                                                            if amt_val is not None:
                                                                                for dl in dlegs:
                                                                                    dsel_nodes = ((dl.get("dividendSelections") or {}).get("nodes") or [])
                                                                                    for dsel in dsel_nodes:
                                                                                        sid = dsel.get("id")
                                                                                        if sid:
                                                                                            mp[str(sid)] = float(amt_val)
                                                                        except Exception:
                                                                            continue
                                                                    return mp
                                                                fetched_amounts = await _fetch_map()
                                                            except Exception:
                                                                fetched_amounts = {}
                                                        if fetched_amounts:
                                                            amt = fetched_amounts.get(str(sel_id))
                                                            if amt is not None:
                                                                rows.append({
                                                                    "product_id": str(pid),
                                                                    "selection": str(sel_id),
                                                                    "dividend": float(amt),
                                                                    "ts": str(ts),
                                                                })
                                    if rows and q is not None:
                                        for r in rows:
                                            await q.put(("upsert_tote_product_dividends", r))
                                except Exception:
                                    pass

                        # SelectionStatusChanged → selection status log
                        elif message_type == "SelectionStatusChanged" and _is_bq_sink(conn):
                            try:
                                pid = msg.get("ProductId")
                                sid = msg.get("SelectionId")
                                st = msg.get("Status")
                                if pid and sid and st is not None:
                                    if q is not None:
                                        await q.put((
                                            "upsert_tote_selection_status_log",
                                            {"product_id": str(pid), "selection_id": str(sid), "ts_ms": ts, "status": str(st)},
                                        ))
                            except Exception:
                                pass

                        # CompetitorStatusChanged → competitor status per event
                        elif message_type == "CompetitorStatusChanged" and _is_bq_sink(conn):
                            try:
                                eid = msg.get("EventId")
                                cid = msg.get("CompetitorId")
                                st = msg.get("Status")
                                if eid and cid and st is not None:
                                    if q is not None:
                                        await q.put((
                                            "upsert_tote_competitor_status_log",
                                            {"event_id": str(eid), "competitor_id": str(cid), "ts_ms": ts, "status": str(st)},
                                        ))
                            except Exception:
                                pass

                        # ProductStatusChanged → product status log + realtime publish (with event context)
                        elif message_type == "ProductStatusChanged":
                            try:
                                pid = msg.get("ProductId")
                                st = msg.get("Status")
                                if pid and st is not None:
                                    # Enrich with product context (event_id, bet_type) for UI consumers
                                    ctx = _get_product_ctx(str(pid)) or {}
                                    try:
                                        if rt_bus is not None:
                                            rt_bus.publish(
                                                "product_status_changed",
                                                {
                                                    "product_id": str(pid),
                                                    "event_id": ctx.get("event_id"),
                                                    "bet_type": ctx.get("bet_type"),
                                                    "status": str(st),
                                                    "ts_ms": ts,
                                                },
                                            )
                                    except Exception:
                                        pass
                                    if _is_bq_sink(conn) and q is not None:
                                        await q.put((
                                            "upsert_tote_product_status_log",
                                            {"product_id": str(pid), "ts_ms": ts, "status": str(st)},
                                        ))
                                    if event_callback:
                                        # Also publish via injected callback (used by webapp SSE bus)
                                        event_callback(
                                            "product_status_changed",
                                            {
                                                "product_id": str(pid),
                                                "event_id": ctx.get("event_id"),
                                                "bet_type": ctx.get("bet_type"),
                                                "status": str(st),
                                                "ts_ms": ts,
                                            },
                                        )
                            except asyncio.QueueFull:
                                pass
                            except Exception:
                                pass

                        # Bet lifecycle messages
                        elif message_type in ("BetAccepted", "BetRejected", "BetFailed", "BetCancelled", "BetResulted", "BetSettled"):
                            if _is_bq_sink(conn):
                                try:
                                    bet_id = msg.get("BetId")
                                    customer_bet_id = msg.get("CustomerBetId")
                                    reason = msg.get("Reason")
                                    return_amount = msg.get("ReturnAmount")
                                    
                                    row = {
                                        "raw_id": f"sub:{message_type}:{ts}",
                                        "endpoint": message_type,
                                        "entity_id": str(bet_id or ""),
                                        "sport": "horse_racing",
                                        "fetched_ts": time.strftime(
                                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts / 1000.0)
                                        ),
                                        "payload": json.dumps({
                                            "MessageType": message_type,
                                            "BetId": bet_id,
                                            "CustomerBetId": customer_bet_id,
                                            "Reason": reason,
                                            "ReturnAmount": return_amount
                                        }, ensure_ascii=False),
                                    }
                                    if q is not None:
                                        await q.put(("upsert_raw_tote", row))
                                except Exception:
                                    pass
                    except Exception:
                        # best-effort parse; continue on errors
                        pass
        except Exception:
            # Reconnect with backoff
            try:
                await asyncio.sleep(min(30.0, backoff))
            except Exception:
                pass
            backoff = min(30.0, backoff * 2.0)
    # Ensure background tasks are stopped
    try:
        stop_event.set()
        if refresher_task:
            await asyncio.sleep(0)  # allow task to see stop
            try:
                await asyncio.wait_for(refresher_task, timeout=1.0)
            except Exception:
                pass
        if worker_task:
            try:
                await asyncio.wait_for(worker_task, timeout=2.0)
            except Exception:
                pass
    except Exception:
        pass


def run_subscriber(conn, *, audit: bool = False, duration: Optional[int] = None, event_callback=None):
    url = cfg.tote_subscriptions_url or "wss://hub.production.racing.tote.co.uk/partner/connections/graphql/"
    if websockets is None:
        raise RuntimeError("websockets is not installed; pip install websockets")
    try:
        asyncio.run(_subscribe_pools(url, conn, duration=duration, event_callback=event_callback))
    except RuntimeError:
        # Fallback for environments with an existing loop policy
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_subscribe_pools(url, conn, duration=duration, event_callback=event_callback))
        finally:
            try:
                loop.close()
            except Exception:
                pass
