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

    Key improvements:
      - Larger WS buffers to avoid backpressure during bursts
      - Async queue + single batching worker for BigQuery upserts
      - Pre-warmed product context cache (refreshed periodically)
      - Expanded subscription to include additional channels
      - Error-frame handling and cleaner reconnects
    """
    assert websockets is not None, "websockets package not installed"

    # GraphQL WS handshake (try modern then legacy)

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

    headers = {"Authorization": f"{cfg.tote_auth_scheme} {cfg.tote_api_key}"}
    subprotocols = ["graphql-transport-ws", "graphql-ws"]
    # Server enforces one root field per subscription operation.
    # Prepare a list of single-field subscription documents we will start with unique IDs.
    def _subs() -> list[tuple[str, str]]:
        docs: list[tuple[str, str]] = []
        # 1) Pool totals
        docs.append((
            "onPoolTotalChanged",
            """
            subscription { onPoolTotalChanged {
              isFinalized
              productId
              total {
                netAmounts {
                  decimalAmount
                  currency { code }
                }
                grossAmounts {
                  decimalAmount
                  currency { code }
                }
              }
            } }
            """.strip(),
        ))
        # 2) Dividends
        docs.append((
            "onPoolDividendChanged",
            """
            subscription { onPoolDividendChanged {
              productId
              dividends {
                dividend {
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
            } }
            """.strip(),
        ))
        # 3) Event result
        docs.append((
            "onEventResultChanged",
            """
            subscription { onEventResultChanged {
              eventId
              competitorResults {
                competitorId
                finishingPosition
                status
              }
            } }
            """.strip(),
        ))
        # 4) Event status
        docs.append((
            "onEventStatusChanged",
            "subscription { onEventStatusChanged { eventId status } }",
        ))
        # 5) Competitor status
        docs.append((
            "onCompetitorStatusChanged",
            """
            subscription { onCompetitorStatusChanged {
              eventId
              competitorId
              status
            } }
            """.strip(),
        ))
        # 6) Product status
        docs.append((
            "onProductStatusChanged",
            "subscription { onProductStatusChanged { productId status } }",
        ))
        # 7) Selection status
        docs.append((
            "onSelectionStatusChanged",
            "subscription { onSelectionStatusChanged { productId selectionId status } }",
        ))
        # 8) Event updated
        docs.append((
            "onEventUpdated",
            "subscription { onEventUpdated { eventId } }",
        ))
        # 9) Lines changed
        docs.append((
            "onLinesChanged",
            "subscription { onLinesChanged { productId } }",
        ))
        # 10-15) Bet lifecycle (lightweight payloads)
        docs.append(("onBetAccepted", "subscription { onBetAccepted { betId } }"))
        docs.append(("onBetRejected", "subscription { onBetRejected { betId } }"))
        docs.append(("onBetFailed", "subscription { onBetFailed { betId } }"))
        docs.append(("onBetCancelled", "subscription { onBetCancelled { betId } }"))
        docs.append(("onBetResulted", "subscription { onBetResulted { betId } }"))
        docs.append(("onBetSettled", "subscription { onBetSettled { betId } }"))
        return docs
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
                      pool { total { grossAmount { decimalAmount amount minorUnitsTotalAmount } netAmount { decimalAmount amount minorUnitsTotalAmount } } }
                    }
                    type { ... on BettingProduct { pool { total { grossAmount { decimalAmount amount minorUnitsTotalAmount } netAmount { decimalAmount amount minorUnitsTotalAmount } } } } }
                  }
                }
                """
            )
            # Run sync HTTP request in a thread so we don't block the event loop
            data = await asyncio.to_thread(http_client.graphql, query, {"id": pid})
            node = data.get("product") or {}
            src = (node.get("type") or node) or {}
            total = ((src.get("pool") or {}).get("total") or {})
            g_val = _money_to_float(total.get("grossAmount"))
            n_val = _money_to_float(total.get("netAmount"))
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
            async with websockets.connect(
                url,
                subprotocols=subprotocols,
                extra_headers=headers,
                ssl=ssl.create_default_context(),
                ping_interval=20,
                ping_timeout=20,
                max_queue=None,
                max_size=10_000_000,
                read_limit=10_000_000,
            ) as ws:  # type: ignore
                proto = getattr(ws, "subprotocol", None) or ""
                try:
                    print(f"[PoolSub] Connected to {url} (proto={proto or 'default'})")
                except Exception:
                    pass
                # Send connection init based on negotiated protocol
                if proto == "graphql-transport-ws":
                    await ws.send(json.dumps({"type": "connection_init", "payload": {}}))
                    # wait for ack or keep-alive
                    while True:
                        m = json.loads(await ws.recv())
                        if m.get("type") in ("connection_ack", "ka"):
                            break
                    # Start multiple single-field subscriptions with unique ids
                    for idx, (_name, qdoc) in enumerate(_subs(), start=1):
                        try:
                            await ws.send(json.dumps({"id": str(idx), "type": "subscribe", "payload": {"query": qdoc}}))
                        except Exception:
                            continue
                else:
                    await ws.send(json.dumps({"type": "connection_init", "payload": {"authorization": f"{cfg.tote_auth_scheme} {cfg.tote_api_key}"}}))
                    for idx, (_name, qdoc) in enumerate(_subs(), start=1):
                        try:
                            await ws.send(json.dumps({"id": str(idx), "type": "start", "payload": {"query": qdoc}}))
                        except Exception:
                            continue

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
                    # Handle server error frames explicitly
                    t = str(msg.get("type") or "")
                    if t == "connection_error":
                        try:
                            print(f"[PoolSub] Connection error: {msg}")
                        except Exception:
                            pass
                        try:
                            await ws.close(code=1011)
                        except Exception:
                            pass
                        break  # reconnect
                    if t == "error":
                        # Subscription-level error (e.g., bad query). Log and continue other subs.
                        try:
                            print(f"[PoolSub] Subscription error: {msg}")
                        except Exception:
                            pass
                        continue
                    # Normalize to a common shape
                    if t in ("next", "data"):
                        payload = (msg.get("payload") or {}).get("data") or {}
                    elif t in ("ka", "connection_ack", "complete"):
                        continue
                    else:
                        payload = (msg.get("payload") or {}).get("data") or {}


                    # Parse message data (payload is already the GraphQL data object)
                    try:
                        data = payload if isinstance(payload, dict) else {}
                        # onPoolTotalChanged → snapshots
                        if "onPoolTotalChanged" in data:
                            node = data["onPoolTotalChanged"]
                            if node and isinstance(node, dict):
                                pid = node.get("productId")
                                total = (node.get("total") or {})
                                net = _money_to_float((total.get("netAmounts") or [{}])[0])
                                gross = _money_to_float((total.get("grossAmounts") or [{}])[0])
                                # If values are still missing, fetch via HTTP GraphQL as a fallback
                                if (net is None or gross is None) and pid:
                                    try:
                                        n2, g2 = await _fetch_totals_http(str(pid)) # noqa
                                        net = net if net is not None else n2
                                        gross = gross if gross is not None else g2
                                    except Exception:
                                        pass
                                # carryIn is no longer in the subscription, so rollover is assumed 0 from this message.
                                # It might be present on the product record itself.
                                rollover = 0.0
                                if pid:
                                    # Publish realtime update for UI consumers
                                    try:
                                        if rt_bus is not None:
                                            rt_bus.publish("pool_total_changed", {
                                                "product_id": str(pid),
                                                "total_net": net,
                                                "total_gross": gross,
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

                        # onEventStatusChanged → update tote_events.status
                        if "onEventStatusChanged" in data:
                            node = data["onEventStatusChanged"]
                            if node and isinstance(node, dict):
                                eid = node.get("eventId") or node.get("EventId")
                                status = node.get("status") or node.get("Status")
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
                                            if rows and q is not None:
                                                for r in rows:
                                                    await q.put(("upsert_tote_event_results_log", r))
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
                                        # Optional HTTP fallback for amounts if WS payload lacks them
                                        fetched_amounts: dict[str, float] | None = None
                                        for d in dvs:
                                            amounts_list = ((d.get("dividend") or {}).get("amounts") or [])
                                            amount = _money_to_float(amounts_list[0] if amounts_list else None)
                                            legs = (d.get("legs") or [])
                                            for lg in legs:
                                                sels = (lg.get("selections") or [])
                                                for s in sels:
                                                    sel_id = s.get("id")
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

                        # onSelectionStatusChanged → selection status log
                        if "onSelectionStatusChanged" in data and _is_bq_sink(conn):
                            try:
                                node = data["onSelectionStatusChanged"]
                                pid = (node or {}).get("productId") or (node or {}).get("ProductId")
                                sid = (node or {}).get("selectionId") or (node or {}).get("SelectionId")
                                st = (node or {}).get("status") or (node or {}).get("Status")
                                if pid and sid and st is not None:
                                    if q is not None:
                                        await q.put((
                                            "upsert_tote_selection_status_log",
                                            {"product_id": str(pid), "selection_id": str(sid), "ts_ms": ts, "status": str(st)},
                                        ))
                            except Exception:
                                pass

                        # onCompetitorStatusChanged → competitor status per event
                        if "onCompetitorStatusChanged" in data and _is_bq_sink(conn):
                            try:
                                node = data["onCompetitorStatusChanged"]
                                eid = (node or {}).get("eventId") or (node or {}).get("EventId")
                                cid = (node or {}).get("competitorId") or (node or {}).get("CompetitorId")
                                st = (node or {}).get("status") or (node or {}).get("Status")
                                if eid and cid and st is not None:
                                    if q is not None:
                                        await q.put((
                                            "upsert_tote_competitor_status_log",
                                            {"event_id": str(eid), "competitor_id": str(cid), "ts_ms": ts, "status": str(st)},
                                        ))
                            except Exception:
                                pass

                        # onProductStatusChanged → product status log + realtime publish (with event context)
                        if "onProductStatusChanged" in data:
                            try:
                                node = data["onProductStatusChanged"]
                                pid = (node or {}).get("productId") or (node or {}).get("ProductId")
                                st = (node or {}).get("status") or (node or {}).get("Status")
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

                        # Other updates: log raw payloads for future processing
                        for key in (
                            "onEventUpdated",
                            "onLinesChanged",
                            "onProductStatusChanged",
                            "onSelectionStatusChanged",
                            "onBetAccepted",
                            "onBetRejected",
                            "onBetFailed",
                            "onBetCancelled",
                            "onBetResulted",
                            "onBetSettled",
                        ):
                            if key in data and _is_bq_sink(conn):
                                try:
                                    row = {
                                        "raw_id": f"sub:{key}:{ts}",
                                        "endpoint": key,
                                        "entity_id": str(
                                            (data.get(key) or {}).get("productId")
                                            or (data.get(key) or {}).get("eventId")
                                            or ""
                                        ),
                                        "sport": "horse_racing",
                                        "fetched_ts": time.strftime(
                                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts / 1000.0)
                                        ),
                                        "payload": json.dumps({key: data.get(key)}, ensure_ascii=False),
                                    }
                                    if q is not None:
                                        await q.put(("upsert_raw_tote", row))
                                except Exception:
                                    pass

                        # Dedicated logs for event/product updates where tables exist
                        if _is_bq_sink(conn):
                            try:
                                if "onEventUpdated" in data and q is not None:
                                    node = data["onEventUpdated"] or {}
                                    eid = node.get("eventId") or node.get("EventId")
                                    if eid:
                                        await q.put((
                                            "upsert_tote_event_updated_log",
                                            {"event_id": str(eid), "ts_ms": ts, "payload": json.dumps(node, ensure_ascii=False)},
                                        ))
                            except Exception:
                                pass
                            try:
                                if "onLinesChanged" in data and q is not None:
                                    node = data["onLinesChanged"] or {}
                                    pid = node.get("productId") or node.get("ProductId")
                                    if pid:
                                        await q.put((
                                            "upsert_tote_lines_changed_log",
                                            {"product_id": str(pid), "ts_ms": ts, "payload": json.dumps(node, ensure_ascii=False)},
                                        ))
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
