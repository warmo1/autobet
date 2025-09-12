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
try:
    from ..realtime import bus as rt_bus  # optional; not required for ingest
except Exception:  # pragma: no cover
    rt_bus = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_bq_sink(obj) -> bool:
    return hasattr(obj, "upsert_tote_pool_snapshots") and hasattr(obj, "upsert_tote_events")


async def _subscribe_pools(url: str, conn, *, duration: Optional[int] = None) -> None:
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
    headers = {"Authorization": f"Api-Key {cfg.tote_api_key}"}
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
                netAmount { currency amount }
                grossAmount { currency amount }
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
                dividend { name type status amounts { decimalAmount } }
                legs { legId selections { id finishingPosition } }
              }
            } }
            """.strip(),
        ))
        # 3) Event result
        docs.append((
            "onEventResultChanged",
            "subscription { onEventResultChanged { eventId competitorResults { competitorId finishingPosition status } } }",
        ))
        # 4) Event status
        docs.append((
            "onEventStatusChanged",
            "subscription { onEventStatusChanged { eventId status } }",
        ))
        # 5) Competitor status
        docs.append((
            "onCompetitorStatusChanged",
            "subscription { onCompetitorStatusChanged { eventId competitorId status } }",
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
        if not _is_bq_sink(conn):
            return None
        if not pid:
            return None
        if pid in ctx_cache:
            return ctx_cache.get(pid)
        try:
            from google.cloud import bigquery  # type: ignore
            job_cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pid", "STRING", pid)
            ])
            rs = conn.query(
                "SELECT event_id, UPPER(bet_type) AS bet_type, currency, start_iso FROM tote_products WHERE product_id=@pid LIMIT 1",
                job_config=job_cfg,
            )
            df = rs.to_dataframe(create_bqstorage_client=False)
            if not df.empty:
                row = df.iloc[0].to_dict()
                ctx_cache[pid] = row
                return row
        except Exception:
            return None
        return None

    async def _refresh_ctx_cache_periodically():
        if not _is_bq_sink(conn):
            return
        # Initial warm load and periodic refresh
        while not stop_event.is_set():
            try:
                # Load products in the last ~36 hours (today + yesterday typically)
                sql = (
                    """
                    SELECT product_id, event_id, UPPER(bet_type) AS bet_type, currency, start_iso
                    FROM tote_products
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
                    # Send auth in both upgrade header and init payload for compatibility
                    await ws.send(json.dumps({
                        "type": "connection_init",
                        "payload": {"authorization": f"Api-Key {cfg.tote_api_key}"},
                    }))
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
                    await ws.send(json.dumps({"type": "connection_init", "payload": {"authorization": f"Api-Key {cfg.tote_api_key}"}}))
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

                    # Store raw when not using a BigQuery sink (legacy/local)
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

                    # Parse message data (payload is already the GraphQL data object)
                    try:
                        data = payload if isinstance(payload, dict) else {}
                        # onPoolTotalChanged → snapshots
                        if "onPoolTotalChanged" in data:
                            node = data["onPoolTotalChanged"]
                            if node and isinstance(node, dict):
                                pid = node.get("productId")
                                total = (node.get("total") or {})
                                net_node = (total.get("netAmount") or {})
                                gross_node = (total.get("grossAmount") or {})
                                try:
                                    net = float(net_node.get("amount")) if (net_node and net_node.get("amount") is not None) else None
                                except Exception:
                                    net = None
                                try:
                                    gross = float(gross_node.get("amount")) if (gross_node and gross_node.get("amount") is not None) else None
                                except Exception:
                                    gross = None
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
                                                    },
                                                ))
                                        except Exception:
                                            pass
                                    else:
                                        try:
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
                                                await q.put((
                                                    "upsert_tote_event_status_log",
                                                    {"event_id": str(eid), "ts_ms": ts, "status": str(status)},
                                                ))
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
                                            if rows and q is not None:
                                                for r in rows:
                                                    await q.put(("upsert_tote_event_results_log", r))
                                        except Exception:
                                            pass
                                    else:
                                        try:
                                            conn.execute(
                                                "UPDATE tote_events SET result_status=COALESCE(result_status,'RESULTED') WHERE event_id=?",
                                                (eid,),
                                            )
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

                        # onProductStatusChanged → product status log
                        if "onProductStatusChanged" in data and _is_bq_sink(conn):
                            try:
                                node = data["onProductStatusChanged"]
                                pid = (node or {}).get("productId") or (node or {}).get("ProductId")
                                st = (node or {}).get("status") or (node or {}).get("Status")
                                if pid and st is not None and q is not None:
                                    await q.put((
                                        "upsert_tote_product_status_log",
                                        {"product_id": str(pid), "ts_ms": ts, "status": str(st)},
                                    ))
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
                                    else:
                                        conn.upsert_raw_tote([row])
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
