from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from google.cloud import bigquery

from .config import cfg
from .bq import get_bq_sink
from .db import get_db
from .providers.tote_bets import place_audit_superfecta
from .superfecta_planner import (
    SUPERFECTA_RISK_PRESETS,
    compute_superfecta_plan,
    group_superfecta_predictions,
    safe_float)

def _now_ms() -> int:
    return int(time.time() * 1000)


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except ValueError:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def _minutes_to_post(start_ts: Optional[datetime], now: Optional[datetime] = None) -> Optional[float]:
    if start_ts is None:
        return None
    now = now or datetime.now(tz=timezone.utc)
    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=timezone.utc)
    delta = start_ts - now
    return delta.total_seconds() / 60.0


def _load_event_context(db, product_id: str) -> Optional[Dict[str, Any]]:
    dataset = f"{db.project}.{db.dataset}"
    sql = (
        f"SELECT * FROM `{dataset}.vw_superfecta_predictions_latest` "
        "WHERE product_id = @pid"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("pid", "STRING", product_id)]
    )
    df = db.query_dataframe(sql, job_config=job_config)
    if df.empty:
        return None
    _, events_map = group_superfecta_predictions(df)
    return events_map.get(product_id)


def _prepare_plan(
    event: Dict[str, Any],
    preset_key: str,
    bankroll_override: Optional[float] = None) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    errors: List[str] = []
    preset_key = preset_key if preset_key in SUPERFECTA_RISK_PRESETS else cfg.superfecta_default_preset
    tote_bank = bankroll_override if bankroll_override is not None else cfg.superfecta_default_bankroll
    result = compute_superfecta_plan(event, preset_key, tote_bank)
    errors.extend(result.get("errors", []))
    plan = result.get("plan") if not errors else None
    return plan, errors


def _default_run_id(product_id: str, run_date: str) -> str:
    return f"{run_date}-{product_id}"


def run_morning_scan(
    *,
    target_date: Optional[str] = None,
    author: str = "automation") -> Dict[str, Any]:
    """Evaluate morning superfecta opportunities and persist recommendations."""
    db = get_db()
    sink = get_bq_sink()
    if sink is None:
        raise RuntimeError("BigQuery sink is not configured for writes.")

    if target_date:
        run_date = target_date
    else:
        run_date = datetime.now(tz=timezone.utc).date().isoformat()

    dataset = f"{db.project}.{db.dataset}"
    sql = (
        "SELECT product_id, event_id, event_name, venue, country, start_iso, currency, "
        "       total_net, total_gross, rollover, n_competitors, roi_current "
        f"FROM `{dataset}.vw_today_gb_superfecta_be` "
        "WHERE start_iso IS NOT NULL AND DATE(SUBSTR(start_iso, 1, 10)) = @run_date "
        "ORDER BY start_iso"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("run_date", "DATE", run_date)])
    df = db.query_dataframe(sql, job_config=job_config)

    accepted = 0
    filtered = 0
    errored = 0
    morning_rows: List[Dict[str, Any]] = []
    recommendation_rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    now = datetime.now(tz=timezone.utc)

    max_candidates = max(1, cfg.superfecta_morning_max_candidates)
    records = df.to_dict("records")[:max_candidates] if not df.empty else []

    for row in records:
        product_id = row.get("product_id")
        if not product_id:
            continue
        start_iso = row.get("start_iso")
        start_ts = _parse_iso(start_iso)
        minutes_to_post = _minutes_to_post(start_ts, now)
        n_comp = int(row.get("n_competitors") or 0)
        roi_current = safe_float(row.get("roi_current"), 0.0)
        rollover = safe_float(row.get("rollover"))
        reason_parts: List[str] = []
        status = "accepted"

        if n_comp < cfg.superfecta_min_competitors:
            status = "filtered"
            reason_parts.append(f"competitors<{cfg.superfecta_min_competitors}")
        if n_comp and n_comp > cfg.superfecta_max_competitors:
            status = "filtered"
            reason_parts.append(f"competitors>{cfg.superfecta_max_competitors}")
        if cfg.superfecta_require_rollover and rollover <= 0:
            status = "filtered"
            reason_parts.append("no-rollover")
        if roi_current and roi_current < cfg.superfecta_min_roi:
            status = "filtered"
            reason_parts.append(f"roi<{cfg.superfecta_min_roi:.2f}")

        run_id = _default_run_id(product_id, run_date)
        seen.add(run_id)
        plan_summary: Optional[Dict[str, Any]] = None
        plan_errors: List[str] = []
        recommendation_status = "monitoring"

        if status == "accepted":
            event = _load_event_context(db, product_id)
            if not event:
                status = "error"
                plan_errors.append("missing predictions")
            else:
                plan_summary, plan_errors = _prepare_plan(event, cfg.superfecta_default_preset)
                if plan_errors:
                    status = "error"
                elif not plan_summary or safe_float(plan_summary.get("total_stake"), 0.0) <= 0:
                    status = "filtered"
                    reason_parts.append("no-lines")
                else:
                    expected_profit = safe_float(plan_summary.get("expected_profit"))
                    if expected_profit <= 0:
                        recommendation_status = "hold"
                        reason_parts.append("non-positive-ev")
                    else:
                        recommendation_status = "monitoring"
                        accepted += 1

        if status == "filtered":
            filtered += 1
        elif status == "error":
            errored += 1

        plan_json = json.dumps(plan_summary, default=float) if plan_summary else None
        selections_text = plan_summary.get("selections_text") if plan_summary else None
        total_stake = safe_float(plan_summary.get("total_stake")) if plan_summary else None
        expected_profit = safe_float(plan_summary.get("expected_profit")) if plan_summary else None
        expected_return = safe_float(plan_summary.get("expected_return")) if plan_summary else None
        hit_rate = safe_float(plan_summary.get("hit_rate")) if plan_summary else None
        bankroll_allocated = safe_float(plan_summary.get("bankroll_allocated")) if plan_summary else None

        morning_rows.append(
            {
                "run_id": run_id,
                "run_date": run_date,
                "run_ts": _now_ms(),
                "product_id": product_id,
                "event_id": row.get("event_id"),
                "event_name": row.get("event_name"),
                "venue": row.get("venue"),
                "country": row.get("country"),
                "start_iso": start_iso,
                "currency": row.get("currency"),
                "n_competitors": n_comp,
                "total_net": safe_float(row.get("total_net")),
                "total_gross": safe_float(row.get("total_gross")),
                "rollover": rollover,
                "roi_current": roi_current,
                "preset_key": cfg.superfecta_default_preset,
                "bankroll_allocated": bankroll_allocated,
                "total_stake": total_stake,
                "expected_profit": expected_profit,
                "expected_return": expected_return,
                "hit_rate": hit_rate,
                "plan_json": plan_json,
                "selections_text": selections_text,
                "status": status,
                "filter_reason": ";".join(reason_parts) if reason_parts else None,
                "notes": "\n".join(plan_errors) if plan_errors else None,
                "created_by": author,
                "minutes_to_post": minutes_to_post,
                "created_ts": _now_ms(),
            }
        )

        recommendation_rows.append(
            {
                "recommendation_id": run_id,
                "run_id": run_id,
                "product_id": product_id,
                "event_id": row.get("event_id"),
                "run_date": run_date,
                "created_ts": _now_ms(),
                "updated_ts": _now_ms(),
                "status": recommendation_status if status == "accepted" else status,
                "preset_key": cfg.superfecta_default_preset,
                "bankroll_allocated": bankroll_allocated,
                "total_stake": total_stake,
                "expected_profit": expected_profit,
                "expected_return": expected_return,
                "hit_rate": hit_rate,
                "currency": row.get("currency"),
                "plan_json": plan_json,
                "selections_text": selections_text,
                "auto_place": cfg.superfecta_auto_place_ready,
                "decision_reason": ";".join(reason_parts) if reason_parts else None,
                "minutes_to_post": minutes_to_post,
                "source": author,
            }
        )

    if morning_rows:
        sink.upsert_superfecta_morning(morning_rows)
    if recommendation_rows:
        sink.upsert_superfecta_recommendations(recommendation_rows)

    return {
        "considered": len(records),
        "accepted": accepted,
        "filtered": filtered,
        "errored": errored,
    }


def run_live_monitor(
    *,
    run_date: Optional[str] = None) -> Dict[str, Any]:
    """Re-evaluate live EV for recommendations and update statuses."""
    db = get_db()
    sink = get_bq_sink()
    if sink is None:
        raise RuntimeError("BigQuery sink is not configured for writes.")

    run_date = run_date or datetime.now(tz=timezone.utc).date().isoformat()
    dataset = f"{db.project}.{db.dataset}"
    sql = (
        "SELECT r.*, p.event_name, p.venue, p.start_iso, p.currency AS product_currency, "
        "       p.total_net AS latest_total_net, p.total_gross AS latest_total_gross, "
        "       p.rollover AS latest_rollover, p.status AS product_status "
        f"FROM `{dataset}.tote_superfecta_recommendations` r "
        f"LEFT JOIN `{dataset}.vw_products_latest_totals` p USING (product_id) "
        "WHERE r.run_date = @run_date AND r.status IN ('monitoring', 'hold', 'ready')"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("run_date", "DATE", run_date)])
    df = db.query_dataframe(sql, job_config=job_config)
    if df.empty:
        return {"evaluated": 0, "ready": 0, "skipped": 0}

    now = datetime.now(tz=timezone.utc)
    ready_count = 0
    skipped_count = 0
    updates: List[Dict[str, Any]] = []
    live_checks: List[Dict[str, Any]] = []

    for row in df.to_dict("records"):
        recommendation_id = row.get("recommendation_id")
        if not recommendation_id:
            continue
        product_id = row.get("product_id")
        start_iso = row.get("start_iso") or row.get("start_iso_1")
        start_ts = _parse_iso(start_iso)
        minutes_to_post = _minutes_to_post(start_ts, now)
        preset_key = row.get("preset_key") or cfg.superfecta_default_preset
        prev_status = (row.get("status") or "monitoring").lower()
        product_status = (row.get("product_status") or "").upper()
        bankroll_allocated = safe_float(row.get("bankroll_allocated"))
        preset = SUPERFECTA_RISK_PRESETS.get(preset_key, SUPERFECTA_RISK_PRESETS[cfg.superfecta_default_preset])
        pct = float(preset.get("bankroll_pct", 0.0))
        tote_bank = bankroll_allocated / pct if pct > 0 else cfg.superfecta_default_bankroll

        event = _load_event_context(db, product_id)
        plan_summary, plan_errors = (None, ["missing predictions"]) if not event else _prepare_plan(event, preset_key, tote_bank)
        if plan_summary and plan_errors:
            plan_errors = []
        if plan_errors:
            plan_summary = None

        total_stake = safe_float(plan_summary.get("total_stake")) if plan_summary else None
        expected_profit = safe_float(plan_summary.get("expected_profit")) if plan_summary else None
        expected_return = safe_float(plan_summary.get("expected_return")) if plan_summary else None
        hit_rate = safe_float(plan_summary.get("hit_rate")) if plan_summary else None
        roi = (expected_profit / total_stake) if plan_summary and total_stake else None
        decision_met = False
        notes: List[str] = []
        new_status = prev_status

        if plan_errors:
            notes.extend(plan_errors)
            new_status = "error"
        elif plan_summary is None or not total_stake or total_stake <= 0:
            notes.append("no-stake")
            new_status = "hold"
        else:
            min_profit = cfg.superfecta_live_min_expected_profit
            min_roi = cfg.superfecta_live_min_roi
            if expected_profit >= min_profit and (roi is None or roi >= min_roi):
                decision_met = True
            else:
                notes.append("threshold-not-met")

        if decision_met:
            ready_window = cfg.superfecta_decision_minutes_before
            if minutes_to_post is not None and minutes_to_post <= ready_window:
                if minutes_to_post < -cfg.superfecta_auto_cancel_minutes:
                    new_status = "skipped"
                    skipped_count += 1
                    notes.append("past-post")
                elif product_status not in {"OPEN", "SUSPENDED"}:
                    new_status = "hold"
                    notes.append(f"status:{product_status}")
                else:
                    new_status = "ready"
                    ready_count += 1
            else:
                new_status = prev_status if prev_status != "ready" else "ready"
        elif prev_status == "ready" and not decision_met:
            new_status = "monitoring"

        if minutes_to_post is not None and minutes_to_post < -cfg.superfecta_auto_cancel_minutes and prev_status not in {"placed", "skipped"}:
            new_status = "skipped"
            skipped_count += 1
            notes.append("post-time-expired")

        live_checks.append(
            {
                "check_id": f"chk-{uuid.uuid4().hex[:10]}",
                "recommendation_id": recommendation_id,
                "run_id": row.get("run_id"),
                "product_id": product_id,
                "check_ts": _now_ms(),
                "minutes_to_post": minutes_to_post,
                "pool_total_net": safe_float(row.get("latest_total_net")),
                "pool_total_gross": safe_float(row.get("latest_total_gross")),
                "rollover": safe_float(row.get("latest_rollover")),
                "roi_current": roi,
                "expected_profit": expected_profit,
                "expected_return": expected_return,
                "hit_rate": hit_rate,
                "plan_total_stake": total_stake,
                "status": new_status,
                "notes": ";".join(notes) if notes else None,
                "plan_json": json.dumps(plan_summary, default=float) if plan_summary else None,
                "decision_threshold_met": decision_met,
            }
        )

        updates.append(
            {
                "recommendation_id": recommendation_id,
                "updated_ts": _now_ms(),
                "status": new_status,
                "plan_json": json.dumps(plan_summary, default=float) if plan_summary else row.get("plan_json"),
                "expected_profit": expected_profit,
                "expected_return": expected_return,
                "total_stake": total_stake,
                "hit_rate": hit_rate,
                "minutes_to_post": minutes_to_post,
                "last_check_ts": _now_ms(),
                "decision_reason": ";".join(notes) if notes else row.get("decision_reason"),
            }
        )

    if live_checks:
        sink.upsert_superfecta_live_checks(live_checks)
    if updates:
        sink.upsert_superfecta_recommendations(updates)

    return {"evaluated": len(df), "ready": ready_count, "skipped": skipped_count}


def execute_ready_recommendations(
    *,
    auto_place: Optional[bool] = None) -> Dict[str, Any]:
    """Place bets for ready recommendations (or perform a dry run)."""
    db = get_db()
    sink = get_bq_sink()
    if sink is None:
        raise RuntimeError("BigQuery sink is not configured for writes.")

    if auto_place is None:
        auto_place = cfg.superfecta_auto_place_ready

    dataset = f"{db.project}.{db.dataset}"
    sql = (
        f"SELECT r.*, p.start_iso, p.currency AS product_currency, p.status AS product_status "
        f"FROM `{dataset}.tote_superfecta_recommendations` r "
        f"LEFT JOIN `{dataset}.vw_products_latest_totals` p USING (product_id) "
        "WHERE r.status = 'ready'"
    )
    df = db.query_dataframe(sql)
    if df.empty:
        return {"attempted": 0, "placed": 0, "skipped": 0}

    placed = 0
    skipped = 0
    attempted = 0
    updates: List[Dict[str, Any]] = []

    for row in df.to_dict("records"):
        recommendation_id = row.get("recommendation_id")
        if not recommendation_id:
            continue
        attempted += 1
        if row.get("bet_id"):
            continue
        product_id = row.get("product_id")
        plan_json = row.get("plan_json")
        try:
            plan_summary = json.loads(plan_json) if plan_json else {}
        except json.JSONDecodeError:
            plan_summary = {}
        lines = plan_summary.get("lines") or []
        line_amounts = {str(line.get("line")): safe_float(line.get("stake")) for line in lines if line.get("line")}
        line_amounts = {k: v for k, v in line_amounts.items() if v > 0}
        total_stake = sum(line_amounts.values())
        selections = list(line_amounts.keys())
        currency = row.get("currency") or row.get("product_currency") or "GBP"
        start_iso = row.get("start_iso")
        minutes_to_post = _minutes_to_post(_parse_iso(start_iso))
        product_status = (row.get("product_status") or "").upper()
        decision_reason = row.get("decision_reason")

        if not selections or total_stake <= 0:
            skipped += 1
            updates.append(
                {
                    "recommendation_id": recommendation_id,
                    "status": "skipped",
                    "decision_reason": "no-lines",
                    "updated_ts": _now_ms(),
                    "final_ts": _now_ms(),
                }
            )
            continue

        if minutes_to_post is not None and minutes_to_post < -cfg.superfecta_auto_cancel_minutes:
            skipped += 1
            updates.append(
                {
                    "recommendation_id": recommendation_id,
                    "status": "skipped",
                    "decision_reason": "post-time-expired",
                    "updated_ts": _now_ms(),
                    "final_ts": _now_ms(),
                }
            )
            continue

        if product_status not in {"OPEN", "SUSPENDED"}:
            skipped += 1
            updates.append(
                {
                    "recommendation_id": recommendation_id,
                    "status": "skipped",
                    "decision_reason": f"status:{product_status}",
                    "updated_ts": _now_ms(),
                    "final_ts": _now_ms(),
                }
            )
            continue

        if not auto_place:
            skipped += 1
            updates.append(
                {
                    "recommendation_id": recommendation_id,
                    "status": "ready",
                    "decision_reason": "auto-place-disabled",
                    "updated_ts": _now_ms(),
                }
            )
            continue

        resp = place_audit_superfecta(
            db,
            product_id=product_id,
            selections=selections,
            stake=total_stake,
            currency=currency,
            post=True,
            stake_type="total",
            mode="audit",
            line_amounts=line_amounts)
        error = resp.get("error") if isinstance(resp, Mapping) else None
        bet_id = resp.get("bet_id") if isinstance(resp, Mapping) else None
        if error:
            skipped += 1
            updates.append(
                {
                    "recommendation_id": recommendation_id,
                    "status": "error",
                    "decision_reason": str(error),
                    "updated_ts": _now_ms(),
                }
            )
        else:
            placed += 1
            updates.append(
                {
                    "recommendation_id": recommendation_id,
                    "status": "placed",
                    "bet_id": bet_id,
                    "decision_reason": decision_reason or "auto", 
                    "updated_ts": _now_ms(),
                    "final_ts": _now_ms(),
                }
            )

    if updates:
        sink.upsert_superfecta_recommendations(updates)

    return {"attempted": attempted, "placed": placed, "skipped": skipped}
