from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

import pandas as pd

from .providers.pl_calcs import calculate_pl_strategy


SUPERFECTA_RISK_PRESETS: Dict[str, Dict[str, Any]] = {
    "conservative": {
        "label": "Conservative",
        "bankroll_pct": 0.02,
        "desired_profit_pct": 12.0,
        "concentration": 0.15,
        "market_inefficiency": 0.05,
        "key_horses": 1,
        "poor_horses": 1,
        "min_stake_per_line": 0.1,
    },
    "balanced": {
        "label": "Balanced",
        "bankroll_pct": 0.03,
        "desired_profit_pct": 25.0,
        "concentration": 0.25,
        "market_inefficiency": 0.08,
        "key_horses": 2,
        "poor_horses": 1,
        "min_stake_per_line": 0.1,
    },
    "aggressive": {
        "label": "Aggressive",
        "bankroll_pct": 0.05,
        "desired_profit_pct": 40.0,
        "concentration": 0.4,
        "market_inefficiency": 0.12,
        "key_horses": 3,
        "poor_horses": 0,
        "min_stake_per_line": 0.0,
    },
}


def safe_float(value: Any, default: float = 0.0) -> float:
    """Best-effort float coercion that tolerates None and NaN values."""
    try:
        if value is None:
            return default
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def group_superfecta_predictions(
    df: pd.DataFrame,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Group runner-level predictions into per-product event dictionaries."""
    events: List[Dict[str, Any]] = []
    events_map: Dict[str, Dict[str, Any]] = {}
    if df.empty:
        return events, events_map

    for product_id, group in df.groupby("product_id"):
        group_sorted = group.sort_values("p_place1", ascending=False)
        anchor = group_sorted.iloc[0]
        start_iso = anchor.get("start_iso") or ""
        event = {
            "product_id": product_id,
            "event_id": anchor.get("event_id"),
            "event_name": anchor.get("event_name"),
            "venue": anchor.get("venue"),
            "country": anchor.get("country"),
            "start_iso": start_iso,
            "status": anchor.get("status"),
            "currency": anchor.get("currency"),
            "total_gross": safe_float(anchor.get("total_gross")),
            "total_net": safe_float(anchor.get("total_net")),
            "rollover": safe_float(anchor.get("rollover")),
            "deduction_rate": safe_float(anchor.get("deduction_rate"), 0.30),
            "model_id": anchor.get("model_id"),
            "model_version": anchor.get("model_version"),
            "scored_at": anchor.get("scored_at"),
            "event_date": anchor.get("event_date"),
            "runners": [],
        }
        for _, runner in group_sorted.iterrows():
            event["runners"].append(
                {
                    "horse_id": runner.get("horse_id"),
                    "horse_name": runner.get("horse_name"),
                    "cloth_number": runner.get("cloth_number"),
                    "p_place1": float(runner.get("p_place1") or 0.0),
                    "recent_runs": runner.get("recent_runs"),
                    "avg_finish": runner.get("avg_finish"),
                    "wins_last5": runner.get("wins_last5"),
                    "places_last5": runner.get("places_last5"),
                    "days_since_last_run": runner.get("days_since_last_run"),
                    "going": runner.get("going"),
                }
            )
        event["top_runners"] = event["runners"][:4]
        events.append(event)
        events_map[product_id] = event

    events.sort(key=lambda ev: ev.get("start_iso") or "")
    return events, events_map


def compute_superfecta_plan(
    event: Dict[str, Any],
    preset_key: str,
    tote_bank: float,
) -> Dict[str, Any]:
    """Generate a Plackett–Luce staking plan for a single event."""
    preset = SUPERFECTA_RISK_PRESETS.get(preset_key, SUPERFECTA_RISK_PRESETS["balanced"])
    errors: List[str] = []

    bankroll = tote_bank * float(preset.get("bankroll_pct", 0.0))
    if bankroll <= 0:
        errors.append("Bankroll allocation must be positive.")

    runners = event.get("runners") or []
    if len(runners) < 4:
        errors.append("Not enough runners with predictions to build a Superfecta plan.")

    if errors:
        return {"errors": errors}

    runners_sorted = sorted(runners, key=lambda r: r.get("p_place1", 0.0), reverse=True)
    key_count = min(int(preset.get("key_horses", 0)), len(runners_sorted))
    poor_count = min(int(preset.get("poor_horses", 0)), len(runners_sorted))

    prepared_runners: List[Dict[str, Any]] = []
    for idx, runner in enumerate(runners_sorted):
        prob = max(float(runner.get("p_place1") or 0.0), 1e-6)
        fair_odds = max(1.05, 1.0 / prob)
        cloth = runner.get("cloth_number") or (idx + 1)
        prepared_runners.append(
            {
                "name": runner.get("horse_name") or runner.get("horse_id") or f"Runner {idx+1}",
                "odds": fair_odds,
                "number": cloth,
                "is_key": idx < key_count,
                "is_poor": idx >= len(runners_sorted) - poor_count if poor_count else False,
            }
        )

    take_rate = event.get("deduction_rate")
    take_rate = float(take_rate) if take_rate is not None else 0.30
    pool_other = max(safe_float(event.get("total_net")) - bankroll, 0.0)
    rollover = safe_float(event.get("rollover"))

    result = calculate_pl_strategy(
        runners=prepared_runners,
        bet_type="SUPERFECTA",
        bankroll=bankroll,
        key_horse_mult=1.2,
        poor_horse_mult=0.85,
        concentration=float(preset.get("concentration", 0.2)),
        market_inefficiency=float(preset.get("market_inefficiency", 0.05)),
        desired_profit_pct=float(preset.get("desired_profit_pct", 20.0)),
        take_rate=take_rate,
        net_rollover=rollover,
        inc_self=True,
        div_mult=1.0,
        f_fix=None,
        pool_gross_other=pool_other,
        min_stake_per_line=float(preset.get("min_stake_per_line", 0.0)),
    )

    errors.extend(result.get("errors", []))
    if errors:
        return {"errors": errors}

    pl_model = result.get("pl_model") or {}
    best = pl_model.get("best_scenario") or {}
    staking_plan = pl_model.get("staking_plan") or []

    total_stake = sum(float(line.get("stake", 0.0)) for line in staking_plan)
    lines: List[Dict[str, Any]] = []
    selections: List[str] = []
    for line in staking_plan:
        ids = [str(x) for x in (line.get("ids") or [])]
        combo = "-".join(ids)
        selections.append(combo)
        lines.append(
            {
                "line": combo or line.get("line"),
                "probability": float(line.get("probability", 0.0)),
                "stake": float(line.get("stake", 0.0)),
                "stake_rounded": round(float(line.get("stake", 0.0)), 2),
            }
        )

    adjustment = result.get("auto_adjusted_params")

    summary = {
        "lines": lines,
        "total_stake": total_stake,
        "total_stake_rounded": round(total_stake, 2),
        "expected_profit": best.get("expected_profit"),
        "expected_return": best.get("expected_return"),
        "hit_rate": best.get("hit_rate"),
        "f_share_used": best.get("f_share_used"),
        "bankroll_allocated": bankroll,
        "selections_text": "\n".join(selections),
        "preset_key": preset_key,
        "adjustment": adjustment,
    }
    return {"plan": summary, "errors": []}
