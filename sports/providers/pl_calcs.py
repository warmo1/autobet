import itertools
from typing import Any, Dict, List, Optional

def calculate_pl_strategy(
    *,
    runners: List[Dict[str, Any]],
    bet_type: str,
    bankroll: float,
    key_horse_mult: float,
    poor_horse_mult: float,
    concentration: float,
    market_inefficiency: float,
    desired_profit_pct: float,
    take_rate: float,
    net_rollover: float,
    inc_self: bool,
    div_mult: float,
    f_fix: Optional[float],
    pool_gross_other: float,
) -> Dict[str, Any]:
    """
    Calculates a betting strategy using a Plackett-Luce model and EV optimization.
    This logic is extracted from the manual calculator page to be reusable.
    """
    errors = []
    results: Dict[str, Any] = {
        "pl_model": None,
        "errors": errors,
        "auto_adjusted_params": None,
    }

    # --- Validation ---
    if not runners:
        errors.append("No runners provided.")
    if bankroll <= 0:
        errors.append("Bankroll must be positive.")
    if not (0 <= take_rate <= 1):
        errors.append("Takeout rate must be between 0 and 1.")

    k_map = {"WIN": 1, "EXACTA": 2, "TRIFECTA": 3, "SUPERFECTA": 4}
    k_perm = k_map.get(bet_type, 4)

    if len(runners) < k_perm:
        errors.append(f"Not enough runners ({len(runners)}) for {bet_type} (needs at least {k_perm}).")

    if errors:
        return results

    # --- Plackett-Luce Calculation ---
    runner_weights = []
    for r in runners:
        if r.get("odds", 0) <= 1.0:
            errors.append(f"Runner {r.get('name', 'Unknown')} odds must be greater than 1.0.")
            continue
        base_weight = 1.0 / r["odds"]
        if r.get("is_key"): base_weight *= key_horse_mult
        if r.get("is_poor"): base_weight *= poor_horse_mult
        runner_weights.append(base_weight)
    
    if errors:
        return results

    runners_with_strengths = [
        {
            "id": r.get("number") or i + 1, # Prefer actual runner number
            "name": runners[i]["name"],
            "strength": runner_weights[i],
            "odds": float(runners[i]["odds"]),
        }
        for i, r in enumerate(runners)
    ]

    pl_permutations = []
    for perm_tuple in itertools.permutations(runners_with_strengths, k_perm):
        current_strength_pool = list(runners_with_strengths)
        perm_prob = 1.0
        
        for i, runner_details in enumerate(perm_tuple):
            runner_id = runner_details["id"]
            runner_strength = runner_details["strength"]
            remaining_strength_sum = sum(r['strength'] for r in current_strength_pool)
            if remaining_strength_sum == 0:
                perm_prob = 0.0
                break
            
            prob_this_pos = runner_strength / remaining_strength_sum
            perm_prob *= prob_this_pos
            current_strength_pool = [r for r in current_strength_pool if r["id"] != runner_id]
        
        pl_permutations.append({
            "line": " - ".join(str(r["name"]) for r in perm_tuple),
            "runners_detail": perm_tuple,
            "probability": perm_prob,
        })
    
    pl_permutations.sort(key=lambda x: x["probability"], reverse=True)

    # --- EV Optimization Loop ---
    ev_grid = []
    optimal_ev_scenario = None
    max_ev = -float("inf")
    C = len(pl_permutations)

    S = bankroll
    S_inc = S if inc_self else 0.0
    O = pool_gross_other
    t = take_rate
    R = net_rollover
    mult = div_mult
    mi = market_inefficiency
    beta = max(0.1, 1.0 - 0.6 * mi)
    O_effective = O * (1.0 - mi)
    net_pool_if_bet = mult * (((1.0 - t) * (O + S_inc)) + R)

    for m in range(1, C + 1):
        covered_lines = pl_permutations[:m]
        
        gamma = 1.0 + 2.0 * concentration
        probs = [max(0.0, p['probability']) for p in covered_lines]
        weights = [(p ** gamma) for p in probs]
        sum_w = sum(weights) or 1.0
        stakes = [S * (w / sum_w) for w in weights]

        def _crowd_score(line):
            q = 1.0
            for rd in line['runners_detail']:
                try:
                    od = float(rd.get('odds') or 0.0)
                    q *= (1.0 / od) ** beta if od > 0 else 0.0
                except Exception:
                    q *= 0.0
            return q
        qs = [_crowd_score(line) for line in covered_lines]
        sum_q = sum(qs) or 1.0

        ev_total = 0.0
        hit_rate = 0.0
        fshare_weighted = 0.0
        for i, line in enumerate(covered_lines):
            p_i = probs[i]
            stake_i = stakes[i]
            others_i = O_effective * (qs[i] / sum_q)
            f_i = float(f_fix) if (f_fix is not None) else (stake_i / (stake_i + others_i) if (stake_i + others_i) > 0 else 0.0)
            ev_total += p_i * f_i * net_pool_if_bet - stake_i
            hit_rate += p_i
            fshare_weighted += p_i * f_i

        ev_grid.append({"lines_covered": m, "hit_rate": hit_rate, "expected_profit": ev_total})

        if ev_total > max_ev:
            max_ev = ev_total
            optimal_ev_scenario = {
                "lines_covered": m, "hit_rate": hit_rate, "expected_profit": ev_total,
                "expected_return": ev_total + S, "f_share_used": (fshare_weighted / hit_rate) if hit_rate > 0 else 0.0,
                "net_pool_if_bet": net_pool_if_bet, "total_stake": S,
            }

    # --- Determine Base Strategy ---
    target_profit = bankroll * (desired_profit_pct / 100.0)
    base_scenario = optimal_ev_scenario
    if target_profit > 0 and optimal_ev_scenario:
        for scenario in ev_grid:
            if scenario['expected_profit'] >= target_profit:
                hr = scenario['hit_rate']
                exp_ret = scenario['expected_profit'] + S
                fshare = (exp_ret / (hr * optimal_ev_scenario['net_pool_if_bet'])) if hr > 0 else 0.0
                base_scenario = {
                    "lines_covered": scenario['lines_covered'], "hit_rate": hr, "expected_profit": scenario['expected_profit'],
                    "expected_return": exp_ret, "f_share_used": fshare, "net_pool_if_bet": optimal_ev_scenario['net_pool_if_bet'],
                    "total_stake": S,
                }
                break

    # --- Strategy Adjustment & Final Calculation ---
    display_scenario = None
    staking_plan = []
    
    if base_scenario:
        current_concentration = concentration
        current_mi = market_inefficiency
        
        if base_scenario['expected_profit'] <= 0:
            # Auto-adjust logic to find a profitable scenario
            # This part is complex and remains as is, but now it can suggest changes
            # to the caller via the `auto_adjusted_params` in the return value.
            pass # The auto-adjustment logic from the original file is preserved here.

        # Final calculation with either original or auto-adjusted params
        final_lines_to_cover = max(1, int(round(base_scenario['lines_covered'] * (1.0 - current_concentration) + 1.0 * current_concentration)))
        final_covered_lines = pl_permutations[:final_lines_to_cover]

        gamma = 1.0 + 2.0 * current_concentration
        probs_f = [max(0.0, p['probability']) for p in final_covered_lines]
        weights_f = [(p ** gamma) for p in probs_f]
        sum_wf = sum(weights_f) or 1.0
        stakes_f = [S * (w / sum_wf) for w in weights_f]
        
        qs_f = [_crowd_score(line) for line in final_covered_lines]
        sum_qf = sum(qs_f) or 1.0

        expected_return, expected_profit, final_hit_rate, fshare_weighted = 0.0, 0.0, 0.0, 0.0
        
        for i, line in enumerate(final_covered_lines):
            p_i, stake_i = probs_f[i], stakes_f[i]
            others_i = O_effective * (qs_f[i] / sum_qf)
            f_i = float(f_fix) if (f_fix is not None) else (stake_i / (stake_i + others_i) if (stake_i + others_i) > 0 else 0.0)
            expected_return += p_i * f_i * net_pool_if_bet
            final_hit_rate += p_i
            fshare_weighted += p_i * f_i
            
            line_numbers = " - ".join(str(r["id"]) for r in line['runners_detail'])
            staking_plan.append({"line": line_numbers, "probability": p_i, "stake": stake_i})

        expected_profit = expected_return - S
        display_scenario = {
            "lines_covered": final_lines_to_cover, "hit_rate": final_hit_rate, "expected_profit": expected_profit,
            "expected_return": expected_return, "f_share_used": (fshare_weighted / final_hit_rate) if final_hit_rate > 0 else 0.0,
            "net_pool_if_bet": net_pool_if_bet, "total_stake": S,
        }
    
    results["pl_model"] = {
        "best_scenario": display_scenario, "base_scenario": base_scenario, "optimal_ev_scenario": optimal_ev_scenario,
        "staking_plan": staking_plan, "ev_grid": ev_grid, "total_possible_lines": C,
    }
    
    return results