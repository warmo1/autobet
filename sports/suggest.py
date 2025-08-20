import time
import pandas as pd
from .db import list_matches, recent_suggestions, record_suggestion
from .model_football import fit_poisson, match_goal_rates, match_probs_from_rates
from .odds import fair_probs_from_odds, choose_back_or_lay, kelly_fraction

def suggest_football(conn, days:int=7, min_edge:float=0.03, stake_kelly_mult:float=0.25):
    # Use all historical matches with results to fit
    import pandas as pd
    rows = list_matches(conn, "football")
    df = pd.DataFrame(rows, columns=["id","date","home","away","fthg","ftag","ftr","comp","season"])
    if df.empty:
        print("[suggest] No matches ingested yet.")
        return []
    df_train = df.dropna(subset=["fthg","ftag"])  # use completed matches
    att,deff,mu_h,mu_a = fit_poisson(df_train.rename(columns={"fthg":"fthg","ftag":"ftag","home":"home","away":"away"}))
    # For simplicity, propose suggestions for the *most recent* day(s) present in DB without results (future fixtures would need separate feed)
    # Here we just score last N rows (demo).
    picks = []
    tail = df.tail(50)  # latest 50
    for _, r in tail.iterrows():
        lam_h, lam_a = match_goal_rates(r["home"], r["away"], att, deff, mu_h, mu_a)
        pH,pD,pA = match_probs_from_rates(lam_h, lam_a)
        # fake odds from last known book in DB for demo: use 2.4/3.2/3.0 if missing
        oddH, oddD, oddA = 2.4, 3.2, 3.0
        # choose best edge among H/D/A
        options = [("home", pH, oddH), ("draw", pD, oddD), ("away", pA, oddA)]
        best = None
        for sel, p, o in options:
            side, edge = choose_back_or_lay(p, o)
            if best is None or edge > best[3]:
                best = (sel, p, o, edge, side)
        if best and best[3] >= min_edge:
            stake = round(max(0.0, kelly_fraction(best[1], best[2]-1)*stake_kelly_mult*100), 2)
            rec = dict(
                created_ts=int(time.time()*1000),
                sport="football",
                date=r["date"],
                home=r["home"],
                away=r["away"],
                market="match_odds",
                side=best[4],
                selection=best[0],
                model_prob=float(best[1]),
                market_odds=float(best[2]),
                edge=float(best[3]),
                stake=float(stake),
                note="Poisson"
            )
            record_suggestion(conn, **rec)
            picks.append(rec)
    return picks
    from .llm import generate_suggestion

def suggest_llm(conn, match_details):
    llm_suggestion = generate_suggestion(match_details)
    # Parse the LLM's response and format it into a suggestion record
    # ... your parsing logic here ...

    # Record the suggestion in the database
    record_suggestion(conn, **rec)
