import time
import pandas as pd
from .db import list_matches, record_suggestion
from .model_football import fit_poisson, match_goal_rates, match_probs_from_rates
from .odds import choose_back_or_lay, kelly_fraction
from .llm import generate_suggestion # Import the new function

def suggest_football(conn, days:int=7, min_edge:float=0.03, stake_kelly_mult:float=0.25):
    """
    Generates football betting suggestions by combining a statistical Poisson model
    with qualitative analysis from an LLM.
    """
    # Use all historical matches with results to fit the statistical model
    rows = list_matches(conn, "football")
    df = pd.DataFrame(rows, columns=["id","date","home","away","fthg","ftag","ftr","comp","season"])
    
    if df.empty:
        print("[suggest] No matches ingested yet. Cannot generate suggestions.")
        return []

    df_train = df.dropna(subset=["fthg", "ftag"])
    if df_train.empty:
        print("[suggest] No completed matches with scores available to train the model.")
        return []

    print(f"[suggest] Training Poisson model on {len(df_train)} historical matches...")
    att, deff, mu_h, mu_a = fit_poisson(df_train)
    
    # Identify upcoming fixtures to generate suggestions for (demo: using last 50 rows)
    # In a real system, you would fetch upcoming fixtures from an API.
    upcoming_fixtures = df.tail(50)
    picks = []

    print(f"[suggest] Analyzing {len(upcoming_fixtures)} upcoming fixtures...")
    for _, fixture in upcoming_fixtures.iterrows():
        # 1. Calculate statistical probabilities using the Poisson model
        lam_h, lam_a = match_goal_rates(fixture["home"], fixture["away"], att, deff, mu_h, mu_a)
        pH, pD, pA = match_probs_from_rates(lam_h, lam_a)

        # 2. Get LLM analysis for qualitative insight
        match_details = {"home": fixture["home"], "away": fixture["away"]}
        model_probs = {"home": pH, "draw": pD, "away": pA}
        llm_reasoning = generate_suggestion(match_details, model_probs)
        
        # 3. Evaluate betting options against mock odds
        # In a real system, you would fetch live odds from an API (e.g., Betfair).
        mock_odds_h, mock_odds_d, mock_odds_a = 2.4, 3.2, 3.0
        
        options = [
            ("home", pH, mock_odds_h),
            ("draw", pD, mock_odds_d),
            ("away", pA, mock_odds_a)
        ]
        
        best_option = None
        for selection, model_prob, market_odds in options:
            side, edge = choose_back_or_lay(model_prob, market_odds)
            if best_option is None or edge > best_option[3]:
                best_option = (selection, model_prob, market_odds, edge, side)
        
        # 4. If a valuable edge is found, record the suggestion
        if best_option and best_option[3] >= min_edge:
            selection, model_prob, market_odds, edge, side = best_option
            
            # Calculate stake using Kelly Criterion
            stake = round(max(0.0, kelly_fraction(model_prob, market_odds - 1) * stake_kelly_mult * 100), 2)
            
            if stake > 0:
                suggestion_record = dict(
                    created_ts=int(time.time() * 1000),
                    sport="football",
                    date=fixture["date"],
                    home=fixture["home"],
                    away=fixture["away"],
                    market="match_odds",
                    side=side,
                    selection=selection,
                    model_prob=float(model_prob),
                    market_odds=float(market_odds),
                    edge=float(edge),
                    stake=float(stake),
                    note=llm_reasoning  # Store the LLM's reasoning
                )
                record_suggestion(conn, **suggestion_record)
                picks.append(suggestion_record)

    print(f"[suggest] Finished. Generated {len(picks)} new suggestions.")
    return picks
