import time
import pandas as pd
import sqlite3
import requests
from .llm import generate_suggestion
from .model_football import fit_poisson, match_goal_rates, match_probs_from_rates

def get_all_completed_matches(conn: sqlite3.Connection, sport: str) -> pd.DataFrame:
    """Fetches all completed matches for a given sport to train a model."""
    query = """
    SELECT
        e.start_date,
        e.home_team,
        e.away_team,
        r.home_score,
        r.away_score
    FROM events e
    JOIN results r ON e.event_id = r.event_id
    WHERE e.sport = ? AND e.status = 'completed' AND r.home_score IS NOT NULL AND r.away_score IS NOT NULL
    """
    df = pd.read_sql_query(query, conn, params=(sport,))
    return df

def get_upcoming_fixtures(conn: sqlite3.Connection, sport: str) -> pd.DataFrame:
    """Fetches all scheduled (upcoming) fixtures for a given sport."""
    query = "SELECT event_id, start_date, home_team, away_team FROM events WHERE sport = ? AND status = 'scheduled'"
    df = pd.read_sql_query(query, conn, params=(sport,))
    return df

def get_historical_h2h(conn: sqlite3.Connection, team1: str, team2: str) -> str:
    """Gets the last 5 head-to-head results for two teams."""
    query = """
    SELECT e.start_date, e.home_team, e.away_team, r.home_score, r.away_score
    FROM events e JOIN results r ON e.event_id = r.event_id
    WHERE ((e.home_team = ? AND e.away_team = ?) OR (e.home_team = ? AND e.away_team = ?))
    AND e.status = 'completed'
    ORDER BY e.start_date DESC LIMIT 5
    """
    df = pd.read_sql_query(query, conn, params=(team1, team2, team2, team1))
    if df.empty:
        return "No recent head-to-head matches found."
    return df.to_string(index=False)

def get_team_form(conn: sqlite3.Connection, team: str) -> str:
    """Gets the last 5 results for a single team."""
    query = """
    SELECT e.start_date, e.home_team, e.away_team, r.outcome
    FROM events e JOIN results r ON e.event_id = r.event_id
    WHERE (e.home_team = ? OR e.away_team = ?) AND e.status = 'completed'
    ORDER BY e.start_date DESC LIMIT 5
    """
    df = pd.read_sql_query(query, conn, params=(team, team))
    if df.empty:
        return "No recent form data found."
    return df.to_string(index=False)

def get_weather_forecast(city: str) -> str:
    """Gets the weather forecast for a given city."""
    return "Weather forecast: 15°C, clear skies, light breeze." # Placeholder

def generate_football_suggestions(conn: sqlite3.Connection):
    """
    Generates betting suggestions for upcoming football matches using a statistical model and rich context for an LLM.
    """
    print("[Suggest] Loading historical data to train model...")
    historical_df = get_all_completed_matches(conn, 'football')
    if historical_df.empty or len(historical_df) < 20: # Need sufficient data to train
        print("[Suggest] Not enough historical data found to train the model. Aborting.")
        return

    # **FIX**: Train the statistical model properly
    historical_df = historical_df.rename(columns={'home_score': 'fthg', 'away_score': 'ftag', 'home_team': 'home', 'away_team': 'away'})
    att, deff, mu_h, mu_a = fit_poisson(historical_df)
    print("[Suggest] Statistical model training complete.")

    print("[Suggest] Fetching upcoming fixtures...")
    fixtures_df = get_upcoming_fixtures(conn, 'football')
    if fixtures_df.empty:
        print("[Suggest] No upcoming fixtures found in the database.")
        return

    print(f"[Suggest] Analyzing {len(fixtures_df)} upcoming fixtures...")
    for _, fixture in fixtures_df.iterrows():
        event_id = fixture['event_id']
        home_team = fixture['home_team']
        away_team = fixture['away_team']
        
        # 1. Gather Rich Context
        h2h_stats = get_historical_h2h(conn, home_team, away_team)
        home_form = get_team_form(conn, home_team)
        away_form = get_team_form(conn, away_team)
        weather = get_weather_forecast("London")

        # **FIX**: Calculate real probabilities from the trained model
        lam_h, lam_a = match_goal_rates(home_team, away_team, att, deff, mu_h, mu_a)
        prob_h, prob_d, prob_a = match_probs_from_rates(lam_h, lam_a)

        # 2. Construct Detailed Prompt for the AI with valid probabilities
        match_details = {
            "sport": "Football",
            "home": home_team,
            "away": away_team,
            "h2h": h2h_stats,
            "form_home": home_form,
            "form_away": away_form,
            "weather": weather,
            "prob_home": prob_h,
            "prob_draw": prob_d,
            "prob_away": prob_a,
            "context": "Standard league match."
        }
        
        llm_reasoning = generate_suggestion(match_details)

        # 3. Save the generated suggestion
        conn.execute(
            """INSERT INTO suggestions (created_ts, event_id, market, selection, side, note, model_prob) 
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (int(time.time()), int(event_id), 'AI Analysis', 'N/A', 'N/A', llm_reasoning, prob_h) # Storing home win prob as an example
        )
    
    conn.commit()
    print(f"[Suggest] Successfully generated and saved {len(fixtures_df)} new football suggestions.")
