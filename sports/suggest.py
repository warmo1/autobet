import time
import pandas as pd
import sqlite3
import requests
from .llm import generate_suggestion
from .model_football import fit_poisson, match_goal_rates, match_probs_from_rates

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
    # In a real application, you would use a weather API here.
    # This requires signing up for a service and adding an API key to your .env file.
    return "Weather forecast: 15°C, clear skies, light breeze." # Placeholder

def generate_football_suggestions(conn: sqlite3.Connection):
    """
    Generates betting suggestions for upcoming football matches using rich context.
    """
    print("[Suggest] Fetching upcoming fixtures...")
    fixtures_df = pd.read_sql_query("SELECT event_id, home_team, away_team FROM events WHERE sport = 'football' AND status = 'scheduled'", conn)

    if fixtures_df.empty:
        print("[Suggest] No upcoming fixtures found in the database.")
        return

    print(f"[Suggest] Analyzing {len(fixtures_df)} upcoming fixtures...")
    for _, fixture in fixtures_df.iterrows():
        event_id = fixture['event_id']
        home_team = fixture['home_team']
        away_team = fixture['away_team']
        
        # 1. Gather Rich Context from your database
        print(f"[Suggest] Gathering context for {home_team} vs {away_team}...")
        h2h_stats = get_historical_h2h(conn, home_team, away_team)
        home_form = get_team_form(conn, home_team)
        away_form = get_team_form(conn, away_team)
        weather = get_weather_forecast("London") # Placeholder city, can be improved

        # 2. Construct Detailed Prompt for the AI
        match_details = {
            "sport": "Football",
            "home": home_team,
            "away": away_team,
            "h2h": h2h_stats,
            "form_home": home_form,
            "form_away": away_form,
            "weather": weather,
            "context": "This is a standard league match." # Placeholder context
        }
        
        # 3. Call the LLM to get an expert analysis
        llm_reasoning = generate_suggestion(match_details)

        # 4. Save the generated suggestion to the database
        conn.execute(
            """INSERT INTO suggestions (created_ts, event_id, market, selection, side, note) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            (int(time.time()), int(event_id), 'AI Analysis', 'N/A', 'N/A', llm_reasoning)
        )
    
    conn.commit()
    print(f"[Suggest] Successfully generated and saved {len(fixtures_df)} new football suggestions.")
