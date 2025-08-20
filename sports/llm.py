import os
import time
from .config import cfg

def generate_suggestion(match_details: dict):
    """
    Analyzes a sports event using an LLM with detailed context to provide a high-quality prediction.
    """
    # --- **ENHANCED PROMPT** ---
    prompt = f"""
    **Task:** Provide an expert analysis and performance prediction for the following event.

    **Sport:** {match_details.get('sport', 'Unknown')}

    **Event:** {match_details.get('home')} vs {match_details.get('away')}
    
    **Statistical Model Snapshot:**
    - Home/Player 1 Win Probability: {match_details.get('prob_home', 'N/A'):.2%}
    - Draw Probability: {match_details.get('prob_draw', 'N/A'):.2%}
    - Away/Player 2 Win Probability: {match_details.get('prob_away', 'N/A'):.2%}

    **Key Contextual Data:**
    - **Recent Form (Last 5):**
        - {match_details.get('home')}: {match_details.get('form_home', 'No data available.')}
        - {match_details.get('away')}: {match_details.get('form_away', 'No data available.')}
    - **Head-to-Head (Last 3):** {match_details.get('h2h', 'No data available.')}
    - **League/Tournament Context:**
        {match_details.get('context', 'No additional context.')}

    **Instructions:**
    1.  Synthesize all the provided data to form a cohesive analysis (2-3 sentences).
    2.  Identify the most likely outcome based on your analysis.
    3.  Suggest a primary prediction and two alternative predictions (e.g., total goals, sets, points).
    4.  The entire response must be a single, concise paragraph.

    **Your Expert Analysis:**
    """
    
    # ... (existing LLM call and error handling logic)
