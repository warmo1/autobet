# In sports/llm.py

def generate_suggestion(match_details):
    prompt = f"""
    Analyze the following football match and provide a betting suggestion.

    **Match:** {match_details['home']} vs {match_details['away']}

    Please provide:
    1. A primary bet suggestion (e.g., match odds, over/under goals).
    2. The reasoning for your suggestion, based on recent form, team news, and any other relevant factors.
    3. Suggestions for alternate markets, such as corners, cards, or player props.
    """
    if cfg.llm_provider == "openai":
        # ... your OpenAI logic
    else:
        # ... your Gemini logic
        model = genai.GenerativeModel(cfg.llm_model)
        r = model.generate_content(prompt)
        return r.text.strip()
