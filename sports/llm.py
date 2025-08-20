import os
import time
import re
from .config import cfg

def summarise_news(items, max_chars=800):
    """Summarises a list of news articles for a betting dashboard."""
    text = "\n".join([f"- {i['title']} ({i['url']})" for i in items])[:4000]
    prompt = f"Summarise these sports news items for a betting dashboard. Be concise, note injuries or suspensions, and quantify market-moving signals. Return <= {max_chars} characters.\n{text}"
    
    # This function can be expanded with the same retry/error handling as generate_suggestion
    if cfg.llm_provider == "openai":
        from openai import OpenAI
        client = OpenAI(api_key=cfg.openai_key)
        r = client.chat.completions.create(model=cfg.llm_model, messages=[{"role":"user","content":prompt}])
        return r.choices[0].message.content.strip()
    else:
        import google.generativeai as genai
        genai.configure(api_key=cfg.gemini_key)
        model = genai.GenerativeModel(cfg.llm_model)
        r = model.generate_content(prompt)
        return r.text.strip()

def generate_suggestion(match_details: dict):
    """
    Analyzes a sports event using an LLM with detailed context to provide a high-quality prediction.
    """
    prompt = f"""
    **Task:** Analyze the upcoming football match and provide a performance prediction.

    **Match:** {match_details['home']} vs {match_details['away']}

    **Statistical Model Probabilities:**
    - Home Win Probability: {match_details.get('prob_home', 0):.2%}
    - Draw Probability: {match_details.get('prob_draw', 0):.2%}
    - Away Win Probability: {match_details.get('prob_away', 0):.2%}

    **Instructions:**
    1.  Provide a brief, sharp analysis (2-3 sentences) explaining your primary prediction.
    2.  Conclude with a primary match outcome prediction (e.g., Home Win, Draw) and one or two alternative statistical predictions (e.g., Total Goals Over/Under 2.5, Both Teams to Score).
    3.  The entire response must be a single paragraph.

    **Your Analysis:**
    """
    
    max_retries = 3
    print(f"[LLM] Analyzing: {match_details['home']} vs {match_details['away']}")
    for attempt in range(max_retries):
        try:
            if cfg.llm_provider == "openai":
                from openai import OpenAI
                client = OpenAI(api_key=cfg.openai_key)
                response = client.chat.completions.create(
                    model=cfg.llm_model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7,
                    max_tokens=150
                )
                return response.choices[0].message.content.strip()
            else:
                import google.generativeai as genai
                genai.configure(api_key=cfg.gemini_key)
                
                safety_settings = [
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                ]
                
                model = genai.GenerativeModel(cfg.llm_model, safety_settings=safety_settings)
                response = model.generate_content(prompt)
                
                if response.parts:
                    return response.text.strip()
                else:
                    finish_reason = response.candidates[0].finish_reason.name if response.candidates else 'UNKNOWN'
                    print(f"[LLM Warning] Empty response. Finish reason: {finish_reason}.")
                    return "AI analysis was blocked or returned empty."

        except Exception as e:
            print(f"[LLM Error] Attempt {attempt + 1}/{max_retries} failed: {e}")
            if "500" in str(e) and attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                return "LLM analysis failed due to a persistent error."
    
    return "LLM analysis failed after multiple retries."
