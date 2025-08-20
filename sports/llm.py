import os
import time
from .config import cfg

def summarise_news(items, max_chars=800):
    """Summarises a list of news articles for a betting dashboard."""
    text = "\n".join([f"- {i['title']} ({i['url']})" for i in items])[:4000]
    prompt = f"Summarise these sports news items for a betting dashboard. Be concise, note injuries or suspensions, and quantify market-moving signals. Return <= {max_chars} characters.\n{text}"
    
    if cfg.llm_provider == "openai":
        # This block handles the OpenAI API call
        from openai import OpenAI
        client = OpenAI(api_key=cfg.openai_key)
        r = client.chat.completions.create(model=cfg.llm_model, messages=[{"role":"user","content":prompt}])
        return r.choices[0].message.content.strip()
    else:
        # This block handles the Gemini API call
        import google.generativeai as genai
        genai.configure(api_key=cfg.gemini_key)
        model = genai.GenerativeModel(cfg.llm_model)
        r = model.generate_content(prompt)
        return r.text.strip()

def generate_suggestion(match_details: dict, model_probs: dict):
    """
    Analyzes a football match using an LLM to provide a betting suggestion and reasoning.
    Includes retry logic and robust error handling.
    """
    prompt = f"""
    **Task:** Analyze the upcoming football match and provide a concise betting insight.

    **Match:** {match_details['home']} vs {match_details['away']}

    **Statistical Model Probabilities:**
    - Home Win: {model_probs['home']:.2%}
    - Draw: {model_probs['draw']:.2%}
    - Away Win: {model_probs['away']:.2%}

    **Instructions:**
    1.  Provide a brief, sharp analysis (max 2-3 sentences) explaining your primary betting angle.
    2.  Consider factors like team form, key player news (injuries/suspensions), and head-to-head records.
    3.  Conclude with a primary bet suggestion and one or two alternative market ideas (e.g., Over/Under 2.5 Goals, Both Teams to Score, Corners, Cards).
    4.  The entire response should be a single paragraph.

    **Example Response:**
    "Manchester City's dominant home form and Liverpool's defensive frailties suggest a high-scoring affair. With Haaland in prolific form, City are likely to control the game. Primary Bet: Manchester City to Win. Alternatives: Over 2.5 Goals, Both Teams to Score."

    **Your Analysis:**
    """
    
    max_retries = 3
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
                
                # Configure model with less strict safety settings to avoid blocking responses
                safety_settings = [
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_ONLY_HIGH"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_ONLY_HIGH"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_ONLY_HIGH"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_ONLY_HIGH"},
                ]
                
                model = genai.GenerativeModel(cfg.llm_model, safety_settings=safety_settings)
                response = model.generate_content(prompt)
                
                # **Crucial Fix**: Check for valid content before accessing .text
                # This handles cases where safety filters block the response.
                if response.parts:
                    return response.text.strip()
                else:
                    finish_reason = response.candidates[0].finish_reason if response.candidates else 'UNKNOWN'
                    print(f"[LLM Warning] Received empty response from API. Finish reason: {finish_reason}. This might be due to safety filters.")
                    return "AI analysis was blocked or returned empty."

        except Exception as e:
            print(f"[LLM Error] Attempt {attempt + 1}/{max_retries} failed: {e}")
            # Retry on 500-series errors
            if "500" in str(e) and attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
                continue
            else:
                return "LLM analysis could not be generated due to a persistent error."
    
    return "LLM analysis failed after multiple retries."
