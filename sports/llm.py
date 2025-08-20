import os
from .config import cfg

def summarise_news(items, max_chars=800):
    text = "\n".join([f"- {i['title']} ({i['url']})" for i in items])[:4000]
    prompt = f"Summarise these sports news items for a betting dashboard. Be concise, note injuries or suspensions, and quantify market-moving signals. Return <= {max_chars} characters.\n{text}"
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
