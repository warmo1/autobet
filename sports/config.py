import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

@dataclass
class Config:
    # --- Database and Core Settings ---
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///sports_bot.db")
    paper_starting_bankroll: float = float(os.getenv("PAPER_STARTING_BANKROLL", "1000"))
    admin_token: str = os.getenv("ADMIN_TOKEN", "")

    # --- LLM/AI Configuration ---
    llm_provider: str = os.getenv("LLM_PROVIDER", "openai").lower()
    
    # Note: "gpt-5" is not yet released. The llm.py script should handle this
    # by defaulting to a model like gpt-4o if gpt-5 is not available.
    llm_model_openai: str = os.getenv("LLM_MODEL_OPENAI", "gpt-5")
    llm_model_gemini: str = os.getenv("LLM_MODEL_GEMINI", "gemini-2.5-pro")
    
    # --- API Keys ---
    openai_key: str = os.getenv("OPENAI_API_KEY", "")
    gemini_key: str = os.getenv("GEMINI_API_KEY", "")
    telegram_token: str = os.getenv("TELEGRAM_TOKEN", "")
    
    # --- Betdaq (Placeholder) ---
    betdaq_username: str = os.getenv("BETDAQ_USERNAME", "")
    betdaq_password: str = os.getenv("BETDAQ_PASSWORD", "")

    # --- RapidAPI (for historical data ingestors) ---
    rapidapi_key: str = os.getenv("RAPIDAPI_KEY", "")
    rapidapi_host_football: str = os.getenv("RAPIDAPI_HOST_FOOTBALL", "sportapi7.p.rapidapi.com")
    rapidapi_host_tennis: str = os.getenv("RAPIDAPI_HOST_TENNIS", "")
    rapidapi_host_horse_racing: str = os.getenv("RAPIDAPI_HOST_HORSE_RACING", "")

    # --- Tote API ---
    # Provide via .env locally or Secret Manager on Cloud Run
    tote_api_key: str = os.getenv("TOTE_API_KEY", "")
    tote_graphql_url: str = os.getenv("TOTE_GRAPHQL_URL", "")  # e.g. https://hub.production.racing.tote.co.uk/partner/connections/graphql/
    tote_subscriptions_url: str = os.getenv("TOTE_SUBSCRIPTIONS_URL", "")  # e.g. wss://.../graphql/

cfg = Config()
