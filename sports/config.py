import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# --- Explicitly locate and load the .env file ---
# This makes sure the app can find the .env file regardless of how it's run.
# It finds the root directory (autobet) by going up from this file's location.
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

@dataclass
class Config:
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///sports_bot.db")
    paper_starting_bankroll: float = float(os.getenv("PAPER_STARTING_BANKROLL", "1000"))
    
    # Telegram
    telegram_token: str = os.getenv("TELEGRAM_TOKEN", "")

    # Betfair
    betfair_app_key: str = os.getenv("BETFAIR_APP_KEY", "")
    betfair_username: str = os.getenv("BETFAIR_USERNAME", "")
    betfair_password: str = os.getenv("BETFAIR_PASSWORD", "")
    betfair_cert_dir: str = os.getenv("BETFAIR_CERT_DIR", "")
    
    # LLM
    llm_provider: str = os.getenv("LLM_PROVIDER", "gemini").lower()
    llm_model: str = os.getenv("LLM_MODEL", "gemini-1.5-flash")
    gemini_key: str = os.getenv("GEMINI_API_KEY", "")
    openai_key: str = os.getenv("OPENAI_API_KEY", "")
    
    # Web
    admin_token: str = os.getenv("ADMIN_TOKEN", "")

    # RapidAPI
    rapidapi_key: str = os.getenv("RAPIDAPI_KEY", "")
    # **FIX**: Updated the football host to SportAPI7
    rapidapi_host_football: str = os.getenv("RAPIDAPI_HOST_FOOTBALL", "sportapi7.p.rapidapi.com")
    rapidapi_host_tennis: str = os.getenv("RAPIDAPI_HOST_TENNIS", "")
    rapidapi_host_horse_racing: str = os.getenv("RAPIDAPI_HOST_HORSE_RACING", "")

cfg = Config()
