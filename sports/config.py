import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

@dataclass
class Config:
    # --- Database and Core Settings ---
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
    # Optional separate audit credentials/URL (recommended if audit requires different key)
    tote_audit_api_key: str = os.getenv("TOTE_AUDIT_API_KEY", "")
    tote_audit_graphql_url: str = os.getenv("TOTE_AUDIT_GRAPHQL_URL", "")  # e.g. https://hub.production.racing.tote.co.uk/partner/gateway/audit/graphql/
    # Authorization scheme control (defaults to "Api-Key"). Examples: "Api-Key", "Bearer".
    tote_auth_scheme: str = os.getenv("TOTE_AUTH_SCHEME", "Api-Key")
    tote_audit_auth_scheme: str = os.getenv("TOTE_AUDIT_AUTH_SCHEME", "Api-Key")

    # --- BigQuery (writes + optional web reads) ---
    # These are referenced by sports/bq.py and sports/webapp.py
    bq_write_enabled: bool = os.getenv("BQ_WRITE_ENABLED", "false").lower() in ("1", "true", "yes", "on")
    # Default to the shared production dataset so local tooling can run
    # read-only queries without additional configuration. Environment
    # variables still override these defaults when present.
    bq_project: str = os.getenv("BQ_PROJECT", "autobet-470818")
    bq_dataset: str = os.getenv("BQ_DATASET", "autobet")
    bq_location: str = os.getenv("BQ_LOCATION", "EU")

    # --- BigQuery client options ---
    # Use the BigQuery Storage API for faster dataframe reads.
    bq_use_storage_api: bool = os.getenv("BQ_USE_STORAGE_API", "true").lower() in ("1", "true", "yes", "on")

    # --- Redis cache (optional shared cache for web/sql_df) ---
    redis_url: str = os.getenv("REDIS_URL", "")
    redis_cache_prefix: str = os.getenv("REDIS_CACHE_PREFIX", "autobet:web")

    # --- Web SQL cache (applies to sql_df) ---
    # Enable a small in-process TTL cache for repeated SELECTs.
    web_sqldf_cache_enabled: bool = os.getenv("WEB_SQLDF_CACHE", "true").lower() in ("1", "true", "yes", "on")
    web_sqldf_cache_ttl_s: int = int(os.getenv("WEB_SQLDF_CACHE_TTL", "30"))
    web_sqldf_cache_max_entries: int = int(os.getenv("WEB_SQLDF_CACHE_MAX", "512"))
    # Optional soft cap on returned rows from sql_df; 0 disables.
    web_sqldf_max_rows: int = int(os.getenv("WEB_SQLDF_MAX_ROWS", "0"))

    # --- Superfecta automation defaults ---
    superfecta_default_bankroll: float = float(os.getenv("SUPERFECTA_DEFAULT_BANKROLL", "500"))
    superfecta_default_preset: str = os.getenv("SUPERFECTA_DEFAULT_PRESET", "balanced")
    superfecta_min_competitors: int = int(os.getenv("SUPERFECTA_MIN_COMPETITORS", "7"))
    superfecta_max_competitors: int = int(os.getenv("SUPERFECTA_MAX_COMPETITORS", "18"))
    superfecta_require_rollover: bool = os.getenv("SUPERFECTA_REQUIRE_ROLLOVER", "false").lower() in ("1", "true", "yes", "on")
    superfecta_min_roi: float = float(os.getenv("SUPERFECTA_MIN_ROI", "0.05"))
    superfecta_morning_max_candidates: int = int(os.getenv("SUPERFECTA_MORNING_MAX_CANDIDATES", "5"))
    superfecta_live_min_roi: float = float(os.getenv("SUPERFECTA_LIVE_MIN_ROI", "0.05"))
    superfecta_live_min_expected_profit: float = float(os.getenv("SUPERFECTA_LIVE_MIN_EXPECTED_PROFIT", "5"))
    superfecta_decision_minutes_before: int = int(os.getenv("SUPERFECTA_DECISION_MINUTES_BEFORE", "7"))
    superfecta_auto_cancel_minutes: int = int(os.getenv("SUPERFECTA_AUTO_CANCEL_MINUTES", "2"))
    superfecta_auto_place_ready: bool = os.getenv("SUPERFECTA_AUTO_PLACE_READY", "false").lower() in ("1", "true", "yes", "on")
    superfecta_monitor_hours_ahead: int = int(os.getenv("SUPERFECTA_MONITOR_HOURS_AHEAD", "6"))

cfg = Config()
