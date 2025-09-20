import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.helpers import escape_markdown
from .config import cfg
from .db import get_db, init_db # Import get_db and init_db for BigQuery
# from .schema import init_schema # No longer needed for BigQuery-native
from google.cloud import bigquery # Needed for QueryJobConfig
import pandas as pd # Already imported, but explicitly used for clarity

def _get_db_conn():
    """Helper to get a DB connection."""
    return get_db() # Returns a BigQuerySink instance


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a welcome message when the /start command is issued."""
    welcome_text = (
        "Hello! I'm your sports betting bot. Here are the available commands:\n\n"
        "/start - Show this welcome message\n"
        "/suggestion - Get the latest top betting suggestion"
    )
    await update.message.reply_text(welcome_text)


async def daily_suggestion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetches and sends the most recent betting suggestion from the new schema."""
    print("[Telegram] Received /suggestion command.")
    db = _get_db_conn() # This is now a BigQuerySink object
    # init_schema(conn) # Not needed for BigQuery, init_db() handles views

    query = """
    SELECT s.*, e.home_team, e.away_team, e.start_date
    FROM suggestions s
    JOIN events e ON s.event_id = e.event_id
    ORDER BY s.created_ts DESC LIMIT 1
    """

    # For BigQuery, we need to use the BigQuerySink's query method
    # and then convert the result to a DataFrame.
    # Assuming 'suggestions' and 'events' tables exist in BigQuery.
    try:
        job_config = bigquery.QueryJobConfig(
        default_dataset=f"{cfg.bq_project}.{cfg.bq_dataset}",
        location=cfg.bq_location
    )
        suggestion_df = db.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        print(f"[Telegram Error] Failed to fetch suggestion from BigQuery: {e}")
        await update.message.reply_text("Sorry, I'm having trouble fetching suggestions right now.")
        return

    # No db.close() needed for BigQuerySink

    if not suggestion_df.empty:
        s = suggestion_df.iloc[0]

        # Escape all parts of the message to prevent formatting errors
        home_team = escape_markdown(s['home_team'], version=2)
        away_team = escape_markdown(s['away_team'], version=2)
        start_date = escape_markdown(s['start_date'], version=2)
        note = escape_markdown(s['note'], version=2)

        message = (
            f"*📈 Daily Top Suggestion 📈*\n\n"
            f"*⚽️ Match:* {home_team} vs {away_team}\n"
            f"*🗓️ Date:* {start_date}\n\n"
            f"*AI Reasoning:*\n_{note}_"
        )
        await update.message.reply_text(message, parse_mode='MarkdownV2')
    else:
        await update.message.reply_text("Sorry, no suggestions are available at the moment.")


def run_bot():
    """Initializes and runs the Telegram bot."""
    if not cfg.telegram_token:
        print("[Telegram Error] TELEGRAM_TOKEN is not set in your .env file. The bot cannot start.")
        return

    print("[Telegram] Starting bot...")
    init_db() # Ensure BigQuery views are initialized when the bot starts
    application = ApplicationBuilder().token(cfg.telegram_token).build()

    # Register command handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('suggestion', daily_suggestion))

    # Start polling for updates
    print("[Telegram] Bot is now polling for messages.")
    application.run_polling()
