import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from .config import cfg
from .db import get_conn, init_schema, recent_suggestions

# --- Command Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a welcome message when the /start command is issued."""
    welcome_text = (
        "Hello! I'm your sports betting bot. Here are the available commands:\n\n"
        "/start - Show this welcome message\n"
        "/daily_suggestion - Get the latest top betting suggestion"
    )
    await update.message.reply_text(welcome_text)

async def daily_suggestion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetches and sends the most recent betting suggestion."""
    print("[Telegram] Received /daily_suggestion command.")
    conn = get_conn(cfg.database_url)
    init_schema(conn)
    suggestions_df = recent_suggestions(conn, limit=1)
    
    if not suggestions_df.empty:
        suggestion = suggestions_df.iloc[0]
        message = (
            f"📈 **Daily Top Suggestion** 📈\n\n"
            f"⚽️ **Match:** {suggestion['home']} vs {suggestion['away']}\n"
            f"🗓️ **Date:** {suggestion['date']}\n\n"
            f"**Market:** {suggestion['market']}\n"
            f"**Selection:** {suggestion['selection'].capitalize()}\n"
            f"**Side:** {suggestion['side'].upper()}\n\n"
            f"**Model Probability:** {suggestion['model_prob']:.2%}\n"
            f"**Market Odds:** {suggestion['market_odds']}\n"
            f"**Edge:** {suggestion['edge']:.2%}\n"
            f"**Suggested Stake:** £{suggestion['stake']}\n\n"
            f"**AI Reasoning:**\n_{suggestion['note']}_"
        )
        await update.message.reply_text(message, parse_mode='Markdown')
    else:
        await update.message.reply_text("Sorry, no suggestions are available at the moment. Please run the suggestion engine first.")

# --- Main Bot Function ---

def run_bot():
    """Initializes and runs the Telegram bot."""
    if not cfg.telegram_token:
        print("[Telegram Error] TELEGRAM_TOKEN is not set in your .env file. The bot cannot start.")
        return

    print("[Telegram] Starting bot...")
    application = ApplicationBuilder().token(cfg.telegram_token).build()

    # Register command handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('daily_suggestion', daily_suggestion))

    # Start polling for updates
    print("[Telegram] Bot is now polling for messages.")
    application.run_polling()

