import asyncio
import re
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from .config import cfg
from .db import get_conn, init_schema, recent_suggestions

# --- Helper Functions ---

def escape_markdown(text: str) -> str:
    """
    Escapes special characters in a string for Telegram MarkdownV2 compatibility.
    """
    # Characters to escape for MarkdownV2. Note the inclusion of '.' and '-'
    escape_chars = r'\_*[]()~`>#+-=|{}.!'
    # Use a regular expression to add a backslash before each special character
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

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
        
        # **FIX**: Convert all parts to strings and escape them individually
        home = escape_markdown(str(suggestion['home']))
        away = escape_markdown(str(suggestion['away']))
        date = escape_markdown(str(suggestion['date']))
        market = escape_markdown(str(suggestion['market']))
        selection = escape_markdown(str(suggestion['selection']).capitalize())
        side = escape_markdown(str(suggestion['side']).upper())
        
        # Format numbers, then convert to string and escape
        model_prob = escape_markdown(f"{suggestion['model_prob']:.2%}")
        market_odds = escape_markdown(str(suggestion['market_odds']))
        edge = escape_markdown(f"{suggestion['edge']:.2%}")
        stake = escape_markdown(f"£{suggestion['stake']:.2f}")
        
        safe_note = escape_markdown(str(suggestion['note']))
        
        message = (
            f"*📈 Daily Top Suggestion 📈*\n\n"
            f"*⚽️ Match:* {home} vs {away}\n"
            f"*🗓️ Date:* {date}\n\n"
            f"*Market:* {market}\n"
            f"*Selection:* {selection}\n"
            f"*Side:* {side}\n\n"
            f"*Model Probability:* {model_prob}\n"
            f"*Market Odds:* {market_odds}\n"
            f"*Edge:* {edge}\n"
            f"*Suggested Stake:* {stake}\n\n"
            f"*AI Reasoning:*\n_{safe_note}_"
        )
        
        try:
            # Use 'MarkdownV2' as the parse mode
            await update.message.reply_text(message, parse_mode='MarkdownV2')
        except Exception as e:
            print(f"[Telegram Error] Failed to send message: {e}")
            # As a fallback, send the message without formatting
            await update.message.reply_text(message.replace('*', '').replace('_', ''))

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
