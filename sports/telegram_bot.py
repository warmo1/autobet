from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.helpers import escape_markdown
from .config import cfg
from .db import connect
from .schema import init_schema
import pandas as pd

async def daily_suggestion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetches and sends the most recent betting suggestion from the new schema."""
    conn = connect(cfg.database_url)
    init_schema(conn)
    query = """
    SELECT s.*, e.home_team, e.away_team, e.start_date
    FROM suggestions s
    JOIN events e ON s.event_id = e.event_id
    ORDER BY s.created_ts DESC LIMIT 1
    """
    suggestion_df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not suggestion_df.empty:
        s = suggestion_df.iloc[0]
        # ... (rest of the message formatting logic)
        message = f"*Suggestion:* {escape_markdown(s['note'], version=2)}"
        await update.message.reply_text(message, parse_mode='MarkdownV2')
    else:
        await update.message.reply_text("No suggestions available.")

# ... (rest of the bot setup)
def run_bot():
    application = ApplicationBuilder().token(cfg.telegram_token).build()
    application.add_handler(CommandHandler('daily_suggestion', daily_suggestion))
    application.run_polling()
