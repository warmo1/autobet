import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from sports.db import get_conn, recent_suggestions
from sports.config import cfg
import pandas as pd

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hello! I'm your sports betting bot. Use /daily_suggestion to get the latest suggestion.")

async def daily_suggestion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_conn(cfg.database_url)
    suggestions = recent_suggestions(conn, limit=1)
    if not suggestions.empty:
        suggestion = suggestions.iloc[0]
        message = f"""
        **Daily Suggestion**
        **Match:** {suggestion['home']} vs {suggestion['away']}
        **Market:** {suggestion['market']}
        **Selection:** {suggestion['selection']}
        **Side:** {suggestion['side']}
        **Odds:** {suggestion['market_odds']}
        **Edge:** {suggestion['edge']:.2f}
        **Stake:** {suggestion['stake']}
        """
        await update.message.reply_text(message, parse_mode='Markdown')
    else:
        await update.message.reply_text("No suggestions available at the moment.")

def main():
    application = ApplicationBuilder().token(cfg.telegram_token).build()

    start_handler = CommandHandler('start', start)
    suggestion_handler = CommandHandler('daily_suggestion', daily_suggestion)

    application.add_handler(start_handler)
    application.add_handler(suggestion_handler)

    application.run_polling()

if __name__ == '__main__':
    main()
