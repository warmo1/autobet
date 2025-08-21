import asyncio
from datetime import datetime, time as dtime, timezone, timedelta
import sqlite3
from typing import List, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes

LONDON = timezone(timedelta(hours=0))  # Keep simple; if you have pytz/zoneinfo, use Europe/London.

def _get_conn(db_url: str) -> sqlite3.Connection:
    from sports.db import connect
    return connect(db_url)

def list_subscribers(conn) -> List[int]:
    rows = conn.execute("SELECT chat_id FROM subscriptions").fetchall()
    return [r[0] for r in rows]

def add_subscription(conn, chat_id: int):
    conn.execute("INSERT OR IGNORE INTO subscriptions(chat_id) VALUES (?)", (chat_id,))

def remove_subscription(conn, chat_id: int):
    conn.execute("DELETE FROM subscriptions WHERE chat_id=?", (chat_id,))

def get_today_fixtures(conn, sport: str) -> List[Tuple]:
    sql = """
    SELECT m.match_id, m.date, t1.name, t2.name, COALESCE(m.comp, '')
    FROM matches m
    JOIN teams t1 ON t1.team_id=m.home_id
    JOIN teams t2 ON t2.team_id=m.away_id
    WHERE m.sport=? AND date(m.date)=date('now','localtime')
    ORDER BY m.comp, m.date, t1.name
    """
    return conn.execute(sql, (sport,)).fetchall()

def get_top_suggestions(conn, sport: str, min_edge: float = 0.03, limit: int = 10):
    sql = """
    SELECT s.match_id, s.sel, s.model_prob, s.book, s.price, s.edge, s.kelly,
           m.date, th.name, ta.name, COALESCE(m.comp,'')
    FROM suggestions s
    JOIN matches m ON m.match_id=s.match_id
    JOIN teams th ON th.team_id=m.home_id
    JOIN teams ta ON ta.team_id=m.away_id
    WHERE m.sport=? AND s.edge >= ?
    ORDER BY s.edge DESC
    LIMIT ?
    """
    return conn.execute(sql, (sport, min_edge, limit)).fetchall()

def _format_fixtures(rows) -> List[str]:
    if not rows:
        return ["No fixtures today."]
    lines = []
    last_comp = None
    for match_id, date_iso, home, away, comp in rows:
        if comp != last_comp:
            if last_comp is not None:
                lines.append("")  # blank line between comps
            lines.append(f"🏆 {comp or 'Fixtures'}")
            last_comp = comp
        lines.append(f"• {home} vs {away}  —  {date_iso}")
    # chunk into Telegram-safe messages
    chunks, cur = [], []
    size = 0
    for ln in lines:
        if size + len(ln) > 3800:  # leave buffer for safety
            chunks.append("\n".join(cur)); cur = [ln]; size = len(ln)
        else:
            cur.append(ln); size += len(ln)
    if cur: chunks.append("\n".join(cur))
    return chunks

def _format_pick(row) -> str:
    (match_id, sel, p, book, price, edge, kelly,
     date_iso, home, away, comp) = row
    dir_map = {"H": home, "D": "Draw", "A": away}
    return (f"📈 {comp or 'Match'}  {home} vs {away} ({date_iso})\n"
            f"Pick: {sel} ({dir_map.get(sel, sel)})\n"
            f"Model prob: {p:.2%}   Price: {price or 0:.2f}   Edge: {edge or 0:.2%}\n"
            f"Kelly fraction: {kelly or 0:.2%}   Book: {book or 'n/a'}\n"
            f"id: {match_id}")

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hi 👋  Commands:\n"
        "/today [sport] – today’s fixtures (default football)\n"
        "/suggest [sport] – top value picks\n"
        "/subscribe – daily 08:30 UK digest\n"
        "/unsubscribe – stop daily digest"
    )

async def cmd_today(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sport = (ctx.args[0] if ctx.args else "football").lower()
    db_url = ctx.bot_data["db_url"]
    conn = _get_conn(db_url)
    rows = get_today_fixtures(conn, sport)
    for chunk in _format_fixtures(rows):
        await update.message.reply_text(chunk)

async def cmd_suggest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sport = (ctx.args[0] if ctx.args else "football").lower()
    db_url = ctx.bot_data["db_url"]
    conn = _get_conn(db_url)
    picks = get_top_suggestions(conn, sport, min_edge=0.03, limit=8)
    if not picks:
        return await update.message.reply_text("No value edges right now.")
    for p in picks:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Back", callback_data=f"back:{p[0]}:{p[1]}"),
            InlineKeyboardButton("Lay",  callback_data=f"lay:{p[0]}:{p[1]}"),
            InlineKeyboardButton("Details", callback_data=f"more:{p[0]}")
        ]])
        await update.message.reply_text(_format_pick(p), reply_markup=kb)

async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    action, match_id, *rest = q.data.split(":")
    await q.edit_message_reply_markup(None)
    await q.message.reply_text(f"Recorded action: {action} on {match_id} (analytics only).")

async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    db_url = ctx.bot_data["db_url"]; conn = _get_conn(db_url)
    add_subscription(conn, update.effective_chat.id)
    await update.message.reply_text("Subscribed to daily fixtures & edges at 08:30 UK.")

async def cmd_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    db_url = ctx.bot_data["db_url"]; conn = _get_conn(db_url)
    remove_subscription(conn, update.effective_chat.id)
    await update.message.reply_text("Unsubscribed.")

def _build_daily_digest(conn) -> str:
    fixtures = get_today_fixtures(conn, "football")
    parts = ["📅 Today’s Football"]
    parts.extend(_format_fixtures(fixtures))
    picks = get_top_suggestions(conn, "football", min_edge=0.03, limit=5)
    if picks:
        parts.append("")
        parts.append("💡 Top Edges")
        for p in picks:
            parts.append(_format_pick(p))
    return "\n".join(parts[:3900])

async def send_daily(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    db_url = context.bot_data["db_url"]
    conn = _get_conn(db_url)
    txt = _build_daily_digest(conn)
    await context.bot.send_message(chat_id=chat_id, text=txt)

def run_bot(token: str, *, db_url: str, digest_hour: int = 8, digest_minute: int = 30):
    from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler
    app = ApplicationBuilder().token(token).build()
    app.bot_data["db_url"] = db_url

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("suggest", cmd_suggest))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CallbackQueryHandler(on_button))

    # schedule jobs for existing subscribers
    conn = _get_conn(db_url)
    for chat_id in list_subscribers(conn):
        app.job_queue.run_daily(
            send_daily,
            time=dtime(hour=digest_hour, minute=digest_minute),
            chat_id=chat_id
        )

    app.run_polling()
