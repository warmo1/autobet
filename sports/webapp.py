import os
import time
import pandas as pd
from datetime import date, timedelta, datetime
from flask import Flask, render_template, request, redirect, flash, url_for
from .config import cfg
from .db import connect
from .schema import init_schema

# This check allows the app to work with both the old and new db.py files
try:
    from .db import connect as get_conn
except ImportError:
    from .db import get_conn

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv("FLASK_SECRET", "dev-secret")

def _get_db_conn():
    """Helper to get a DB connection."""
    return get_conn(cfg.database_url)

def _today_iso() -> str:
    """Returns today's date in YYYY-MM-DD format."""
    return date.today().isoformat()

@app.route("/")
def index():
    """Renders the main dashboard page."""
    conn = _get_db_conn()
    init_schema(conn)
    
    d = request.args.get("date") or _today_iso()
    try:
        datetime.fromisoformat(d)
    except Exception:
        d = _today_iso()

    fixtures_query = "SELECT e.* FROM events e WHERE e.start_date = ? ORDER BY e.comp, e.home_team"
    fixtures = pd.read_sql_query(fixtures_query, conn, params=(d,))

    suggestions_query = "SELECT s.*, e.home_team, e.away_team, e.start_date, e.comp FROM suggestions s JOIN events e ON s.event_id = e.event_id ORDER BY s.created_ts DESC LIMIT 10"
    suggestions = pd.read_sql_query(suggestions_query, conn)
    
    bank_row = conn.execute("SELECT value FROM bankroll_state WHERE key = 'bankroll'").fetchone()
    bankroll = float(bank_row[0]) if bank_row else cfg.paper_starting_bankroll
    conn.close()
    
    return render_template(
        "index.html",
        date_iso=d,
        fixtures=fixtures.to_dict("records"),
        suggestions=suggestions.to_dict("records"),
        balance=round(bankroll, 2)
    )

@app.route("/fixtures")
def fixtures_page():
    """Renders the fixtures page for a given date."""
    d = request.args.get("date", _today_iso())
    try:
        dt = datetime.fromisoformat(d).date()
    except ValueError:
        dt = date.today()
        d = dt.isoformat()

    conn = _get_db_conn()
    query = "SELECT * FROM events WHERE start_date = ? AND sport = 'football' ORDER BY comp, home_team"
    fixtures = pd.read_sql_query(query, conn, params=(d,))
    conn.close()

    prev_d = (dt - timedelta(days=1)).isoformat()
    next_d = (dt + timedelta(days=1)).isoformat()
    
    return render_template("fixtures.html", date_iso=d, fixtures=fixtures.to_dict("records"), prev_date=prev_d, next_date=next_d)

@app.route("/suggestions")
def suggestions_page():
    """Renders the suggestions page."""
    conn = _get_db_conn()
    query = "SELECT s.*, e.home_team, e.away_team, e.start_date, e.comp FROM suggestions s JOIN events e ON s.event_id = e.event_id ORDER BY s.created_ts DESC"
    all_suggestions = pd.read_sql_query(query, conn)
    conn.close()
    return render_template("suggestions.html", suggestions=all_suggestions.to_dict("records"))

@app.route("/bets", methods=["GET", "POST"])
def bets_page():
    """Handles displaying and placing paper bets."""
    conn = _get_db_conn()
    if request.method == "POST":
        try:
            event_id = int(request.form.get("event_id"))
            price = float(request.form.get("price"))
            stake = float(request.form.get("stake"))
            selection = request.form.get("sel")
            
            conn.execute(
                "INSERT INTO bets (ts, mode, event_id, market, selection, side, price, stake) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (int(time.time() * 1000), 'paper', event_id, '1X2', selection, 'back', price, stake)
            )
            conn.commit()
            flash("Paper bet recorded successfully.", "success")
        except Exception as e:
            flash(f"Error placing bet: {e}", "error")
        finally:
            conn.close()
        return redirect(url_for("bets_page"))

    query = "SELECT b.*, e.home_team, e.away_team, e.start_date, e.comp FROM bets b JOIN events e ON b.event_id = e.event_id ORDER BY b.ts DESC"
    all_bets = pd.read_sql_query(query, conn)
    conn.close()

    if not all_bets.empty:
        all_bets['formatted_ts'] = pd.to_datetime(all_bets['ts'], unit='ms').dt.strftime('%Y-%m-%d %H:%M')
    return render_template("bets.html", bets=all_bets.to_dict("records"))

@app.route("/bank", methods=["GET", "POST"])
def bank_page():
    """Handles updating the bankroll."""
    conn = _get_db_conn()
    if request.method == "POST":
        try:
            new_bal = float(request.form.get("balance", "0"))
            conn.execute("INSERT OR REPLACE INTO bankroll_state (key, value) VALUES ('bankroll', ?)", (str(new_bal),))
            conn.commit()
            flash("Bankroll updated successfully.", "success")
        except Exception as e:
            flash(f"Error updating bankroll: {e}", "error")
        finally:
            conn.close()
        return redirect(url_for("bank_page"))

    bank_row = conn.execute("SELECT value FROM bankroll_state WHERE key = 'bankroll'").fetchone()
    bankroll = float(bank_row[0]) if bank_row else cfg.paper_starting_bankroll
    conn.close()
    return render_template("bank.html", balance=bankroll)

def create_app():
    return app
