import os
import time
import pandas as pd
from flask import Flask, render_template, request, redirect, flash, url_for
from werkzeug.utils import secure_filename
from .config import cfg
from .db import connect
from .schema import init_schema
from .ingest.football_fd import ingest_dir as ingest_fd_dir

app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24)
app.config['UPLOAD_FOLDER'] = '/tmp/autobet_uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def _get_db_conn():
    """Helper to get a DB connection."""
    return connect(cfg.database_url)

@app.route("/")
def dashboard():
    """Renders the main dashboard page."""
    conn = _get_db_conn()
    init_schema(conn)
    
    suggestions_query = "SELECT s.*, e.home_team, e.away_team, e.start_date FROM suggestions s JOIN events e ON s.event_id = e.event_id ORDER BY s.created_ts DESC LIMIT 10"
    suggestions = pd.read_sql_query(suggestions_query, conn)
    
    bets_query = "SELECT b.*, e.home_team, e.away_team FROM bets b JOIN events e ON b.event_id = e.event_id ORDER BY b.ts DESC LIMIT 10"
    bets = pd.read_sql_query(bets_query, conn)

    bank_row = conn.execute("SELECT value FROM bankroll_state WHERE key = 'bankroll'").fetchone()
    bankroll = float(bank_row[0]) if bank_row else cfg.paper_starting_bankroll
    conn.close()
    
    return render_template(
        "dashboard.html",
        bankroll=round(bankroll, 2),
        suggestions=suggestions.to_dict("records"),
        recent_bets=bets.to_dict("records")
    )

@app.route("/suggestions")
def suggestions():
    """Renders the suggestions page."""
    conn = _get_db_conn()
    query = "SELECT s.*, e.home_team, e.away_team, e.start_date FROM suggestions s JOIN events e ON s.event_id = e.event_id ORDER BY s.created_ts DESC"
    all_suggestions = pd.read_sql_query(query, conn)
    conn.close()
    return render_template("suggestions.html", suggestions=all_suggestions.to_dict("records"))

@app.route("/bets")
def bets():
    """Renders the page showing all paper bets."""
    conn = _get_db_conn()
    query = "SELECT b.*, e.home_team, e.away_team FROM bets b JOIN events e ON b.event_id = e.event_id ORDER BY b.ts DESC"
    all_bets = pd.read_sql_query(query, conn)
    conn.close()
    return render_template("bets.html", bets=all_bets.to_dict("records"))

@app.route("/paper_bet", methods=["POST"])
def paper_bet():
    """Handles the form submission for placing a new paper bet."""
    conn = _get_db_conn()
    
    event_id = int(request.form.get("event_id"))
    price = float(request.form.get("price"))
    stake = float(request.form.get("stake"))
    market = request.form.get("market")
    selection = request.form.get("selection")
    side = request.form.get("side")

    # Update bankroll
    bank_row = conn.execute("SELECT value FROM bankroll_state WHERE key = 'bankroll'").fetchone()
    bankroll = float(bank_row[0]) if bank_row else cfg.paper_starting_bankroll
    new_bankroll = bankroll - stake
    conn.execute("INSERT OR REPLACE INTO bankroll_state (key, value) VALUES ('bankroll', ?)", (str(new_bankroll),))

    # Insert the new bet
    conn.execute(
        """INSERT INTO bets (ts, mode, event_id, market, selection, side, price, stake)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (int(time.time()), 'paper', event_id, market, selection, side, price, stake)
    )
    conn.commit()
    conn.close()
    
    flash(f"Paper bet of £{stake:.2f} placed successfully.", "success")
    return redirect(url_for('bets'))


def create_app():
    return app
