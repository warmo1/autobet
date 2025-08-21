import os
import time
import pandas as pd
import subprocess
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

@app.route("/data")
def data_management():
    """Renders the new data management control panel."""
    return render_template("data_management.html")

@app.route("/run_task", methods=["POST"])
def run_task():
    """Runs a command-line task in the background."""
    task = request.form.get("task")
    
    task_map = {
        "ingest-fixtures": ["ingest-fixtures", "--sport", "football"],
        "generate-suggestions": ["generate-suggestions", "--sport", "football"],
        "fetch-openfootball": ["fetch-openfootball", "--mode", "init"]
    }
    
    if task not in task_map:
        flash(f"Unknown task: {task}", "error")
        return redirect(url_for('data_management'))

    command = ["python3", "-m", "sports.run"] + task_map[task]
    
    try:
        print(f"[WebApp] Running task: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print(f"[WebApp] Task output: {result.stdout}")
        flash(f"Task '{task}' completed successfully!", "success")
    except subprocess.CalledProcessError as e:
        print(f"[WebApp] Task error: {e.stderr}")
        flash(f"Task '{task}' failed: {e.stderr}", "error")
    except Exception as e:
        print(f"[WebApp] General error: {e}")
        flash(f"An unexpected error occurred: {e}", "error")

    return redirect(url_for('data_management'))

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

# **FIX**: Added the missing upload_file route
@app.route('/upload', methods=['POST'])
def upload_file():
    if cfg.admin_token and request.form.get("token") != cfg.admin_token:
        flash("Invalid admin token. Upload failed.", "error")
        return redirect(url_for('dashboard'))

    if 'file' not in request.files:
        flash('No file part in the request.', 'error')
        return redirect(url_for('dashboard'))
    
    file = request.files['file']
    if file.filename == '':
        flash('No file selected for uploading.', 'error')
        return redirect(url_for('dashboard'))

    if file and file.filename.endswith('.csv'):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        conn = _get_db_conn()
        # This is a generic uploader; you might want to have different logic
        # based on the filename or a form dropdown.
        rows_ingested = ingest_fd_dir(conn, os.path.dirname(filepath)) # Example: using football-data ingestor
        
        os.remove(filepath)
        
        flash(f"Successfully uploaded and processed '{filename}'. Ingested {rows_ingested} new records.", "success")
    else:
        flash("Invalid file type. Please upload a CSV file.", "error")

    return redirect(url_for('dashboard'))

def create_app():
    return app
