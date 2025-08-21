import os
import pandas as pd
from flask import Flask, render_template, request, redirect, flash, url_for
from werkzeug.utils import secure_filename
from .config import cfg
from .db import connect
from .schema import init_schema
from .ingest.football_fd import ingest_dir as ingest_fd_dir
from .ingest.football_premier_league import ingest_dir as ingest_pl_dir # Corrected import

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
    
    suggestions_query = """
    SELECT s.*, e.home_team, e.away_team, e.start_date
    FROM suggestions s
    JOIN events e ON s.event_id = e.event_id
    ORDER BY s.created_ts DESC LIMIT 10
    """
    suggestions = pd.read_sql_query(suggestions_query, conn)
    
    bets = pd.read_sql_query("SELECT * FROM bets ORDER BY ts DESC LIMIT 10", conn)
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
    query = """
    SELECT s.*, e.home_team, e.away_team, e.start_date
    FROM suggestions s
    JOIN events e ON s.event_id = e.event_id
    ORDER BY s.created_ts DESC
    """
    all_suggestions = pd.read_sql_query(query, conn)
    conn.close()
    return render_template("suggestions.html", suggestions=all_suggestions.to_dict("records"))

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
