import os
from flask import Flask, render_template, request, redirect, abort, flash, url_for
from werkzeug.utils import secure_filename
from .config import cfg
from .db import get_conn, init_schema, recent_suggestions, recent_paper_bets, bank_get, bank_set
from .ingest import ingest_football_csv_file # Import the new single-file ingestor

# --- App Setup ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24) # Needed for flash messages
app.config['UPLOAD_FOLDER'] = '/tmp/autobet_uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# --- Helper Functions ---
def _compute_summary():
    conn = get_conn(cfg.database_url); init_schema(conn)
    sugg = recent_suggestions(conn, limit=50)
    bets = recent_paper_bets(conn, limit=100)
    bank = float(bank_get(conn, "bankroll", str(cfg.paper_starting_bankroll)))
    return dict(
        bankroll=round(bank,2),
        suggestions=([] if sugg is None or sugg.empty else sugg.to_dict("records")),
        recent_bets=([] if bets is None or bets.empty else bets.to_dict("records"))
    )

def _now_ms():
    import time
    return int(time.time()*1000)

# --- App Routes ---
@app.route("/")
def dashboard():
    data = _compute_summary()
    return render_template("dashboard.html", **data)

@app.route("/suggestions")
def suggestions():
    data = _compute_summary()
    return render_template("suggestions.html", **data)

@app.route("/bets")
def bets():
    data = _compute_summary()
    return render_template("bets.html", **data)

@app.route("/paper_bet", methods=["POST"])
def paper_bet():
    if cfg.admin_token and request.form.get("token") != cfg.admin_token:
        flash("Invalid admin token.", "error")
        return redirect(url_for('suggestions'))
        
    symbol = request.form.get("symbol","Match")
    odds = float(request.form.get("odds","2.0"))
    stake = float(request.form.get("stake","2.0"))
    
    conn = get_conn(cfg.database_url); init_schema(conn)
    bank = float(bank_get(conn, "bankroll", str(cfg.paper_starting_bankroll)))
    bank = max(0.0, bank - stake)
    bank_set(conn, "bankroll", str(bank))
    
    from .db import record_paper_bet
    record_paper_bet(conn, ts=_now_ms(), sport="football", match=symbol, market="match_odds", side="back", selection="custom", odds=odds, stake=stake, result=None, pnl=None)
    
    flash(f"Paper bet of £{stake} on '{symbol}' recorded.", "success")
    return redirect(url_for('bets'))

@app.route('/upload', methods=['POST'])
def upload_file():
    # 1. Security Check: Validate admin token
    if cfg.admin_token and request.form.get("token") != cfg.admin_token:
        flash("Invalid admin token. Upload failed.", "error")
        return redirect(url_for('dashboard'))

    # 2. File Check: Ensure a file was submitted
    if 'file' not in request.files:
        flash('No file part in the request.', 'error')
        return redirect(url_for('dashboard'))
    
    file = request.files['file']
    if file.filename == '':
        flash('No file selected for uploading.', 'error')
        return redirect(url_for('dashboard'))

    # 3. Process File: Save and ingest the data
    if file and file.filename.endswith('.csv'):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Ingest the saved file
        conn = get_conn(cfg.database_url); init_schema(conn)
        rows_ingested = ingest_football_csv_file(conn, filepath)
        
        # Clean up the uploaded file
        os.remove(filepath)
        
        flash(f"Successfully uploaded and processed '{filename}'. Ingested {rows_ingested} new records.", "success")
    else:
        flash("Invalid file type. Please upload a CSV file.", "error")

    return redirect(url_for('dashboard'))

def create_app():
    return app
