import os
from flask import Flask, render_template, request, redirect, abort
from .config import cfg
from .db import get_conn, init_schema, recent_suggestions, recent_paper_bets, bank_get, bank_set, insert_insight

app = Flask(__name__)

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
        return abort(403)
    # For brevity, we only simulate: deduct stake from bankroll now; PnL setting is manual later or via result settlement
    symbol = request.form.get("symbol","Match")
    odds = float(request.form.get("odds","2.0"))
    stake = float(request.form.get("stake","2.0"))
    conn = get_conn(cfg.database_url); init_schema(conn)
    bank = float(bank_get(conn, "bankroll", str(cfg.paper_starting_bankroll)))
    bank = max(0.0, bank - stake)
    bank_set(conn, "bankroll", str(bank))
    from .db import record_paper_bet
    record_paper_bet(conn, ts=_now_ms(), sport="football", match=symbol, market="match_odds", side="back", selection="custom", odds=odds, stake=stake, result=None, pnl=None)
    return redirect("/bets")

def _now_ms():
    import time
    return int(time.time()*1000)

def create_app():
    return app
