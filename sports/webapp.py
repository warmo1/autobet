import os
from datetime import datetime, date, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash

# Prefer connect(); fall back to legacy get_conn if needed
try:
    from .db import connect as get_conn
except Exception:  # pragma: no cover
    from .db import get_conn  # type: ignore

from .schema import init_schema

EXTRA_SCHEMA = """
CREATE TABLE IF NOT EXISTS bank (
  id INTEGER PRIMARY KEY CHECK (id=1),
  balance REAL NOT NULL
);
INSERT OR IGNORE INTO bank(id, balance) VALUES (1, 1000.0);

CREATE TABLE IF NOT EXISTS paper_bets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id TEXT NOT NULL,
  sel TEXT NOT NULL,
  stake REAL NOT NULL,
  price REAL NOT NULL,
  created_ts TEXT DEFAULT CURRENT_TIMESTAMP,
  result TEXT,
  settled_ts TEXT
);
"""


def _ensure_extra_schema(conn):
    conn.executescript(EXTRA_SCHEMA)


def _today_iso() -> str:
    return date.today().isoformat()


def bank_get(conn) -> float:
    row = conn.execute("SELECT balance FROM bank WHERE id=1").fetchone()
    return float(row[0]) if row else 0.0


def bank_set(conn, value: float) -> None:
    conn.execute("UPDATE bank SET balance=? WHERE id=1", (float(value),))


def recent_paper_bets(conn, limit: int = 25):
    sql = """
    SELECT b.id, b.match_id, b.sel, b.stake, b.price, b.created_ts,
           m.date, th.name AS home, ta.name AS away, COALESCE(m.comp,'') as comp
    FROM paper_bets b
    LEFT JOIN matches m ON m.match_id=b.match_id
    LEFT JOIN teams th ON th.team_id=m.home_id
    LEFT JOIN teams ta ON ta.team_id=m.away_id
    ORDER BY b.created_ts DESC
    LIMIT ?
    """
    return conn.execute(sql, (limit,)).fetchall()


def fixtures_for_date(conn, date_iso: str, sport: str = "football"):
    sql = """
    SELECT m.match_id, m.date, th.name AS home, ta.name AS away, COALESCE(m.comp,'') as comp
    FROM matches m
    JOIN teams th ON th.team_id=m.home_id
    JOIN teams ta ON ta.team_id=m.away_id
    WHERE m.sport=? AND date(m.date)=date(?)
    ORDER BY comp, m.date, home
    """
    return conn.execute(sql, (sport, date_iso)).fetchall()


def top_suggestions(conn, sport: str = "football", min_edge: float = 0.0, limit: int = 50):
    try:
        sql = """
        SELECT s.match_id, s.sel, s.model_prob, s.book, s.price, s.edge, s.kelly,
               m.date, th.name AS home, ta.name AS away, COALESCE(m.comp,'') as comp
        FROM suggestions s
        JOIN matches m ON m.match_id=s.match_id
        JOIN teams th ON th.team_id=m.home_id
        JOIN teams ta ON ta.team_id=m.away_id
        WHERE m.sport=? AND s.edge >= ?
        ORDER BY s.edge DESC
        LIMIT ?
        """
        return conn.execute(sql, (sport, float(min_edge), int(limit))).fetchall()
    except Exception:
        return []


def create_app(db_url: str | None = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET", "dev-secret")
    app.config["DB_URL"] = db_url or os.getenv("DATABASE_URL", "sqlite:///sports_bot.db")

    # Ensure schema on boot
    with get_conn(app.config["DB_URL"]) as conn:
        init_schema(conn)
        _ensure_extra_schema(conn)

    @app.route("/health")
    def health():
        return {"status": "ok"}

    @app.route("/")
    def index():
        d = request.args.get("date") or _today_iso()
        try:
            datetime.fromisoformat(d)
        except Exception:
            d = _today_iso()
        with get_conn(app.config["DB_URL"]) as conn:
            fx = fixtures_for_date(conn, d)
            sug = top_suggestions(conn, min_edge=0.03, limit=15)
            bal = bank_get(conn)
        return render_template("index.html", date_iso=d, fixtures=fx, suggestions=sug, balance=bal)

    @app.route("/fixtures")
    def fixtures_page():
        d = request.args.get("date") or _today_iso()
        try:
            datetime.fromisoformat(d)
        except Exception:
            d = _today_iso()
        with get_conn(app.config["DB_URL"]) as conn:
            fx = fixtures_for_date(conn, d)
        dt = datetime.fromisoformat(d).date()
        prev_d = (dt - timedelta(days=1)).isoformat()
        next_d = (dt + timedelta(days=1)).isoformat()
        return render_template("fixtures.html", date_iso=d, fixtures=fx, prev_date=prev_d, next_date=next_d)

    @app.route("/suggestions")
    def suggestions_page():
        min_edge = float(request.args.get("min_edge", "0.03"))
        with get_conn(app.config["DB_URL"]) as conn:
            sug = top_suggestions(conn, min_edge=min_edge, limit=100)
        return render_template("suggestions.html", min_edge=min_edge, suggestions=sug)

    @app.route("/bets", methods=["GET", "POST"])
    def bets_page():
        if request.method == "POST":
            match_id = request.form.get("match_id", "").strip()
            sel = (request.form.get("sel") or "").strip().upper()
            stake = request.form.get("stake")
            price = request.form.get("price")
            try:
                if not match_id or sel not in ("H", "D", "A"):
                    raise ValueError("Provide match_id and sel in {H,D,A}.")
                stake_f = float(stake)
                price_f = float(price)
                with get_conn(app.config["DB_URL"]) as conn:
                    conn.execute(
                        "INSERT INTO paper_bets(match_id, sel, stake, price) VALUES (?,?,?,?)",
                        (match_id, sel, stake_f, price_f),
                    )
                flash("Paper bet recorded.", "success")
            except Exception as e:
                flash(f"Error: {e}", "danger")
            return redirect(url_for("bets_page"))

        with get_conn(app.config["DB_URL"]) as conn:
            rows = recent_paper_bets(conn, limit=50)
        return render_template("bets.html", bets=rows)

    @app.route("/bank", methods=["GET", "POST"])
    def bank_page():
        if request.method == "POST":
            try:
                new_bal = float(request.form.get("balance", "0"))
                with get_conn(app.config["DB_URL"]) as conn:
                    bank_set(conn, new_bal)
                flash("Balance updated.", "success")
            except Exception as e:
                flash(f"Error: {e}", "danger")
            return redirect(url_for("bank_page"))
        with get_conn(app.config["DB_URL"]) as conn:
            bal = bank_get(conn)
        return render_template("bank.html", balance=bal)

    return app


# Allow `python -m flask --app sports.webapp run`
app = create_app()
