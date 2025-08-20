import argparse, time
from .config import cfg
from .db import get_conn, init_schema
from .ingest import ingest_football_csv_dir
from .suggest import suggest_football
from .news import fetch_and_store_news
from .llm import summarise_news
from .db import insert_insight
from .webapp import create_app

def cmd_ingest(args):
    conn = get_conn(cfg.database_url); init_schema(conn)
    if args.sport == 'football':
        n = ingest_football_csv_dir(conn, args.csv_dir)
        print(f"Ingested {n} rows from {args.csv_dir}")
    else:
        print("Only football ingestion is implemented in this starter.")

def cmd_train(args):
    print("Training models... (football Poisson trained on-the-fly during suggest in this starter)")

def cmd_suggest(args):
    conn = get_conn(cfg.database_url); init_schema(conn)
    if args.sport == 'football':
        picks = suggest_football(conn, days=args.days, min_edge=args.min_edge, stake_kelly_mult=args.kelly_mult)
        print(f"Generated {len(picks)} suggestions (stored in DB)")
    else:
        print("Only football suggest implemented in this starter.")

def cmd_news(args):
    conn = get_conn(cfg.database_url); init_schema(conn)
    fetch_and_store_news(conn)
    print("Fetched news.")

def cmd_insights(args):
    conn = get_conn(cfg.database_url); init_schema(conn)
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM news_articles ORDER BY published_ts DESC LIMIT 40", conn)
    if df.empty:
        print("No news; run 'news' first."); return
    items = df.to_dict("records")
    summary = summarise_news(items)
    insert_insight(conn, headline="Weekly sports digest", summary=summary)
    print("Insight stored.")

def cmd_web(args):
    app = create_app()
    app.run(host="0.0.0.0", port=8010, debug=True)

def cmd_paper(args):
    print("Paper betting loop could monitor suggestions and simulate stakes over time (not implemented in this starter loop). Use dashboard / paper_bet form.")

def cmd_live(args):
    if args.confirm != "BET":
        print("Pass --confirm BET to enable live orders (not implemented here).");
        return

def main(argv=None):
    p = argparse.ArgumentParser(description="Sports Betting Bot (Starter)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("ingest", help="Ingest historical CSVs") 
    sp.add_argument("--sport", type=str, default="football")
    sp.add_argument("--csv-dir", type=str, required=True)
    sp.set_defaults(func=cmd_ingest)

    sp = sub.add_parser("train", help="Train models (football auto in suggest)")
    sp.add_argument("--sport", type=str, default="football")
    sp.set_defaults(func=cmd_train)

    sp = sub.add_parser("suggest", help="Generate value suggestions") 
    sp.add_argument("--sport", type=str, default="football")
    sp.add_argument("--days", type=int, default=7)
    sp.add_argument("--min-edge", type=float, default=0.03)
    sp.add_argument("--kelly-mult", type=float, default=0.25, dest="kelly_mult")
    sp.set_defaults(func=cmd_suggest)

    sp = sub.add_parser("news", help="Fetch sports news") 
    sp.set_defaults(func=cmd_news)

    sp = sub.add_parser("insights", help="Summarise news with LLM") 
    sp.set_defaults(func=cmd_insights)

    sp = sub.add_parser("web", help="Run dashboard") 
    sp.set_defaults(func=cmd_web)

    sp = sub.add_parser("paper", help="Paper bet loop") 
    sp.set_defaults(func=cmd_paper)

    sp = sub.add_parser("live", help="Live (Betfair) -- not implemented") 
    sp.add_argument("--confirm", type=str, default="")
    sp.set_defaults(func=cmd_live)

    args = p.parse_args(argv)
    return args.func(args)

if __name__ == "__main__":
    main()
