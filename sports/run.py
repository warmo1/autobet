# ... (imports)
from .suggest import run_suggestions
from .ingest import ingest_horse_racing_data, ingest_tennis_data

def cmd_ingest(args):
    conn = get_conn(cfg.database_url); init_schema(conn)
    if args.sport == 'football':
        n = ingest_football_csv_dir(conn, args.csv_dir)
        print(f"Ingested {n} football rows.")
    elif args.sport == 'horse_racing':
        n = ingest_horse_racing_data(conn)
        print(f"Ingested {n} horse racing entries.")
    elif args.sport == 'tennis':
        n = ingest_tennis_data(conn)
        print(f"Ingested {n} tennis matches.")

def cmd_suggest(args):
    conn = get_conn(cfg.database_url); init_schema(conn)
    picks = run_suggestions(conn, args.sport)
    print(f"Generated {len(picks)} suggestions for {args.sport}.")

# ... (rest of the file, with updated argparse for ingest and suggest)
# In main():
# For ingest:
sp.add_argument("--sport", type=str, required=True, choices=['football', 'horse_racing', 'tennis'])
# For suggest:
sp.add_argument("--sport", type=str, required=True, choices=['football', 'horse_racing', 'tennis'])
