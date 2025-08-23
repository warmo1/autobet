import os
import sys
import argparse
import importlib

def _try_import(mod_name, alias=None):
    try:
        mod = importlib.import_module(mod_name)
        if alias:
            globals()[f"_mod_{alias}"] = mod
        return mod
    except ImportError:
        return None

_mod_web = _try_import('sports.webapp', alias='webapp')

def main():
    ap = argparse.ArgumentParser(description="Autobet CLI")
    ap.add_argument("--db", help="Database URL")
    sub = ap.add_subparsers(dest="command")

    # Other subcommands here...

    # --- Web server ---
    if _mod_web and hasattr(_mod_web, 'create_app'):
        sp_web = sub.add_parser("web", help="Run the Flask web app")
        sp_web.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
        sp_web.add_argument("--port", type=int, default=8010, help="Port (default: 8010)")
        sp_web.add_argument("--debug", action="store_true", help="Run Flask in debug mode")
        def _cmd_web(args):
            db_url = args.db if hasattr(args, 'db') and args.db else os.getenv("DATABASE_URL", "sqlite:///sports_bot.db")
            app = _mod_web.create_app(db_url)
            print(f"[WEB] Starting Flask on http://{args.host}:{args.port} (debug={args.debug}) DB={db_url}")
            app.run(host=args.host, port=args.port, debug=args.debug)
        sp_web.set_defaults(func=_cmd_web)

    args = ap.parse_args()

    if not hasattr(args, 'func'):
        ap.print_help()
        sys.exit(1)

    args.func(args)

if __name__ == "__main__":
    main()
