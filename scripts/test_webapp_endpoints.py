from __future__ import annotations

import os
import sys
import time
import json
import requests


BASE = os.getenv("WEBAPP_BASE", "http://127.0.0.1:8010")

ENDPOINTS = [
    "/",
    "/tote-events",
    "/tote-superfecta",
    "/tote/calculators",
    "/tote/viability",
    "/audit/bets",
    "/imports",
]


def get(path: str) -> tuple[int, int]:
    url = BASE.rstrip("/") + path
    r = requests.get(url, timeout=10)
    return r.status_code, len(r.content or b"")


def main() -> None:
    errors = []
    print(f"Testing base={BASE}")
    for ep in ENDPOINTS:
        try:
            code, size = get(ep)
            ok = 200 <= code < 400
            print(f"{ep:28s} -> {code} ({size} bytes)")
            if not ok:
                errors.append((ep, code))
        except Exception as e:
            print(f"{ep:28s} -> ERROR {e}")
            errors.append((ep, str(e)))
    if errors:
        print("\nFailures:")
        for ep, err in errors:
            print(f"  {ep}: {err}")
        sys.exit(1)
    print("\nAll endpoints OK.")


if __name__ == "__main__":
    main()

