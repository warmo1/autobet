"""
Lightweight compatibility shim so imports like `autobet.sports.*` work
even when the repository is checked out as a top-level folder named
`autobet` without an internal package layout.

This maps the existing `sports` package to `autobet.sports` at import time.
It avoids forcing a repo restructure and keeps Docker/Procfile imports
(`autobet.sports.gcp_ingest_service:app`) working in CI and local runs.
"""
from __future__ import annotations

import sys

try:
    import sports as _sports  # existing package in this repo
except Exception as _e:  # pragma: no cover
    # If `sports` isn't importable, leave gracefully; regular failures will surface.
    _sports = None  # type: ignore

if _sports is not None:
    # Present the same module object under the autobet.sports name
    sys.modules.setdefault("autobet.sports", _sports)

