from __future__ import annotations

"""Lightweight ingest package initializer.

Avoid importing heavy submodules on package import to prevent circular or
environment-dependent import errors. Re-export minimal helpers needed by
callers.
"""

__all__ = [
    "ingest_products",
]

from .tote_products import ingest_products  # noqa: F401
