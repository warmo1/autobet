from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import requests

from sports.config import cfg


class ToteError(RuntimeError):
    pass


class ToteClient:
    """Minimal Tote API client supporting GraphQL and audit helpers.

    Expects env/config:
    - cfg.tote_api_key: API key string (required)
    - cfg.tote_graphql_url: HTTPS GraphQL endpoint base (required)
    """

    def __init__(self, *, timeout: float = 15.0, max_retries: int = 2) -> None:
        if not cfg.tote_graphql_url:
            raise ToteError("TOTE_GRAPHQL_URL is not configured")
        if not cfg.tote_api_key:
            raise ToteError("TOTE_API_KEY is not configured")
        self.base_url = cfg.tote_graphql_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max(0, int(max_retries))
        self.session = requests.Session()
        self.headers = {
            "Authorization": f"Api-Key {cfg.tote_api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "autobet/0.1 (+tote)"
        }

    def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self.session.post(url, headers=self.headers, json=payload, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_err = e
                # simple backoff
                if attempt < self.max_retries:
                    time.sleep(0.5 * (2 ** attempt))
        raise ToteError(str(last_err) if last_err else "request failed")

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self.base_url
        payload = {"query": query, "variables": variables or {}}
        data = self._post_json(url, payload)
        if isinstance(data, dict) and data.get("errors"):
            raise ToteError(json.dumps(data.get("errors")))
        return (data.get("data") if isinstance(data, dict) else data) or {}

    # For audit endpoint compatibility (some deployments separate audit path).
    # If no separate endpoint, reuse the same.
    def graphql_audit(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.graphql(query, variables)

    def graphql_sdl(self) -> str:
        query = """
        query IntrospectionQuery { __schema { types { name } } }
        """
        data = self.graphql(query, {})
        # return a compact view to confirm access
        return json.dumps(data)[:4000]


def store_raw(conn, *, endpoint: str, entity_id: Optional[str], sport: Optional[str], payload: Dict[str, Any]) -> str:
    """Archive raw Tote payload into raw_tote table.

    Returns the generated raw_id.
    """
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    raw_id = f"{endpoint}:{entity_id or ''}:{int(time.time())}"
    conn.execute(
        """
        INSERT OR REPLACE INTO raw_tote(raw_id, endpoint, entity_id, sport, fetched_ts, payload)
        VALUES(?,?,?,?,?,?)
        """,
        (raw_id, endpoint, entity_id, sport, ts, json.dumps(payload)),
    )
    return raw_id

