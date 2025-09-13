from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import requests
import os
import threading

from ..config import cfg


class ToteError(RuntimeError):
    pass


class ToteClient:
    """Minimal Tote API client supporting GraphQL and audit helpers.

    Expects env/config:
    - cfg.tote_api_key: API key string (required)
    - cfg.tote_graphql_url: HTTPS GraphQL endpoint base (required)
    """

    def __init__(self, *, timeout: float = 15.0, max_retries: int = 2, base_url: Optional[str] = None, api_key: Optional[str] = None) -> None:
        if not (base_url or cfg.tote_graphql_url):
            raise ToteError("TOTE_GRAPHQL_URL is not configured")
        if not (api_key or cfg.tote_api_key):
            raise ToteError("TOTE_API_KEY is not configured")
        # Use configured or provided HTTP GraphQL endpoint; normalize to gateway path.
        base_in = (base_url or cfg.tote_graphql_url or "").rstrip("/")
        self.base_url = self._normalize_gateway_url(base_in, audit=False)
        self.timeout = timeout
        self.max_retries = max(0, int(max_retries))
        self.session = requests.Session()
        # Auth headers: mirror legacy working set that previously worked in production
        self.api_key = (api_key or cfg.tote_api_key)
        self.auth_scheme = (cfg.tote_auth_scheme or "Api-Key").strip()
        # Use minimal header set known to work with partner gateway and avoid WAF false positives
        base_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "autobet/0.1 (+tote)",
        }
        # Primary Authorization header if scheme is not x-api-key
        if self.auth_scheme.lower() != "x-api-key":
            base_headers["Authorization"] = f"{self.auth_scheme} {self.api_key}"
        else:
            # When using x-api-key, do not set Authorization by default
            pass
        # Add x-api-key variants to maximize compatibility with partner WAF/routing
        base_headers.setdefault("X-Api-Key", self.api_key)
        base_headers.setdefault("x-api-key", self.api_key)
        self.headers = base_headers

    @staticmethod
    def _normalize_gateway_url(url: str, audit: bool = False) -> str:
        """Normalize known Tote endpoints to gateway graphql endpoints.

        - connections/graphql -> gateway/graphql
        - add /audit/ when audit=True (if not explicitly provided)
        """
        if not url:
            return url
        u = url.rstrip("/")
        try:
            if "/connections/graphql" in u:
                u = u.replace("/connections/graphql", "/gateway/graphql")
            # Ensure we are on /gateway/graphql, preserving any /partner prefix
            if not u.endswith("/graphql"):
                # Common case: already /gateway/graphql or /gateway/audit/graphql
                pass
            # Inject audit segment if needed and not present
            if audit:
                if "/audit/graphql" not in u:
                    u = u.replace("/gateway/graphql", "/gateway/audit/graphql")
        except Exception:
            pass
        return u

    # (No custom header builder; use the fixed, known-good header set above.)

    def _post_json(self, url: str, payload: Dict[str, Any], *, headers_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                _rate_limiter.acquire()
                send_headers = dict(headers_override or self.headers)
                # Heuristic: for live gateway (non-audit), ensure x-api-key headers are present.
                # This improves compatibility where Authorization: Api-Key is not accepted on /gateway/graphql.
                try:
                    u = (url or "")
                    if "/gateway/graphql" in u and "/audit/" not in u:
                        send_headers.setdefault("X-Api-Key", self.api_key)
                        send_headers.setdefault("x-api-key", self.api_key)
                        # Some partners require no Authorization header on this route; allow opt-out via env
                        if os.getenv("TOTE_DROP_AUTH_ON_LIVE", "0").lower() in ("1","true","yes","on"):
                            send_headers.pop("Authorization", None)
                except Exception:
                    pass
                resp = self.session.post(url, headers=send_headers, json=payload, timeout=self.timeout)
                if resp.status_code >= 400:
                    # Include a snippet of body to aid debugging of 4xx/5xx
                    snippet = ""
                    try:
                        t = resp.text or ""
                        snippet = (": " + t[:300].replace("\n"," ")) if t else ""
                    except Exception:
                        snippet = ""
                    raise requests.HTTPError(f"{resp.status_code} Client Error: {resp.reason} for url: {url}{snippet}")
                # OK
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
        """Preserve legacy behavior: use the current client's base_url and headers.

        Callers that need a distinct audit endpoint should set `client.base_url`
        before calling this method (as existing code already does in webapp paths).
        """
        return self.graphql(query, variables)

    def graphql_sdl(self) -> str:
        """Return schema SDL. Tries introspection; falls back to GET ?sdl on gateway.

        Many partner endpoints disable introspection; in that case we fetch
        SDL from the documented gateway endpoint `...?sdl` using auth headers.
        """
        introspection_query = """
        query IntrospectionQuery {
          __schema {
            queryType { name }
            mutationType { name }
            subscriptionType { name }
            types {
              ...FullType
            }
            directives {
              name
              description
              locations
              args {
                ...InputValue
              }
            }
          }
        }

        fragment FullType on __Type {
          kind
          name
          description
          fields(includeDeprecated: true) {
            name
            description
            args {
              ...InputValue
            }
            type {
              ...TypeRef
            }
            isDeprecated
            deprecationReason
          }
          inputFields {
            ...InputValue
          }
          interfaces {
            ...TypeRef
          }
          enumValues(includeDeprecated: true) {
            name
            description
            isDeprecated
            deprecationReason
          }
          possibleTypes {
            ...TypeRef
          }
        }

        fragment InputValue on __InputValue {
          name
          description
          type { ...TypeRef }
          defaultValue
        }

        fragment TypeRef on __Type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        try:
            data = self.graphql(introspection_query, {})
            import json
            return json.dumps(data, indent=2)
        except Exception:
            # Fallback to GET ?sdl
            sdl_url = self.base_url
            # Ensure we are targeting the gateway endpoint
            if "/gateway/graphql" not in sdl_url:
                try:
                    sdl_url = sdl_url.replace("/graphql", "/gateway/graphql")
                except Exception:
                    pass
            if "?" in sdl_url:
                sdl_url = sdl_url + "&sdl"
            else:
                sdl_url = sdl_url + "?sdl"
            resp = self.session.get(sdl_url, headers={
                "Authorization": f"Api-Key {cfg.tote_api_key}",
                "Accept": "text/plain, text/graphql, */*",
            }, timeout=self.timeout)
            resp.raise_for_status()
            return resp.text


class _RateLimiter:
    """Simple token-bucket rate limiter shared across Tote requests.

    Controlled via env vars:
      TOTE_RPS   – average requests per second (default 5)
      TOTE_BURST – bucket size (default 10)
    """
    def __init__(self) -> None:
        try:
            rps = float(os.getenv("TOTE_RPS", "5"))
            burst = int(os.getenv("TOTE_BURST", "10"))
        except Exception:
            rps, burst = 5.0, 10
        self.capacity = max(1, burst)
        self.refill_per_sec = max(0.1, rps)
        self.tokens = float(self.capacity)
        self.last = time.time()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.time()
            dt = now - self.last
            if dt > 0:
                self.tokens = min(self.capacity, self.tokens + dt * self.refill_per_sec)
                self.last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            # need to wait
            need = 1.0 - self.tokens
            wait = need / self.refill_per_sec
        if wait > 0:
            time.sleep(wait)
        # After sleeping, try to deduct token
        with self._lock:
            self.tokens = max(0.0, self.tokens - 1.0)


_rate_limiter = _RateLimiter()


def rate_limited_get(url: str, *, headers: Dict[str, Any] | None = None, timeout: float = 15.0) -> requests.Response:
    _rate_limiter.acquire()
    resp = requests.get(url, headers=headers or {}, timeout=timeout)
    return resp


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
