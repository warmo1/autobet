from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import requests
import os
import threading

from ..config import cfg
from urllib.parse import urljoin


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
        # Use the configured URL exactly as provided (main branch behavior)
        # Do not mutate or add trailing slashes; some frontends are strict.
        self.base_url = (base_url or cfg.tote_graphql_url or "").strip()
        self.timeout = timeout
        self.max_retries = max(0, int(max_retries))
        self.session = requests.Session()
        # Auth headers: mirror legacy working set that previously worked in production
        self.api_key = (api_key or cfg.tote_api_key)
        self.auth_scheme = (cfg.tote_auth_scheme or "Api-Key").strip()
        # Match main branch auth: Authorization header only.
        self.headers = {
            "Authorization": f"{self.auth_scheme} {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "autobet/0.1 (+tote)",
        }

    # No alternate URL swapping for HTTP GraphQL; stick to gateway

    # (No custom header builder; use the fixed, known-good header set above.)

    def _post_json(self, url: str, payload: Dict[str, Any], *, headers_override: Optional[Dict[str, Any]] = None, keep_auth: Optional[bool] = None) -> Dict[str, Any]:
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                _rate_limiter.acquire()
                send_headers = dict(headers_override or self.headers)
                resp = self.session.post(url, headers=send_headers, json=payload, timeout=self.timeout)
                # Handle redirects explicitly (301/302/303/307/308)
                if 300 <= resp.status_code < 400:
                    loc = resp.headers.get("Location")
                    if loc:
                        try:
                            target = urljoin(url + ("/" if not url.endswith("/") else ""), loc)
                            resp = self.session.post(target, headers=send_headers, json=payload, timeout=self.timeout)
                        except Exception as e:
                            last_err = e
                            raise
                    else:
                        raise requests.HTTPError(f"{resp.status_code} Redirect without Location for url: {url}")
                if resp.status_code >= 400:
                    # Include a snippet of body to aid debugging of 4xx/5xx
                    snippet = ""
                    try:
                        t = resp.text or ""
                        snippet = (": " + t[:300].replace("\n"," ")) if t else ""
                    except Exception:
                        snippet = ""
                    raise requests.HTTPError(f"{resp.status_code} Client Error: {resp.reason} for url: {url}{snippet}")
                # OK – parse JSON or raise a descriptive error
                try:
                    return resp.json()
                except Exception:
                    ctype = resp.headers.get("Content-Type", "")
                    t = None
                    try:
                        t = resp.text or ""
                    except Exception:
                        t = ""
                    snippet = (t[:300].replace("\n"," ") if t else "")
                    raise requests.HTTPError(f"Invalid JSON (Content-Type: {ctype}) for url: {resp.url}: {snippet}")
            except Exception as e:
                last_err = e
                # simple backoff
                if attempt < self.max_retries:
                    time.sleep(0.5 * (2 ** attempt))
        raise ToteError(str(last_err) if last_err else "request failed")

    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None, *, keep_auth: Optional[bool] = None) -> Dict[str, Any]:
        url = self.base_url
        payload = {"query": query, "variables": variables or {}}
        data = self._post_json(url, payload, keep_auth=keep_auth)
        if isinstance(data, dict) and data.get("errors"):
            errs = data.get("errors")
            # Raise including endpoint used; do not swap to connections for HTTP GraphQL
            raise ToteError(f"GraphQL errors on {url}: {json.dumps(errs)}")
        return (data.get("data") if isinstance(data, dict) else data) or {}

    # For audit endpoint compatibility (some deployments separate audit path).
    # If no separate endpoint, reuse the same.
    def graphql_audit(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Use audit credentials/URL if configured, matching main behavior.

        - If cfg.tote_audit_graphql_url is set, call that endpoint for audit.
        - If cfg.tote_audit_api_key is set, use it (and scheme) for Authorization.
        Fallback to the regular endpoint/key when audit-specific are absent.
        """
        url = (cfg.tote_audit_graphql_url or self.base_url).strip()
        audit_key = (cfg.tote_audit_api_key or self.api_key)
        audit_scheme = (cfg.tote_audit_auth_scheme or self.auth_scheme)
        headers = dict(self.headers)
        headers["Authorization"] = f"{audit_scheme} {audit_key}"
        payload = {"query": query, "variables": variables or {}}
        data = self._post_json(url, payload, headers_override=headers)
        if isinstance(data, dict) and data.get("errors"):
            errs = data.get("errors")
            raise ToteError(f"GraphQL errors on {url}: {json.dumps(errs)}")
        return (data.get("data") if isinstance(data, dict) else data) or {}

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
            # Keep URL unmodified to avoid CloudFront 403s on some partners
            if "?" in sdl_url:
                sdl_url = sdl_url + "&sdl"
            else:
                sdl_url = sdl_url + "?sdl"
            resp = self.session.get(sdl_url, headers={
                "Authorization": f"{self.auth_scheme} {self.api_key}",
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
