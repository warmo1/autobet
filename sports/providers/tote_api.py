from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

import requests
import os
import threading

from ..config import cfg
from urllib.parse import urljoin, urlparse, urlunparse


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
        raw_base = (base_url or cfg.tote_graphql_url or "").strip()
        # Normalize misconfigured HTTP endpoints (some setups still point to /connections/)
        self.base_url = raw_base
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

    @staticmethod
    def _normalize_http_endpoint(url: str) -> str:
        """Redirect HTTP(S) endpoints accidentally pointing at /connections/ to /gateway/."""
        if not url:
            return url
        parsed = urlparse(url)
        if parsed.scheme in ("http", "https"):
            path = parsed.path or ""
            if "/connections/" in path:
                path = path.replace("/connections/", "/gateway/")
                parsed = parsed._replace(path=path)
                url = urlunparse(parsed)
        return url

    @property
    def base_url(self) -> str:
        return getattr(self, "_base_url", "")

    @base_url.setter
    def base_url(self, value: str) -> None:
        self._base_url = self._normalize_http_endpoint(value or "")

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
        url = self._normalize_http_endpoint((cfg.tote_audit_graphql_url or self.base_url).strip())
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


def normalize_probable_lines(line_nodes: Any) -> List[Dict[str, Any]]:
    """Normalize GraphQL probable odds lines into the raw REST-like structure."""

    if isinstance(line_nodes, dict):
        maybe_nodes = line_nodes.get("nodes")
        nodes = maybe_nodes if isinstance(maybe_nodes, list) else [line_nodes]
    else:
        nodes = line_nodes or []

    normalized: List[Dict[str, Any]] = []
    for ln in nodes:
        if not isinstance(ln, dict):
            continue

        odds_obj = ln.get("odds")
        decimal_val_raw = None
        if isinstance(odds_obj, dict):
            decimal_val_raw = odds_obj.get("decimal")
        elif isinstance(odds_obj, list):
            for item in odds_obj:
                if isinstance(item, dict) and item.get("decimal") is not None:
                    decimal_val_raw = item.get("decimal")
                    break

        decimal_val = None
        if decimal_val_raw is not None:
            try:
                decimal_val = float(decimal_val_raw)
            except (TypeError, ValueError):
                decimal_val = None

        legs_src = ln.get("legs")
        legs_iter: List[Dict[str, Any]] = []
        if isinstance(legs_src, dict):
            leg_nodes = legs_src.get("nodes")
            if isinstance(leg_nodes, list):
                legs_iter.extend([leg for leg in leg_nodes if isinstance(leg, dict)])
            else:
                legs_iter.append(legs_src)
        elif isinstance(legs_src, list):
            legs_iter.extend([leg for leg in legs_src if isinstance(leg, dict)])

        norm_legs: List[Dict[str, Any]] = []
        for leg in legs_iter:
            selections_obj = leg.get("lineSelections")
            selection_items: List[Dict[str, Any]] = []
            if isinstance(selections_obj, dict):
                maybe_nodes = selections_obj.get("nodes")
                if isinstance(maybe_nodes, list):
                    selection_items.extend([sel for sel in maybe_nodes if isinstance(sel, dict)])
                else:
                    selection_items.append(selections_obj)
            elif isinstance(selections_obj, list):
                selection_items.extend([sel for sel in selections_obj if isinstance(sel, dict)])

            norm_selections = []
            for sel in selection_items:
                sel_id = sel.get("selectionId")
                if not sel_id and isinstance(sel.get("selection"), dict):
                    sel_id = sel["selection"].get("id")
                if sel_id:
                    norm_selections.append({"selectionId": sel_id})
            if norm_selections:
                norm_legs.append({"lineSelections": norm_selections})

        if not norm_legs:
            continue

        normalized.append({"legs": norm_legs, "odds": {"decimal": decimal_val}})

    return normalized


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

# Note: Legacy SQLite helpers removed. All raw payload archiving is handled via
# BigQuery upsert helpers in sports.bq (e.g., upsert_raw_tote, upsert_raw_tote_probable_odds).
