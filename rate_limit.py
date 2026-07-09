"""Per-IP rate limiting for the cost-bearing public endpoints (readiness gap E2).

The engine spends real money per request (Decodo-billed SERP fetches) and
`/api/fragrances/details/requeue` lets callers enqueue enrichment work, yet the
only protection before this module was global concurrency semaphores — nothing
bounded what a single anonymous caller could burn. CORS does not protect
against non-browser callers.

Design mirrors the sCAST web app's limiter semantics:

* fail-open — a limit of 0 disables that rule, and any internal error lets the
  request through (protection must never become an outage);
* env-tuned — every limit is a `RATE_LIMIT_*_PER_MIN` env var, so ops can widen
  a rule under a NAT-heavy audience without a deploy;
* in-process — correct at the current single-replica/single-worker deployment
  (the same trade Redis-less sCAST makes; revisit alongside its H1 trigger).

Sliding 60s window per (rule, client IP). Client IP comes from the first entry
of X-Forwarded-For (Railway's edge sets it) with the socket peer as fallback.
"""
from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Optional

WINDOW_SECONDS = 60.0
# Past this many distinct keys, empty deques are swept on the next hit so an
# address-rotating scanner cannot grow the dict without bound.
_SWEEP_THRESHOLD = 4096


class SlidingWindowLimiter:
    """Thread-safe fixed-cost sliding-window counter, one window per key."""

    def __init__(self, limit: int, window_seconds: float = WINDOW_SECONDS) -> None:
        self.limit = int(limit)
        self.window = float(window_seconds)
        self._hits: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        if self.limit <= 0:  # 0 = rule disabled (fail-open switch)
            return True
        now = time.monotonic()
        cutoff = now - self.window
        with self._lock:
            hits = self._hits[key]
            while hits and hits[0] < cutoff:
                hits.popleft()
            if len(hits) >= self.limit:
                return False
            hits.append(now)
            if len(self._hits) > _SWEEP_THRESHOLD:
                for stale in [k for k, v in self._hits.items() if not v]:
                    del self._hits[stale]
            return True


def _env_limit(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def build_rules() -> Dict[tuple[str, str], SlidingWindowLimiter]:
    """(method, exact path) -> limiter. Rebuilt via this call in tests."""
    return {
        # Live Decodo-billed SERP fan-out per query.
        ("GET", "/api/fragrances/search"): SlidingWindowLimiter(
            _env_limit("RATE_LIMIT_SEARCH_PER_MIN", 30)
        ),
        # Decodo-billed image SERP.
        ("GET", "/api/fragrances/image-search"): SlidingWindowLimiter(
            _env_limit("RATE_LIMIT_IMAGE_SEARCH_PER_MIN", 30)
        ),
        # Scrape/cache-backed detail bundle.
        ("POST", "/api/fragrances/details"): SlidingWindowLimiter(
            _env_limit("RATE_LIMIT_DETAILS_PER_MIN", 60)
        ),
        # Enqueues enrichment jobs for caller-supplied identities: the cheapest
        # endpoint for an attacker to burn worker quota with, so the strictest
        # default. Idempotent upsert dedupes repeats of the SAME url; this
        # bounds DISTINCT junk per caller.
        ("POST", "/api/fragrances/details/requeue"): SlidingWindowLimiter(
            _env_limit("RATE_LIMIT_REQUEUE_PER_MIN", 5)
        ),
    }


def client_ip(request: Any) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        first = forwarded.split(",")[0].strip()
        if first:
            return first
    client = getattr(request, "client", None)
    return getattr(client, "host", None) or "unknown"


def check(rules: Dict[tuple[str, str], SlidingWindowLimiter], request: Any) -> Optional[dict]:
    """Return a rejection payload if the request exceeds its rule, else None.

    Fail-open on any internal error: limiting is protection, not a dependency.
    """
    try:
        limiter = rules.get((request.method.upper(), request.url.path))
        if limiter is None or limiter.allow(client_ip(request)):
            return None
        return {
            "status_code": 429,
            "content": {
                "detail": "Rate limit exceeded for this endpoint; retry shortly."
            },
            "headers": {"Retry-After": "30"},
        }
    except Exception:  # pragma: no cover - deliberate fail-open
        return None
