#!/usr/bin/env python3
"""Guards for readiness gaps E2 (per-IP rate limits) and E4 (/readyz probe).

Fully offline (no DB, no network); pytest-style, run via ``python -m pytest``.
The middleware integration test swaps api._RATE_RULES for its own tiny-limit
table and restores it, so the shared default windows are never consumed and no
other test in the suite can be flaked by this one.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import api
import db
import rate_limit
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# E2: limiter unit behavior
# ---------------------------------------------------------------------------


def test_limiter_allows_within_limit_and_rejects_over_it():
    limiter = rate_limit.SlidingWindowLimiter(2)
    assert limiter.allow("1.2.3.4")
    assert limiter.allow("1.2.3.4")
    assert not limiter.allow("1.2.3.4"), "third hit inside the window must be rejected"


def test_limiter_keys_are_independent_per_ip():
    limiter = rate_limit.SlidingWindowLimiter(1)
    assert limiter.allow("1.1.1.1")
    assert limiter.allow("2.2.2.2"), "a different caller has its own window"
    assert not limiter.allow("1.1.1.1")


def test_limiter_window_slides():
    limiter = rate_limit.SlidingWindowLimiter(1, window_seconds=0.05)
    assert limiter.allow("ip")
    assert not limiter.allow("ip")
    time.sleep(0.06)
    assert limiter.allow("ip"), "hits older than the window must expire"


def test_zero_limit_disables_the_rule():
    limiter = rate_limit.SlidingWindowLimiter(0)
    assert all(limiter.allow("ip") for _ in range(50)), "0 = fail-open switch"


def test_env_limit_parses_and_defaults(monkeypatch):
    monkeypatch.setenv("RATE_LIMIT_SEARCH_PER_MIN", "7")
    monkeypatch.setenv("RATE_LIMIT_REQUEUE_PER_MIN", "garbage")
    rules = rate_limit.build_rules()
    assert rules[("GET", "/api/fragrances/search")].limit == 7
    assert rules[("POST", "/api/fragrances/details/requeue")].limit == 5, (
        "unparseable env value falls back to the default"
    )


def test_default_rules_cover_every_cost_bearing_endpoint():
    rules = rate_limit.build_rules()
    assert set(rules) == {
        ("GET", "/api/fragrances/search"),
        ("GET", "/api/fragrances/image-search"),
        ("POST", "/api/fragrances/details"),
        ("POST", "/api/fragrances/details/requeue"),
    }
    # The requeue default must stay the strictest: it is the cheapest endpoint
    # for an attacker to burn worker quota with.
    assert rules[("POST", "/api/fragrances/details/requeue")].limit <= min(
        r.limit for r in rules.values()
    )


class _FakeURL:
    def __init__(self, path: str) -> None:
        self.path = path


class _FakeClient:
    def __init__(self, host: str) -> None:
        self.host = host


class _FakeRequest:
    def __init__(self, method: str, path: str, headers=None, host: str = "9.9.9.9"):
        self.method = method
        self.url = _FakeURL(path)
        self.headers = headers or {}
        self.client = _FakeClient(host)


def test_client_ip_prefers_x_forwarded_for():
    req = _FakeRequest("GET", "/x", headers={"x-forwarded-for": "8.8.8.8, 10.0.0.1"})
    assert rate_limit.client_ip(req) == "8.8.8.8"
    assert rate_limit.client_ip(_FakeRequest("GET", "/x")) == "9.9.9.9"


def test_check_ignores_unlisted_routes_and_rejects_listed_ones():
    rules = {("GET", "/limited"): rate_limit.SlidingWindowLimiter(1)}
    assert rate_limit.check(rules, _FakeRequest("GET", "/unlimited")) is None
    assert rate_limit.check(rules, _FakeRequest("GET", "/limited")) is None
    rejection = rate_limit.check(rules, _FakeRequest("GET", "/limited"))
    assert rejection is not None
    assert rejection["status_code"] == 429
    assert rejection["headers"]["Retry-After"]


# ---------------------------------------------------------------------------
# E2: middleware wired into the real app
# ---------------------------------------------------------------------------


def test_requeue_endpoint_returns_429_past_the_limit():
    original_rules = api._RATE_RULES
    api._RATE_RULES = {
        ("POST", "/api/fragrances/details/requeue"): rate_limit.SlidingWindowLimiter(2)
    }
    try:
        client = TestClient(api.app)
        payload = {"source_url": "https://www.fragrantica.com/perfume/X/Y-1.html"}
        first = client.post("/api/fragrances/details/requeue", json=payload)
        second = client.post("/api/fragrances/details/requeue", json=payload)
        assert first.status_code != 429 and second.status_code != 429, (
            "requests inside the window must reach the route "
            f"(got {first.status_code}/{second.status_code})"
        )
        third = client.post("/api/fragrances/details/requeue", json=payload)
        assert third.status_code == 429, "third hit inside the window must be limited"
        assert third.headers.get("retry-after")
    finally:
        api._RATE_RULES = original_rules


# ---------------------------------------------------------------------------
# E4: readiness probe
# ---------------------------------------------------------------------------


def test_readyz_is_lenient_when_db_is_disabled(monkeypatch):
    monkeypatch.setattr(db, "ENABLED", False)
    res = TestClient(api.app).get("/readyz")
    assert res.status_code == 200
    assert res.json() == {"ok": True, "db": "disabled"}


def test_readyz_503_when_db_ping_fails(monkeypatch):
    monkeypatch.setattr(db, "ENABLED", True)

    def _boom(timeout: float = 2.0):
        raise RuntimeError("pool unreachable")

    monkeypatch.setattr(db, "ping", _boom)
    res = TestClient(api.app).get("/readyz")
    assert res.status_code == 503
    assert res.json() == {"ok": False, "db": "unreachable"}


def test_readyz_ok_when_db_ping_succeeds(monkeypatch):
    monkeypatch.setattr(db, "ENABLED", True)
    monkeypatch.setattr(db, "ping", lambda timeout=2.0: None)
    res = TestClient(api.app).get("/readyz")
    assert res.status_code == 200
    assert res.json() == {"ok": True, "db": "ok"}
