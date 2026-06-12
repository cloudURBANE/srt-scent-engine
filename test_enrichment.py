#!/usr/bin/env python3
"""
test_enrichment.py

Lightweight verification for the Pass 1 + Pass 2 enrichment pipeline.

Runs as a plain script (no pytest required):

    python test_enrichment.py

Two tiers:

  * Always-on checks -- run without a database. They cover token protection
    and the graceful-degradation contract (worker endpoints 503 when storage
    or the worker token is unconfigured; the public status endpoint still
    answers). These exercise api.py only.

  * DB-backed checks -- run only when DATABASE_URL is set. They drive db.py
    directly through the full job lifecycle (enqueue -> list -> claim ->
    complete -> fg_detail_cache lookup) plus stale-claim reclaim and the
    empty-frag_cards rejection. They use a throwaway job_key and clean up
    after themselves, so they are safe against a real (even shared) database.

Exit code is non-zero if any check fails.
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
from pathlib import Path
from http.cookies import SimpleCookie
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi.testclient import TestClient
from starlette.requests import Request
import requests

import api
import db
import mobile
import scripts.enrichment_worker as enrichment_worker

_FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        _FAILURES.append(label)


# ---------------------------------------------------------------------------
# Always-on checks (no database required)
# ---------------------------------------------------------------------------


def test_no_db_contract() -> None:
    print("Always-on checks (no DATABASE_URL required):")
    client = TestClient(api.app)

    # Public status endpoint answers even with storage disabled.
    r = client.get("/api/enrichment/status")
    check("public status endpoint responds 200", r.status_code == 200, str(r.status_code))

    # Worker endpoints reject a missing token. With ENRICHMENT_WORKER_TOKEN
    # unset the dependency returns 503 (unconfigured); with it set, 401.
    saved = api._ENRICHMENT_WORKER_TOKEN
    saved_public_diagnostics = api._PUBLIC_DIAGNOSTICS
    try:
        api._PUBLIC_DIAGNOSTICS = False
        api._ENRICHMENT_WORKER_TOKEN = ""
        r = client.get("/api/enrichment/jobs")
        check("worker endpoint 503 when token unconfigured", r.status_code == 503, str(r.status_code))

        r = client.get("/api/diagnostics/memory")
        check("diagnostics endpoint 503 when token unconfigured", r.status_code == 503, str(r.status_code))

        api._ENRICHMENT_WORKER_TOKEN = "test-secret-token"
        r = client.get("/api/enrichment/jobs")
        check("worker endpoint 401 without bearer token", r.status_code == 401, str(r.status_code))

        r = client.get("/api/diagnostics/memory")
        check("diagnostics endpoint 401 without bearer token", r.status_code == 401, str(r.status_code))

        r = client.get(
            "/api/enrichment/jobs",
            headers={"Authorization": "Bearer wrong-token"},
        )
        check("worker endpoint 401 with wrong token", r.status_code == 401, str(r.status_code))

        # Correct token passes auth; without a DB it then 503s on storage.
        r = client.get(
            "/api/enrichment/jobs",
            headers={"Authorization": "Bearer test-secret-token"},
        )
        expected = {200, 503}  # 503 when no DB, 200 when DB is configured
        check(
            "worker endpoint accepts valid token (then 200/503)",
            r.status_code in expected,
            str(r.status_code),
        )

        r = client.get(
            "/api/diagnostics/memory",
            headers={"Authorization": "Bearer test-secret-token"},
        )
        check("diagnostics endpoint accepts valid token", r.status_code == 200, str(r.status_code))

        api._ENRICHMENT_WORKER_TOKEN = ""
        api._PUBLIC_DIAGNOSTICS = True
        r = client.get("/api/diagnostics/memory")
        check("diagnostics endpoint supports explicit public override", r.status_code == 200, str(r.status_code))

        api._PUBLIC_DIAGNOSTICS = False
        api._ENRICHMENT_WORKER_TOKEN = "test-secret-token"
        r = client.patch(
            "/api/enrichment/jobs/does-not-exist",
            headers={"Authorization": "Bearer test-secret-token"},
            json={"fg_url": "https://www.fragrantica.com/perfume/Brand/Name-1.html"},
        )
        check(
            "patch job endpoint requires DB storage",
            r.status_code in expected,
            str(r.status_code),
        )

        r = client.post(
            "/api/fragrances/details/requeue",
            json={"source_url": _TEST_JOB_KEY},
        )
        check(
            "public requeue endpoint requires DB storage",
            r.status_code in expected,
            str(r.status_code),
        )
    finally:
        api._ENRICHMENT_WORKER_TOKEN = saved
        api._PUBLIC_DIAGNOSTICS = saved_public_diagnostics

    @dataclass
    class ParserLeak:
        amount: Decimal
        captured_at: datetime

    sanitized = api._json_for_db_blob(
        {
            "card": ParserLeak(Decimal("1.25"), datetime(2026, 1, 2, 3, 4, 5)),
            "notes": {"top": {"bergamot", "lemon"}},
        }
    )
    check(
        "db blob sanitizer accepts parser-native objects",
        sanitized["card"] == {"amount": 1.25, "captured_at": "2026-01-02T03:04:05"}
        and sorted(sanitized["notes"]["top"]) == ["bergamot", "lemon"],
    )

    response = requests.Response()
    response.status_code = 500
    response.headers["Content-Type"] = "application/json"
    response._content = b'{"detail":"complete_job failed (TypeError): bad payload"}'
    check(
        "worker extracts structured API error detail",
        enrichment_worker._safe_response_text(response) == "complete_job failed (TypeError): bad payload",
    )

    class FakeSession:
        def __init__(self) -> None:
            self.last_json: dict[str, object] | None = None

        def request(self, _method: str, _url: str, **kwargs: object) -> requests.Response:
            self.last_json = kwargs.get("json")  # type: ignore[assignment]
            ok = requests.Response()
            ok.status_code = 200
            ok._content = b"{}"
            return ok

    fake_session = FakeSession()
    worker_client = enrichment_worker.ApiClient("https://api.example.test", "token")
    worker_client.session = fake_session  # type: ignore[assignment]
    worker_client._request(
        "POST",
        "/api/enrichment/jobs/test/complete",
        json={
            "card": ParserLeak(Decimal("2.50"), datetime(2026, 2, 3, 4, 5, 6)),
            "tags": {"warm", "amber"},
        },
    )
    sent = fake_session.last_json or {}
    check(
        "worker normalizes outbound JSON payloads before requests",
        sent.get("card") == {"amount": 2.5, "captured_at": "2026-02-03T04:05:06"}
        and sorted(sent.get("tags") or []) == ["amber", "warm"],
    )

    saved_enqueue_state = db.enqueue_job_state
    try:
        db.enqueue_job_state = lambda **_kwargs: {  # type: ignore[assignment]
            "status": "completed",
            "requested_count": 82,
        }
        selected = api.engine.UnifiedFragrance(
            name="Completed Partial",
            brand="Test House",
            year="2026",
            frag_url=(
                "https://www.fragrantica.com/perfume/"
                "Test-House/Completed-Partial-82.html"
            ),
        )
        job_state = api._enqueue_enrichment_job(
            selected,
            api.DetailRequest(source_url=selected.frag_url),
        )
        status, requested_count = api._enrichment_status_from_job_state(job_state)
        check("duplicate completed queue row stays completed", status == "completed")
        check(
            "duplicate completed queue row keeps requested count",
            requested_count == 82,
        )
    finally:
        db.enqueue_job_state = saved_enqueue_state


def test_fragrantica_challenge_and_static_parse() -> None:
    print("Fragrantica detail parser checks:")
    engine = enrichment_worker.engine

    class FakeResponse:
        def __init__(self, text: str, status_code: int = 200) -> None:
            self.text = text
            self.content = text.encode("utf-8")
            self.status_code = status_code

    challenge = FakeResponse(
        """
        <html><head><title>Just a moment...</title></head>
        <body>
          Checking if the site connection is secure.
          Verify you are human. Enable JavaScript and cookies to continue.
          <div class="cf-turnstile"></div>
        </body></html>
        """
    )
    check(
        "Fragrantica 200 challenge shell is detected",
        bool(engine._response_has_challenge(challenge)),
    )

    original_get = engine.Http.get
    try:
        engine.Http.get = staticmethod(lambda *args, **kwargs: challenge)
        details = engine.UnifiedDetails(notes=engine.NotesList())
        engine.FragranticaEngine.fetch_details(
            object(),
            "https://www.fragrantica.com/perfume/Thameen/Royal-Sapphire-55049.html",
            details,
            deadline=engine.Deadline(5),
        )
        check(
            "challenge shell is reported as fetch error",
            details.fetch_errors.get("fg") == "challenge_page",
            str(details.fetch_errors),
        )
        check(
            "challenge shell does not masquerade as parser-empty cards",
            not details.frag_cards,
            str(details.frag_cards),
        )
    finally:
        engine.Http.get = original_get

    interstitial = FakeResponse(
        """
        <!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title>
        <meta http-equiv="refresh" content="360">
        <script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1?ray=a0a5"></script>
        </head><body>
        <noscript><span id="challenge-error-text">Enable JavaScript and cookies to continue</span></noscript>
        <script>window._cf_chl_opt = {cvId: "3"};</script>
        </body></html>
        """
    )
    check(
        "real-shape CF interstitial (orchestrate/chl_page) is detected",
        bool(engine._response_has_challenge(interstitial)),
    )

    static_page = FakeResponse(
        """
        <html><body>
          <h1>Royal Sapphire Thameen for women and men</h1>
          <h6>main accords</h6>
          <p>citrus white floral amber woody sweet</p>
          <section>User Ratings Rating When To Wear</section>
          <p>Perfume rating 4.21 out of 5 with 466 votes</p>
        </body></html>
        """
    )
    check(
        "normal Fragrantica detail HTML is not a challenge",
        not engine._response_has_challenge(static_page),
    )

    cleared_page = FakeResponse(
        """
        <html><head><title>Royal Sapphire Thameen perfume</title></head><body>
          <h1>Royal Sapphire Thameen for women and men</h1>
          <form action="/login"><div class="cf-turnstile" data-sitekey="x"></div></form>
          <p>Perfume rating 4.21 out of 5 with 466 votes</p>
          <script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script>
        </body></html>
        """
    )
    check(
        "cleared page with CF telemetry + Turnstile login widget is not a challenge",
        not engine._response_has_challenge(cleared_page),
    )

    try:
        engine.Http.get = staticmethod(lambda *args, **kwargs: static_page)
        details = engine.UnifiedDetails(notes=engine.NotesList())
        engine.FragranticaEngine.fetch_details(
            object(),
            "https://www.fragrantica.com/perfume/Thameen/Royal-Sapphire-55049.html",
            details,
            deadline=engine.Deadline(5),
        )
        check(
            "static Fragrantica rating text yields a frag card",
            "Fragrantica Rating" in details.frag_cards,
            str(details.frag_cards),
        )
    finally:
        engine.Http.get = original_get


def test_fragrantica_clearance_session_lifecycle() -> None:
    print("Fragrantica clearance session checks:")
    engine = enrichment_worker.engine

    def response(text: str, status_code: int = 200) -> requests.Response:
        res = requests.Response()
        res.status_code = status_code
        res._content = text.encode("utf-8")
        res.encoding = "utf-8"
        return res

    class FakeOptions:
        def set_browser_path(self, _path: str) -> None:
            pass

        def set_argument(self, _argument: str) -> None:
            pass

        def headless(self, _enabled: bool) -> None:
            pass

        def auto_port(self) -> None:
            pass

    class FakePage:
        def __init__(self, _options: object | None = None) -> None:
            self.title = "Fragrantica"
            self.url = "about:blank"
            self.html = "<html><title>Fragrantica</title><body>Perfume rating 4.1 out of 5</body></html>"
            self.quit_count = 0

        def get(self, url: str, timeout: float | None = None) -> None:
            self.url = url

        def cookies(self) -> list[dict[str, str]]:
            return [{"name": "cf_clearance", "value": "ok"}]

        def run_js(self, _script: str) -> str:
            return (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
            )

        def quit(self) -> None:
            self.quit_count += 1

    class NoCookieFakePage(FakePage):
        def cookies(self) -> list[dict[str, str]]:
            return []

    class BadReplaySession:
        def get(self, _url: str, timeout: float = 5) -> requests.Response:
            return response(
                "<html><title>Just a moment...</title><body>cf-challenge</body></html>",
                403,
            )

    page = FakePage()
    browser_session = engine._FragranticaBrowserSession(
        page,
        "test-ua",
        {"cf_clearance": "ok"},
    )
    try:
        check(
            "browser-backed Fragrantica session validates non-challenge pages",
            engine._validate_fragrantica_session(browser_session),
        )
    finally:
        browser_session.close()
    check("browser-backed session closes its page", page.quit_count == 1)

    saved = {
        "ChromiumOptions": engine.ChromiumOptions,
        "ChromiumPage": engine.ChromiumPage,
        "new_session": engine._new_fragrantica_http_session,
        "save_cache": engine._save_fragrantica_cache,
        "last_error": engine._FRAGRANTICA_LAST_MINT_ERROR,
        "session": engine._FRAGRANTICA_SESSION,
        "env_path": os.environ.get("FRAGRANTICA_CHROMIUM_PATH"),
        "env_wait": os.environ.get("FRAGRANTICA_CLEARANCE_WAIT"),
    }
    cache_saves: list[tuple[str, dict[str, str]]] = []
    minted_session = None
    try:
        engine.reset_fragrantica_scraper(clear_cache=False)
        engine.ChromiumOptions = FakeOptions  # type: ignore[assignment]
        engine.ChromiumPage = FakePage  # type: ignore[assignment]
        engine._new_fragrantica_http_session = lambda *_args: BadReplaySession()  # type: ignore[assignment]
        engine._save_fragrantica_cache = lambda ua, cookies: cache_saves.append((ua, dict(cookies)))  # type: ignore[assignment]
        os.environ["FRAGRANTICA_CHROMIUM_PATH"] = sys.executable
        os.environ["FRAGRANTICA_CLEARANCE_WAIT"] = "1"

        minted_session = engine._mint_fragrantica_clearance()
        check(
            "mint falls back to browser-backed session when cookie replay fails",
            isinstance(minted_session, engine._FragranticaBrowserSession),
            str(type(minted_session)),
        )
        check(
            "curl-invalid Fragrantica clearance is not saved as durable cache",
            not cache_saves,
            str(cache_saves),
        )
        if hasattr(minted_session, "close"):
            minted_session.close()
        minted_session = None

        engine.ChromiumPage = NoCookieFakePage  # type: ignore[assignment]
        cache_saves.clear()
        minted_session = engine._mint_fragrantica_clearance()
        check(
            "mint accepts validated browser-backed session without reusable cookies",
            isinstance(minted_session, engine._FragranticaBrowserSession),
            str(type(minted_session)),
        )
        check(
            "cookie-less browser-backed Fragrantica session is not saved as durable cache",
            not cache_saves,
            str(cache_saves),
        )
    finally:
        if hasattr(minted_session, "close"):
            minted_session.close()
        engine.ChromiumOptions = saved["ChromiumOptions"]  # type: ignore[assignment]
        engine.ChromiumPage = saved["ChromiumPage"]  # type: ignore[assignment]
        engine._new_fragrantica_http_session = saved["new_session"]  # type: ignore[assignment]
        engine._save_fragrantica_cache = saved["save_cache"]  # type: ignore[assignment]
        engine._FRAGRANTICA_LAST_MINT_ERROR = saved["last_error"]
        engine._FRAGRANTICA_SESSION = saved["session"]
        if saved["env_path"] is None:
            os.environ.pop("FRAGRANTICA_CHROMIUM_PATH", None)
        else:
            os.environ["FRAGRANTICA_CHROMIUM_PATH"] = saved["env_path"]
        if saved["env_wait"] is None:
            os.environ.pop("FRAGRANTICA_CLEARANCE_WAIT", None)
        else:
            os.environ["FRAGRANTICA_CLEARANCE_WAIT"] = saved["env_wait"]

    env_keys = (
        "FRAGRANTICA_CLEARANCE_UA",
        "FRAGRANTICA_CLEARANCE_COOKIE_HEADER",
        "FRAGRANTICA_CLEARANCE_COOKIES_JSON",
    )
    saved_env = {key: os.environ.get(key) for key in env_keys}
    saved_cache_file = engine._FRAGRANTICA_CACHE_FILE
    saved_new_session = engine._new_fragrantica_http_session
    saved_session = engine._FRAGRANTICA_SESSION
    try:
        for key in env_keys:
            os.environ.pop(key, None)
        with tempfile.TemporaryDirectory() as tmp:
            cache_file = Path(tmp) / "fg-clearance.json"
            cache_file.write_text(
                '{"ua":"ua","cookies":{"cf_clearance":"bad"}}',
                encoding="utf-8",
            )
            engine._FRAGRANTICA_CACHE_FILE = cache_file
            engine._FRAGRANTICA_SESSION = None
            engine._new_fragrantica_http_session = lambda *_args: BadReplaySession()  # type: ignore[assignment]
            fallback = object()
            result = engine.get_fragrantica_scraper(fallback, mint_clearance=False)
            check("invalid saved Fragrantica clearance falls back cleanly", result is fallback)
            check("invalid saved Fragrantica clearance cache is discarded", not cache_file.exists())
    finally:
        engine._FRAGRANTICA_CACHE_FILE = saved_cache_file
        engine._new_fragrantica_http_session = saved_new_session  # type: ignore[assignment]
        engine._FRAGRANTICA_SESSION = saved_session
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    saved_mint = engine._mint_fragrantica_clearance
    saved_session = engine._FRAGRANTICA_SESSION
    saved_headless = os.environ.get("FRAGRANTICA_CHROMIUM_HEADLESS")
    saved_bn_path = os.environ.get("BASENOTES_CHROMIUM_PATH")
    fake_session = object()
    try:
        engine._FRAGRANTICA_SESSION = None
        engine._mint_fragrantica_clearance = lambda: fake_session  # type: ignore[assignment]
        enrichment_worker._run_automatic_clearance_mint()
        check(
            "dashboard direct mint installs returned Fragrantica session",
            engine._FRAGRANTICA_SESSION is fake_session,
        )
    finally:
        engine._mint_fragrantica_clearance = saved_mint  # type: ignore[assignment]
        engine._FRAGRANTICA_SESSION = saved_session
        if saved_headless is None:
            os.environ.pop("FRAGRANTICA_CHROMIUM_HEADLESS", None)
        else:
            os.environ["FRAGRANTICA_CHROMIUM_HEADLESS"] = saved_headless
        if saved_bn_path is None:
            os.environ.pop("BASENOTES_CHROMIUM_PATH", None)
        else:
            os.environ["BASENOTES_CHROMIUM_PATH"] = saved_bn_path


def test_mobile_new_device_session_binding() -> None:
    print("Mobile auth checks (no DATABASE_URL required):")
    now = datetime.now(timezone.utc)
    account_row = {
        "id": "account-1",
        "email": "worker@example.test",
        "label": "Worker",
        "pin_hash": "stored",
        "disabled": False,
        "pin_strikes": 5,
        "created_at": now,
        "last_login_at": None,
    }
    expired = db._account_to_dict(
        {**account_row, "locked_until": now - timedelta(seconds=1)}
    )
    active = db._account_to_dict(
        {**account_row, "locked_until": now + timedelta(minutes=15)}
    )
    check("expired mobile lockout is inactive", expired["locked_until"] is None)
    check("active mobile lockout remains visible", active["locked_until"] is not None)

    saved = {
        "enabled": db.ENABLED,
        "consume_magic_link": db.consume_magic_link,
        "get_worker_account": db.get_worker_account,
        "reset_pin_strikes": db.reset_pin_strikes,
        "create_session": db.create_session,
        "verify_pin": mobile.verify_pin,
    }
    created: dict[str, str] = {}
    token = "test-magic-token"

    def fake_create_session(
        *, account_id: str, device_fingerprint: str, ttl_days: int
    ) -> str:
        created["account_id"] = account_id
        created["device"] = device_fingerprint
        created["ttl_days"] = str(ttl_days)
        return "session-1"

    try:
        db.ENABLED = True
        db.consume_magic_link = lambda token_hash: (
            {"account_id": "account-1"}
            if token_hash == mobile._hash_token(token)
            else None
        )
        db.get_worker_account = lambda account_id: {
            "id": account_id,
            "email": "worker@example.test",
            "pin_hash": "stored",
            "disabled": False,
            "locked_until": None,
        }
        db.reset_pin_strikes = lambda account_id: None
        db.create_session = fake_create_session
        mobile.verify_pin = lambda pin, stored: pin == "123456"

        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/m/verify",
                "headers": [],
                "query_string": b"",
                "client": ("127.0.0.1", 12345),
                "server": ("testserver", 443),
                "scheme": "https",
            }
        )
        response = mobile.m_verify(request, token=token, pin="123456")

        cookie = SimpleCookie()
        for header, value in response.raw_headers:
            if header.lower() == b"set-cookie":
                cookie.load(value.decode("latin-1"))

        check("mobile verify redirects after valid token and PIN", response.status_code == 302)
        check("mobile verify emits a device cookie", mobile.DEVICE_COOKIE in cookie)
        check("mobile verify emits a session cookie", mobile.SESSION_COOKIE in cookie)
        check(
            "new mobile session is bound to the emitted device cookie",
            created.get("device") == cookie[mobile.DEVICE_COOKIE].value,
        )
    finally:
        db.ENABLED = saved["enabled"]
        db.consume_magic_link = saved["consume_magic_link"]
        db.get_worker_account = saved["get_worker_account"]
        db.reset_pin_strikes = saved["reset_pin_strikes"]
        db.create_session = saved["create_session"]
        mobile.verify_pin = saved["verify_pin"]


def test_hydration_dedup() -> None:
    print("Detail hydration checks (no DATABASE_URL required):")

    def make_entry() -> dict[str, object]:
        return {
            "frag_cards": {
                "Community Interest": [{"label": "Have", "count": "12"}],
                "Performance": [{"label": "Longevity", "count": "8"}],
            },
            "notes": {
                "has_pyramid": True,
                "top": ["bergamot"],
                "heart": ["rose"],
                "base": ["musk"],
                "flat": [],
            },
            "pros_cons": ["long lasting", "great projection"],
            "reviews": [
                {"text": "A modern classic.", "source": "fragrantica"},
                {"text": "A touch sharp on me.", "source": "fragrantica"},
            ],
        }

    # Re-hydrating the same bundle twice must not accumulate duplicate rows --
    # _hydrate_details_from_entry can run more than once over one bundle.
    details = api.engine.UnifiedDetails(notes=api.engine.NotesList())
    first = api._hydrate_details_from_entry(details, make_entry())
    check("first hydration applies cached Fragrantica data", first is True)
    pros_after_first = list(details.pros_cons)
    reviews_after_first = len(details.reviews)
    api._hydrate_details_from_entry(details, make_entry())
    check(
        "re-hydration does not duplicate pros_cons",
        details.pros_cons == pros_after_first,
        f"{details.pros_cons}",
    )
    check(
        "re-hydration does not duplicate reviews",
        len(details.reviews) == reviews_after_first,
        f"{len(details.reviews)} review(s)",
    )

    # A complete cache entry backfills a partial bundle: live rows are kept
    # exactly once, and only genuinely new cached rows are appended.
    partial = api.engine.UnifiedDetails(
        notes=api.engine.NotesList(top=["lemon"]),
        pros_cons=["long lasting"],
    )
    partial.reviews.append(api.engine.Review(text="A modern classic.", source="live"))
    applied = api._hydrate_details_from_entry(partial, make_entry())
    check("hydration applies to a partial bundle", applied is True)
    check(
        "live pros_cons row kept exactly once after overlay",
        partial.pros_cons.count("long lasting") == 1,
        f"{partial.pros_cons}",
    )
    check(
        "new cached pros_cons row is appended",
        "great projection" in partial.pros_cons,
    )
    check(
        "complete cache entry backfills missing frag_cards",
        "Performance" in (partial.frag_cards or {}),
    )
    check(
        "same review text with a different source stays distinct",
        sum(1 for r in partial.reviews if r.text == "A modern classic.") == 2,
        f"{[(r.text, r.source) for r in partial.reviews]}",
    )


def test_concentration_pipeline() -> None:
    print("Concentration enrichment checks (no DATABASE_URL required):")

    # 1. The /complete payload model accepts a concentration field.
    payload = api.CompleteJobRequest(
        fg_url="https://www.fragrantica.com/perfume/Test-House/Test-1.html",
        frag_cards={"Community Interest": [{"label": "Have", "count": "1"}]},
        concentration="Eau de Parfum",
    )
    check(
        "complete payload model accepts concentration",
        payload.concentration == "Eau de Parfum",
    )

    # 2. Worker tier-1 resolution works without the SERP/browser tier.
    # Fictional names keep the Parfinity catalog tier from short-circuiting.
    resolved = enrichment_worker._resolve_concentration(
        "Testhouse", "Imaginary Extrait de Parfum", allow_serp=False
    )
    check(
        "worker resolves explicit concentration token",
        bool(resolved) and resolved["concentration"] == "Extrait",
        str(resolved),
    )
    resolved = enrichment_worker._resolve_concentration(
        "Xerjoff", "Totally Made Up Scent", allow_serp=False
    )
    check(
        "worker resolves brand-prior concentration",
        bool(resolved) and resolved["concentration"] == "Parfum",
        str(resolved),
    )
    check(
        "worker concentration meta carries source",
        bool(resolved) and resolved["concentration_meta"]["source"] == "tier1_brand_prior",
        str(resolved),
    )

    # 3. _apply_concentration injects into payload + raw_identity.
    worker_payload = {"raw_identity": {"name": "Imaginary Extrait de Parfum"}}
    enrichment_worker._apply_concentration(
        worker_payload, "Testhouse", "Imaginary Extrait de Parfum"
    )
    check(
        "worker payload carries top-level concentration",
        worker_payload.get("concentration") == "Extrait",
        str(worker_payload),
    )
    check(
        "worker raw_identity carries concentration + meta",
        worker_payload["raw_identity"].get("concentration") == "Extrait"
        and isinstance(worker_payload["raw_identity"].get("concentration_meta"), dict),
        str(worker_payload["raw_identity"]),
    )

    # 4. Cache hydration fills details.concentration (fill-only, live wins).
    details = api.engine.UnifiedDetails(notes=api.engine.NotesList())
    entry = {
        "frag_cards": {"Community Interest": [{"label": "Have", "count": "1"}]},
        "raw_identity": {"concentration": "Eau de Parfum"},
    }
    api._hydrate_details_from_entry(details, entry)
    check(
        "hydration fills concentration from cached raw_identity",
        getattr(details, "concentration", None) == "Eau de Parfum",
    )
    setattr(details, "concentration", "Extrait")
    api._hydrate_details_from_entry(details, entry)
    check(
        "hydration never overwrites an existing concentration",
        getattr(details, "concentration", None) == "Extrait",
    )

    # 5. Stored fragrance_records surface concentration from fg_raw.
    record = {
        "record_key": "fg:test",
        "canonical_fg_url": "https://www.fragrantica.com/perfume/Test-House/Test-1.html",
        "bn_url": None,
        "name": "Test",
        "house": "Test House",
        "year": 2026,
        "gender": "Unisex",
        "image_url": None,
        "search": {},
        "fg_raw": {
            "frag_cards": {"Community Interest": [{"label": "Have", "count": "1"}]},
            "concentration": "Extrait",
        },
        "bn_raw": {},
        "derived_metrics": None,
    }
    stored_details = api._details_from_fragrance_record(record)
    check(
        "stored record surfaces concentration onto details",
        getattr(stored_details, "concentration", None) == "Extrait",
    )

    # 6. The backlog sweeper flags rows missing base fields.
    import scripts.enrich_database_metrics as sweeper

    complete_dm = {
        "source_coverage": {
            "performance_score": True,
            "value_score": True,
            "community_interest_score": True,
            "wear_profile": True,
        }
    }
    full_row = {
        "derived_metrics": complete_dm,
        "gender": "Unisex",
        "year": 2026,
        "concentration": "Eau de Parfum",
    }
    check(
        "sweeper accepts a row with metrics and base fields",
        sweeper._stored_metrics_complete(full_row) is True,
    )
    for missing in ("gender", "year", "concentration"):
        partial_row = {k: v for k, v in full_row.items() if k != missing}
        check(
            f"sweeper flags a row missing {missing}",
            sweeper._stored_metrics_complete(partial_row) is False,
        )
    check(
        "sweeper treats explicit Unknown concentration as incomplete",
        sweeper._stored_metrics_complete({**full_row, "concentration": "Unknown"}) is False,
    )
    check(
        "sweeper treats default gender as incomplete",
        sweeper._stored_metrics_complete({**full_row, "gender": "Unisex / Unspecified"}) is False,
    )
    nested_row = {
        "derived_metrics": complete_dm,
        "concentration": "Eau de Parfum",
        "product": {"gender": "Unisex", "year": 2026},
    }
    check(
        "sweeper reads base fields nested under product",
        sweeper._stored_metrics_complete(nested_row) is True,
    )

    # 7. Backfill writes the missing base fields in place.
    row = {"concentration": ""}
    sweeper._backfill_base_fields(
        row, gender="Men", year=2020, brand="Testhouse", name="Imaginary Extrait de Parfum"
    )
    check(
        "backfill fills gender/year/concentration",
        row.get("gender") == "Men"
        and row.get("year") == 2020
        and row.get("concentration") == "Extrait",
        str(row),
    )

    # 8. Fact helpers derive app-facing family/wear fields from derived_metrics.
    import enrichment_facts

    derived = {
        "main_accords": {
            "top_accords": ["woody", "aromatic", "citrus"],
        },
        "wear_profile": {
            "primary_seasons": ["Spring", "Summer"],
            "primary_time": "Day",
        },
    }
    families = enrichment_facts.derive_families(derived, existing_family="Unknown Family")
    check("family derivation maps main accords", families["primary_family"] == "Woody", str(families))
    check(
        "default family is not complete",
        enrichment_facts.is_fact_complete("Unknown Family", "family") is False,
    )
    check(
        "unsupported Universal season is not complete",
        enrichment_facts.is_fact_complete("Universal", "season") is False,
    )

    payload = {}
    sweeper._apply_derived_facts(payload, derived)
    check(
        "sweeper projects derived family and season",
        payload.get("family") == "Woody" and payload.get("season") == "Spring",
        str(payload),
    )

    selected = api.engine.UnifiedFragrance(name="Test", brand="House", year="2026")
    detail_payload = api._details_to_dict(
        selected,
        api.engine.UnifiedDetails(
            notes=api.engine.NotesList(),
            derived_metrics=derived,
        ),
    )
    check(
        "details response projects family facts",
        detail_payload.get("family") == "Woody"
        and detail_payload.get("families") == ["Woody", "Aromatic", "Citrus"],
        str(detail_payload),
    )
    check(
        "details response projects wear profile",
        detail_payload.get("wear_profile", {}).get("primary_time") == "Day",
        str(detail_payload.get("wear_profile")),
    )

    merged_raw_identity = db._cache_row_raw_identity(
        {
            "raw_identity": {"name": "Test"},
            "concentration": "Eau de Parfum",
            "concentration_meta": {"source": "unit_test", "confidence": 99},
        }
    )
    check(
        "complete_job raw_identity merge keeps concentration durable",
        merged_raw_identity.get("concentration") == "Eau de Parfum"
        and merged_raw_identity.get("concentration_meta", {}).get("source") == "unit_test",
        str(merged_raw_identity),
    )


def test_stored_detail_completion_logic() -> None:
    print("Stored detail completion logic checks (no DATABASE_URL required):")

    saved_enabled = db.ENABLED
    saved_lookup_rec = db.lookup_fragrance_record
    saved_lookup_cache = db.lookup_detail_cache
    saved_enqueue = db.enqueue_job
    saved_recover = db.recover_or_enqueue_job
    saved_upsert = db.upsert_fragrance_details

    enqueue_calls = []
    recover_calls = []

    # A record representing a completed worker job but with missing metrics (wear_profile missing):
    mock_frag_record_partial = {
        "record_key": "fg:https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
        "canonical_fg_url": "https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
        "bn_url": None,
        "name": "Test Fragrance",
        "house": "Test Brand",
        "year": 2026,
        "gender": "Unisex",
        "image_url": None,
        "search": {},
        # No wear_profile
        "fg_raw": {
            "frag_cards": {
                "Community Interest": [{"label": "Have", "count": "10"}],
                "Performance": [{"label": "Longevity", "count": "5"}],
                "Price Value": [{"label": "Good Value", "count": "3"}],
            }
        },
        "bn_raw": {},
        "derived_metrics": None,
    }

    try:
        db.ENABLED = True

        def fake_lookup_fragrance_record(canonical_fg_url, bn_url):
            return mock_frag_record_partial

        def fake_lookup_detail_cache(canonical_fg_url):
            return {
                "canonical_fg_url": canonical_fg_url,
                "name": "Test Fragrance",
                "house": "Test Brand",
                "quality_status": "complete",
                "frag_cards": mock_frag_record_partial["fg_raw"]["frag_cards"],
            }

        def fake_enqueue_job(*args, **kwargs):
            enqueue_calls.append((args, kwargs))
            return 1

        def fake_recover_or_enqueue_job(*args, **kwargs):
            recover_calls.append((args, kwargs))
            return {"status": "pending", "requested_count": 83}

        def fake_upsert_fragrance_details(*args, **kwargs):
            pass

        db.lookup_fragrance_record = fake_lookup_fragrance_record
        db.lookup_detail_cache = fake_lookup_detail_cache
        db.enqueue_job = fake_enqueue_job
        db.recover_or_enqueue_job = fake_recover_or_enqueue_job
        db.upsert_fragrance_details = fake_upsert_fragrance_details

        client = TestClient(api.app)

        res1 = client.post(
            "/api/fragrances/details",
            json={"id": "source:https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html"}
        )
        check("details response 200 on first call", res1.status_code == 200)
        data1 = res1.json()

        check("enrichment block is present", "enrichment" in data1)
        if "enrichment" in data1:
            check("incomplete stored detail reopens as pending", data1["enrichment"]["status"] == "pending")
            check("incomplete stored detail requires worker", data1["enrichment"]["requires_worker"] is True)
            check("incomplete stored detail surfaces requested count", data1["enrichment"]["requested_count"] == 83)

        check("source_coverage is present", "source_coverage" in data1)
        if "source_coverage" in data1:
            check(
                "fragrantica_metrics_complete is false (since metrics are incomplete)",
                data1["source_coverage"]["fragrantica_metrics_complete"] is False
            )

        check("no enqueue job calls were made", len(enqueue_calls) == 0, f"calls={enqueue_calls}")
        check("recovery job was called by default", len(recover_calls) == 1, f"calls={recover_calls}")

        res2 = client.post(
            "/api/fragrances/details",
            json={
                "id": "source:https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
                "recover_incomplete": True,
            },
        )
        check("details response 200 with recovery flag", res2.status_code == 200)
        data2 = res2.json()
        check("incomplete completed detail reopens as pending", data2["enrichment"]["status"] == "pending")
        check("reopened detail requires worker", data2["enrichment"]["requires_worker"] is True)
        check("reopened detail surfaces requested count", data2["enrichment"]["requested_count"] == 83)
        check("recovery flag remains compatible", len(recover_calls) == 2, f"calls={recover_calls}")

        parfumo_url = "https://www.parfumo.com/Perfumes/Creed/Bayrhum_Vetiver"
        mock_parfumo_record = {
            "record_key": f"fg:{parfumo_url}",
            "canonical_fg_url": parfumo_url,
            "bn_url": "https://basenotes.com/fragrances/bayrhum-vetiver-by-creed.26120009",
            "name": "Bayrhum Vetiver",
            "house": "Creed",
            "year": 2006,
            "gender": "Unisex",
            "image_url": None,
            "search": {},
            "fg_raw": {
                "frag_cards": {},
                "notes": {
                    "has_pyramid": True,
                    "top": ["Bay Leaf", "Pepper"],
                    "heart": ["Vetiver"],
                    "base": ["Vetiver"],
                    "flat": [],
                },
                "raw_identity": {"parfumo_url": parfumo_url},
                "source": "parfumo",
                "quality_status": "partial",
            },
            "bn_raw": {},
            "derived_metrics": {
                "notes": {
                    "has_pyramid": True,
                    "top": ["Bay Leaf", "Pepper"],
                    "heart": ["Vetiver"],
                    "base": ["Vetiver"],
                    "flat": [],
                },
                "source_coverage": {
                    "performance_score": False,
                    "value_score": False,
                    "community_interest_score": False,
                    "wear_profile": False,
                    "notes": True,
                },
            },
        }

        def fake_lookup_parfumo_record(canonical_fg_url, bn_url):
            return mock_parfumo_record

        db.lookup_fragrance_record = fake_lookup_parfumo_record
        db.lookup_detail_cache = lambda canonical_fg_url: None
        selected = api.engine.UnifiedFragrance(
            name="Bayrhum Vetiver",
            brand="Creed",
            year="",
            bn_url="https://basenotes.com/fragrances/bayrhum-vetiver-by-creed.26120009",
            frag_url=parfumo_url,
        )
        res3 = client.post("/api/fragrances/details", json={"id": api._encode_id(selected)})
        check("parfumo stored detail response 200", res3.status_code == 200, str(res3.status_code))
        data3 = res3.json()
        check("parfumo fallback is surfaced as partial", data3["enrichment"]["status"] == "partial", str(data3.get("enrichment")))
        check("parfumo fallback is not marked worker-complete", data3["source_coverage"]["complete"] is False, str(data3.get("source_coverage")))
        check("parfumo fallback keeps parsed top notes", data3["raw"]["notes"]["top"] == ["Bay Leaf", "Pepper"], str(data3["raw"]["notes"]))

    finally:
        db.ENABLED = saved_enabled
        db.lookup_fragrance_record = saved_lookup_rec
        db.lookup_detail_cache = saved_lookup_cache
        db.enqueue_job = saved_enqueue
        db.recover_or_enqueue_job = saved_recover
        db.upsert_fragrance_details = saved_upsert


def test_completeness_self_heal_sweep() -> None:
    print("Completeness self-heal sweep checks (no DATABASE_URL required):")
    import enrichment_facts

    def make_complete_record() -> dict[str, object]:
        return {
            "record_key": "fg:https://www.fragrantica.com/perfume/Test-House/Complete-1.html",
            "canonical_fg_url": "https://www.fragrantica.com/perfume/Test-House/Complete-1.html",
            "bn_url": None,
            "name": "Complete",
            "house": "Test House",
            "year": 2024,
            "gender": "Unisex",
            "image_url": None,
            "search": {},
            "fg_raw": {
                "frag_cards": {"Community Interest": [{"label": "Have", "count": "9"}]},
                "notes": {"top": ["bergamot"], "heart": ["rose"], "base": ["musk"], "flat": []},
                "reviews": [{"text": "lovely", "source": "fragrantica"}],
                "raw_identity": {"concentration": "Eau de Parfum"},
            },
            "bn_raw": {},
            "derived_metrics": {
                "main_accords": {"top_accords": ["woody", "citrus"]},
                "wear_profile": {"primary_seasons": ["Spring"], "primary_time": "Day"},
                "source_coverage": {
                    "performance_score": True,
                    "value_score": True,
                    "community_interest_score": True,
                    "wear_profile": True,
                },
            },
        }

    # 1. Granular fact audit: a fully-populated record has nothing missing.
    check(
        "complete record reports no missing facts",
        enrichment_facts.missing_facts(make_complete_record()) == [],
        str(enrichment_facts.missing_facts(make_complete_record())),
    )

    # 2. Each removed fact is reported by name -- granular down to reviews.
    record = make_complete_record()
    record["gender"] = "Unisex / Unspecified"
    record["fg_raw"]["raw_identity"].pop("concentration")
    record["fg_raw"]["reviews"] = []
    record["derived_metrics"]["source_coverage"]["performance_score"] = False
    missing = enrichment_facts.missing_facts(record)
    check(
        "missing facts are granular per field",
        missing == ["gender", "concentration", "performance_score", "reviews"],
        str(missing),
    )
    bare = make_complete_record()
    bare["derived_metrics"] = None
    bare_missing = enrichment_facts.missing_facts(bare)
    check(
        "absent derived_metrics flags family/accords/wear/scores",
        {"family", "main_accords", "wear_profile", "performance_score",
         "value_score", "community_interest_score"} <= set(bare_missing),
        str(bare_missing),
    )

    # 3. Endpoint contract: token-gated, then granular audit + heal sweep.
    saved_token = api._ENRICHMENT_WORKER_TOKEN
    saved_enabled = db.ENABLED
    saved_list = db.list_fragrance_records
    saved_count = db.count_fragrance_records
    saved_jobs_by_keys = db.get_jobs_by_keys
    saved_recover = db.recover_or_enqueue_job
    recover_calls: list[dict[str, object]] = []
    client = TestClient(api.app)
    auth = {"Authorization": "Bearer test-secret-token"}
    try:
        api._ENRICHMENT_WORKER_TOKEN = "test-secret-token"
        r = client.get("/api/enrichment/completeness")
        check("completeness endpoint requires bearer token", r.status_code == 401, str(r.status_code))
        r = client.post("/api/enrichment/heal", json={})
        check("heal endpoint requires bearer token", r.status_code == 401, str(r.status_code))

        db.ENABLED = True
        incomplete_record = make_complete_record()
        incomplete_record["fg_raw"]["raw_identity"].pop("concentration")
        incomplete_record["fg_raw"]["reviews"] = []
        ignored_record = make_complete_record()
        ignored_record["record_key"] = "fg:https://www.fragrantica.com/perfume/Test-House/Ignored-2.html"
        ignored_record["canonical_fg_url"] = "https://www.fragrantica.com/perfume/Test-House/Ignored-2.html"
        ignored_record["gender"] = ""
        db.list_fragrance_records = lambda limit=200, offset=0: [
            make_complete_record(), incomplete_record, ignored_record,
        ]
        db.count_fragrance_records = lambda: 3
        db.get_jobs_by_keys = lambda keys: {
            ignored_record["canonical_fg_url"]: {
                "status": "ignored", "priority": 0, "requested_count": 4,
                "failure_count": 0, "last_error": None, "last_requested_at": None,
            }
        }

        def fake_recover(**kwargs):
            recover_calls.append(kwargs)
            return {"id": "job-1", "status": "pending", "priority": kwargs.get("priority"),
                    "requested_count": 2}

        db.recover_or_enqueue_job = fake_recover

        r = client.get("/api/enrichment/completeness", headers=auth)
        check("completeness audit responds 200", r.status_code == 200, str(r.status_code))
        body = r.json()
        check("audit counts incomplete fragrances", body.get("incomplete") == 2, str(body))
        check(
            "audit reports granular missing facts per fragrance",
            any(item["missing"] == ["concentration", "reviews"] for item in body.get("items", [])),
            str(body.get("items")),
        )
        check(
            "audit annotates existing durable job state",
            any((item.get("job") or {}).get("status") == "ignored" for item in body.get("items", [])),
            str(body.get("items")),
        )

        r = client.post("/api/enrichment/heal", json={"dry_run": True}, headers=auth)
        body = r.json()
        check("dry-run heal queues nothing durable", len(recover_calls) == 0 and body.get("queued") == 1, str(body))
        check("dry-run heal skips ignored jobs", body.get("skipped") == 1, str(body.get("skipped_items")))

        r = client.post("/api/enrichment/heal", json={"priority": 7}, headers=auth)
        body = r.json()
        check("heal sweep requeues each incomplete fragrance once", len(recover_calls) == 1, str(recover_calls))
        check(
            "heal sweep passes identity and priority to the durable queue",
            recover_calls
            and recover_calls[0].get("job_key") == incomplete_record["canonical_fg_url"]
            and recover_calls[0].get("priority") == 7,
            str(recover_calls),
        )
        check("heal sweep reports queued job state", body.get("queued") == 1 and body["queued_items"][0]["job"]["status"] == "pending", str(body))

        recover_calls.clear()
        r = client.post("/api/enrichment/heal", json={"fields": ["year"]}, headers=auth)
        body = r.json()
        check("field-filtered heal skips rows missing other facts", body.get("queued") == 0 and len(recover_calls) == 0, str(body))

        r = client.post("/api/enrichment/heal", json={"fields": ["not_a_fact"]}, headers=auth)
        check("heal rejects unknown fact fields", r.status_code == 400, str(r.status_code))

        db.get_jobs_by_keys = lambda keys: {
            incomplete_record["canonical_fg_url"]: {
                "status": "completed", "priority": 0, "requested_count": 9,
                "failure_count": 0, "last_error": None, "last_requested_at": None,
            }
        }
        recover_calls.clear()
        r = client.post("/api/enrichment/heal", json={"max_requested_count": 5}, headers=auth)
        body = r.json()
        check(
            "heal honors max_requested_count churn protection",
            all(c.get("job_key") != incomplete_record["canonical_fg_url"] for c in recover_calls)
            and any(s["reason"] == "max_requested_count_reached" for s in body.get("skipped_items", [])),
            str(body.get("skipped_items")),
        )
    finally:
        api._ENRICHMENT_WORKER_TOKEN = saved_token
        db.ENABLED = saved_enabled
        db.list_fragrance_records = saved_list
        db.count_fragrance_records = saved_count
        db.get_jobs_by_keys = saved_jobs_by_keys
        db.recover_or_enqueue_job = saved_recover

    # 4. The worker's idle tick drives the sweep: throttled, paged, wrapping.
    class FakeHealClient:
        def __init__(self, audited: int) -> None:
            self.audited = audited
            self.calls: list[dict[str, object]] = []

        def heal_sweep(self, **kwargs: object) -> dict[str, object]:
            self.calls.append(kwargs)
            return {"audited": self.audited, "incomplete": 5, "queued": 3}

    full_page = FakeHealClient(enrichment_worker.HEAL_SWEEP_PAGE_LIMIT)
    auto_state: dict[str, object] = {}
    enrichment_worker._maybe_run_heal_sweep(full_page, auto_state)
    check("idle worker tick triggers a heal sweep", len(full_page.calls) == 1, str(full_page.calls))
    check(
        "heal sweep advances the audit page",
        auto_state.get("heal_offset") == enrichment_worker.HEAL_SWEEP_PAGE_LIMIT,
        str(auto_state),
    )
    enrichment_worker._maybe_run_heal_sweep(full_page, auto_state)
    check("heal sweep is throttled between idle ticks", len(full_page.calls) == 1, str(full_page.calls))

    short_page = FakeHealClient(3)
    auto_state = {"heal_offset": 400}
    enrichment_worker._maybe_run_heal_sweep(short_page, auto_state)
    check(
        "final audit page wraps the sweep back to the start",
        len(short_page.calls) == 1
        and short_page.calls[0].get("offset") == 400
        and auto_state.get("heal_offset") == 0,
        f"{short_page.calls} {auto_state}",
    )


def test_worker_fact_summary_and_serp_retry() -> None:
    print("Per-fragrance fact summary + bounded SERP retry checks (no DATABASE_URL required):")

    # 1. The worker reports filled/missing facts with the same contract the
    #    heal sweep audits stored records with.
    payload = {
        "name": "Layton",
        "house": "Parfums de Marly",
        "year": "2016",
        "gender": "for men and women",
        "frag_cards": {"longevity": {"long lasting": 10}},
        "notes": {"top": ["apple"], "heart": [], "base": ["vanilla"], "flat": []},
        "reviews": [{"text": "great"}],
        "raw_identity": {"concentration": "Eau de Parfum"},
        "concentration": "Eau de Parfum",
    }
    dm = {
        "source_coverage": {
            "performance_score": True,
            "value_score": True,
            "community_interest_score": True,
            "wear_profile": True,
        },
        "main_accords": {"top_accords": ["vanilla", "woody"]},
        "wear_profile": {"primary_seasons": ["Winter"], "primary_time": "Night"},
    }
    filled, missing = enrichment_worker._payload_fact_summary(payload, dm)
    check("full payload reports every fact filled", not missing, str(missing))
    check(
        "full payload fills the whole FACT_FIELDS contract",
        set(filled) == set(enrichment_worker.FACT_FIELDS),
        str(filled),
    )

    sparse, sparse_missing = enrichment_worker._payload_fact_summary(
        {"name": "X", "notes": {"top": [], "heart": [], "base": [], "flat": []}}, None
    )
    check(
        "sparse payload reports concrete missing facts",
        {"house", "concentration", "performance_score", "notes", "reviews"} <= set(sparse_missing),
        str(sparse_missing),
    )
    check("quality gate rejects absent derived metrics", enrichment_worker._dm_metrics_complete(None) is False)
    check("quality gate accepts full coverage", enrichment_worker._dm_metrics_complete(dm) is True)

    # 2. A DDG SERP that loaded fine with zero results is a real answer:
    #    _retry_fetch must not burn retries (and sleeps) on it. Only a fetch
    #    failure (None) is retryable.
    import concentration_grabber as cg

    calls = {"n": 0}
    orig = cg.SemanticScentEngine._fetch_serp
    try:
        def loaded_but_empty(page, q, sf, timeout=None):
            calls["n"] += 1
            return []

        cg.SemanticScentEngine._fetch_serp = staticmethod(loaded_but_empty)
        rows = cg.SemanticScentEngine._retry_fetch(None, "q", "", attempts=3)
        check("empty SERP is returned without retrying", rows == [] and calls["n"] == 1, str(calls))

        calls["n"] = 0

        def fetch_failed(page, q, sf, timeout=None):
            calls["n"] += 1
            return None

        cg.SemanticScentEngine._fetch_serp = staticmethod(fetch_failed)
        rows = cg.SemanticScentEngine._retry_fetch(None, "q", "", attempts=2)
        check("fetch failures retry up to the attempt cap", rows == [] and calls["n"] == 2, str(calls))
    finally:
        cg.SemanticScentEngine._fetch_serp = orig

    class _FakeWait:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def load_start(self, **kwargs) -> None:
            self.calls.append(kwargs)
            return None

    class _EmptyPage:
        def __init__(self, ok: bool) -> None:
            self.ok = ok
            self.wait = _FakeWait()
            self.get_calls: list[dict[str, object]] = []
            self.eles_calls: list[dict[str, object]] = []

        def get(self, url, **kwargs):
            self.get_calls.append({"url": url, **kwargs})
            return self.ok

        def eles(self, selector, **kwargs):
            self.eles_calls.append({"selector": selector, **kwargs})
            return []

    page = _EmptyPage(ok=True)
    rows = cg.SemanticScentEngine._fetch_serp(page, "nonexistent scent", "", timeout=3)
    check("loaded empty page is a non-retryable empty result", rows == [], str(rows))
    check(
        "DDG navigation disables DrissionPage's hidden retries",
        page.get_calls and page.get_calls[0].get("retry") == 0 and page.get_calls[0].get("timeout") == 3,
        str(page.get_calls),
    )
    check(
        "empty SERP does not wait on all result selectors",
        len(page.eles_calls) == 1 and page.eles_calls[0].get("timeout") <= 3,
        str(page.eles_calls),
    )
    check(
        "load-start wait is bounded for DDG pages",
        page.wait.calls and page.wait.calls[0].get("timeout") <= 3,
        str(page.wait.calls),
    )

    failed_page = _EmptyPage(ok=False)
    rows = cg.SemanticScentEngine._retry_fetch(failed_page, "q", "", attempts=2)
    check(
        "page.get(False) is treated as retryable fetch failure",
        rows == [] and len(failed_page.get_calls) == 2,
        str(failed_page.get_calls),
    )


def test_worker_query_normalization_and_retry_grace() -> None:
    """Resolver-input hygiene: alias houses, strip concentration suffixes,
    dedupe duplicated tokens, promote URL-bearing queries, and keep the first
    resolver misses retryable. Pure functions — no network, no DB."""
    print("\nWorker query normalization + retry grace:")
    w = enrichment_worker

    check(
        "concentration suffix stripped from query",
        w._normalize_query_text("Hermes 24 Faubourg Eau De Parfum") == "Hermes 24 Faubourg",
    )
    check(
        "EDT abbreviation stripped",
        w._normalize_query_text("Tom Ford Black Orchid EDT") == "Tom Ford Black Orchid",
    )
    check(
        "bare concentration query is preserved, not emptied",
        w._normalize_query_text("Eau de Cologne") == "Eau de Cologne",
    )
    check(
        "duplicated tokens collapsed",
        w._normalize_query_text("Classique Eau De Toilette Jean Paul Gaultier Classique")
        == "Classique Jean Paul Gaultier",
    )
    check(
        "house alias rewrites inside a query",
        w._normalize_query_text("Lataffa Liam Grey") == "Lattafa Liam Grey",
    )
    check(
        "house alias map canonicalizes Initio",
        w._canonical_house("Initio") == "Initio Parfums Privés",
    )
    check(
        "longer alias key wins; no token duplication for sub-brand spellings",
        w._apply_house_aliases("Initio Parfums Oud for Greatness")
        == "Initio Parfums Privés Oud for Greatness",
        detail=repr(w._apply_house_aliases("Initio Parfums Oud for Greatness")),
    )
    check(
        "alias rewrite is idempotent on an already-canonical query",
        w._apply_house_aliases("Initio Parfums Privés Oud for Greatness")
        == "Initio Parfums Privés Oud for Greatness",
        detail=repr(w._apply_house_aliases("Initio Parfums Privés Oud for Greatness")),
    )
    check(
        "house alias map canonicalizes Penhaligons",
        w._canonical_house("Penhaligons") == "Penhaligon's",
    )
    check(
        "unknown house passes through untouched",
        w._canonical_house("Chanel") == "Chanel",
    )

    variants = w._query_variants({"query": "Black Orchid Eau De Parfum", "house": "Tom Ford"})
    check(
        "query variants: normalized first, raw fallback second",
        variants == ["Black Orchid", "Black Orchid Eau De Parfum"],
        detail=repr(variants),
    )

    check(
        "garbled name repaired via NAME_ALIASES",
        w._canonical_name("Le Jardin Retrouve", "Verveine Dete") == "Verveine d'Été",
    )
    check(
        "unknown name passes through untouched",
        w._canonical_name("Chanel", "Allure Homme") == "Allure Homme",
    )
    repaired_variants = w._query_variants({"house": "Le Jardin Retrouve", "name": "Verveine Dete"})
    check(
        "repaired identity is the first search variant",
        repaired_variants
        and repaired_variants[0] == "Le Jardin Retrouvé Verveine d'Été"
        and "Le Jardin Retrouve Verveine Dete" in repaired_variants,
        detail=repr(repaired_variants),
    )

    fg = "https://www.fragrantica.com/perfume/Chanel/Allure-Homme-Edition-Blanche-Eau-de-Parfum-15660.html"
    promoted = w._fg_url_from_job_query({"query": fg})
    check(
        "URL-bearing query promoted to fg_url",
        promoted.startswith("https://www.fragrantica.com/perfume/"),
        detail=repr(promoted),
    )
    check(
        "non-perfume URL query is not promoted",
        w._fg_url_from_job_query({"query": "https://www.fragrantica.com/designers/Chanel.html"}) == "",
    )
    check(
        "plain-text query is not promoted",
        w._fg_url_from_job_query({"query": "Chanel Allure Homme"}) == "",
    )

    check(
        "resolver miss retryable on first attempt",
        w._resolver_miss_retryable({"failure_count": 0}) is True,
    )
    check(
        "resolver miss retryable within grace window",
        w._resolver_miss_retryable({"failure_count": w.RESOLVER_RETRY_GRACE - 1}) is True,
    )
    check(
        "resolver miss terminal once grace exhausted",
        w._resolver_miss_retryable({"failure_count": w.RESOLVER_RETRY_GRACE}) is False,
    )


def test_worker_url_gating_catalog_tier_and_retirement() -> None:
    """Bad-URL hygiene, args isolation, the deterministic designer-catalog
    tier, and no-Fragrantica retirement. No network, no DB."""
    print("\nWorker URL gating + catalog tier + retirement:")
    w = enrichment_worker
    engine = w.engine

    perfume = "https://www.fragrantica.com/perfume/Chanel/Allure-Homme-15660.html"
    designer = "https://www.fragrantica.com/designers/Chanel.html"
    check(
        "perfume URL canonicalized",
        w._canonical_fg_url(perfume).startswith("https://www.fragrantica.com/perfume/"),
    )
    check("designer URL rejected as fg_url", w._canonical_fg_url(designer) == "")
    check("empty URL rejected", w._canonical_fg_url("") == "")
    check(
        "job with designer fg_url counts as missing",
        w._job_missing_fg_url({"fg_url": designer}) is True,
    )
    check(
        "job with perfume fg_url counts as present",
        w._job_missing_fg_url({"fg_url": perfume}) is False,
    )

    results = [
        engine.UnifiedFragrance(name="bad", brand="Chanel", year="", frag_url=designer),
        engine.UnifiedFragrance(name="good", brand="Chanel", year="", frag_url=perfume),
    ]
    linked = w._first_linked_result(results)
    check(
        "first linked result skips non-perfume URLs",
        linked is not None and linked.frag_url == perfume,
        detail=repr(getattr(linked, "frag_url", None)),
    )

    # resolve_candidate must ignore a stored non-perfume fg_url (resolve by
    # query instead) and must not mutate the shared args namespace.
    search_calls: list[tuple[str, str]] = []

    def fake_search(scraper, query, args, *, debug=False):
        search_calls.append((query, getattr(args, "brand", "")))
        return [engine.UnifiedFragrance(name="Allure Homme", brand="Chanel", year="", frag_url=perfume)]

    shared_args = w._build_engine_args()
    shared_brand_before = getattr(shared_args, "brand", "<unset>")
    old_search = w._search_candidates
    w._search_candidates = fake_search
    try:
        candidate = w.resolve_candidate(
            None, shared_args, {"fg_url": designer, "house": "Chanel", "name": "Allure Homme"}
        )
    finally:
        w._search_candidates = old_search
    check(
        "stored designer fg_url is ignored; job resolved by query",
        candidate.frag_url == perfume and search_calls,
        detail=repr(search_calls),
    )
    check(
        "search ran with the job's brand on a per-job args copy",
        search_calls and search_calls[0][1] == "Chanel",
        detail=repr(search_calls),
    )
    check(
        "shared args namespace is not mutated across jobs",
        getattr(shared_args, "brand", "<unset>") == shared_brand_before,
        detail=repr(getattr(shared_args, "brand", "<unset>")),
    )

    # Deterministic designer-catalog tier: the house catalog is enumerated and
    # identity-scored, so sibling flankers resolve to the right URL.
    twist_urls = {
        "Basil": "https://www.fragrantica.com/perfume/Clive-Christian/1872-Twist-Basil-90001.html",
        "Geranium": "https://www.fragrantica.com/perfume/Clive-Christian/1872-Twist-Geranium-90002.html",
        "Vetiver": "https://www.fragrantica.com/perfume/Clive-Christian/1872-Twist-Vetiver-90003.html",
    }
    catalog = [
        engine.CatalogItem(name=f"1872 Twist {flavor}", brand="Clive Christian", year="", url=url)
        for flavor, url in twist_urls.items()
    ]

    def fake_catalog(scraper, brand, deadline, slug_limit=6, page_timeout=3.5, trace=None):
        return catalog

    old_catalog = engine.SearchSniper.catalog_candidates_for_brand
    engine.SearchSniper.catalog_candidates_for_brand = staticmethod(fake_catalog)
    try:
        hit = w._designer_catalog_candidate(
            None, {"house": "Clive Christian", "name": "1872 Twist Geranium"}
        )
        miss = w._designer_catalog_candidate(
            None, {"house": "Clive Christian", "name": "Totally Unrelated Nonexistent Thing"}
        )
        no_identity = w._designer_catalog_candidate(None, {"house": "", "name": "1872 Twist Geranium"})
    finally:
        engine.SearchSniper.catalog_candidates_for_brand = old_catalog
    check(
        "catalog tier resolves the exact sibling flanker",
        hit is not None and hit.frag_url == twist_urls["Geranium"],
        detail=repr(getattr(hit, "frag_url", None)),
    )
    check("catalog tier rejects below-accept identities", miss is None, detail=repr(miss))
    check("catalog tier needs both house and name", no_identity is None)

    # Duplicate rows of the same perfume (same URL listed twice across designer
    # label pages) must collapse before the sibling-margin check — a duplicate
    # of the winner is not a sibling near-tie.
    dup_catalog = [
        engine.CatalogItem(
            name="1872 Twist Geranium", brand="Clive Christian", year="", url=twist_urls["Geranium"]
        ),
        engine.CatalogItem(
            name="1872 Twist Geranium", brand="Clive Christian", year="", url=twist_urls["Geranium"]
        ),
    ]

    def fake_dup_catalog(scraper, brand, deadline, slug_limit=6, page_timeout=3.5, trace=None):
        return dup_catalog

    engine.SearchSniper.catalog_candidates_for_brand = staticmethod(fake_dup_catalog)
    try:
        dup_hit = w._designer_catalog_candidate(
            None, {"house": "Clive Christian", "name": "1872 Twist Geranium"}
        )
    finally:
        engine.SearchSniper.catalog_candidates_for_brand = old_catalog
    check(
        "duplicate catalog rows do not fake a sibling collision",
        dup_hit is not None and dup_hit.frag_url == twist_urls["Geranium"],
        detail=repr(getattr(dup_hit, "frag_url", None)),
    )

    check(
        "B&BW retires on terminal resolver miss",
        w._matches_no_fragrantica_line({"house": "Bath And Body Works", "name": "Brightest Bloom"}) is True,
    )
    check(
        "VS Pink line retires",
        w._matches_no_fragrantica_line({"house": "Victorias Secret", "name": "Pink Hot Petals"}) is True,
    )
    check(
        "VS real perfumes do not retire",
        w._matches_no_fragrantica_line({"house": "Victorias Secret", "name": "Bombshell"}) is False,
    )
    check(
        "CK body mists retire",
        w._matches_no_fragrantica_line({"house": "Calvin Klein", "name": "Silky Coconut Body Mist"}) is True,
    )
    check(
        "CK real perfumes do not retire",
        w._matches_no_fragrantica_line({"house": "Calvin Klein", "name": "Eternity"}) is False,
    )
    check(
        "unrelated houses never retire",
        w._matches_no_fragrantica_line({"house": "Chanel", "name": "Pink"}) is False,
    )

    # process_job: a terminal resolver miss on a no-Fragrantica line calls
    # ignore_job, not fail_job.
    class _StubClient:
        def __init__(self):
            self.ignored: list[tuple[str, str]] = []
            self.failed: list[tuple[str, str, bool]] = []
            self.requeued: list[tuple[str, int]] = []

        def claim_job(self, job_id):
            return {"claimed": True}

        def ignore_job(self, job_id, note=None):
            self.ignored.append((job_id, note or ""))
            return {"ignored": True}

        def fail_job(self, job_id, error, retryable):
            self.failed.append((job_id, error, retryable))
            return {"failed": True}

        def requeue_job(self, job_id, *, priority=10):
            self.requeued.append((job_id, priority))
            return {"queued": True}

    def raise_terminal_miss(client, scraper, args, job, *, debug=False):
        raise w.WorkerError("fg_url_missing_after_resolution", "resolver returned no Fragrantica URL", retryable=False)

    config = w.WorkerConfig(
        api_base_url="http://localhost", token="t", delay=0, jitter=0,
        limit=1, detail_timeout=1.0, debug=False, dry_run=False,
    )
    stub = _StubClient()
    old_resolve = w.resolve_candidate_for_job
    w.resolve_candidate_for_job = raise_terminal_miss
    try:
        bbw_job = {"id": "job-1", "house": "Bath And Body Works", "name": "Brightest Bloom"}
        w.process_job(stub, None, None, bbw_job, index_label="[t]", config=config)
        chanel_job = {"id": "job-2", "house": "Chanel", "name": "Some Real Perfume"}
        w.process_job(stub, None, None, chanel_job, index_label="[t]", config=config)
    finally:
        w.resolve_candidate_for_job = old_resolve
    check(
        "terminal miss on no-FG line is retired via ignore_job",
        stub.ignored and stub.ignored[0][0] == "job-1" and all(jid != "job-1" for jid, *_ in stub.failed),
        detail=f"ignored={stub.ignored} failed={stub.failed}",
    )
    check(
        "terminal miss on a real house still goes to fail_job",
        any(jid == "job-2" and not retryable for jid, _err, retryable in stub.failed)
        and all(jid != "job-2" for jid, _ in stub.ignored),
        detail=f"ignored={stub.ignored} failed={stub.failed}",
    )

    def raise_blocked_miss(client, scraper, args, job, *, debug=False):
        raise w.WorkerError("fg_resolver_blocked", "designer catalog unreachable", retryable=True)

    stub = _StubClient()
    w.resolve_candidate_for_job = raise_blocked_miss
    try:
        w.process_job(
            stub,
            None,
            None,
            {"id": "job-3", "house": "Pull Bear", "name": "Frosted Pink", "priority": 7},
            index_label="[t]",
            config=config,
        )
    finally:
        w.resolve_candidate_for_job = old_resolve
    check(
        "blocked resolver miss requeues without fail_job",
        stub.requeued == [("job-3", 10)] and not stub.failed and not stub.ignored,
        detail=f"requeued={stub.requeued} failed={stub.failed} ignored={stub.ignored}",
    )

    # "Retry failed jobs": requeueing a terminal failure resets its
    # failure_count to 0, so the retry batch must also match those rows by id —
    # otherwise the action requeues them and then skips them in the same run.
    pending = [
        {"id": "fresh", "failure_count": 0},
        {"id": "retryable-history", "failure_count": 2},
        {"id": "just-requeued", "failure_count": 0},
    ]
    batch_ids = [j["id"] for j in w._retry_batch(pending, {"just-requeued"})]
    check(
        "retry batch keeps failure-history rows and just-requeued rows, skips fresh",
        batch_ids == ["retryable-history", "just-requeued"],
        detail=repr(batch_ids),
    )

    class _RequeueClient:
        def __init__(self):
            self.requeued: list[tuple[str, int]] = []

        def list_jobs(self, status, limit=10, offset=0):
            assert status == "failed"
            return [{"id": "dead-1", "priority": 0}, {"id": "dead-2", "priority": 12}]

        def requeue_job(self, job_id, *, priority=10):
            self.requeued.append((job_id, priority))
            return {"queued": True}

    rq_client = _RequeueClient()
    requeued_ids = w.requeue_terminal_failed_jobs(rq_client, config, limit=5)
    check(
        "requeue_terminal_failed_jobs returns the requeued ids",
        requeued_ids == ["dead-1", "dead-2"]
        and rq_client.requeued == [("dead-1", 10), ("dead-2", 13)],
        detail=f"ids={requeued_ids} calls={rq_client.requeued}",
    )

    class _DiagClient:
        def list_jobs(self, status, limit=10, offset=0):
            rows = {
                "failed": [
                    {
                        "id": "failed-bbw",
                        "status": "failed",
                        "house": "Bath & Body Works",
                        "name": "Pink Body Mist",
                        "failure_count": 3,
                        "last_error": "resolver returned no Fragrantica URL",
                        "fg_url": None,
                    }
                ],
                "pending": [
                    {
                        "id": "pending-blocked",
                        "status": "pending",
                        "house": "Chanel",
                        "name": "No 5",
                        "failure_count": 1,
                        "last_error": "fg_resolver_blocked",
                    },
                    {"id": "pending-clean", "status": "pending", "failure_count": 0},
                ],
                "ignored": [{"id": "ignored-1", "status": "ignored", "last_error": ""}],
            }
            return rows.get(status, [])

    diag = w._queue_diagnostic_rows(_DiagClient())
    check(
        "queue diagnostics separates pending retry history from terminal failed rows",
        diag["counts"]["failed"] == 1
        and diag["counts"]["pending_with_failures"] == 1
        and diag["buckets"]["pending_with_failures"] == {"fg_resolver_blocked": 1},
        detail=repr(diag.get("counts")),
    )
    check(
        "queue diagnostics only marks conservative no-FG rows as ignored candidates",
        diag["counts"]["ignored_candidates"] == 1
        and diag["ignored_candidates"][0]["id"] == "failed-bbw",
        detail=repr(diag.get("ignored_candidates")),
    )
    with tempfile.TemporaryDirectory() as tmp:
        json_path, csv_path = w.export_queue_diagnostics(_DiagClient(), tmp)
        check("queue diagnostics writes JSON", json_path.exists(), str(json_path))
        csv_text = csv_path.read_text(encoding="utf-8")
        check(
            "queue diagnostics writes CSV buckets",
            "pending_with_failures" in csv_text and "fg_resolver_blocked" in csv_text,
            detail=csv_text,
        )


def test_worker_resolver_hardening() -> None:
    """New resolver hardening: garbled-name repair variants, recombined
    house/line identities, year-confirmed subset acceptance, blocked-catalog
    retryability, and the job-pinned Parfumo URL path. No network, no DB."""
    print("\nWorker resolver hardening:")
    w = enrichment_worker
    engine = w.engine

    # --- apostrophe garble repair -------------------------------------------------
    check(
        "apostrophe repair fixes d'/l' elisions",
        w._apostrophe_repaired_name("Parfum Dhabit") == "Parfum d'Habit"
        and w._apostrophe_repaired_name("Ombre Rose Loriginal") == "Ombre Rose l'Original"
        and w._apostrophe_repaired_name("Terre Dhermes Eau Intense Vetiver").startswith("Terre d'Hermes"),
        detail=repr(w._apostrophe_repaired_name("Parfum Dhabit")),
    )
    check(
        "apostrophe repair leaves clean names alone",
        w._apostrophe_repaired_name("Sauvage Elixir") == ""
        and w._apostrophe_repaired_name("") == "",
    )
    variants = w._query_variants(
        {"house": "Maitre Parfumeur Et Gantier", "name": "Parfum Dhabit"}
    )
    check(
        "query variants include the apostrophe-repaired form",
        any("d'Habit" in v for v in variants),
        detail=repr(variants),
    )

    # --- recombined house/line identities ------------------------------------------
    recombined = w._recombined_identities(
        {"house": "The Fireplace By Martin Margiela", "name": "Replica"}
    )
    check(
        "swapped house/line recombines into brand + line name",
        recombined == [("Martin Margiela", "Replica The Fireplace")],
        detail=repr(recombined),
    )
    check(
        "houses that merely start with By do not recombine",
        w._recombined_identities({"house": "By Kilian", "name": "Black Phantom"}) == [],
    )
    margiela_variants = w._query_variants(
        {"house": "The Fireplace By Martin Margiela", "name": "Replica"}
    )
    check(
        "query variants include the recombined identity",
        any("Martin Margiela Replica The Fireplace" in v for v in margiela_variants),
        detail=repr(margiela_variants),
    )

    # --- The Body Shop house alias --------------------------------------------------
    check(
        "Body Shop aliases to The Body Shop",
        w._canonical_house("Body Shop") == "The Body Shop",
    )

    # --- sub_brand_label no longer emits dropped stopwords --------------------------
    check(
        "sub_brand_label skips stopword-led names",
        engine.IdentityTools.sub_brand_label("Le Beau Paradise Garden", "Jean Paul Gaultier") == "",
        detail=repr(engine.IdentityTools.sub_brand_label("Le Beau Paradise Garden", "Jean Paul Gaultier")),
    )
    check(
        "sub_brand_label still finds real sub-lines",
        engine.IdentityTools.sub_brand_label("Casamorati Mefisto", "Bulgari") == "Casamorati",
        detail=repr(engine.IdentityTools.sub_brand_label("Casamorati Mefisto", "Bulgari")),
    )

    # --- unaccented designer slugs ---------------------------------------------------
    slugs = engine.SearchSniper.designer_slug_candidates("Le Jardin Retrouvé", limit=8)
    check(
        "designer slugs include the unaccented form",
        any(s == "Le-Jardin-Retrouve" for s in slugs),
        detail=repr(slugs),
    )

    # --- year-confirmed subset acceptance in the catalog tier ------------------------
    vetiver_url = "https://www.fragrantica.com/perfume/Clive-Christian/1872-Vetiver-39179.html"
    subset_catalog = [
        engine.CatalogItem(name="1872 Vetiver", brand="Clive Christian", year="2016", url=vetiver_url),
        engine.CatalogItem(name="1872 Mandarin", brand="Clive Christian", year="2016",
                           url="https://www.fragrantica.com/perfume/Clive-Christian/1872-Mandarin-39178.html"),
    ]

    def fake_subset_catalog(scraper, brand, deadline, slug_limit=6, page_timeout=3.5, trace=None):
        return subset_catalog

    old_catalog = engine.SearchSniper.catalog_candidates_for_brand
    engine.SearchSniper.catalog_candidates_for_brand = staticmethod(fake_subset_catalog)
    try:
        hit = w._designer_catalog_candidate(
            None, {"house": "Clive Christian", "name": "1872 Twist Vetiver", "year": 2016}
        )
        no_year = w._designer_catalog_candidate(
            None, {"house": "Clive Christian", "name": "1872 Twist Vetiver"}
        )
    finally:
        engine.SearchSniper.catalog_candidates_for_brand = old_catalog
    check(
        "unique subset match with exact year is accepted",
        hit is not None and hit.frag_url == vetiver_url
        and hit.resolver_source == "worker_designer_catalog_subset",
        detail=repr(getattr(hit, "frag_url", None)),
    )
    check(
        "subset match without year confirmation is rejected",
        no_year is None,
        detail=repr(no_year),
    )

    # The flanker shape must never subset-accept: catalog has extra non-stopword
    # tokens ("Sauvage" probe vs "Sauvage Elixir" row).
    flanker_catalog = [
        engine.CatalogItem(name="Sauvage Elixir", brand="Dior", year="2021",
                           url="https://www.fragrantica.com/perfume/Dior/Sauvage-Elixir-67110.html"),
    ]
    engine.SearchSniper.catalog_candidates_for_brand = staticmethod(
        lambda scraper, brand, deadline, slug_limit=6, page_timeout=3.5, trace=None: flanker_catalog
    )
    try:
        flanker = w._designer_catalog_candidate(
            None, {"house": "Dior", "name": "Sauvage", "year": 2021}
        )
    finally:
        engine.SearchSniper.catalog_candidates_for_brand = old_catalog
    check("base name never subset-accepts a flanker row", flanker is None, detail=repr(flanker))

    # Stopword-only difference is accepted even without years ("Lys" vs "The Lys").
    lys_url = "https://www.fragrantica.com/perfume/Le-Jardin-Retrouve/The-Lys-7256.html"
    lys_catalog = [
        engine.CatalogItem(name="The Lys", brand="Le Jardin Retrouve", year="", url=lys_url),
        engine.CatalogItem(name="Citron Boboli", brand="Le Jardin Retrouve", year="1977",
                           url="https://www.fragrantica.com/perfume/Le-Jardin-Retrouve/Citron-Boboli-7261.html"),
    ]
    engine.SearchSniper.catalog_candidates_for_brand = staticmethod(
        lambda scraper, brand, deadline, slug_limit=6, page_timeout=3.5, trace=None: lys_catalog
    )
    try:
        lys = w._designer_catalog_candidate(
            None, {"house": "Le Jardin Retrouve", "name": "Lys", "year": 1989}
        )
    finally:
        engine.SearchSniper.catalog_candidates_for_brand = old_catalog
    check(
        "stopword-only name difference is accepted",
        lys is not None and lys.frag_url == lys_url,
        detail=repr(getattr(lys, "frag_url", None)),
    )

    # --- blocked catalog window => retryable fg_resolver_blocked ---------------------
    def fake_blocked_catalog(scraper, brand, deadline, slug_limit=6, page_timeout=3.5, trace=None):
        engine.SearchSniper._catalog_fetch_health.all_failed = True
        return []

    def fake_empty_search(scraper, query, args, *, debug=False):
        return []

    old_search = w._search_candidates
    old_parfumo_enabled = engine.ParfumoEngine.enabled
    engine.SearchSniper.catalog_candidates_for_brand = staticmethod(fake_blocked_catalog)
    w._search_candidates = fake_empty_search
    engine.ParfumoEngine.enabled = staticmethod(lambda: False)
    blocked_code = terminal_code = None
    try:
        args = w._build_engine_args()
        try:
            w.resolve_candidate(None, args, {"house": "Chanel", "name": "No 5 Eau Premiere", "failure_count": 9})
        except w.WorkerError as exc:
            blocked_code = (exc.code, exc.retryable)
        # And when the catalog genuinely loads but has no match, the old
        # terminal behavior is preserved.
        engine.SearchSniper.catalog_candidates_for_brand = staticmethod(
            lambda scraper, brand, deadline, slug_limit=6, page_timeout=3.5, trace=None: (
                setattr(engine.SearchSniper._catalog_fetch_health, "all_failed", False) or []
            )
        )
        try:
            w.resolve_candidate(None, args, {"house": "Chanel", "name": "No 5 Eau Premiere", "failure_count": 9})
        except w.WorkerError as exc:
            terminal_code = (exc.code, exc.retryable)
    finally:
        engine.SearchSniper.catalog_candidates_for_brand = old_catalog
        w._search_candidates = old_search
        engine.ParfumoEngine.enabled = old_parfumo_enabled
    check(
        "blocked designer catalog raises retryable fg_resolver_blocked",
        blocked_code == ("fg_resolver_blocked", True),
        detail=repr(blocked_code),
    )
    check(
        "a loaded-but-no-match catalog still goes terminal after grace",
        terminal_code == ("fg_url_missing_after_resolution", False),
        detail=repr(terminal_code),
    )

    # --- job-pinned Parfumo URL ------------------------------------------------------
    parfumo_url = "https://www.parfumo.com/Perfumes/Clive_Christian/the-art-of-travel-collection-miami-poolside"
    rec = engine.ParfumoRecord(
        url=parfumo_url,
        name="The Art of Travel Collection - Miami Poolside",
        house="Clive Christian",
        year="2019",
        notes_flat=["Lime", "Mint"],
    )
    old_fetch = engine.ParfumoEngine.fetch_record
    engine.ParfumoEngine.enabled = staticmethod(lambda: True)
    engine.ParfumoEngine.fetch_record = staticmethod(lambda scraper, url, timeout=15.0: rec)
    try:
        pinned = w._parfumo_candidate_from_job_url(
            None,
            {"house": "Clive Christian", "name": "Miami Poolside", "year": 2019, "fg_url": parfumo_url},
        )
        not_pinned = w._parfumo_candidate_from_job_url(
            None, {"house": "Clive Christian", "name": "Miami Poolside", "year": 2019}
        )
        mismatched = w._parfumo_candidate_from_job_url(
            None,
            {"house": "Chanel", "name": "Totally Different Perfume", "fg_url": parfumo_url},
        )
    finally:
        engine.ParfumoEngine.fetch_record = old_fetch
        engine.ParfumoEngine.enabled = old_parfumo_enabled
    check(
        "job-pinned Parfumo URL resolves to a parfumo_fallback candidate",
        pinned is not None
        and pinned.resolver_source == "parfumo_fallback"
        and getattr(pinned, "parfumo_record", None) is rec,
        detail=repr(getattr(pinned, "resolver_source", None)),
    )
    check("no pinned URL means no direct-Parfumo candidate", not_pinned is None)
    check(
        "identity mismatch rejects the pinned Parfumo URL",
        mismatched is None,
        detail=repr(mismatched),
    )


def test_detail_fetch_saturation_returns_retryable_503() -> None:
    print("Detail fetch saturation checks (no DATABASE_URL required):")
    old_gate = api._DETAIL_FETCH_SEMAPHORE
    old_timeout = api._DETAIL_FETCH_QUEUE_TIMEOUT
    old_fetch = api.engine.fetch_selected_details
    gate = threading.BoundedSemaphore(1)
    gate.acquire()
    called = {"fetch": 0}
    try:
        api._DETAIL_FETCH_SEMAPHORE = gate
        api._DETAIL_FETCH_QUEUE_TIMEOUT = 0.0

        def fail_fetch(*args, **kwargs):
            called["fetch"] += 1
            raise AssertionError("detail fetch should not run when the gate is saturated")

        api.engine.fetch_selected_details = fail_fetch
        client = TestClient(api.app)
        response = client.post(
            "/api/fragrances/details",
            json={
                "id": "source:https://www.fragrantica.com/perfume/Le-Labo/Santal-33-12201.html"
            },
        )
    finally:
        try:
            gate.release()
        except ValueError:
            pass
        api._DETAIL_FETCH_SEMAPHORE = old_gate
        api._DETAIL_FETCH_QUEUE_TIMEOUT = old_timeout
        api.engine.fetch_selected_details = old_fetch

    check("saturated detail fetch returns 503", response.status_code == 503, str(response.status_code))
    check("saturated detail fetch sets Retry-After", response.headers.get("retry-after") == "2", str(response.headers))
    check("saturated detail fetch does not call engine", called["fetch"] == 0, str(called))


# ---------------------------------------------------------------------------
# DB-backed checks (only when DATABASE_URL is set)
# ---------------------------------------------------------------------------

_TEST_JOB_KEY = "https://www.fragrantica.com/perfume/_test_/enrichment-selftest-0.html"


def _cleanup() -> None:
    ctx, conn = db._conn()
    try:
        conn.execute("DELETE FROM enrichment_jobs WHERE job_key = %s", (_TEST_JOB_KEY,))
        conn.execute(
            "DELETE FROM fg_detail_cache WHERE canonical_fg_url = %s", (_TEST_JOB_KEY,)
        )
        conn.execute(
            "DELETE FROM fragrance_records WHERE canonical_fg_url = %s", (_TEST_JOB_KEY,)
        )
    finally:
        ctx.__exit__(None, None, None)


def test_db_lifecycle() -> None:
    if not db.ENABLED:
        return
    print("DB-backed checks (DATABASE_URL is set):")
    db.init_db()
    _cleanup()
    try:
        # Enqueue, then enqueue again -> upsert, requested_count bumps to 2.
        enqueue_counts = [
            db.enqueue_job(
                job_key=_TEST_JOB_KEY,
                query="enrichment selftest",
                name="Enrichment Selftest",
                house="_test_",
                year=2026,
                bn_url=None,
                fg_url=_TEST_JOB_KEY,
            )
            for _ in range(2)
        ]
        check(
            "enqueue_job returns the incrementing requested_count",
            enqueue_counts == [1, 2],
            str(enqueue_counts),
        )
        jobs = [j for j in db.list_jobs("pending", 100) if j["job_key"] == _TEST_JOB_KEY]
        check("enqueue creates exactly one pending job", len(jobs) == 1)
        job = jobs[0]
        check("duplicate enqueue upserts (requested_count == 2)", job["requested_count"] == 2)

        # Claim it.
        claim = db.claim_job(job["id"], lease_seconds=900)
        check("pending job can be claimed", claim.get("claimed") is True)
        reclaim = db.claim_job(job["id"], lease_seconds=900)
        check("active claim is not re-claimable", reclaim.get("claimed") is False)

        # Stale-claim reclaim: expire the lease, then it is claimable again.
        ctx, conn = db._conn()
        try:
            conn.execute(
                "UPDATE enrichment_jobs SET claim_expires_at = now() - interval '1 hour' "
                "WHERE id = %s",
                (job["id"],),
            )
        finally:
            ctx.__exit__(None, None, None)
        stale = db.claim_job(job["id"], lease_seconds=900)
        check("stale processing job can be reclaimed", stale.get("claimed") is True)

        # Complete with valid frag_cards -> job completed + cache row written.
        cache_row = {
            "canonical_fg_url": _TEST_JOB_KEY,
            "name": "Enrichment Selftest",
            "house": "_test_",
            "year": 2026,
            "schema_version": 1,
            "source": "selftest",
            "captured_at": None,
            "frag_cards": {"Community Interest": [{"label": "Have", "count": "1"}]},
            "notes": {"top": ["bergamot"], "heart": [], "base": [], "flat": [], "has_pyramid": True},
            "pros_cons": ["smells good"],
            "reviews": [{"text": "nice", "source": "selftest"}],
            "raw_identity": {"name": "Enrichment Selftest"},
            "concentration": "Eau de Parfum",
            "concentration_meta": {"source": "selftest", "confidence": 99},
            "quality_status": "complete",
        }
        updated = db.complete_job(job["id"], cache_row)
        check("complete_job marks job completed", updated and updated["status"] == "completed")

        cached = db.lookup_detail_cache(_TEST_JOB_KEY)
        check("complete_job upserts fg_detail_cache", cached is not None)
        check(
            "cached entry carries non-empty frag_cards",
            bool(cached and cached.get("frag_cards")),
        )
        check(
            "cached entry quality_status is complete",
            bool(cached and cached.get("quality_status") == "complete"),
        )
        check(
            "cached raw_identity carries top-level concentration",
            bool(
                cached
                and cached.get("raw_identity", {}).get("concentration") == "Eau de Parfum"
                and cached.get("raw_identity", {}).get("concentration_meta", {}).get("source") == "selftest"
            ),
        )

        requeued = db.requeue_job(job["id"], priority=25)
        check("requeue_job resurrects completed jobs", requeued and requeued["status"] == "pending")
        check("requeue_job raises priority", bool(requeued and requeued["priority"] >= 25))

        refreshed = db.complete_job(
            job["id"],
            {
                **cache_row,
                "name": "Enrichment Selftest Fresh",
                "house": "_fresh_",
                "image_url": "https://img.example.test/fresh.jpg",
                "frag_cards": {"Community Interest": [{"label": "Have", "count": "2"}]},
            },
        )
        check("refreshed job completes", refreshed and refreshed["status"] == "completed")
        cached = db.lookup_detail_cache(_TEST_JOB_KEY)
        check(
            "complete_job overwrites cached name",
            bool(cached and cached.get("name") == "Enrichment Selftest Fresh"),
        )
        check(
            "complete_job overwrites cached image_url",
            bool(cached and cached.get("image_url") == "https://img.example.test/fresh.jpg"),
        )

        requeued = db.requeue_job(job["id"], priority=30)
        check("requeue_job resurrects refreshed jobs", requeued and requeued["status"] == "pending")
        original_aggregate_writer = db._upsert_completed_fragrance_record
        try:
            db._upsert_completed_fragrance_record = lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("simulated aggregate failure")
            )
            recovered = db.complete_job(
                job["id"],
                {
                    **cache_row,
                    "name": "Enrichment Selftest Recovered",
                    "house": "_recovered_",
                    "image_url": "https://img.example.test/recovered.jpg",
                    "frag_cards": {"Community Interest": [{"label": "Have", "count": "3"}]},
                },
            )
        finally:
            db._upsert_completed_fragrance_record = original_aggregate_writer

        check(
            "complete_job tolerates aggregate fragrance_records failures",
            recovered and recovered["status"] == "completed",
        )
        cached = db.lookup_detail_cache(_TEST_JOB_KEY)
        check(
            "aggregate failure still leaves fg_detail_cache updated",
            bool(cached and cached.get("name") == "Enrichment Selftest Recovered"),
        )

        # Legacy-complete decision: a cache_row that omits quality_status falls
        # back to 'complete' (matches the fg_detail_cache column DEFAULT). This
        # is the behaviour legacy rows -- written before the column existed --
        # inherit via the ADD COLUMN migration in db.py.
        legacy_row = {k: v for k, v in cache_row.items() if k != "quality_status"}
        legacy_row["name"] = "Enrichment Selftest Legacy"
        db.complete_job(job["id"], legacy_row)
        cached = db.lookup_detail_cache(_TEST_JOB_KEY)
        check(
            "cache_row without quality_status defaults to complete",
            bool(cached and cached.get("quality_status") == "complete"),
        )

        # patch_job: attach fg_url to a pending row missing one.
        patch_key = "https://www.fragrantica.com/perfume/_test_/enrichment-patch-selftest.html"
        _cleanup()
        db.enqueue_job(
            job_key="patch-selftest",
            query=None,
            name="For Tony Iommi",
            house="Xerjoff",
            year=2024,
            bn_url=None,
            fg_url=None,
        )
        patch_jobs = [j for j in db.list_jobs("pending", 100) if j.get("job_key") == "patch-selftest"]
        check("patch enqueue creates pending job", len(patch_jobs) == 1)
        patch_job_row = patch_jobs[0]
        patched = db.patch_job(patch_job_row["id"], fg_url=patch_key)
        check("patch_job sets fg_url", bool(patched and patched.get("fg_url") == patch_key))
        check(
            "patch_job deduped query builder input",
            enrichment_worker._build_query(patch_job_row) == "Xerjoff For Tony Iommi",
        )
        conn_ctx, conn = db._conn()
        try:
            conn.execute("DELETE FROM enrichment_jobs WHERE job_key = %s", ("patch-selftest",))
        finally:
            conn_ctx.__exit__(None, None, None)
    finally:
        _cleanup()


def main() -> int:
    test_no_db_contract()
    test_fragrantica_challenge_and_static_parse()
    test_fragrantica_clearance_session_lifecycle()
    test_mobile_new_device_session_binding()
    test_hydration_dedup()
    test_concentration_pipeline()
    test_stored_detail_completion_logic()
    test_completeness_self_heal_sweep()
    test_worker_fact_summary_and_serp_retry()
    test_worker_query_normalization_and_retry_grace()
    test_worker_url_gating_catalog_tier_and_retirement()
    test_worker_resolver_hardening()
    test_detail_fetch_saturation_returns_retryable_503()
    if db.ENABLED:
        test_db_lifecycle()
    else:
        print("DB-backed checks: SKIPPED (DATABASE_URL not set).")

    print()
    if _FAILURES:
        print(f"{len(_FAILURES)} check(s) FAILED: {', '.join(_FAILURES)}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
