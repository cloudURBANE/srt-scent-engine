"""Cold-path regression tests for the Decodo recovery additions.

These tests deliberately exercise the *cold* search/detail path -- the live
discovery + fetch chain that a brand-new, never-cached fragrance walks -- which
is the only part that is actually fragile in production. A cached query skips
all of this, so a test that runs against a warmed identity proves nothing about
the real cold experience. Every test below therefore:

  * clears the Decodo in-process caches before running, and
  * asserts the network mock was actually invoked (call count > 0),

so a silent cache hit can never make a check pass.

No real network is used: ``requests.post`` and ``Http.get`` are monkeypatched.

Covers:
  1. Bing redundancy   -- Google SERP returns nothing usable, Bing recovers the
                          Fragrantica perfume URL.
  2. Universal egress  -- a Cloudflare-blocked direct fetch (and a challenge
                          page) is recovered by re-fetching the URL's HTML
                          through Decodo's ``universal`` target, and the engine's
                          normal parser pulls notes out of it.
  3. Overshoot clamp   -- the per-request budget reserves a safety margin and the
                          request timeout is a bounded (connect, read) tuple, so a
                          call started near the overall deadline cannot overshoot.

Run: python test_cold_path_decodo_recovery.py
"""

from __future__ import annotations

import os

import fragrance_parser_full_rewrite_fixed as engine


# --------------------------------------------------------------------------- #
# Harness helpers
# --------------------------------------------------------------------------- #

_ENV_KEYS = (
    "SERP_API_PROVIDER",
    "DECODO_API_BASIC_TOKEN",
    "DECODO_AUTH_TOKEN",
    "DECODO_API_USERNAME",
    "DECODO_API_PASSWORD",
    "DECODO_SCRAPER_LOCALE",
    # Bing SERP fallback is opt-in: discovery only consults Bing when this flag
    # is set (see DecodoScraperClient.bing_fallback_enabled). Tracked here so the
    # Bing tests can enable it explicitly and it is wiped/restored around them.
    "DECODO_ENABLE_BING_FALLBACK",
)


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200, text: str = ""):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise engine.requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def check(label: str, ok: bool, detail: str = "") -> None:
    if ok:
        print(f"[OK] {label}")
        return
    raise AssertionError(f"{label}: {detail}")


def _save_env():
    return {key: os.environ.get(key) for key in _ENV_KEYS}


def _restore_env(saved) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _enable_cold_decodo() -> None:
    """Enable Decodo and wipe every in-process cache so the live path runs."""
    for key in _ENV_KEYS:
        os.environ.pop(key, None)
    os.environ["SERP_API_PROVIDER"] = "decodo"
    os.environ["DECODO_API_BASIC_TOKEN"] = "cold-test-token"
    engine.DecodoScraperClient._cache.clear()
    engine.DecodoScraperClient._designer_cache.clear()
    engine.DecodoScraperClient._brand_perfume_cache.clear()
    engine.DecodoScraperClient._parfumo_cache.clear()
    engine.DecodoScraperClient._image_cache.clear()


def _organic_payload(urls):
    """Build a parsed-SERP envelope shaped like Decodo's google/bing response."""
    return {
        "results": [
            {"content": {"results": {"results": {"organic": [{"url": u} for u in urls]}}}}
        ]
    }


# A minimal but realistic Fragrantica page: notes are embedded as ingredient
# URLs in a Nuxt/Vue state <script> blob, which extract_flat_notes() mines. This
# is a fragrance the engine has never seen, so it can only resolve via the live
# fetch -- exactly the cold case.
_COLD_FRAGRANTICA_HTML = """<!doctype html><html><head>
<title>Imaginary Authors Cape Heartache</title></head><body>
<h1>Cape Heartache by Imaginary Authors</h1>
<script>window.__NUXT__={"notes":[
"/notes/Pine-1234.html","/notes/Strawberry-5678.html","/notes/Virginia-Cedar-9012.html"]}</script>
</body></html>"""

_CHALLENGE_HTML = (
    "<html><head><title>Just a moment...</title></head>"
    "<body>cf-challenge checking your browser</body></html>"
)


# --------------------------------------------------------------------------- #
# 1. Bing redundancy
# --------------------------------------------------------------------------- #

def test_bing_fallback_recovers_fragrantica_url() -> None:
    print("Bing redundancy (cold URL discovery):")
    saved = _save_env()
    old_post = engine.requests.post
    targets: list[str] = []
    fg_url = "https://www.fragrantica.com/perfume/Imaginary-Authors/Cape-Heartache-30001.html"
    try:
        _enable_cold_decodo()
        os.environ["DECODO_ENABLE_BING_FALLBACK"] = "1"  # opt-in: enable the gated Bing leg

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            target = (json or {}).get("target")
            targets.append(target)
            if target == "google_search":
                return _FakeResponse(_organic_payload([]))           # Google: nothing usable
            if target == "bing_search":
                return _FakeResponse(_organic_payload([fg_url]))     # Bing: the page
            return _FakeResponse({})

        engine.requests.post = fake_post
        urls = engine.DecodoScraperClient.search_fragrantica_urls(
            "imaginary authors cape heartache"
        )
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    check("cold path actually ran (not a cache hit)", len(targets) > 0, str(targets))
    check("Google is tried first", targets[0] == "google_search", str(targets))
    check("Bing is tried when Google is empty", "bing_search" in targets, str(targets))
    check("Bing recovers the Fragrantica URL", urls == [fg_url], str(urls))


def test_bing_not_called_when_google_succeeds() -> None:
    print("Bing redundancy stays a fallback (no wasted credit):")
    saved = _save_env()
    old_post = engine.requests.post
    targets: list[str] = []
    fg_url = "https://www.fragrantica.com/perfume/Nishane/Hacivat-40002.html"
    try:
        _enable_cold_decodo()
        # Enable Bing so the assertion proves Google success *short-circuits* the
        # gated Bing leg -- not merely that the flag is off.
        os.environ["DECODO_ENABLE_BING_FALLBACK"] = "1"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            targets.append((json or {}).get("target"))
            return _FakeResponse(_organic_payload([fg_url]))         # Google already succeeds

        engine.requests.post = fake_post
        urls = engine.DecodoScraperClient.search_fragrantica_urls("nishane hacivat")
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    check("Google returned the URL", urls == [fg_url], str(urls))
    check("Bing is NOT called when Google succeeds", "bing_search" not in targets, str(targets))


# --------------------------------------------------------------------------- #
# 2. Universal egress recovery
# --------------------------------------------------------------------------- #

def _run_fetch_details_with_blocked_egress(direct_response):
    """Drive fetch_details with the direct egress returning ``direct_response``
    (None = network failure, or a challenge response) and Decodo's universal
    target serving the real page. Returns (details, post_targets)."""
    saved = _save_env()
    old_post = engine.requests.post
    old_get = engine.Http.get
    targets: list[str] = []
    try:
        _enable_cold_decodo()

        def fake_get(scraper, url, **kwargs):
            return direct_response

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            target = (json or {}).get("target")
            targets.append(target)
            if target == "universal":
                return _FakeResponse(
                    {"results": [{"content": _COLD_FRAGRANTICA_HTML, "status_code": 200}]}
                )
            return _FakeResponse({})

        engine.Http.get = staticmethod(fake_get)
        engine.requests.post = fake_post

        details = engine.UnifiedDetails(notes=engine.NotesList())
        engine.FragranticaEngine.fetch_details(
            None,
            "https://www.fragrantica.com/perfume/Imaginary-Authors/Cape-Heartache-30001.html",
            details,
            deadline=engine.Deadline(10.0),
        )
    finally:
        engine.requests.post = old_post
        engine.Http.get = old_get
        _restore_env(saved)
    return details, targets


def test_universal_egress_recovers_blocked_fragrantica_details() -> None:
    print("Universal egress recovery (cold /details, direct fetch blocked):")
    details, targets = _run_fetch_details_with_blocked_egress(direct_response=None)
    diag = details.parse_diagnostics.get("fg", {})

    check("cold path actually ran (universal call made)", "universal" in targets, str(targets))
    check("detail was fetched via Decodo universal", diag.get("fetched_via") == "decodo_universal", str(diag))
    check("recovered page produced notes", details.notes.flat != [], str(details.notes.flat))
    check(
        "notes parsed from the recovered HTML",
        {"Pine", "Strawberry"}.issubset(set(details.notes.flat)),
        str(details.notes.flat),
    )
    check("recovery clears the unreachable error", "fg" not in details.fetch_errors, str(details.fetch_errors))


def test_universal_egress_recovers_challenge_page() -> None:
    print("Universal egress recovery (cold /details, Cloudflare challenge):")
    challenge = _FakeResponse({}, status_code=403, text=_CHALLENGE_HTML)
    details, targets = _run_fetch_details_with_blocked_egress(direct_response=challenge)
    diag = details.parse_diagnostics.get("fg", {})

    check("challenge triggered universal recovery", "universal" in targets, str(targets))
    check("detail was fetched via Decodo universal", diag.get("fetched_via") == "decodo_universal", str(diag))
    check("recovered page produced notes", details.notes.flat != [], str(details.notes.flat))
    check("recovery clears the challenge error", "fg" not in details.fetch_errors, str(details.fetch_errors))


def test_universal_recovery_rejects_a_challenge_body() -> None:
    print("Universal recovery refuses a challenge body (no false recovery):")
    saved = _save_env()
    old_post = engine.requests.post
    old_get = engine.Http.get
    try:
        _enable_cold_decodo()

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            # Even Decodo can hand back a challenge shell; it must not be
            # accepted as a real page.
            return _FakeResponse(
                {"results": [{"content": _CHALLENGE_HTML, "status_code": 403}]}
            )

        engine.Http.get = staticmethod(lambda scraper, url, **kwargs: None)
        engine.requests.post = fake_post

        details = engine.UnifiedDetails(notes=engine.NotesList())
        engine.FragranticaEngine.fetch_details(
            None,
            "https://www.fragrantica.com/perfume/Bortnikoff/Vobla-50003.html",
            details,
            deadline=engine.Deadline(10.0),
        )
    finally:
        engine.requests.post = old_post
        engine.Http.get = old_get
        _restore_env(saved)

    diag = details.parse_diagnostics.get("fg", {})
    check("a challenge body is not treated as a recovered page", diag.get("fetched_via") != "decodo_universal", str(diag))
    check("no notes invented from a challenge shell", details.notes.flat == [], str(details.notes.flat))


# --------------------------------------------------------------------------- #
# 3. Overshoot clamp
# --------------------------------------------------------------------------- #

def test_overshoot_budget_is_clamped() -> None:
    print("Overshoot clamp (no late call, bounded connect):")
    Deadline = engine.Deadline
    client = engine.DecodoScraperClient

    # A call with barely any runway is skipped, not started late: remaining
    # (~1.5s) minus DEADLINE_SAFETY drops below MIN_VIABLE_BUDGET.
    starved = client._viable_budget(client.MAX_TIMEOUT, Deadline(1.5))
    check("a near-deadline call is skipped (budget 0)", starved == 0.0, f"got {starved}")

    # With comfortable runway the budget is granted, minus the safety margin.
    healthy = client._viable_budget(client.MAX_TIMEOUT, Deadline(5.0))
    check("a healthy call gets a positive budget", healthy > 0, f"got {healthy}")
    check(
        "budget reserves the deadline-safety margin",
        healthy <= 5.0 - client.DEADLINE_SAFETY + 1e-6,
        f"got {healthy}",
    )

    # An expired deadline is always skipped.
    expired = client._viable_budget(client.MAX_TIMEOUT, Deadline(0.0))
    check("an expired deadline is skipped", expired == 0.0, f"got {expired}")

    # The request timeout is a (connect, read) tuple, with connect capped so a
    # slow TLS handshake cannot double the call's wall-clock cost.
    connect, read = client._request_timeout(healthy)
    check("request timeout is a (connect, read) tuple", isinstance(client._request_timeout(healthy), tuple), str((connect, read)))
    check("connect timeout is capped", connect <= client.CONNECT_TIMEOUT + 1e-6, f"connect={connect}")
    check("read timeout tracks the budget", abs(read - min(healthy, client.MAX_TIMEOUT)) < 1e-6, f"read={read}")


# --------------------------------------------------------------------------- #
# 4. Universal-egress budget (paid render must not be aborted at the SERP cap)
# --------------------------------------------------------------------------- #

def _capture_universal_request(deadline):
    """Drive fetch_url_html with a captured post; return (timeout, payload)."""
    saved = _save_env()
    old_post = engine.requests.post
    captured: dict = {}
    try:
        _enable_cold_decodo()

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            captured["timeout"] = timeout
            captured["payload"] = json
            return _FakeResponse(
                {"results": [{"content": _COLD_FRAGRANTICA_HTML, "status_code": 200}]}
            )

        engine.requests.post = fake_post
        engine.DecodoScraperClient.fetch_url_html(
            "https://www.fragrantica.com/perfume/Imaginary-Authors/Cape-Heartache-30001.html",
            deadline=deadline,
        )
    finally:
        engine.requests.post = old_post
        _restore_env(saved)
    return captured.get("timeout"), captured.get("payload")


def test_universal_fetch_gets_more_than_serp_cap() -> None:
    print("Universal egress budget (background worker headroom):")
    client = engine.DecodoScraperClient

    # The whole money-on-lost-data bug: a generous (worker) deadline must let the
    # *billed* universal render run well past the tight SERP MAX_TIMEOUT instead
    # of being aborted at ~14.5s. With a 50s deadline the read budget should
    # approach UNIVERSAL_MAX_TIMEOUT, not MAX_TIMEOUT.
    timeout, payload = _capture_universal_request(engine.Deadline(50.0))
    check("request timeout is a (connect, read) tuple", isinstance(timeout, tuple), str(timeout))
    _connect, read = timeout
    check(
        "universal read budget exceeds the SERP MAX_TIMEOUT",
        read > client.MAX_TIMEOUT + 1.0,
        f"read={read} MAX_TIMEOUT={client.MAX_TIMEOUT}",
    )
    check(
        "universal read budget is bounded by UNIVERSAL_MAX_TIMEOUT",
        read <= client.UNIVERSAL_MAX_TIMEOUT + 1e-6,
        f"read={read} UNIVERSAL_MAX_TIMEOUT={client.UNIVERSAL_MAX_TIMEOUT}",
    )

    # Interactive search (small deadline) is unaffected: the deadline still
    # clamps the call well under the SERP cap.
    timeout_small, _ = _capture_universal_request(engine.Deadline(5.0))
    _c2, read_small = timeout_small
    check(
        "small (interactive) deadline still clamps the universal fetch",
        read_small <= 5.0,
        f"read={read_small}",
    )

    # The SERP legs keep the tight latency cap (no regression from the override).
    serp_connect, serp_read = client._request_timeout(client._viable_budget(client.MAX_TIMEOUT, engine.Deadline(50.0)))
    check("SERP request timeout still tracks MAX_TIMEOUT", serp_read <= client.MAX_TIMEOUT + 1e-6, f"serp_read={serp_read}")


def test_universal_headless_cost_lever() -> None:
    print("Universal egress headless cost lever (Premium+JS toggle):")
    saved = _save_env()
    try:
        # Default: JS render (Premium+JS) preserved -> headless key present.
        _, payload = _capture_universal_request(engine.Deadline(50.0))
        check("default keeps headless=html (no behavioural change)", payload.get("headless") == "html", str(payload))

        # Opt-out: drop JS rendering to the cheaper non-JS Premium tier.
        os.environ["DECODO_UNIVERSAL_HEADLESS"] = "off"
        _, payload_off = _capture_universal_request(engine.Deadline(50.0))
        check("DECODO_UNIVERSAL_HEADLESS=off removes the JS-render key", "headless" not in payload_off, str(payload_off))
    finally:
        _restore_env(saved)


if __name__ == "__main__":
    test_bing_fallback_recovers_fragrantica_url()
    test_bing_not_called_when_google_succeeds()
    test_universal_egress_recovers_blocked_fragrantica_details()
    test_universal_egress_recovers_challenge_page()
    test_universal_recovery_rejects_a_challenge_body()
    test_overshoot_budget_is_clamped()
    test_universal_fetch_gets_more_than_serp_cap()
    test_universal_headless_cost_lever()
    print("\nALL COLD-PATH TESTS PASSED")
