import base64
import os

import fragrance_parser_full_rewrite_fixed as engine


_SAVED_ENV_KEYS = (
    "SERP_API_PROVIDER",
    "DECODO_API_BASIC_TOKEN",
    "DECODO_SCRAPER_BASIC_TOKEN",
    "DECODO_BASIC_TOKEN",
    "DECODO_AUTH_TOKEN",
    "DECODO_API_USERNAME",
    "DECODO_API_PASSWORD",
    "DECODO_SCRAPER_USERNAME",
    "DECODO_SCRAPER_PASSWORD",
    "DECODO_USERNAME",
    "DECODO_PASSWORD",
    "DECODO_SCRAPER_LOCALE",
)


class _FakeDecodoResponse:
    def __init__(
        self, payload: dict, status_code: int = 200, headers: dict | None = None
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.text = ""

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise engine.requests.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._payload


_DECODO_SEARCH_PAYLOAD = {
    "results": [
        {
            "content": {
                "results": {
                    "parse_status_code": 12000,
                    "results": {
                        "organic": [
                            {"url": "https://www.fragrantica.com/perfume/Xerjoff/XJ-1861-Naxos-30529.html"},
                            {"link": "https://example.com/not-fragrance"},
                            {"link": "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html?utm=1"},
                        ]
                    },
                }
            }
        }
    ]
}


# Regression fixture for the v2/scrape envelope drift that PR #71 fixed: Decodo
# began wrapping each results[] item in a dict that carries the SERP *request*
# URL at the top level (results[].url) alongside the nested content/results
# container holding the real `organic` array. That top-level `url` is an
# _ENTRY_FIELD_KEYS member, so the pre-fix _looks_like_entry mistook the wrapper
# for a leaf result and short-circuited _collect_entries before it ever reached
# `organic` -- every cold Decodo discovery returned zero Fragrantica URLs and
# uncached fragrances silently collapsed to Basenotes-only. This drifted shape is
# only exercised by a network/token-gated live canary; capturing it offline turns
# the next such drift into a CI failure instead of a silent prod degradation.
_DECODO_SEARCH_PAYLOAD_WRAPPED = {
    "results": [
        {
            # The wrapper's own SERP request URL -- the field that fooled the walk.
            "url": "https://www.google.com/search?q=xerjoff+naxos+site:fragrantica.com/perfume",
            "content": {
                "results": {
                    "parse_status_code": 12000,
                    "results": {
                        "organic": [
                            {"url": "https://www.fragrantica.com/perfume/Xerjoff/XJ-1861-Naxos-30529.html"},
                            {"link": "https://example.com/not-fragrance"},
                            {"link": "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html?utm=1"},
                        ]
                    },
                }
            },
        }
    ]
}


_DECODO_IMAGE_PAYLOAD = {
    "results": [
        {
            "content": {
                "results": [
                    {
                        "title": "Xerjoff Naxos perfume bottle",
                        "high_res_image": "https://images.example.test/naxos.jpg",
                        "image": "data:image/jpeg;base64,not-a-real-image",
                        "thumbnail_url": "https://images.example.test/naxos-thumb.jpg",
                        "source_url": "https://retailer.example.test/xerjoff-naxos",
                        "source": "retailer.example.test",
                        "width": "1200",
                        "height": 900,
                        "position": 1,
                    },
                    {
                        "title": "Duplicate bottle",
                        "image_url": "https://images.example.test/naxos.jpg",
                        "width": 1200,
                        "height": 900,
                    },
                    {
                        "alt": "Creed Aventus bottle",
                        "imageUrl": "https://images.example.test/aventus.webp",
                        "thumbnailUrl": "https://images.example.test/aventus-thumb.webp",
                        "link": "https://source.example.test/aventus",
                        "imageWidth": 800,
                        "imageHeight": "800 px",
                    },
                ]
            }
        }
    ]
}


def check(label: str, ok: bool, detail: str = "") -> None:
    if ok:
        print(f"[OK] {label}")
        return
    raise AssertionError(f"{label}: {detail}")


def _save_env() -> dict[str, str | None]:
    return {key: os.environ.get(key) for key in _SAVED_ENV_KEYS}


def _restore_env(saved: dict[str, str | None]) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _clear_decodo_env() -> None:
    for key in _SAVED_ENV_KEYS:
        os.environ.pop(key, None)
    engine.DecodoScraperClient._cache.clear()
    engine.DecodoScraperClient._designer_cache.clear()
    engine.DecodoScraperClient._brand_perfume_cache.clear()
    engine.DecodoScraperClient._parfumo_cache.clear()
    engine.DecodoScraperClient._image_cache.clear()


def test_decodo_scraper_search_payload_and_parsing() -> None:
    print("Decodo scraper search checks:")
    saved = _save_env()
    old_post = engine.requests.post
    seen: dict[str, object] = {}
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            seen["url"] = url
            seen["json"] = dict(json or {})
            seen["headers"] = dict(headers or {})
            seen["timeout"] = timeout
            return _FakeDecodoResponse(_DECODO_SEARCH_PAYLOAD)

        engine.requests.post = fake_post
        urls = engine.DecodoScraperClient.search_fragrantica_urls("xerjoff naxos")
        rows = engine.DecodoScraperClient.discover_fragrances("xerjoff naxos")
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    body = seen.get("json") or {}
    headers = seen.get("headers") or {}
    check("Decodo client is enabled by provider and token", urls != [], str(urls))
    check("Decodo request hits scraper API endpoint", seen.get("url") == engine.DecodoScraperClient.ENDPOINT, str(seen))
    check("Decodo search uses google_search target", body.get("target") == "google_search", str(body))
    check("Decodo search scopes query to Fragrantica", body.get("query") == "xerjoff naxos site:fragrantica.com/perfume", str(body))
    check("Decodo search requests parsed JSON", body.get("parse") is True, str(body))
    check("Decodo target templates omit universal-only proxy_pool", "proxy_pool" not in body, str(body))
    check("Decodo search uses en-us locale by default", body.get("locale") == "en-us", str(body))
    check("Decodo normal search does not send image tbm", "google_tbm" not in body, str(body))
    check("Decodo auth uses Basic header", headers.get("Authorization") == "Basic encoded-test-token", str(headers))
    check("Decodo accepts JSON", headers.get("Accept") == "application/json", str(headers))
    check(
        "Decodo search keeps only canonical Fragrantica URLs",
        urls == [
            "https://www.fragrantica.com/perfume/Xerjoff/XJ-1861-Naxos-30529.html",
            "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html",
        ],
        str(urls),
    )
    check(
        "Decodo discovered rows carry Decodo resolver label",
        any(row.resolver_source == "decodo_fragrantica_search" for row in rows),
        str([row.resolver_source for row in rows]),
    )


def test_decodo_envelope_wrapper_does_not_starve_url_discovery() -> None:
    """Regression gate for PR #71: the v2 envelope wrapper carries a top-level
    request `url`, but URL discovery must still descend into the nested `organic`
    array. Pre-fix this returned [] (cold queries -> Basenotes-only)."""
    print("Decodo envelope-wrapper regression checks:")
    saved = _save_env()
    old_post = engine.requests.post
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            del url, json, headers, timeout, kwargs
            return _FakeDecodoResponse(_DECODO_SEARCH_PAYLOAD_WRAPPED)

        engine.requests.post = fake_post
        urls = engine.DecodoScraperClient.search_fragrantica_urls("xerjoff naxos")
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    check(
        "Wrapper envelope still yields Fragrantica organic URLs (not starved)",
        urls == [
            "https://www.fragrantica.com/perfume/Xerjoff/XJ-1861-Naxos-30529.html",
            "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html",
        ],
        str(urls),
    )


def test_decodo_scraper_image_payload_and_mapping() -> None:
    print("Decodo scraper image checks:")
    saved = _save_env()
    old_post = engine.requests.post
    seen: dict[str, object] = {}
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            seen["json"] = dict(json or {})
            return _FakeDecodoResponse(_DECODO_IMAGE_PAYLOAD)

        engine.requests.post = fake_post
        candidates = engine.DecodoScraperClient.search_image_candidates(
            "xerjoff naxos fragrance bottle",
            max_results=12,
        )
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    body = seen.get("json") or {}
    check("Decodo image search uses google_search target", body.get("target") == "google_search", str(body))
    check("Decodo image search sends google_tbm=isch", body.get("google_tbm") == "isch", str(body))
    check("Decodo image search requests parsed JSON", body.get("parse") is True, str(body))
    check("Decodo image candidates dedupe image URLs", len(candidates) == 2, str(candidates))
    first = candidates[0]
    check("Decodo image URL maps to Serper-compatible imageUrl", first["imageUrl"].endswith("/naxos.jpg"), str(first))
    check("Decodo image width is coerced", first["imageWidth"] == 1200, str(first))
    check("Decodo image height is coerced", first["imageHeight"] == 900, str(first))
    check("Decodo source link maps to link", first["link"] == "https://retailer.example.test/xerjoff-naxos", str(first))
    check("Decodo candidate source is preserved", first["source"] == "retailer.example.test", str(first))
    check("Decodo candidates carry source provider", first["source_provider"] == "decodo", str(first))


def test_decodo_image_negative_cache_expires_but_positive_sticks() -> None:
    print("Decodo image negative-cache TTL checks:")
    saved = _save_env()
    old_post = engine.requests.post
    old_ttl = engine.DecodoScraperClient._IMAGE_EMPTY_TTL
    calls = {"n": 0}
    empty_payload = {"results": [{"content": {"results": []}}]}
    try:
        _clear_decodo_env()
        engine.DecodoScraperClient._image_cache_at.clear()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"

        # Phase 1: an empty image result is a SHORT-LIVED negative cache entry; a
        # repeat call inside the TTL must not re-hit the network.
        engine.DecodoScraperClient._IMAGE_EMPTY_TTL = 600.0

        def empty_post(url, json=None, headers=None, timeout=None, **kwargs):
            calls["n"] += 1
            return _FakeDecodoResponse(empty_payload)

        engine.requests.post = empty_post
        first = engine.DecodoScraperClient.search_image_candidates("aurora missing bottle", max_results=8)
        second = engine.DecodoScraperClient.search_image_candidates("aurora missing bottle", max_results=8)
        check("Empty image result returns no candidates", first == [] and second == [], str((first, second)))
        check("Empty image result is not re-queried within TTL", calls["n"] == 1, f"calls={calls['n']}")

        # Phase 2: once the negative TTL lapses, the SAME query is re-queried so a
        # since-published image can finally surface (fixes permanent "no image").
        engine.DecodoScraperClient._IMAGE_EMPTY_TTL = 0.0
        calls["n"] = 0

        def image_post(url, json=None, headers=None, timeout=None, **kwargs):
            calls["n"] += 1
            return _FakeDecodoResponse(_DECODO_IMAGE_PAYLOAD)

        engine.requests.post = image_post
        recovered = engine.DecodoScraperClient.search_image_candidates("aurora missing bottle", max_results=8)
        check("Expired negative image cache is re-queried", calls["n"] == 1, f"calls={calls['n']}")
        check("Recovered image candidates are returned", len(recovered) == 2, str(recovered))

        # Phase 3: a positive result is served for the whole run regardless of TTL.
        repeat = engine.DecodoScraperClient.search_image_candidates("aurora missing bottle", max_results=8)
        check("Positive image result stays cached without re-query", calls["n"] == 1, f"calls={calls['n']}")
        check("Cached positive returns same candidates", len(repeat) == 2, str(repeat))
    finally:
        engine.requests.post = old_post
        engine.DecodoScraperClient._IMAGE_EMPTY_TTL = old_ttl
        _restore_env(saved)


def test_decodo_username_password_auth_fallback() -> None:
    print("Decodo username/password auth checks:")
    saved = _save_env()
    old_post = engine.requests.post
    seen: dict[str, object] = {}
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_USERNAME"] = "decodo-user"
        os.environ["DECODO_API_PASSWORD"] = "decodo-pass"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            seen["headers"] = dict(headers or {})
            return _FakeDecodoResponse(_DECODO_SEARCH_PAYLOAD)

        engine.requests.post = fake_post
        urls = engine.DecodoScraperClient.search_fragrantica_urls("creed aventus")
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    expected = base64.b64encode(b"decodo-user:decodo-pass").decode("ascii")
    headers = seen.get("headers") or {}
    check("username/password fallback returns results", urls != [], str(urls))
    check("username/password fallback builds Basic token", headers.get("Authorization") == f"Basic {expected}", str(headers))


def test_decodo_auth_token_alias_and_casing() -> None:
    print("Decodo auth-token alias and casing checks:")
    saved = _save_env()
    try:
        _clear_decodo_env()
        os.environ["DECODO_AUTH_TOKEN"] = "alias-test-token"
        check(
            "DECODO_AUTH_TOKEN alias builds the Basic header",
            engine.DecodoScraperClient._auth_header() == "Basic alias-test-token",
            engine.DecodoScraperClient._auth_header(),
        )
        check(
            "DECODO_AUTH_TOKEN alias enables the client",
            engine.DecodoScraperClient.enabled(),
        )

        # os.environ is case-insensitive on Windows but case-sensitive on the
        # Linux deploy host; emulate the latter with a plain dict so the
        # fallback scan is exercised on every platform.
        _clear_decodo_env()
        old_environ = engine.os.environ
        engine.os.environ = {"Decodo_auth_token": "mixed-case-token"}
        try:
            header = engine.DecodoScraperClient._auth_header()
        finally:
            engine.os.environ = old_environ
        check(
            "Mixed-case token name resolves on case-sensitive platforms",
            header == "Basic mixed-case-token",
            header,
        )

        old_environ = engine.os.environ
        engine.os.environ = {"Decodo_Username": "mixed-user", "Decodo_Password": "mixed-pass"}
        try:
            header = engine.DecodoScraperClient._auth_header()
        finally:
            engine.os.environ = old_environ
        expected = base64.b64encode(b"mixed-user:mixed-pass").decode("ascii")
        check(
            "Mixed-case username/password resolves on case-sensitive platforms",
            header == f"Basic {expected}",
            header,
        )
    finally:
        _restore_env(saved)


def test_structured_search_provider_selection() -> None:
    print("Structured search provider selection checks:")
    saved = _save_env()
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"
        check(
            "Decodo provider is selected when requested",
            engine.structured_search_provider() is engine.DecodoScraperClient,
            str(engine.structured_search_provider()),
        )

        _clear_decodo_env()
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"
        check(
            "Decodo provider is the default when provider env is unset",
            engine.structured_search_provider() is engine.DecodoScraperClient,
            str(engine.structured_search_provider()),
        )

        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "   "
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"
        check(
            "Blank provider env defaults to Decodo",
            engine.structured_search_provider() is engine.DecodoScraperClient,
            str(engine.structured_search_provider()),
        )

        _clear_decodo_env()
        check(
            "No structured provider is selected without Decodo credentials",
            engine.structured_search_provider() is None,
            str(engine.structured_search_provider()),
        )

        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "serper"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"
        check(
            "Legacy Serper provider env does not block Decodo credentials",
            engine.structured_search_provider() is engine.DecodoScraperClient,
            str(engine.structured_search_provider()),
        )
    finally:
        _restore_env(saved)


def test_decodo_designer_and_brand_discovery() -> None:
    print("Decodo designer/brand discovery checks:")
    saved = _save_env()
    old_post = engine.requests.post
    seen_queries: list[str] = []
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            del url, headers, timeout, kwargs
            query = str((json or {}).get("query") or "")
            seen_queries.append(query)
            if "designers" in query:
                return _FakeDecodoResponse({
                    "results": [
                        {
                            "content": {
                                "results": {
                                    "organic": [
                                        {"url": "https://www.fragrantica.com/designers/Thom-Browne.html"},
                                        {"url": "https://www.fragrantica.com/designers/Molton-Brown.html"},
                                    ]
                                }
                            }
                        }
                    ]
                })
            return _FakeDecodoResponse({
                "results": [
                    {
                        "content": {
                            "results": {
                                "organic": [
                                    {"url": "https://www.fragrantica.com/perfume/Thom-Browne/Vetyver-And-Rose-57793.html"},
                                    {"url": "https://www.fragrantica.com/perfume/Molton-Brown/Jasmine-Sun-Rose-52035.html"},
                                ]
                            }
                        }
                    }
                ]
            })

        engine.requests.post = fake_post
        designers = engine.DecodoScraperClient.search_fragrantica_designer_urls("THOMBRONY")
        brand_urls = engine.DecodoScraperClient.search_fragrantica_brand_urls("Thom Browne")
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    check(
        "Decodo keeps canonical Fragrantica designer URLs",
        designers[:1] == ["https://www.fragrantica.com/designers/Thom-Browne.html"]
        and "https://www.fragrantica.com/designers/Molton-Brown.html" in designers,
        str(designers),
    )
    check(
        "Decodo brand URL discovery scopes to the designer perfume path",
        any(query == "site:fragrantica.com/perfume/Thom-Browne" for query in seen_queries),
        str(seen_queries),
    )
    check(
        "Decodo brand URL discovery keeps only matching-brand perfume URLs",
        brand_urls == ["https://www.fragrantica.com/perfume/Thom-Browne/Vetyver-And-Rose-57793.html"],
        str(brand_urls),
    )


def test_parse_retry_after_seconds_and_http_date() -> None:
    print("Decodo Retry-After parsing checks:")
    parse = engine.DecodoScraperClient._parse_retry_after
    check("integer delta-seconds parses", parse("5") == 5.0, str(parse("5")))
    check("negative delta-seconds clamps to 0", parse("-3") == 0.0, str(parse("-3")))
    check("blank header returns None", parse("") is None, str(parse("")))
    check("garbage header returns None", parse("soon") is None, str(parse("soon")))
    # A far-future HTTP-date yields a positive wait; a past date clamps to 0.
    future = parse("Wed, 21 Oct 2099 07:28:00 GMT")
    past = parse("Wed, 21 Oct 1999 07:28:00 GMT")
    check("future HTTP-date yields positive wait", future is not None and future > 0, str(future))
    check("past HTTP-date clamps to 0", past == 0.0, str(past))


def test_decodo_429_retries_then_succeeds() -> None:
    print("Decodo 429 retry-then-succeed checks:")
    saved = _save_env()
    old_post = engine.requests.post
    old_sleep = engine.time.sleep
    calls: list[int] = []
    slept: list[float] = []
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"

        responses = [
            _FakeDecodoResponse({}, status_code=429, headers={"Retry-After": "1"}),
            _FakeDecodoResponse(_DECODO_SEARCH_PAYLOAD),
        ]

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            del url, json, headers, timeout, kwargs
            calls.append(1)
            return responses[len(calls) - 1]

        engine.requests.post = fake_post
        engine.time.sleep = lambda s: slept.append(s)
        # Ample budget so the single retry fits before the deadline.
        out = engine.DecodoScraperClient._post_google("xerjoff naxos", 15.0)
    finally:
        engine.requests.post = old_post
        engine.time.sleep = old_sleep
        _restore_env(saved)

    check("429 then 200 makes exactly two POSTs", len(calls) == 2, str(len(calls)))
    check("retry slept once", len(slept) == 1, str(slept))
    check("Retry-After header drives the wait (1s)", slept and slept[0] == 1.0, str(slept))
    check("successful retry returns the parsed payload", out == _DECODO_SEARCH_PAYLOAD, str(out))


def test_decodo_429_exhausts_and_surfaces_bounded() -> None:
    print("Decodo 429 exhaustion checks:")
    saved = _save_env()
    old_post = engine.requests.post
    old_sleep = engine.time.sleep
    calls: list[int] = []
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            del url, json, headers, timeout, kwargs
            calls.append(1)
            return _FakeDecodoResponse({}, status_code=429, headers={"Retry-After": "0"})

        engine.requests.post = fake_post
        engine.time.sleep = lambda s: None
        raised = False
        try:
            engine.DecodoScraperClient._post_google("xerjoff naxos", 15.0)
        except engine.requests.HTTPError:
            raised = True
    finally:
        engine.requests.post = old_post
        engine.time.sleep = old_sleep
        _restore_env(saved)

    check("a sustained 429 ultimately raises (no silent collapse)", raised, "")
    check(
        "billed POSTs are bounded to MAX_ATTEMPTS + 1 (no request storm)",
        len(calls) == engine.DecodoScraperClient.RETRY_429_MAX_ATTEMPTS + 1,
        str(len(calls)),
    )


def test_decodo_429_does_not_overshoot_deadline() -> None:
    print("Decodo 429 deadline-safety checks:")
    saved = _save_env()
    old_post = engine.requests.post
    old_sleep = engine.time.sleep
    calls: list[int] = []
    slept: list[float] = []
    try:
        _clear_decodo_env()
        os.environ["SERP_API_PROVIDER"] = "decodo"
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            del url, json, headers, timeout, kwargs
            calls.append(1)
            # Retry-After far larger than the tiny remaining budget.
            return _FakeDecodoResponse({}, status_code=429, headers={"Retry-After": "30"})

        engine.requests.post = fake_post
        engine.time.sleep = lambda s: slept.append(s)
        raised = False
        try:
            # Budget too small to wait 30s and still finish a retry -> no sleep.
            engine.DecodoScraperClient._post_google("xerjoff naxos", 0.5)
        except engine.requests.HTTPError:
            raised = True
    finally:
        engine.requests.post = old_post
        engine.time.sleep = old_sleep
        _restore_env(saved)

    check("an unaffordable backoff surfaces immediately", raised, "")
    check("no POST is retried when the wait can't fit the budget", len(calls) == 1, str(len(calls)))
    check("the engine never sleeps past the deadline", slept == [], str(slept))


def main() -> int:
    test_decodo_scraper_search_payload_and_parsing()
    test_decodo_envelope_wrapper_does_not_starve_url_discovery()
    test_decodo_scraper_image_payload_and_mapping()
    test_decodo_username_password_auth_fallback()
    test_decodo_auth_token_alias_and_casing()
    test_structured_search_provider_selection()
    test_decodo_designer_and_brand_discovery()
    test_parse_retry_after_seconds_and_http_date()
    test_decodo_429_retries_then_succeeds()
    test_decodo_429_exhausts_and_surfaces_bounded()
    test_decodo_429_does_not_overshoot_deadline()
    print()
    print("All Decodo scraper API checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
