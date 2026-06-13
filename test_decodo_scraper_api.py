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
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
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


def main() -> int:
    test_decodo_scraper_search_payload_and_parsing()
    test_decodo_scraper_image_payload_and_mapping()
    test_decodo_username_password_auth_fallback()
    test_decodo_auth_token_alias_and_casing()
    test_structured_search_provider_selection()
    test_decodo_designer_and_brand_discovery()
    print()
    print("All Decodo scraper API checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
