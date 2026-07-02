"""Mocked unit checks for Decodo structured retail-price capture.

No live network: every Decodo POST is monkeypatched. Mirrors the harness style
of ``test_decodo_scraper_api.py`` (a ``check`` helper + a ``main`` runner), so it
runs the same way:  ``python test_decodo_price_capture.py``.
"""
import os

import fragrance_parser_full_rewrite_fixed as engine


_SAVED_ENV_KEYS = (
    "SERP_API_PROVIDER",
    "DECODO_API_BASIC_TOKEN",
    "DECODO_DISABLE_PRICE_CAPTURE",
    "DECODO_SCRAPER_LOCALE",
)


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers: dict = {}
        self.text = ""

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise engine.requests.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._payload


# A parsed google_shopping envelope mirroring the proven nesting in
# test_decodo_scraper_api.py: an array key (`organic`) under the
# content/results container chain. Two priced offers; price_from_payload must
# pick the CHEAPEST.
_PRICE_PAYLOAD = {
    "results": [
        {
            "content": {
                "results": {
                    "results": {
                        "organic": [
                            {"title": "Creed Aventus 100ml", "price": "$295.00", "currency": "USD"},
                            {"title": "Creed Aventus 50ml", "price": 129.0},
                            {"title": "Some unrelated row without a price"},
                        ]
                    }
                }
            }
        }
    ]
}

_EMPTY_PAYLOAD = {"results": [{"content": {"results": {"results": {"organic": []}}}}]}


def check(label: str, ok: bool, detail: str = "") -> None:
    if ok:
        print(f"[OK] {label}")
        return
    raise AssertionError(f"{label}: {detail}")


def _save_env() -> dict:
    return {key: os.environ.get(key) for key in _SAVED_ENV_KEYS}


def _restore_env(saved: dict) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _enable_decodo() -> None:
    for key in _SAVED_ENV_KEYS:
        os.environ.pop(key, None)
    os.environ["SERP_API_PROVIDER"] = "decodo"
    os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"


def test_price_payload_uses_google_shopping_template() -> None:
    print("Decodo price payload checks:")
    payload = engine.DecodoScraperClient._price_payload("creed aventus")
    check("price uses google_shopping target", payload.get("target") == "google_shopping", str(payload))
    check("price requests parsed JSON", payload.get("parse") is True, str(payload))
    check("price omits universal-only proxy_pool", "proxy_pool" not in payload, str(payload))
    check("price carries the query", payload.get("query") == "creed aventus", str(payload))


def test_price_from_payload_picks_cheapest_offer() -> None:
    print("Decodo price parse checks:")
    priced = engine.DecodoScraperClient.price_from_payload(_PRICE_PAYLOAD)
    check("a price is parsed", priced is not None, str(priced))
    assert priced is not None
    check("cheapest offer wins", priced.get("price") == 129.0, str(priced))
    check("currency defaults to USD when absent", priced.get("currency") == "USD", str(priced))
    check("source labels the provider", priced.get("source") == "decodo_google_shopping", str(priced))
    # No priced offers -> None, never a fabricated price.
    check("empty offers yield no price", engine.DecodoScraperClient.price_from_payload(_EMPTY_PAYLOAD) is None)
    check("garbage payload yields no price (no throw)", engine.DecodoScraperClient.price_from_payload({"x": 1}) is None)


def test_fetch_price_end_to_end_mocked() -> None:
    print("Decodo fetch_price checks:")
    saved = _save_env()
    old_post = engine.requests.post
    seen: dict = {}
    try:
        _enable_decodo()

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            seen["json"] = dict(json or {})
            return _FakeResponse(_PRICE_PAYLOAD)

        engine.requests.post = fake_post
        priced = engine.DecodoScraperClient.fetch_price("creed aventus")
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    check("fetch_price returns a structured price", priced is not None, str(priced))
    assert priced is not None
    check("fetch_price price is the cheapest offer", priced.get("price") == 129.0, str(priced))
    check("fetch_price hit the google_shopping template", (seen.get("json") or {}).get("target") == "google_shopping", str(seen))


def test_kill_flag_disables_capture() -> None:
    print("Decodo price kill-switch checks:")
    saved = _save_env()
    old_post = engine.requests.post
    posted = {"called": False}
    try:
        _enable_decodo()
        os.environ["DECODO_DISABLE_PRICE_CAPTURE"] = "1"

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            posted["called"] = True
            return _FakeResponse(_PRICE_PAYLOAD)

        engine.requests.post = fake_post
        check("capture disabled by kill flag", engine.DecodoScraperClient.price_capture_enabled() is False)
        priced = engine.DecodoScraperClient.fetch_price("creed aventus")
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    check("disabled capture returns None", priced is None, str(priced))
    check("disabled capture makes no billed call", posted["called"] is False)


def test_fetch_price_swallows_provider_failure() -> None:
    print("Decodo price failure-degradation checks:")
    saved = _save_env()
    old_post = engine.requests.post
    try:
        _enable_decodo()

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            raise engine.requests.ConnectionError("engine down")

        engine.requests.post = fake_post
        priced = engine.DecodoScraperClient.fetch_price("creed aventus")
    finally:
        engine.requests.post = old_post
        _restore_env(saved)

    check("provider failure degrades to None (no throw)", priced is None, str(priced))


def main() -> int:
    test_price_payload_uses_google_shopping_template()
    test_price_from_payload_picks_cheapest_offer()
    test_fetch_price_end_to_end_mocked()
    test_kill_flag_disables_capture()
    test_fetch_price_swallows_provider_failure()
    print()
    print("All Decodo price-capture checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
