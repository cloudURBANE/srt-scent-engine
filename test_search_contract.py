#!/usr/bin/env python3
"""Regression checks for the fragrance search source contract."""
from __future__ import annotations

from types import SimpleNamespace

import fragrance_parser_full_rewrite_fixed as engine


def check(label: str, condition: bool) -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}")
    if not condition:
        raise AssertionError(label)


def test_brand_query_does_not_replace_directory_results() -> None:
    """A brand query that needs repair must not return a designer catalog list.

    Basenotes /directory/?search=<brand>&type=fragrances is the source list for
    brand build-outs. Fragrantica's designer catalog is allowed to enrich normal
    candidates, but it must not replace the directory result set when BN is
    blocked or empty.
    """
    original_search_core = engine._search_core
    original_needs_repair = engine.QueryRepair.needs_repair
    original_catalog = engine.SearchSniper.catalog_candidates_for_brand

    calls = {"catalog": 0}

    def fake_search_core(scraper, query, args, *, allow_repair):
        candidate = engine.UnifiedFragrance(
            name="Weak Non Directory Row",
            brand="Christian Dior",
            year="",
            frag_url="https://www.fragrantica.com/perfume/Dior/Weak-1.html",
            resolver_source="fragrantica_native_search",
            query_score=0.10,
        )
        return [candidate], [], []

    def fake_catalog(*args, **kwargs):
        calls["catalog"] += 1
        return [
            engine.CatalogItem(
                name="Sauvage",
                brand="Christian Dior",
                year="2015",
                url="https://www.fragrantica.com/perfume/Dior/Sauvage-31861.html",
            )
        ]

    try:
        engine._search_core = fake_search_core
        engine.QueryRepair.needs_repair = staticmethod(lambda bn, candidates: True)
        engine.SearchSniper.catalog_candidates_for_brand = staticmethod(fake_catalog)

        args = SimpleNamespace(
            spell_repair_budget=0,
            catalog_budget=3.5,
            catalog_slug_limit=6,
        )
        results = engine.search_once(object(), "dior", args)
    finally:
        engine._search_core = original_search_core
        engine.QueryRepair.needs_repair = original_needs_repair
        engine.SearchSniper.catalog_candidates_for_brand = original_catalog

    check("brand query does not call designer catalog replacement", calls["catalog"] == 0)
    check("weak non-directory fallback rows are rejected", results == [])


def test_brand_query_rejects_fg_only_native_results() -> None:
    """Brand build-outs must not degrade into Fragrantica native search rows."""
    original_search_core = engine._search_core
    original_needs_repair = engine.QueryRepair.needs_repair

    def fake_search_core(scraper, query, args, *, allow_repair):
        candidate = engine.UnifiedFragrance(
            name="J'adore Eau de Toilette 2002",
            brand="Dior",
            year="2002",
            frag_url="https://www.fragrantica.com/perfume/Dior/J-adore-Eau-de-Toilette-2002-5.html",
            resolver_source="fragrantica_native_search",
            query_score=0.85,
        )
        return [candidate], [], [candidate]

    try:
        engine._search_core = fake_search_core
        engine.QueryRepair.needs_repair = staticmethod(lambda bn, candidates: False)
        results = engine.search_once(object(), "dior", SimpleNamespace(spell_repair_budget=0))
    finally:
        engine._search_core = original_search_core
        engine.QueryRepair.needs_repair = original_needs_repair

    check("brand query rejects FG-only native search rows", results == [])


def test_api_brand_query_does_not_use_cache_substitute() -> None:
    """The HTTP layer must not refill brand directory misses from cache."""
    from fastapi.testclient import TestClient

    import api

    original_search_once = api.engine.search_once
    original_get_scraper = api.engine.get_scraper
    original_cache_fallback = api._cache_search_fallback
    calls = {"cache": 0}

    def fake_cache_fallback(query, limit):
        calls["cache"] += 1
        return [
            engine.UnifiedFragrance(
                name="Sauvage",
                brand="Christian Dior",
                frag_url="https://www.fragrantica.com/perfume/Dior/Sauvage-31861.html",
            )
        ], "identity"

    try:
        api.engine.get_scraper = lambda: object()
        api.engine.search_once = lambda scraper, query, args: []
        api._cache_search_fallback = fake_cache_fallback

        response = TestClient(api.app).get("/api/fragrances/search", params={"q": "dior"})
        payload = response.json()
    finally:
        api.engine.search_once = original_search_once
        api.engine.get_scraper = original_get_scraper
        api._cache_search_fallback = original_cache_fallback

    check("api brand query returns 200", response.status_code == 200)
    check("api brand query leaves results empty when directory is empty", payload.get("results") == [])
    check("api brand query does not call cache fallback", calls["cache"] == 0)
    check(
        "api brand query reports basenotes directory source",
        payload.get("diagnostics", {}).get("brand_directory_source") == "basenotes_directory",
    )


def main() -> int:
    print("Search contract checks:")
    test_brand_query_does_not_replace_directory_results()
    test_brand_query_rejects_fg_only_native_results()
    test_api_brand_query_does_not_use_cache_substitute()
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
