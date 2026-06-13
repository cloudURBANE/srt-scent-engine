"""Offline regression tests for the overall search wall-clock budget.

These tests prove that ``search_once`` bounds its cold-query fallback chain
(spell repair -> designer catalog crawl -> structured designer-provider
discovery) to a single overall deadline, so a bare designer/brand query cannot
stack the per-stage budgets past the client/gateway timeout. No network is
used: the slow stages are replaced with sleeping stand-ins.

Run: python test_search_total_budget.py
"""

from __future__ import annotations

import time

import fragrance_parser_full_rewrite_fixed as fp


def _build_args(total_budget, spell=5.0, catalog=5.0):
    args = fp.build_parser().parse_args([])
    args.search_total_budget = total_budget
    args.spell_repair_budget = spell
    args.catalog_budget = catalog
    args.catalog_slug_limit = 6
    args.max_results = 10
    args.brand = ""
    return args


def _snapshot():
    return {
        "_search_core": fp._search_core,
        "needs_repair": fp.QueryRepair.needs_repair,
        "suggest": fp.QueryRepair.suggest,
        "catalog_brand_keys": fp.IdentityTools.catalog_brand_keys,
        "canonical_brand_query": fp.IdentityTools.canonical_brand_query,
        "catalog_candidates_for_brand": fp.SearchSniper.catalog_candidates_for_brand,
        "filter_relevant_candidates": fp.filter_relevant_candidates,
        "structured_search_provider": fp.structured_search_provider,
    }


def _restore(snap):
    fp._search_core = snap["_search_core"]
    fp.QueryRepair.needs_repair = snap["needs_repair"]
    fp.QueryRepair.suggest = snap["suggest"]
    fp.IdentityTools.catalog_brand_keys = snap["catalog_brand_keys"]
    fp.IdentityTools.canonical_brand_query = snap["canonical_brand_query"]
    fp.SearchSniper.catalog_candidates_for_brand = snap["catalog_candidates_for_brand"]
    fp.filter_relevant_candidates = snap["filter_relevant_candidates"]
    fp.structured_search_provider = snap["structured_search_provider"]


def test_overall_budget_bounds_chain():
    """A bare designer query whose every fallback stage is slow still returns
    within the overall budget (not the ~30s sum of every stage)."""
    snap = _snapshot()
    try:
        args = _build_args(total_budget=3.0, spell=5.0, catalog=5.0)
        seen = {}

        fp._search_core = lambda scraper, q, a, allow_repair=True, timing=None: ([], [], [])
        fp.QueryRepair.needs_repair = staticmethod(lambda bn, cands: True)

        def fake_suggest(scraper, query, seconds=0.0):
            seen["spell_seconds"] = seconds
            time.sleep(min(seconds, 10.0))   # consume the budget it was handed
            return None
        fp.QueryRepair.suggest = staticmethod(fake_suggest)

        fp.IdentityTools.catalog_brand_keys = staticmethod(lambda brand, q: ["BrandX"])
        fp.IdentityTools.canonical_brand_query = staticmethod(lambda q: "BrandX")

        def fake_catalog(scraper, label, deadline, slug_limit=6):
            while not deadline.expired():
                time.sleep(0.05)
            return []
        fp.SearchSniper.catalog_candidates_for_brand = staticmethod(fake_catalog)
        fp.filter_relevant_candidates = lambda q, rows: rows

        start = time.monotonic()
        result = fp.search_once(None, "thom browne", args)
        elapsed = time.monotonic() - start

        assert result == [], f"expected empty result, got {result!r}"
        assert elapsed <= args.search_total_budget + 1.0, (
            f"elapsed {elapsed:.2f}s exceeded budget {args.search_total_budget}s (+1s slack)"
        )
        assert seen["spell_seconds"] <= args.search_total_budget + 0.01, (
            f"spell repair seconds {seen['spell_seconds']:.2f} not clamped to overall budget"
        )
        print(
            f"PASS overall_budget_bounds_chain: elapsed={elapsed:.2f}s "
            f"(budget {args.search_total_budget}s), spell_seconds={seen['spell_seconds']:.2f}s"
        )
    finally:
        _restore(snap)


def test_catalog_crawl_is_clamped():
    """With spell repair disabled, a slow catalog crawl is still clamped to the
    overall budget rather than its own (larger) catalog budget."""
    snap = _snapshot()
    try:
        args = _build_args(total_budget=2.0, spell=0.0, catalog=8.0)
        fp._search_core = lambda scraper, q, a, allow_repair=True, timing=None: ([], [], [])
        fp.QueryRepair.needs_repair = staticmethod(lambda bn, cands: True)
        fp.IdentityTools.catalog_brand_keys = staticmethod(lambda brand, q: ["BrandX"])
        fp.IdentityTools.canonical_brand_query = staticmethod(lambda q: "BrandX")

        def fake_catalog(scraper, label, deadline, slug_limit=6):
            while not deadline.expired():
                time.sleep(0.05)
            return []
        fp.SearchSniper.catalog_candidates_for_brand = staticmethod(fake_catalog)
        fp.filter_relevant_candidates = lambda q, rows: rows

        start = time.monotonic()
        result = fp.search_once(None, "brandx", args)
        elapsed = time.monotonic() - start

        assert result == []
        assert elapsed <= args.search_total_budget + 1.0, (
            f"catalog crawl ran {elapsed:.2f}s; should be clamped to overall budget "
            f"{args.search_total_budget}s, not catalog_budget {args.catalog_budget}s"
        )
        print(f"PASS catalog_crawl_is_clamped: elapsed={elapsed:.2f}s (budget {args.search_total_budget}s)")
    finally:
        _restore(snap)


def test_designer_fallback_skips_when_expired():
    """The structured designer-provider stage must not spend a provider call
    once the overall deadline is exhausted."""
    snap = _snapshot()
    try:
        class FakeProvider:
            def search_fragrantica_brand_urls(self, label, deadline=None, max_results=25):
                raise AssertionError("brand-url provider call made after deadline expired")

            def search_fragrantica_designer_urls(self, q, deadline=None, max_results=1):
                raise AssertionError("designer-url provider call made after deadline expired")

        fp.structured_search_provider = lambda: FakeProvider()
        args = _build_args(total_budget=5.0)
        expired = fp.Deadline(0.0)
        time.sleep(0.01)  # ensure it is past its end
        out = fp._designer_provider_fallback_results("thom browne", args, overall_deadline=expired)
        assert out == [], f"expected [], got {out!r}"
        print("PASS designer_fallback_skips_when_expired")
    finally:
        _restore(snap)


def test_unbounded_budget_does_not_clamp():
    """Regression: with search_total_budget=None (CLI/default), the designer
    fallback is NOT short-circuited and still queries the provider."""
    snap = _snapshot()
    try:
        called = {"brand": 0}

        class FakeProvider:
            def search_fragrantica_brand_urls(self, label, deadline=None, max_results=25):
                called["brand"] += 1
                return []

            def search_fragrantica_designer_urls(self, q, deadline=None, max_results=1):
                return []

        fp.structured_search_provider = lambda: FakeProvider()
        args = _build_args(total_budget=None)
        out = fp._designer_provider_fallback_results("thom browne", args, overall_deadline=fp.Deadline(None))
        assert out == []
        assert called["brand"] >= 1, "provider should still be queried when unbounded"
        print(f"PASS unbounded_budget_does_not_clamp: provider brand calls={called['brand']}")
    finally:
        _restore(snap)


if __name__ == "__main__":
    test_overall_budget_bounds_chain()
    test_catalog_crawl_is_clamped()
    test_designer_fallback_skips_when_expired()
    test_unbounded_budget_does_not_clamp()
    print("\nALL TESTS PASSED")
