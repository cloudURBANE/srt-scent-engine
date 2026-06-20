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


def test_structured_discovery_runs_before_spell_repair():
    """With a configured designer-provider reserve (the production HTTP setting),
    the reliable structured Decodo discovery runs BEFORE the spell-repair pass.

    Regression for the live cold-query starvation: the spell-repair pass runs a
    *second* full ``_search_core`` (initial_timeout + fg_timeout) and, when it
    ran first, drained the overall budget to ~1.4s -- below the 2-4s a Decodo URL
    discovery call needs -- so the reliable designer leg read-timed-out and cold
    but correctly-spelled queries ("Kayali Vanilla 28", "Cola Addict", "YSL Y")
    returned zero rows. Running the designer leg first means a query it can
    resolve never burns the spell-repair budget at all.
    """
    snap = _snapshot()
    saved_designer = fp._designer_provider_fallback_results
    try:
        args = _build_args(total_budget=18.0, spell=4.0, catalog=3.5)
        args.designer_provider_reserve = 7.0
        order = []
        row = fp.UnifiedFragrance(
            name="Vanilla 28",
            brand="Kayali",
            year="",
            frag_url="https://www.fragrantica.com/perfume/Kayali/Vanilla-28-1.html",
        )

        fp._search_core = lambda scraper, q, a, allow_repair=True, timing=None: ([], [], [])
        fp.QueryRepair.needs_repair = staticmethod(lambda bn, cands: True)

        def fake_designer(query, a, overall_deadline=None):
            order.append("designer")
            return [row]
        fp._designer_provider_fallback_results = fake_designer

        def fake_suggest(scraper, query, seconds=0.0):
            order.append("spell")
            return None
        fp.QueryRepair.suggest = staticmethod(fake_suggest)

        out = fp.search_once(None, "kayali vanilla 28", args)
        assert out == [row], f"expected the structured designer rows, got {out!r}"
        assert order == ["designer"], (
            "structured discovery must run before (and pre-empt) spell repair when "
            f"a reserve is configured; got call order {order}"
        )
        print("PASS structured_discovery_runs_before_spell_repair")
    finally:
        fp._designer_provider_fallback_results = saved_designer
        _restore(snap)


def test_no_reserve_keeps_spell_repair_first():
    """Guardrail: with no configured reserve (CLI/unbounded default), the
    designer leg does NOT pre-empt spell repair -- the original ordering is kept
    byte-for-byte so this change is scoped to the production HTTP path."""
    snap = _snapshot()
    saved_designer = fp._designer_provider_fallback_results
    try:
        args = _build_args(total_budget=18.0, spell=4.0, catalog=3.5)
        args.designer_provider_reserve = None
        order = []

        fp._search_core = lambda scraper, q, a, allow_repair=True, timing=None: ([], [], [])
        fp.QueryRepair.needs_repair = staticmethod(lambda bn, cands: True)
        fp.IdentityTools.catalog_brand_keys = staticmethod(lambda brand, q: [])
        fp.IdentityTools.canonical_brand_query = staticmethod(lambda q: "")

        def fake_designer(query, a, overall_deadline=None):
            order.append("designer")
            return []
        fp._designer_provider_fallback_results = fake_designer

        def fake_suggest(scraper, query, seconds=0.0):
            order.append("spell")
            return None
        fp.QueryRepair.suggest = staticmethod(fake_suggest)

        fp.search_once(None, "kayali vanilla 28", args)
        assert order and order[0] == "spell", (
            f"with no reserve, spell repair must run first (original ordering); got {order}"
        )
        print("PASS no_reserve_keeps_spell_repair_first")
    finally:
        fp._designer_provider_fallback_results = saved_designer
        _restore(snap)


def test_designer_reserve_after_holds_back_budget():
    """`reserve_after` is subtracted from the early designer leg's deadline so a
    later stage (spell repair) keeps its runway. With reserve_after=0 the budget
    is unchanged (byte-for-byte the prior behavior)."""
    snap = _snapshot()
    try:
        seen = {}

        class FakeProvider:
            def search_fragrantica_brand_urls(self, label, deadline=None, max_results=25):
                # Record the budget this leg was actually handed, once.
                seen.setdefault("brand_seconds", getattr(deadline, "seconds", None))
                return []

            def search_fragrantica_designer_urls(self, q, deadline=None, max_results=1):
                seen.setdefault("designer_seconds", getattr(deadline, "seconds", None))
                return []

        fp.structured_search_provider = lambda: FakeProvider()
        args = _build_args(total_budget=18.0)

        # reserve_after=0 -> clamped only to remaining (~10s here).
        seen.clear()
        fp._designer_provider_fallback_results(
            "thom browne", args, overall_deadline=fp.Deadline(10.0), reserve_after=0.0
        )
        unreserved = seen["brand_seconds"]
        assert unreserved is not None and unreserved <= 10.0 + 0.05, unreserved

        # reserve_after=5 -> the leg must be clamped to leave ~5s for spell repair.
        seen.clear()
        fp._designer_provider_fallback_results(
            "thom browne", args, overall_deadline=fp.Deadline(10.0), reserve_after=5.0
        )
        reserved = seen["brand_seconds"]
        assert reserved is not None and reserved <= 5.0 + 0.05, (
            f"reserve_after=5 should cap the designer leg to ~5s of a 10s budget, got {reserved}"
        )
        assert reserved < unreserved, "reserve_after must reduce the designer leg's budget"
        print(
            f"PASS designer_reserve_after_holds_back_budget: "
            f"unreserved={unreserved:.2f}s reserved={reserved:.2f}s"
        )
    finally:
        _restore(snap)


def test_designer_reserve_after_skips_when_no_runway():
    """When the reserve exceeds the time left, the early designer leg yields
    entirely (returns []) rather than running with a sub-reserve budget, so the
    reserved spell-repair stage is guaranteed its slice."""
    snap = _snapshot()
    try:
        class FakeProvider:
            def search_fragrantica_brand_urls(self, label, deadline=None, max_results=25):
                raise AssertionError("designer leg ran despite insufficient runway for the reserve")

            def search_fragrantica_designer_urls(self, q, deadline=None, max_results=1):
                raise AssertionError("designer leg ran despite insufficient runway for the reserve")

        fp.structured_search_provider = lambda: FakeProvider()
        args = _build_args(total_budget=18.0)
        out = fp._designer_provider_fallback_results(
            "thom browne", args, overall_deadline=fp.Deadline(3.0), reserve_after=5.0
        )
        assert out == [], f"expected [], got {out!r}"
        print("PASS designer_reserve_after_skips_when_no_runway")
    finally:
        _restore(snap)


def test_search_once_reserves_spell_repair_budget():
    """End-to-end wiring: with both a designer-provider reserve and a
    spell-repair reserve configured (the production HTTP setting), the early
    designer leg is invoked with reserve_after == spell_repair_reserve, so a
    typo whose designer leg resolves nothing still reaches spell repair. This is
    the fix for "misspelled queries return nothing in prod"."""
    snap = _snapshot()
    saved_designer = fp._designer_provider_fallback_results
    try:
        args = _build_args(total_budget=18.0, spell=4.0, catalog=3.5)
        args.designer_provider_reserve = 7.0
        args.spell_repair_reserve = 5.0
        order = []
        captured = {}

        fp._search_core = lambda scraper, q, a, allow_repair=True, timing=None: ([], [], [])
        fp.QueryRepair.needs_repair = staticmethod(lambda bn, cands: True)
        fp.IdentityTools.catalog_brand_keys = staticmethod(lambda *a, **k: [])
        fp.IdentityTools.canonical_brand_query = staticmethod(lambda *a, **k: "")

        def fake_designer(query, a, overall_deadline=None, reserve_after=0.0):
            order.append("designer")
            # Record the FIRST (early) designer call -- the one that must reserve
            # spell-repair runway. search_once also runs a final designer pass
            # (with no reserve, correctly) after spell repair / catalog miss.
            captured.setdefault("reserve_after", reserve_after)
            return []   # genuine typo: no brand resolved here
        fp._designer_provider_fallback_results = fake_designer

        def fake_suggest(scraper, query, seconds=0.0):
            order.append("spell")
            captured["spell_seconds"] = seconds
            return None
        fp.QueryRepair.suggest = staticmethod(fake_suggest)

        fp.search_once(None, "blu de chanl", args)
        assert order[:2] == ["designer", "spell"], (
            f"designer leg should run first, then spell repair; got {order}"
        )
        assert abs(captured.get("reserve_after", 0.0) - 5.0) < 1e-9, (
            f"early designer leg must reserve spell_repair_reserve seconds; got "
            f"{captured.get('reserve_after')}"
        )
        assert captured.get("spell_seconds", 0.0) >= fp.QueryRepair.STRUCTURED_MIN_BUDGET, (
            "spell repair must still receive enough budget to fire its structured "
            f"leg; got {captured.get('spell_seconds')}s (need >= "
            f"{fp.QueryRepair.STRUCTURED_MIN_BUDGET}s)"
        )
        print(
            f"PASS search_once_reserves_spell_repair_budget: "
            f"reserve_after={captured['reserve_after']:.2f}s "
            f"spell_seconds={captured['spell_seconds']:.2f}s"
        )
    finally:
        fp._designer_provider_fallback_results = saved_designer
        _restore(snap)


def test_zero_spell_reserve_unchanged():
    """Guardrail: spell_repair_reserve=0 (CLI/script default) leaves the early
    designer leg with the full remaining budget -- reserve_after passed as 0."""
    snap = _snapshot()
    saved_designer = fp._designer_provider_fallback_results
    try:
        args = _build_args(total_budget=18.0, spell=4.0)
        args.designer_provider_reserve = 7.0
        args.spell_repair_reserve = 0.0
        captured = {}

        fp._search_core = lambda scraper, q, a, allow_repair=True, timing=None: ([], [], [])
        fp.QueryRepair.needs_repair = staticmethod(lambda bn, cands: True)
        fp.IdentityTools.catalog_brand_keys = staticmethod(lambda *a, **k: [])
        fp.IdentityTools.canonical_brand_query = staticmethod(lambda *a, **k: "")

        def fake_designer(query, a, overall_deadline=None, reserve_after=0.0):
            captured["reserve_after"] = reserve_after
            return []
        fp._designer_provider_fallback_results = fake_designer
        fp.QueryRepair.suggest = staticmethod(lambda scraper, query, seconds=0.0: None)

        fp.search_once(None, "blu de chanl", args)
        assert captured.get("reserve_after") == 0.0, (
            f"with spell_repair_reserve=0 the designer leg must get reserve_after=0; "
            f"got {captured.get('reserve_after')}"
        )
        print("PASS zero_spell_reserve_unchanged")
    finally:
        fp._designer_provider_fallback_results = saved_designer
        _restore(snap)


if __name__ == "__main__":
    test_overall_budget_bounds_chain()
    test_catalog_crawl_is_clamped()
    test_designer_fallback_skips_when_expired()
    test_unbounded_budget_does_not_clamp()
    test_structured_discovery_runs_before_spell_repair()
    test_no_reserve_keeps_spell_repair_first()
    test_designer_reserve_after_holds_back_budget()
    test_designer_reserve_after_skips_when_no_runway()
    test_search_once_reserves_spell_repair_budget()
    test_zero_spell_reserve_unchanged()
    print("\nALL TESTS PASSED")
