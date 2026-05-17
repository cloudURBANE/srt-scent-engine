#!/usr/bin/env python3
"""Plain-script checks for search result relevance filtering."""
from __future__ import annotations

import sys

import api
import fragrance_parser_full_rewrite_fixed as engine

_FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        _FAILURES.append(label)


def candidate(brand: str, name: str) -> engine.UnifiedFragrance:
    return engine.UnifiedFragrance(name=name, brand=brand, year="")


def test_live_candidate_filter() -> None:
    print("Live-search relevance checks:")
    rows = [
        candidate("Dior", "J'adore Eau de Toilette 2002"),
        candidate("Lanvin", "Arpege Pour Homme"),
        candidate("Azzaro", "Orange Tonic"),
        candidate("Chanel", "Cristalle Eau de Toilette"),
    ]
    filtered = engine.filter_relevant_candidates("dior", rows)
    identities = [(item.brand, item.name) for item in filtered]
    check("brand-only query keeps matching brand rows", identities == [("Dior", "J'adore Eau de Toilette 2002")], str(identities))


def test_brand_plus_name_filter() -> None:
    print("Brand + name relevance checks:")
    rows = [
        candidate("Dior", "J'adore Eau de Toilette 2002"),
        candidate("Dior", "Sauvage"),
        candidate("Dior", "Sauvage Elixir"),
    ]
    filtered = engine.filter_relevant_candidates("dior sauvage", rows)
    names = [item.name for item in filtered]
    check("brand match alone cannot keep unrelated same-house rows", "J'adore Eau de Toilette 2002" not in names, str(names))
    check("matching same-house fragrance remains", names == ["Sauvage", "Sauvage Elixir"], str(names))


def test_api_cache_threshold_matches_engine() -> None:
    print("API fallback threshold checks:")
    check(
        "cache fallback uses the engine relevance floor",
        api._CACHE_SEARCH_MIN_SCORE == engine.QueryRepair.MIN_RESULT_SCORE,
        f"{api._CACHE_SEARCH_MIN_SCORE} != {engine.QueryRepair.MIN_RESULT_SCORE}",
    )


def main() -> int:
    test_live_candidate_filter()
    test_brand_plus_name_filter()
    test_api_cache_threshold_matches_engine()

    print()
    if _FAILURES:
        print(f"{len(_FAILURES)} check(s) FAILED: {', '.join(_FAILURES)}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
