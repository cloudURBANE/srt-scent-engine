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


def test_search_serialization_recovers_fragrantica_identity() -> None:
    print("Fragrantica identity recovery checks:")
    row = engine.UnifiedFragrance(
        name="",
        brand="",
        year="",
        frag_url="https://www.fragrantica.com/perfume/French-Avenue/Liquid-Brun-94713.html",
    )
    payload = api._search_result_to_dict(row)
    token = api._decode_id(payload["id"])
    check("Fragrantica URL backfills display name", payload["name"] == "Liquid Brun", str(payload))
    check("Fragrantica URL backfills display house", payload["house"] == "French Avenue", str(payload))
    check("Fragrantica recovery is preserved in opaque id name", token.get("n") == "Liquid Brun", str(token))
    check("Fragrantica recovery is preserved in opaque id house", token.get("b") == "French Avenue", str(token))


def test_search_serialization_recovers_basenotes_identity() -> None:
    print("Basenotes identity recovery checks:")
    row = engine.UnifiedFragrance(
        name="",
        brand="",
        year="",
        bn_url="https://basenotes.com/fragrances/absolu-aventus-triple-aged-batch-by-creed.26272004",
    )
    payload = api._search_result_to_dict(row)
    token = api._decode_id(payload["id"])
    check(
        "Basenotes URL backfills display name",
        payload["name"] == "Absolu Aventus Triple Aged Batch",
        str(payload),
    )
    check("Basenotes URL backfills display house", payload["house"] == "Creed", str(payload))
    check(
        "Basenotes recovery is preserved in opaque id name",
        token.get("n") == "Absolu Aventus Triple Aged Batch",
        str(token),
    )
    check(
        "Basenotes recovery is preserved in opaque id house",
        token.get("b") == "Creed",
        str(token),
    )


def test_details_request_recovers_identity_from_legacy_blank_id() -> None:
    print("Legacy id recovery checks:")
    legacy = engine.UnifiedFragrance(
        name="",
        brand="",
        year="",
        bn_url="https://basenotes.com/fragrances/absolu-aventus-triple-aged-batch-by-creed.26272004",
    )
    token = api._encode_id(legacy)
    selected = api._candidate_from_request(api.DetailRequest(id=token))
    check(
        "details request recovers blank id name from source URL",
        selected.name == "Absolu Aventus Triple Aged Batch",
        f"{selected.name} | {selected.brand}",
    )
    check(
        "details request recovers blank id house from source URL",
        selected.brand == "Creed",
        f"{selected.name} | {selected.brand}",
    )


def main() -> int:
    test_live_candidate_filter()
    test_brand_plus_name_filter()
    test_api_cache_threshold_matches_engine()
    test_search_serialization_recovers_fragrantica_identity()
    test_search_serialization_recovers_basenotes_identity()
    test_details_request_recovers_identity_from_legacy_blank_id()

    print()
    if _FAILURES:
        print(f"{len(_FAILURES)} check(s) FAILED: {', '.join(_FAILURES)}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
