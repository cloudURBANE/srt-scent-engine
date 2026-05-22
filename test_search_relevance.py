#!/usr/bin/env python3
"""Plain-script checks for search result relevance filtering."""
from __future__ import annotations

import sys
from pathlib import Path

from fastapi import HTTPException

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


def test_sub_brand_catalog_keys_for_casamorati() -> None:
    print("Sub-brand catalog key checks:")
    keys = engine.IdentityTools.catalog_brand_keys(
        "Xerjoff",
        "Casamorati Mefisto",
        bn_url="https://basenotes.com/fragrances/casamorati-mefisto-by-xerjoff.26136299",
    )
    normalized = [engine.TextSanitizer.normalize_identity(key) for key in keys]
    check("parent house is included", "xerjoff" in normalized, str(keys))
    check("sub-line alias Casamorati 1888 is included", "casamorati 1888" in normalized, str(keys))


def test_compatible_catalog_brand_accepts_parent_line() -> None:
    print("Catalog parent/line brand compatibility checks:")
    ok = engine.IdentityTools.compatible_catalog_brand(
        "Xerjoff",
        "Casamorati 1888",
        "Casamorati Mefisto",
    )
    check("Xerjoff house can match Casamorati 1888 catalog brand", ok, "compatible_catalog_brand returned False")


def test_native_search_unusable_detects_junk() -> None:
    print("Native Fragrantica search junk checks:")
    query = "Xerjoff Casamorati Mefisto"
    junk_rows = [
        candidate("Azzaro", "Orange Tonic"),
        candidate("Givenchy", "Amarige"),
        candidate("Lanvin", "Arpege Pour Homme"),
    ]
    check(
        "unrelated FG rows with no query anchors are unusable",
        engine.FragranticaEngine.native_search_unusable(query, junk_rows),
        "expected unusable",
    )
    good_rows = junk_rows + [candidate("Casamorati 1888", "Mefisto")]
    check(
        "FG rows containing a query anchor are usable",
        not engine.FragranticaEngine.native_search_unusable(query, good_rows),
        "expected usable",
    )


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
    check(
        "strong cache precheck uses a stricter floor",
        api._STRONG_CACHE_MIN_SCORE > api._CACHE_SEARCH_MIN_SCORE,
        f"{api._STRONG_CACHE_MIN_SCORE} <= {api._CACHE_SEARCH_MIN_SCORE}",
    )


def test_api_strong_cache_precheck_skips_live_identity_hit() -> None:
    print("API strong-cache precheck checks:")
    old_cache = api._ARGS.fg_cache
    old_allow_search = api._ALLOW_BUNDLED_FG_SEARCH_CACHE
    old_allow_detail = api._ALLOW_BUNDLED_FG_DETAIL_CACHE
    old_record_search = api.db.search_fragrance_records
    old_detail_search = api.db.search_detail_cache
    old_search_once = api.engine.search_once
    try:
        api._ARGS.fg_cache = str(Path(__file__).with_name("fg_cache") / "fg_identity_cache_v2.json")
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = True
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = False
        api.db.search_fragrance_records = lambda query, limit=15: []
        api.db.search_detail_cache = lambda query, limit=15: []

        def fail_live_search(*args, **kwargs):
            raise AssertionError("live search should be skipped")

        api.engine.search_once = fail_live_search
        response = api.search(q="santal 33")
    finally:
        api._ARGS.fg_cache = old_cache
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = old_allow_search
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = old_allow_detail
        api.db.search_fragrance_records = old_record_search
        api.db.search_detail_cache = old_detail_search
        api.engine.search_once = old_search_once

    diagnostics = response.get("diagnostics", {})
    identities = [(row.get("house"), row.get("name")) for row in response.get("results", [])]
    check("identity cache exact hit returns results", ("Le Labo", "Santal 33") in identities, str(identities))
    check("strong precheck labels source", diagnostics.get("cache_source") == "identity", str(diagnostics))
    check("strong precheck labels mode", diagnostics.get("cache_mode") == "precheck", str(diagnostics))
    check("strong precheck reports skipped live search", diagnostics.get("live_search_skipped") is True, str(diagnostics))


def test_api_strong_cache_precheck_does_not_bypass_brand_only() -> None:
    print("API brand-only precheck safety checks:")
    old_cache = api._ARGS.fg_cache
    old_record_search = api.db.search_fragrance_records
    old_detail_search = api.db.search_detail_cache
    old_search_once = api.engine.search_once
    calls = {"live": 0}
    try:
        api._ARGS.fg_cache = str(Path(__file__).with_name("fg_cache") / "fg_identity_cache_v2.json")
        api.db.search_fragrance_records = lambda query, limit=15: []
        api.db.search_detail_cache = lambda query, limit=15: []

        def empty_live_search(*args, **kwargs):
            calls["live"] += 1
            return []

        api.engine.search_once = empty_live_search
        response = api.search(q="xerjoff")
    finally:
        api._ARGS.fg_cache = old_cache
        api.db.search_fragrance_records = old_record_search
        api.db.search_detail_cache = old_detail_search
        api.engine.search_once = old_search_once

    diagnostics = response.get("diagnostics", {})
    check("brand-only query runs live search", calls["live"] == 1, str(calls))
    check("brand-only query is not labelled precheck", diagnostics.get("cache_mode") != "precheck", str(diagnostics))


def test_fragrantica_native_search_bypassed_by_default() -> None:
    print("Native Fragrantica search bypass checks:")
    old_get = engine.Http.get
    calls = {"http": 0}
    try:
        def forbidden_get(*args, **kwargs):
            calls["http"] += 1
            raise AssertionError("Http.get should not be called")

        engine.Http.get = forbidden_get
        rows = engine.FragranticaEngine.extract_search_data(object(), "dior sauvage")
    finally:
        engine.Http.get = old_get

    check("native Fragrantica search returns no rows by default", rows == [], str(rows))
    check("native Fragrantica search does not call Http.get by default", calls["http"] == 0, str(calls))


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


def test_details_request_accepts_source_prefixed_id() -> None:
    print("Source-prefixed id recovery checks:")
    url = "https://www.fragrantica.com/perfume/Le-Labo/Santal-33-12201.html"
    selected = api._candidate_from_request(api.DetailRequest(id=f"source:{url}"))
    check("source: id routes to fragrantica URL", selected.frag_url == url, selected.frag_url)
    check("source: id recovers display name", selected.name == "Santal 33", selected.name)
    check("source: id recovers display house", selected.brand == "Le Labo", selected.brand)


def test_details_request_rejects_bad_source_prefixed_id() -> None:
    print("Source-prefixed id validation checks:")
    try:
        api._candidate_from_request(api.DetailRequest(id="source:javascript:fragrantica.com"))
    except HTTPException as exc:
        check("bad source: id returns 400", exc.status_code == 400, str(exc.status_code))
        check("bad source: id error is clear", "source_url must" in str(exc.detail), str(exc.detail))
    else:
        check("bad source: id returns 400", False, "no exception")


def test_details_request_rejects_app_catalog_ids_clearly() -> None:
    print("App-catalog id routing checks:")
    try:
        api._candidate_from_request(api.DetailRequest(id="catalog:test"))
    except HTTPException as exc:
        check("catalog id returns a clear 400", exc.status_code == 400, str(exc.status_code))
        check("catalog id error is not base64 padding", "padding" not in str(exc.detail).lower(), str(exc.detail))
    else:
        check("catalog id returns a clear 400", False, "no exception")


def test_bundled_identity_cache_rescues_deploy_repros() -> None:
    print("Bundled identity-cache fallback checks:")
    old_cache = api._ARGS.fg_cache
    old_allow_search = api._ALLOW_BUNDLED_FG_SEARCH_CACHE
    old_allow_detail = api._ALLOW_BUNDLED_FG_DETAIL_CACHE
    old_db_search = api.db.search_detail_cache
    try:
        api._ARGS.fg_cache = str(Path(__file__).with_name("fg_cache") / "fg_identity_cache_v2.json")
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = True
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = False
        api.db.search_detail_cache = lambda query, limit=15: []
        xerjoff_rows, xerjoff_source = api._cache_search_fallback("xerjoff", 15)
        santal_rows, santal_source = api._cache_search_fallback("santal 33", 15)
    finally:
        api._ARGS.fg_cache = old_cache
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = old_allow_search
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = old_allow_detail
        api.db.search_detail_cache = old_db_search

    check(
        "xerjoff falls back to shipped identity cache",
        xerjoff_source == "identity" and any(row.brand == "Xerjoff" for row in xerjoff_rows),
        f"{xerjoff_source} {[(row.brand, row.name) for row in xerjoff_rows[:5]]}",
    )
    check(
        "santal 33 falls back to shipped identity cache",
        santal_source == "identity" and any(row.brand == "Le Labo" and row.name == "Santal 33" for row in santal_rows),
        f"{santal_source} {[(row.brand, row.name) for row in santal_rows[:5]]}",
    )


def main() -> int:
    test_live_candidate_filter()
    test_sub_brand_catalog_keys_for_casamorati()
    test_compatible_catalog_brand_accepts_parent_line()
    test_native_search_unusable_detects_junk()
    test_brand_plus_name_filter()
    test_api_cache_threshold_matches_engine()
    test_api_strong_cache_precheck_skips_live_identity_hit()
    test_api_strong_cache_precheck_does_not_bypass_brand_only()
    test_fragrantica_native_search_bypassed_by_default()
    test_search_serialization_recovers_fragrantica_identity()
    test_search_serialization_recovers_basenotes_identity()
    test_details_request_recovers_identity_from_legacy_blank_id()
    test_details_request_accepts_source_prefixed_id()
    test_details_request_rejects_bad_source_prefixed_id()
    test_details_request_rejects_app_catalog_ids_clearly()
    test_bundled_identity_cache_rescues_deploy_repros()

    print()
    if _FAILURES:
        print(f"{len(_FAILURES)} check(s) FAILED: {', '.join(_FAILURES)}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
