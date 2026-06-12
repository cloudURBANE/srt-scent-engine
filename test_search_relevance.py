#!/usr/bin/env python3
"""Plain-script checks for search result relevance filtering."""
from __future__ import annotations

import base64
import json
import os
import sys
import threading
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


def test_line_alias_queries_are_treated_as_brand_tokens() -> None:
    print("Product-line query alias checks:")
    tempio = candidate("Casamorati 1888", "Tempio D Acqua")
    wrong_xerjoff = candidate("Xerjoff", "Casamorati Harrods Edition")
    for query in (
        "casamorati tempio d acqua",
        "tempio d acqua casamorati",
        "xerjoff tempio d acqua",
    ):
        tempio.query_score = engine.IdentityTools.relevance_score(query, tempio)
        wrong_xerjoff.query_score = engine.IdentityTools.relevance_score(query, wrong_xerjoff)
        check(
            f"{query!r} keeps the Casamorati Tempio result",
            engine.candidate_relevance_ok(query, tempio),
            f"{tempio.query_score:.3f} {engine.IdentityTools.query_name_tokens(query, tempio.brand)}",
        )
        check(
            f"{query!r} still rejects an unrelated Xerjoff line result",
            not engine.candidate_relevance_ok(query, wrong_xerjoff),
            f"{wrong_xerjoff.query_score:.3f} {engine.IdentityTools.query_name_tokens(query, wrong_xerjoff.brand)}",
        )


def test_margiela_replica_line_aliases_merge_and_filter() -> None:
    print("Margiela Replica line alias checks:")
    bn_fireplace = engine.UnifiedFragrance(
        name="Replica By The Fireplace",
        brand="Martin Margiela",
        year="2015",
        bn_url="https://basenotes.com/fragrances/replica-by-the-fireplace-by-martin-margiela.26147432",
    )
    fg_fireplace = engine.UnifiedFragrance(
        name="By The Fireplace",
        brand="Maison Martin Margiela",
        year="2015",
        frag_url="https://www.fragrantica.com/perfume/Maison-Martin-Margiela/By-the-Fireplace-31623.html",
        resolver_source="decodo_fragrantica_search",
    )
    merged = engine.Orchestrator.match_and_merge([bn_fireplace], [fg_fireplace])
    check(
        "Replica prefix does not block BN/FG merge",
        merged[0].frag_url == fg_fireplace.frag_url,
        f"{merged[0].brand} | {merged[0].name} | {merged[0].frag_url}",
    )

    jazz = candidate("Martin Margiela", "Replica Jazz Club")
    jazz.query_score = engine.IdentityTools.relevance_score("maison margiela jazz club", jazz)
    check(
        "Maison/Martin Margiela aliases keep Jazz Club",
        engine.candidate_relevance_ok("maison margiela jazz club", jazz),
        f"{jazz.query_score:.3f} {engine.IdentityTools.query_name_tokens('maison margiela jazz club', jazz.brand)}",
    )


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


def test_multi_token_name_requires_distinctive_coverage() -> None:
    print("Multi-token name coverage checks:")
    query = "Creed Bayrhum Vétiver"
    rows = [
        candidate("Creed", "Wild Vétiver"),
        candidate("Creed", "Original Vétiver"),
        candidate("Creed", "Bayrhum Vétiver"),
    ]
    filtered = engine.filter_relevant_candidates(query, rows)
    names = [item.name for item in filtered]
    wild_score = engine.IdentityTools.relevance_score(query, rows[0])
    check("same-house sibling scores near old floor", 0.70 <= wild_score < 0.75, f"{wild_score:.3f}")
    check("same-house sibling missing Bayrhum is rejected", "Wild Vétiver" not in names, str(names))
    check("true Creed Bayrhum Vétiver remains", names == ["Bayrhum Vétiver"], str(names))


def test_normalize_notes_flattens_to_independent_layers() -> None:
    print("Note normalization aliasing checks:")
    notes = engine.NotesList(
        has_pyramid=True,
        top=["Rose"],
        heart=["Rose"],
        base=["Rose"],
    )
    engine.normalize_notes(notes)
    notes.top.append("Bergamot")
    check(
        "top layer can change independently",
        notes.heart == [] and notes.base == [],
        str((notes.top, notes.heart, notes.base)),
    )


def test_cache_search_rejects_same_house_sibling() -> None:
    print("API cache sibling relevance checks:")
    old_record_search = api.db.search_fragrance_records
    try:
        api.db.search_fragrance_records = lambda query, limit=15: [
            {
                "name": "Wild Vétiver",
                "house": "Creed",
                "bn_url": "",
                "canonical_fg_url": "https://www.fragrantica.com/perfume/Creed/Wild-Vetiver-125485.html",
                "source_captured_at": "2099-01-01T00:00:00+00:00",
            },
            {
                "name": "Bayrhum Vétiver",
                "house": "Creed",
                "bn_url": "https://basenotes.com/fragrances/bayrhum-vetiver-by-creed.26120009",
                "canonical_fg_url": "https://www.fragrantica.com/perfume/Creed/Bayrhum-Vetiver-30691.html",
                "image_url": "https://cdn.example.test/wild-vetiver.jpg",
                "source_captured_at": "2099-01-01T00:00:00+00:00",
            },
        ]
        rows = api._fragrance_record_search("Creed Bayrhum Vétiver", 10)
    finally:
        api.db.search_fragrance_records = old_record_search

    identities = [(row.brand, row.name) for row in rows]
    check("cached Wild Vétiver row is dropped", ("Creed", "Wild Vétiver") not in identities, str(identities))
    check("cached Bayrhum Vétiver row remains", identities == [("Creed", "Bayrhum Vétiver")], str(identities))


def test_aggregate_cache_search_does_not_expose_unproven_image() -> None:
    print("API aggregate image provenance checks:")
    old_record_search = api.db.search_fragrance_records
    try:
        api.db.search_fragrance_records = lambda query, limit=15: [
            {
                "name": "Bayrhum Vetiver",
                "house": "Creed",
                "bn_url": "https://basenotes.com/fragrances/bayrhum-vetiver-by-creed.26120009",
                "canonical_fg_url": "https://www.fragrantica.com/perfume/Creed/Bayrhum-Vetiver-30691.html",
                "image_url": "https://cdn.example.test/wild-vetiver.jpg",
                "source_captured_at": "2099-01-01T00:00:00+00:00",
            },
        ]
        rows = api._fragrance_record_search("Creed Bayrhum Vetiver", 10)
    finally:
        api.db.search_fragrance_records = old_record_search

    check("matching aggregate row remains", len(rows) == 1, str(rows))
    check(
        "aggregate search does not expose unproven cached image",
        bool(rows) and not getattr(rows[0], "image_url", ""),
        str(getattr(rows[0], "image_url", "") if rows else ""),
    )


def test_cache_identity_fill_rejects_sibling_image() -> None:
    print("API cache image identity checks:")
    selected = engine.UnifiedFragrance(
        name="Bayrhum Vetiver",
        brand="Creed",
        year="",
        frag_url="https://www.fragrantica.com/perfume/Creed/Bayrhum-Vetiver-30691.html",
    )
    api._fill_selected_identity(
        selected,
        {
            "name": "Wild Vetiver",
            "house": "Creed",
            "canonical_fg_url": "https://www.fragrantica.com/perfume/Creed/Wild-Vetiver-125485.html",
            "image_url": "https://cdn.example.test/wild-vetiver.jpg",
        },
    )
    check(
        "mismatched cache entry does not backfill image",
        not getattr(selected, "image_url", ""),
        str(getattr(selected, "image_url", "")),
    )


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


def test_api_cache_fallback_uses_long_anchor_variants() -> None:
    print("API typo anchor cache fallback checks:")
    old_allow_search = api._ALLOW_BUNDLED_FG_SEARCH_CACHE
    old_allow_detail = api._ALLOW_BUNDLED_FG_DETAIL_CACHE
    old_record_search = api.db.search_fragrance_records
    old_detail_search = api.db.search_detail_cache
    old_search_once = api.engine.search_once
    calls: list[str] = []
    try:
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = False
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = False

        def fake_record_search(query, limit=15):
            calls.append(query)
            if query.lower() != "montaig":
                return []
            return [
                {
                    "name": "Gris Dior Gris Montaigne",
                    "house": "Christian Dior",
                    "year": 2013,
                    "bn_url": "https://basenotes.com/fragrances/gris-dior-gris-montaigne-by-christian-dior.26138833",
                    "canonical_fg_url": "https://www.fragrantica.com/perfume/Dior/Gris-Montaigne-17842.html",
                    "source_captured_at": "2099-01-01T00:00:00+00:00",
                }
            ]

        api.db.search_fragrance_records = fake_record_search
        api.db.search_detail_cache = lambda query, limit=15: []
        api.engine.search_once = lambda *args, **kwargs: []

        response = api.search(q="Grizz Dior Grizz Montaig")
    finally:
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = old_allow_search
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = old_allow_detail
        api.db.search_fragrance_records = old_record_search
        api.db.search_detail_cache = old_detail_search
        api.engine.search_once = old_search_once

    results = response.get("results", [])
    diagnostics = response.get("diagnostics", {})
    check("fallback tried the long typo anchor", "montaig" in calls, str(calls))
    check("typo anchor fallback returns canonical Dior row", len(results) == 1, str(results))
    check(
        "fallback result is the expected fragrance",
        bool(results) and results[0].get("name") == "Gris Dior Gris Montaigne",
        str(results),
    )
    check("fallback source is aggregate DB", diagnostics.get("fallback_source") == "aggregate_db", str(diagnostics))
    check("fallback warning labels aggregate DB cache", "aggregate DB cache" in diagnostics.get("warning", ""), str(diagnostics))


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


def test_api_bn_only_cache_hit_does_not_skip_live_search() -> None:
    print("API BN-only cache safety checks:")
    old_allow_search = api._ALLOW_BUNDLED_FG_SEARCH_CACHE
    old_record_search = api.db.search_fragrance_records
    old_detail_search = api.db.search_detail_cache
    old_search_once = api.engine.search_once
    calls = {"live": 0}
    try:
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = False
        api.db.search_fragrance_records = lambda query, limit=15: [
            {
                "name": "1861 Naxos",
                "house": "Xerjoff",
                "bn_url": "https://basenotes.com/fragrances/1861-naxos-by-xerjoff.26144159",
                "canonical_fg_url": "",
                "source_captured_at": "2099-01-01T00:00:00+00:00",
            }
        ]
        api.db.search_detail_cache = lambda query, limit=15: []

        def live_search(*args, **kwargs):
            calls["live"] += 1
            return [
                engine.UnifiedFragrance(
                    name="1861 Naxos",
                    brand="Xerjoff",
                    year="2015",
                    frag_url="https://www.fragrantica.com/perfume/Xerjoff/1861-Naxos-30529.html",
                )
            ]

        api.engine.search_once = live_search
        response = api.search(q="xerjoff naxos")
    finally:
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = old_allow_search
        api.db.search_fragrance_records = old_record_search
        api.db.search_detail_cache = old_detail_search
        api.engine.search_once = old_search_once

    diagnostics = response.get("diagnostics", {})
    results = response.get("results", [])
    check("BN-only cache hit runs live search", calls["live"] == 1, str(calls))
    check("BN-only cache hit is not labelled precheck", diagnostics.get("cache_mode") != "precheck", str(diagnostics))
    check(
        "live result can restore Fragrantica URL",
        any(row.get("source_url", "").startswith("https://www.fragrantica.com/") for row in results),
        str(results),
    )
    check(
        "diagnostics record disqualified cache fast path",
        "precheck_missing_fragrantica_url" in diagnostics.get("cache_fast_path_disqualified", []),
        str(diagnostics),
    )


def test_api_identity_cache_rescues_bn_only_precheck() -> None:
    print("API BN-only plus identity-cache rescue checks:")
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
        api.db.search_fragrance_records = lambda query, limit=15: [
            {
                "name": "1861 Naxos",
                "house": "Xerjoff",
                "bn_url": "https://basenotes.com/fragrances/1861-naxos-by-xerjoff.26145750",
                "canonical_fg_url": "",
                "source_captured_at": "2099-01-01T00:00:00+00:00",
            }
        ]
        api.db.search_detail_cache = lambda query, limit=15: []

        def fail_live_search(*args, **kwargs):
            raise AssertionError("live search should be skipped when identity cache has a Fragrantica URL")

        api.engine.search_once = fail_live_search
        response = api.search(q="xerjoff naxos")
    finally:
        api._ARGS.fg_cache = old_cache
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = old_allow_search
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = old_allow_detail
        api.db.search_fragrance_records = old_record_search
        api.db.search_detail_cache = old_detail_search
        api.engine.search_once = old_search_once

    diagnostics = response.get("diagnostics", {})
    results = response.get("results", [])
    check("identity cache labels source", diagnostics.get("cache_source") == "identity", str(diagnostics))
    check("identity cache labels precheck", diagnostics.get("cache_mode") == "precheck", str(diagnostics))
    check(
        "identity cache restores Naxos Fragrantica URL",
        any(
            row.get("source_url") == "https://www.fragrantica.com/perfume/Xerjoff/XJ-1861-Naxos-30529.html"
            for row in results
        ),
        str(results),
    )


def test_api_live_search_saturation_uses_cache_fallback() -> None:
    print("API live-search saturation checks:")
    old_gate = api._LIVE_SEARCH_SEMAPHORE
    old_timeout = api._LIVE_SEARCH_QUEUE_TIMEOUT
    old_allow_search = api._ALLOW_BUNDLED_FG_SEARCH_CACHE
    old_allow_detail = api._ALLOW_BUNDLED_FG_DETAIL_CACHE
    old_record_search = api.db.search_fragrance_records
    old_detail_search = api.db.search_detail_cache
    old_search_once = api.engine.search_once
    gate = threading.BoundedSemaphore(1)
    gate.acquire()
    calls = {"record": 0, "live": 0}
    try:
        api._LIVE_SEARCH_SEMAPHORE = gate
        api._LIVE_SEARCH_QUEUE_TIMEOUT = 0.0
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = False
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = False

        def fake_record_search(query, limit=15):
            calls["record"] += 1
            if calls["record"] <= 2:
                return []
            return [
                {
                    "name": "Bayrhum Vetiver",
                    "house": "Creed",
                    "bn_url": "https://basenotes.com/fragrances/bayrhum-vetiver-by-creed.26120009",
                    "canonical_fg_url": "https://www.fragrantica.com/perfume/Creed/Bayrhum-Vetiver-30691.html",
                    "source_captured_at": "2099-01-01T00:00:00+00:00",
                }
            ]

        def fail_live_search(*args, **kwargs):
            calls["live"] += 1
            raise AssertionError("live search should not run when the gate is saturated")

        api.db.search_fragrance_records = fake_record_search
        api.db.search_detail_cache = lambda query, limit=15: []
        api.engine.search_once = fail_live_search
        response = api.search(q="creed bayrhum vetiver")
    finally:
        try:
            gate.release()
        except ValueError:
            pass
        api._LIVE_SEARCH_SEMAPHORE = old_gate
        api._LIVE_SEARCH_QUEUE_TIMEOUT = old_timeout
        api._ALLOW_BUNDLED_FG_SEARCH_CACHE = old_allow_search
        api._ALLOW_BUNDLED_FG_DETAIL_CACHE = old_allow_detail
        api.db.search_fragrance_records = old_record_search
        api.db.search_detail_cache = old_detail_search
        api.engine.search_once = old_search_once

    diagnostics = response.get("diagnostics", {})
    check("saturated live search does not call engine.search_once", calls["live"] == 0, str(calls))
    check("saturated live search falls back to cache", diagnostics.get("fallback_source") == "aggregate_db", str(diagnostics))
    check("saturation is reported in diagnostics", diagnostics.get("live_search_saturated") is True, str(diagnostics))
    check(
        "cache fallback still returns the fragrance",
        any(row.get("name") == "Bayrhum Vetiver" for row in response.get("results", [])),
        str(response.get("results", [])),
    )


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


def test_strip_house_from_name() -> None:
    print("House prefix stripping checks:")
    strip = engine.IdentityTools.strip_house_from_name
    check("Hermes Rocabar -> Rocabar", strip("Hermès Rocabar", "Hermes") == "Rocabar", "")
    check("Hermes Bel Ami -> Bel Ami", strip("Hermes Bel Ami", "Hermes") == "Bel Ami", "")
    check("bare name unchanged", strip("Bel Ami", "Hermes") == "Bel Ami", "")
    check("Dior Sauvage -> Sauvage", strip("Dior Sauvage", "Dior") == "Sauvage", "")
    check(
        "partial Dolce strip does not leave conjunction garbage",
        strip("Dolce and gabana Q", "Dolce") == "Dolce and gabana Q",
        strip("Dolce and gabana Q", "Dolce"),
    )
    check(
        "Dolce & Gabbana Q -> Q",
        strip("Dolce & Gabbana Q", "Dolce & Gabbana") == "Q",
        strip("Dolce & Gabbana Q", "Dolce & Gabbana"),
    )
    check(
        "Dolce Gabbana Q -> Q",
        strip("Dolce Gabbana Q", "Dolce Gabbana") == "Q",
        strip("Dolce Gabbana Q", "Dolce Gabbana"),
    )
    check(
        "multi-token house can keep legitimate De-name",
        strip("Serge Lutens De Profundis", "Serge Lutens") == "De Profundis",
        strip("Serge Lutens De Profundis", "Serge Lutens"),
    )

    card = engine.UnifiedFragrance(
        name="Hermès Equipage",
        brand="Hermes",
        year="",
        frag_url="https://www.fragrantica.com/perfume/Hermes/Equipage-28.html",
    )
    payload = api._search_result_to_dict(card)
    check("search serialization strips duplicated house", payload["name"] == "Equipage", str(payload))


def test_q_relevance_is_word_based() -> None:
    print("Single-letter query relevance checks:")
    q = candidate("Dolce Gabbana", "Q")
    acqua = candidate("Giorgio Armani", "Acqua Di Gio Pour Homme")
    q_score = engine.IdentityTools.relevance_score("q", q)
    acqua_score = engine.IdentityTools.relevance_score("q", acqua)
    dg_score = engine.IdentityTools.relevance_score("Dolce and gabana Q", q)
    short_alias_score = engine.IdentityTools.relevance_score("D&G Q", q)
    check("single-letter exact word scores strongly", q_score >= 0.99, f"{q_score:.3f}")
    check("single-letter substring inside Acqua does not pass", acqua_score < 0.4, f"{acqua_score:.3f}")
    check("misspelled Dolce Gabbana query still matches Q", dg_score > 0.8, f"{dg_score:.3f}")
    check("D&G shorthand query matches canonical house", short_alias_score > 0.85, f"{short_alias_score:.3f}")


def test_brand_alias_query_keeps_house_catalog() -> None:
    print("Brand-alias query relevance checks:")
    rows = [
        candidate("Maison Francis Kurkdjian", "Baccarat Rouge 540"),
        candidate("Maison Francis Kurkdjian", "Grand Soir"),
        candidate("Creed", "Aventus"),
        candidate("Dior", "Sauvage"),
    ]
    filtered = engine.filter_relevant_candidates("MFK", rows)
    houses = {item.brand for item in filtered}
    names = {item.name for item in filtered}
    check("MFK keeps Maison Francis Kurkdjian results", houses == {"Maison Francis Kurkdjian"}, str(houses))
    check("MFK keeps the whole MFK catalog, not one row", {"Baccarat Rouge 540", "Grand Soir"} <= names, str(names))
    check("MFK does not leak into other houses", "Creed" not in houses and "Dior" not in houses, str(houses))

    # A brand-only alias query must score the house strongly, while a
    # brand+fragrance query must still discriminate within the house.
    mfk_score = engine.IdentityTools.relevance_score(
        "MFK", candidate("Maison Francis Kurkdjian", "Baccarat Rouge 540")
    )
    check("MFK alias scores the house strongly", mfk_score >= 0.99, f"{mfk_score:.3f}")
    specific = engine.filter_relevant_candidates(
        "dior sauvage",
        [candidate("Dior", "J'adore Eau de Toilette 2002"), candidate("Dior", "Sauvage")],
    )
    specific_names = [item.name for item in specific]
    check(
        "brand-only boost does not relax brand+name filtering",
        specific_names == ["Sauvage"],
        str(specific_names),
    )


def test_dolce_gabbana_identity_recovery_and_persistence() -> None:
    print("Dolce Gabbana poisoned-row prevention checks:")
    row = engine.UnifiedFragrance(
        name="Dolce and gabana Q",
        brand="Dolce",
        year="",
        frag_url="https://www.fragrantica.com/perfume/Dolce-Gabbana/Q-83367.html",
    )
    payload = api._search_result_to_dict(row)
    check("Fragrantica URL repairs misspelled query-echo name", payload["name"] == "Q", str(payload))
    check("Fragrantica URL repairs partial Dolce house", payload["house"] == "Dolce Gabbana", str(payload))

    old_enabled = api.db.ENABLED
    old_upsert = api.db.upsert_fragrance_search
    captured: list[dict] = []
    try:
        api.db.ENABLED = True
        api.db.upsert_fragrance_search = lambda data: captured.append(data)
        api._persist_search_results("Dolce and gabana Q", [row])
    finally:
        api.db.ENABLED = old_enabled
        api.db.upsert_fragrance_search = old_upsert

    check("persisted search row uses repaired name", bool(captured) and captured[0]["name"] == "Q", str(captured))
    check(
        "persisted search row uses repaired house",
        bool(captured) and captured[0]["house"] == "Dolce Gabbana",
        str(captured),
    )


def test_house_echo_poison_is_suppressed() -> None:
    print("House-echo poison suppression checks:")
    check("Creed/Creed Suicide is detected as house echo", api._identity_is_house_echo("Creed", "Creed Suicide"), "")
    check("self-named house echo is detected", api._identity_is_house_echo("creed", "creed"), "")
    check("real fragrance is not flagged", not api._identity_is_house_echo("Aventus", "Creed"), "")
    check("multi-word name is not flagged", not api._identity_is_house_echo("Santal 33", "Le Labo"), "")

    poison = engine.UnifiedFragrance(name="Creed", brand="Creed Suicide", year="")
    real = engine.UnifiedFragrance(name="Aventus", brand="Creed", year="")
    check("poison row is denied a display identity", not api._candidate_has_display_identity(poison), "")
    check("real row keeps its display identity", api._candidate_has_display_identity(real), "")
    check(
        "poison row is not persisted to the aggregate cache",
        not api._search_identity_is_safe_to_store("Creed", "Creed Suicide", poison),
        "",
    )


def test_brand_only_query_gets_catalog_labels() -> None:
    print("Brand-only designer-catalog fallback checks:")
    # The bug: a single-token house we don't alias produced no catalog labels,
    # so the designer-catalog breadth crawl never ran when Serper/native-FG
    # discovery came back empty.
    check(
        "single-token brand query yields catalog labels",
        engine.IdentityTools.catalog_brand_keys("creed") == ["creed"],
        str(engine.IdentityTools.catalog_brand_keys("creed")),
    )
    check(
        "treating the query as a name still yields nothing (the original bug)",
        engine.IdentityTools.catalog_brand_keys("", "creed") == [],
        str(engine.IdentityTools.catalog_brand_keys("", "creed")),
    )
    check(
        "multi-token brand+name query is left to the name path",
        engine.IdentityTools.catalog_brand_keys("", "dior sauvage") == [],
        str(engine.IdentityTools.catalog_brand_keys("", "dior sauvage")),
    )


def test_poisoned_db_records_are_filtered() -> None:
    print("Poisoned DB record filtering checks:")
    old_record_search = api.db.search_fragrance_records
    try:
        api.db.search_fragrance_records = lambda query, limit=15: [
            {
                "name": "and gabana Q",
                "house": "Dolce",
                "bn_url": "",
                "canonical_fg_url": "",
                "source_captured_at": "2099-01-01T00:00:00+00:00",
            },
            {
                "name": "Q",
                "house": "Dolce Gabbana",
                "bn_url": "",
                "canonical_fg_url": "https://www.fragrantica.com/perfume/Dolce-Gabbana/Q-83367.html",
                "source_captured_at": "2099-01-01T00:00:00+00:00",
            },
        ]
        rows = api._fragrance_record_search("Dolce and gabana Q", 10)
    finally:
        api.db.search_fragrance_records = old_record_search

    identities = [(row.brand, row.name) for row in rows]
    check("poisoned conjunction-leading record is dropped", ("Dolce", "and gabana Q") not in identities, str(identities))
    check("healthy D&G Q record remains", ("Dolce Gabbana", "Q") in identities, str(identities))


def test_short_db_search_uses_word_boundaries() -> None:
    print("Short DB query predicate checks:")
    mode, term = api.db._identity_search_term("Q")
    long_mode, long_term = api.db._identity_search_term("Dior")
    check("single-character DB query uses regex mode", mode == "regex", str((mode, term)))
    check("short regex has non-alphanumeric boundaries", "[^[:alnum:]]" in term and "Q" in term, term)
    check("normal DB query still uses ILIKE mode", (long_mode, long_term) == ("ilike", "%Dior%"), str((long_mode, long_term)))


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


def test_basenotes_slug_splits_on_final_by() -> None:
    print("Basenotes slug parsing checks:")
    parsed = engine.BasenotesEngine._parse_name_metadata(
        "https://basenotes.com/fragrances/replica-by-the-fireplace-by-martin-margiela.26147432",
        "",
        "",
    )
    check(
        "name keeps internal By phrase",
        parsed.name == "Replica By The Fireplace",
        f"{parsed.name} | {parsed.brand}",
    )
    check("brand comes from final By phrase", parsed.brand == "Martin Margiela", f"{parsed.name} | {parsed.brand}")


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


def test_details_request_repairs_legacy_poisoned_id() -> None:
    print("Legacy poisoned id recovery checks:")
    payload = {
        "n": "Dolce and gabana Q",
        "b": "Dolce",
        "y": "",
        "bn": "",
        "fg": "https://www.fragrantica.com/perfume/Dolce-Gabbana/Q-83367.html",
    }
    token = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    selected = api._candidate_from_request(api.DetailRequest(id=token))
    check(
        "details request repairs legacy poisoned id name",
        selected.name == "Q",
        f"{selected.name} | {selected.brand}",
    )
    check(
        "details request repairs legacy poisoned id house",
        selected.brand == "Dolce Gabbana",
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


class _FakeDecodoResponse:
    """Minimal stand-in for a requests.Response from Decodo's scraper API."""

    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise engine.requests.HTTPError(f"HTTP {self.status_code}")
        return None

    def json(self) -> dict:
        return self._payload


_DECODO_SAMPLE_PAYLOAD = {
    "results": [
        {
            "content": {
                "results": {
                    "results": {
                        "organic": [
                            {"url": "https://www.fragrantica.com/perfume/Xerjoff/1861-Naxos-30529.html"},
                            {"link": "https://example.com/not-fragrance"},
                            {"link": "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html?utm=1"},
                        ]
                    }
                }
            }
        }
    ]
}


_DECODO_ENV_KEYS = (
    "SERP_API_PROVIDER",
    "DECODO_API_BASIC_TOKEN",
    "DECODO_SCRAPER_BASIC_TOKEN",
    "DECODO_BASIC_TOKEN",
    "DECODO_API_USERNAME",
    "DECODO_API_PASSWORD",
    "DECODO_SCRAPER_USERNAME",
    "DECODO_SCRAPER_PASSWORD",
    "DECODO_USERNAME",
    "DECODO_PASSWORD",
    "DECODO_SCRAPER_LOCALE",
)


def _set_decodo_env(enabled: bool, *, provider: str | None = "decodo") -> dict[str, str | None]:
    saved = {key: os.environ.get(key) for key in _DECODO_ENV_KEYS}
    for key in _DECODO_ENV_KEYS:
        os.environ.pop(key, None)
    if enabled:
        if provider is not None:
            os.environ["SERP_API_PROVIDER"] = provider
        os.environ["DECODO_API_BASIC_TOKEN"] = "encoded-test-token"
    engine.DecodoScraperClient._cache.clear()
    engine.DecodoScraperClient._parfumo_cache.clear()
    engine.DecodoScraperClient._image_cache.clear()
    return saved


def _restore_decodo_env(saved: dict[str, str | None]) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    engine.DecodoScraperClient._cache.clear()
    engine.DecodoScraperClient._parfumo_cache.clear()
    engine.DecodoScraperClient._image_cache.clear()


def test_pick_launch_year_ignores_house_founding_year() -> None:
    print("Designer catalog launch-year checks:")
    label = "Casamorati 1888 Tempio D Acqua 2026"
    year = engine.TextSanitizer.pick_launch_year(label, brand="Casamorati 1888", name="Tempio D Acqua")
    check("house founding year is not stored as launch year", year == "2026", year)


def test_casamorati_tempio_catalog_identity_score() -> None:
    print("Casamorati Tempio catalog identity checks:")
    item = engine.UnifiedFragrance(name="Tempio D Acqua", brand="Casamorati 1888", year="2026")
    catalog = engine.CatalogItem(
        "Tempio D Acqua",
        "Casamorati 1888",
        engine.TextSanitizer.pick_launch_year(
            "Casamorati 1888 Tempio D Acqua 2026",
            brand="Casamorati 1888",
            name="Tempio D Acqua",
        ),
        "https://www.fragrantica.com/perfume/Casamorati-1888/Tempio-d-Acqua-128356.html",
    )
    score = engine.Orchestrator.identity_score(item, catalog)
    check("catalog row links at accept threshold", score >= engine.Orchestrator.CATALOG_ACCEPT, f"{score:.3f}")


def test_decodo_enabled_with_default_provider() -> None:
    print("Decodo default-provider enablement checks:")
    saved = _set_decodo_env(enabled=True, provider=None)
    try:
        check("DecodoScraperClient.enabled() defaults to Decodo with credentials", engine.DecodoScraperClient.enabled() is True, "")
        check(
            "structured_search_provider() selects Decodo by default",
            engine.structured_search_provider() is engine.DecodoScraperClient,
            str(engine.structured_search_provider()),
        )
    finally:
        _restore_decodo_env(saved)


def test_decodo_disabled_without_credentials() -> None:
    print("Decodo env-gating checks:")
    saved = _set_decodo_env(enabled=False)
    old_post = engine.requests.post
    calls = {"http": 0}
    try:
        def forbidden_post(*args, **kwargs):
            calls["http"] += 1
            raise AssertionError("requests.post must not be called when Decodo is disabled")

        engine.requests.post = forbidden_post
        enabled = engine.DecodoScraperClient.enabled()
        provider = engine.structured_search_provider()
        urls = engine.DecodoScraperClient.search_fragrantica_urls("xerjoff naxos")
        rows = engine.DecodoScraperClient.discover_fragrances("xerjoff naxos")
    finally:
        engine.requests.post = old_post
        _restore_decodo_env(saved)

    check("DecodoScraperClient.enabled() is False without credentials", enabled is False, str(enabled))
    check("structured_search_provider() returns None without credentials", provider is None, str(provider))
    check("disabled Decodo returns no URLs", urls == [], str(urls))
    check("disabled Decodo returns no rows", rows == [], str(rows))
    check("disabled Decodo makes no HTTP call", calls["http"] == 0, str(calls))


def test_decodo_rejects_serper_provider_env() -> None:
    print("Decodo-only provider selection checks:")
    saved = _set_decodo_env(enabled=True, provider="serper")
    try:
        check("Decodo is disabled when provider env explicitly requests Serper", engine.DecodoScraperClient.enabled() is False, "")
        check("Serper env no longer selects a structured provider", engine.structured_search_provider() is None, str(engine.structured_search_provider()))
    finally:
        _restore_decodo_env(saved)


def test_decodo_parses_fragrantica_urls() -> None:
    print("Decodo URL parsing checks:")
    saved = _set_decodo_env(enabled=True)
    old_post = engine.requests.post
    seen: dict[str, object] = {}
    try:
        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            seen["url"] = url
            seen["body"] = dict(json or {})
            seen["auth"] = (headers or {}).get("Authorization")
            return _FakeDecodoResponse(_DECODO_SAMPLE_PAYLOAD)

        engine.requests.post = fake_post
        urls = engine.DecodoScraperClient.search_fragrantica_urls("xerjoff naxos")
        rows = engine.DecodoScraperClient.discover_fragrances("xerjoff naxos")
    finally:
        engine.requests.post = old_post
        _restore_decodo_env(saved)

    body = seen.get("body") or {}
    check(
        "Decodo keeps only canonical Fragrantica perfume URLs",
        urls == [
            "https://www.fragrantica.com/perfume/Xerjoff/1861-Naxos-30529.html",
            "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html",
        ],
        str(urls),
    )
    check("Decodo request hits the scraper API endpoint", seen.get("url") == engine.DecodoScraperClient.ENDPOINT, str(seen.get("url")))
    check("Decodo request uses google_search target", body.get("target") == "google_search", str(body))
    check(
        "Decodo request is site-scoped to fragrantica perfume pages",
        body.get("query") == "xerjoff naxos site:fragrantica.com/perfume",
        str(body.get("query")),
    )
    check("Decodo request asks for parsed JSON", body.get("parse") is True, str(body))
    check("Decodo request sends Basic auth", seen.get("auth") == "Basic encoded-test-token", str(seen.get("auth")))

    naxos = next((row for row in rows if "Naxos" in row.name), None)
    identities = [(row.brand, row.name) for row in rows]
    check(
        "Decodo row recovers Fragrantica identity from the URL",
        naxos is not None and naxos.name == "1861 Naxos" and naxos.brand == "Xerjoff",
        str(identities),
    )
    check(
        "Decodo row carries the canonical Fragrantica URL",
        naxos is not None and naxos.frag_url == "https://www.fragrantica.com/perfume/Xerjoff/1861-Naxos-30529.html",
        str(naxos.frag_url if naxos else None),
    )
    check(
        "Decodo row is labelled with the Decodo resolver source",
        naxos is not None and naxos.resolver_source == "decodo_fragrantica_search",
        str(naxos.resolver_source if naxos else None),
    )


def test_decodo_caches_responses() -> None:
    print("Decodo response cache checks:")
    saved = _set_decodo_env(enabled=True)
    old_post = engine.requests.post
    calls = {"http": 0}
    try:
        def counting_post(url, json=None, headers=None, timeout=None, **kwargs):
            calls["http"] += 1
            return _FakeDecodoResponse(_DECODO_SAMPLE_PAYLOAD)

        engine.requests.post = counting_post
        first = engine.DecodoScraperClient.search_fragrantica_urls("creed aventus")
        second = engine.DecodoScraperClient.search_fragrantica_urls("creed aventus")
    finally:
        engine.requests.post = old_post
        _restore_decodo_env(saved)

    check("repeated Decodo query is served from the in-process cache", calls["http"] == 1, str(calls))
    check(
        "cached Decodo result matches the first live result",
        first == second and first != [],
        str((first, second)),
    )


def test_decodo_structured_spell_repair_evidence() -> None:
    """A typo'd query is repaired from structured SERP URLs alone.

    On datacenter hosts the Google/Bing HTML scrapes are dead, so the only SERP
    evidence QueryRepair.suggest() can harvest comes from the structured
    provider. Http.get is stubbed to return nothing to simulate exactly that.
    """
    print("Decodo structured spell-repair checks:")
    saved = _set_decodo_env(enabled=True)
    old_post = engine.requests.post
    old_get = engine.Http.get
    posted_queries: list[str] = []
    aventus_payload = {
        "results": [
            {
                "content": {
                    "results": {
                        "organic": [
                            {"url": "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html"},
                        ]
                    }
                }
            }
        ]
    }
    try:
        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            posted_queries.append(str((json or {}).get("query") or ""))
            return _FakeDecodoResponse(aventus_payload)

        engine.requests.post = fake_post
        engine.Http.get = staticmethod(lambda *args, **kwargs: None)
        suggestion = engine.QueryRepair.suggest(None, "creed avantus", seconds=2.0)
    finally:
        engine.requests.post = old_post
        engine.Http.get = old_get
        _restore_decodo_env(saved)

    check(
        "structured spell repair queries the provider with the typo'd query",
        any(q.startswith("creed avantus ") for q in posted_queries),
        str(posted_queries),
    )
    check(
        "typo'd query is repaired from structured SERP URLs",
        suggestion.lower() == "creed aventus",
        repr(suggestion),
    )

    # Each abandoned provider call still spends a credit, so a budget too small
    # for Decodo's measured latency must skip the structured leg entirely.
    saved = _set_decodo_env(enabled=True)
    starved_posts: list[str] = []
    try:
        def counting_post(url, json=None, headers=None, timeout=None, **kwargs):
            starved_posts.append(str((json or {}).get("query") or ""))
            return _FakeDecodoResponse(aventus_payload)

        engine.requests.post = counting_post
        engine.Http.get = staticmethod(lambda *args, **kwargs: None)
        engine.QueryRepair.suggest(None, "creed avantus", seconds=0.5)
    finally:
        engine.requests.post = old_post
        engine.Http.get = old_get
        _restore_decodo_env(saved)

    check(
        "structured spell repair skips the provider when the budget is too small",
        starved_posts == [],
        str(starved_posts),
    )


def test_decodo_url_discovery_budget_gate() -> None:
    print("Decodo URL-discovery budget gate checks:")
    # Mirrors the spell-repair gate: an abandoned provider call still spends a
    # credit, so a first-pass FG budget below MIN_VIABLE_BUDGET must skip the
    # request entirely instead of starting one that cannot finish.
    saved = _set_decodo_env(enabled=True)
    old_post = engine.requests.post
    posts: list[str] = []
    try:
        def counting_post(url, json=None, headers=None, timeout=None, **kwargs):
            posts.append(str((json or {}).get("query") or ""))
            return _FakeDecodoResponse(_DECODO_SAMPLE_PAYLOAD)

        engine.requests.post = counting_post
        starved = engine.DecodoScraperClient.search_fragrantica_urls(
            "creed aventus", deadline=engine.Deadline(0.4)
        )
        check(
            "starved Fragrantica discovery budget skips the provider call",
            starved == [] and posts == [],
            str(posts),
        )
        urls = engine.DecodoScraperClient.search_fragrantica_urls(
            "creed aventus", deadline=engine.Deadline(4.5)
        )
        check(
            "viable Fragrantica discovery budget performs the call",
            len(posts) == 1 and any("Aventus" in url for url in urls),
            f"{posts} {urls}",
        )
        parfumo_starved = engine.DecodoScraperClient.search_parfumo_urls(
            "Creed", "Aventus", deadline=engine.Deadline(0.4)
        )
        check(
            "starved Parfumo discovery budget skips the provider call",
            parfumo_starved == [] and len(posts) == 1,
            str(posts),
        )
    finally:
        engine.requests.post = old_post
        _restore_decodo_env(saved)


def test_decodo_url_discovery_failure_degrades_without_caching() -> None:
    print("Decodo URL-discovery failure handling checks:")
    saved = _set_decodo_env(enabled=True)
    old_post = engine.requests.post
    attempts = {"n": 0}
    try:
        def timing_out_post(url, json=None, headers=None, timeout=None, **kwargs):
            attempts["n"] += 1
            raise engine.requests.exceptions.ReadTimeout("Read timed out. (read timeout=3.0)")

        engine.requests.post = timing_out_post
        first = engine.DecodoScraperClient.search_fragrantica_urls("xerjoff naxos")
        second = engine.DecodoScraperClient.search_fragrantica_urls("xerjoff naxos")
        check("provider timeout degrades to no URLs", first == [] and second == [], f"{first} {second}")
        check(
            "a failed discovery call is retried, not cached as empty",
            attempts["n"] == 2,
            str(attempts["n"]),
        )
    finally:
        engine.requests.post = old_post
        _restore_decodo_env(saved)


def test_initio_prives_decodo_url_survives_merge() -> None:
    print("Initio Prives merge checks:")
    bn = engine.UnifiedFragrance(
        name="High Frequency",
        brand="Initio",
        year="2016",
        bn_url="https://basenotes.com/fragrances/high-frequency-by-initio.26158110",
    )
    fg = engine.UnifiedFragrance(
        name="High Frequency",
        brand="Initio Parfums Prives",
        year="2016",
        frag_url="https://www.fragrantica.com/perfume/Initio-Parfums-Prives/High-Frequency-42259.html",
        resolver_source="decodo_fragrantica_search",
    )
    merged = engine.Orchestrator.match_and_merge([bn], [fg])

    check(
        "generic house suffixes do not block the Decodo Fragrantica URL",
        merged[0].frag_url == fg.frag_url
        and merged[0].resolver_source == "decodo_fragrantica_search",
        f"{merged[0].brand} | {merged[0].name} | {merged[0].frag_url}",
    )


def main() -> int:
    test_live_candidate_filter()
    test_sub_brand_catalog_keys_for_casamorati()
    test_compatible_catalog_brand_accepts_parent_line()
    test_line_alias_queries_are_treated_as_brand_tokens()
    test_margiela_replica_line_aliases_merge_and_filter()
    test_native_search_unusable_detects_junk()
    test_brand_plus_name_filter()
    test_multi_token_name_requires_distinctive_coverage()
    test_normalize_notes_flattens_to_independent_layers()
    test_cache_search_rejects_same_house_sibling()
    test_aggregate_cache_search_does_not_expose_unproven_image()
    test_cache_identity_fill_rejects_sibling_image()
    test_api_cache_threshold_matches_engine()
    test_api_cache_fallback_uses_long_anchor_variants()
    test_api_strong_cache_precheck_skips_live_identity_hit()
    test_api_strong_cache_precheck_does_not_bypass_brand_only()
    test_api_bn_only_cache_hit_does_not_skip_live_search()
    test_api_identity_cache_rescues_bn_only_precheck()
    test_api_live_search_saturation_uses_cache_fallback()
    test_fragrantica_native_search_bypassed_by_default()
    test_pick_launch_year_ignores_house_founding_year()
    test_casamorati_tempio_catalog_identity_score()
    test_decodo_disabled_without_credentials()
    test_decodo_enabled_with_default_provider()
    test_decodo_rejects_serper_provider_env()
    test_decodo_parses_fragrantica_urls()
    test_decodo_caches_responses()
    test_decodo_structured_spell_repair_evidence()
    test_initio_prives_decodo_url_survives_merge()
    test_strip_house_from_name()
    test_q_relevance_is_word_based()
    test_brand_alias_query_keeps_house_catalog()
    test_dolce_gabbana_identity_recovery_and_persistence()
    test_house_echo_poison_is_suppressed()
    test_brand_only_query_gets_catalog_labels()
    test_poisoned_db_records_are_filtered()
    test_short_db_search_uses_word_boundaries()
    test_search_serialization_recovers_fragrantica_identity()
    test_search_serialization_recovers_basenotes_identity()
    test_basenotes_slug_splits_on_final_by()
    test_details_request_recovers_identity_from_legacy_blank_id()
    test_details_request_repairs_legacy_poisoned_id()
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
