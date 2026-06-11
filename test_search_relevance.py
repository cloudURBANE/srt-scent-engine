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
        resolver_source="serper_fragrantica_search",
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


class _FakeSerperResponse:
    """Minimal stand-in for a requests.Response from google.serper.dev."""

    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise engine.requests.HTTPError(f"HTTP {self.status_code}")
        return None

    def json(self) -> dict:
        return self._payload


_SERPER_SAMPLE_PAYLOAD = {
    "organic": [
        {"link": "https://www.fragrantica.com/perfume/Xerjoff/1861-Naxos-30529.html"},
        {"link": "https://example.com/not-fragrance"},
        {"link": "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html?utm=1"},
    ]
}


def _set_serper_env(enabled: bool) -> dict[str, str | None]:
    saved = {
        "SERP_API_PROVIDER": os.environ.get("SERP_API_PROVIDER"),
        "SERPER_API_KEY": os.environ.get("SERPER_API_KEY"),
        "SERPER_API_KEYS": os.environ.get("SERPER_API_KEYS"),
    }
    if enabled:
        os.environ["SERP_API_PROVIDER"] = "serper"
        os.environ["SERPER_API_KEY"] = "test-key-not-real"
        os.environ.pop("SERPER_API_KEYS", None)
    else:
        os.environ.pop("SERP_API_PROVIDER", None)
        os.environ.pop("SERPER_API_KEY", None)
        os.environ.pop("SERPER_API_KEYS", None)
    return saved


def _restore_serper_env(saved: dict[str, str | None]) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


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


def test_serper_enabled_with_key_only() -> None:
    print("Serper key-only enablement checks:")
    saved = _set_serper_env(enabled=False)
    os.environ["SERPER_API_KEY"] = "test-key-not-real"
    try:
        check("SerperClient.enabled() with key only", engine.SerperClient.enabled() is True, "")
    finally:
        _restore_serper_env(saved)


def test_serper_rotates_after_exhausted_key() -> None:
    print("Serper key-pool rotation checks:")
    engine.SerperClient._cache.clear()
    saved = _set_serper_env(enabled=False)
    old_post = engine.requests.post
    old_pool = engine._SERPER_POOL
    calls: list[str | None] = []
    try:
        os.environ["SERP_API_PROVIDER"] = "serper"
        os.environ["SERPER_API_KEYS"] = "test-exhausted-key,test-live-key"
        engine._SERPER_POOL = engine.SerperKeyPool(rate_limit_cooldown_s=1.0)

        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            api_key = (headers or {}).get("X-API-KEY")
            calls.append(api_key)
            if api_key == "test-exhausted-key":
                return _FakeSerperResponse({}, status_code=402)
            return _FakeSerperResponse(_SERPER_SAMPLE_PAYLOAD)

        engine.requests.post = fake_post
        urls = engine.SerperClient.search_fragrantica_urls("pool rotation")
        snapshot = engine.serper_key_pool().snapshot()
    finally:
        engine.requests.post = old_post
        engine._SERPER_POOL = old_pool
        _restore_serper_env(saved)

    check(
        "exhausted key is followed by the next pool key",
        calls == ["test-exhausted-key", "test-live-key"],
        str(calls),
    )
    check(
        "rotation still returns Serper results",
        urls[:1] == ["https://www.fragrantica.com/perfume/Xerjoff/1861-Naxos-30529.html"],
        str(urls),
    )
    key_statuses = [entry["status"] for entry in snapshot.get("keys", [])]
    check(
        "exhausted key is retired in diagnostics",
        any(status == "retired" for status in key_statuses),
        str(snapshot),
    )


def test_serper_disabled_without_env() -> None:
    print("Serper env-gating checks:")
    engine.SerperClient._cache.clear()
    saved = _set_serper_env(enabled=False)
    old_post = engine.requests.post
    calls = {"http": 0}
    try:
        def forbidden_post(*args, **kwargs):
            calls["http"] += 1
            raise AssertionError("requests.post must not be called when Serper is disabled")

        engine.requests.post = forbidden_post
        enabled = engine.SerperClient.enabled()
        urls = engine.SerperClient.search_fragrantica_urls("xerjoff naxos")
        rows = engine.SerperClient.discover_fragrances("xerjoff naxos")
    finally:
        engine.requests.post = old_post
        _restore_serper_env(saved)

    check("SerperClient.enabled() is False without env", enabled is False, str(enabled))
    check("disabled Serper returns no URLs", urls == [], str(urls))
    check("disabled Serper returns no rows", rows == [], str(rows))
    check("disabled Serper makes no HTTP call", calls["http"] == 0, str(calls))


def test_serper_parses_fragrantica_urls() -> None:
    print("Serper URL parsing checks:")
    engine.SerperClient._cache.clear()
    saved = _set_serper_env(enabled=True)
    old_post = engine.requests.post
    seen: dict[str, object] = {}
    try:
        def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
            seen["url"] = url
            seen["query"] = (json or {}).get("q")
            seen["api_key"] = (headers or {}).get("X-API-KEY")
            return _FakeSerperResponse(_SERPER_SAMPLE_PAYLOAD)

        engine.requests.post = fake_post
        urls = engine.SerperClient.search_fragrantica_urls("xerjoff naxos")
        rows = engine.SerperClient.discover_fragrances("xerjoff naxos")
    finally:
        engine.requests.post = old_post
        _restore_serper_env(saved)

    check(
        "Serper keeps only canonical Fragrantica perfume URLs",
        urls == [
            "https://www.fragrantica.com/perfume/Xerjoff/1861-Naxos-30529.html",
            "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html",
        ],
        str(urls),
    )
    check(
        "Serper request is site-scoped to fragrantica perfume pages",
        seen.get("query") == "xerjoff naxos site:fragrantica.com/perfume",
        str(seen.get("query")),
    )
    check("Serper request sends the API key header", seen.get("api_key") == "test-key-not-real", str(seen.get("api_key")))
    check("Serper request hits the serper.dev endpoint", seen.get("url") == engine.SerperClient.ENDPOINT, str(seen.get("url")))

    naxos = next((row for row in rows if "Naxos" in row.name), None)
    identities = [(row.brand, row.name) for row in rows]
    check(
        "Serper row recovers Fragrantica identity from the URL",
        naxos is not None and naxos.name == "1861 Naxos" and naxos.brand == "Xerjoff",
        str(identities),
    )
    check(
        "Serper row carries the canonical Fragrantica URL",
        naxos is not None and naxos.frag_url == "https://www.fragrantica.com/perfume/Xerjoff/1861-Naxos-30529.html",
        str(naxos.frag_url if naxos else None),
    )
    check(
        "Serper row is labelled with the serper resolver source",
        naxos is not None and naxos.resolver_source == "serper_fragrantica_search",
        str(naxos.resolver_source if naxos else None),
    )


def test_serper_caches_responses() -> None:
    print("Serper response cache checks:")
    engine.SerperClient._cache.clear()
    saved = _set_serper_env(enabled=True)
    old_post = engine.requests.post
    calls = {"http": 0}
    try:
        def counting_post(url, json=None, headers=None, timeout=None, **kwargs):
            calls["http"] += 1
            return _FakeSerperResponse(_SERPER_SAMPLE_PAYLOAD)

        engine.requests.post = counting_post
        first = engine.SerperClient.search_fragrantica_urls("creed aventus")
        second = engine.SerperClient.search_fragrantica_urls("creed aventus")
    finally:
        engine.requests.post = old_post
        _restore_serper_env(saved)

    check("repeated Serper query is served from the in-process cache", calls["http"] == 1, str(calls))
    check(
        "cached Serper result matches the first live result",
        first == second and first != [],
        str((first, second)),
    )


def test_initio_prives_serper_url_survives_merge() -> None:
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
        resolver_source="serper_fragrantica_search",
    )
    merged = engine.Orchestrator.match_and_merge([bn], [fg])

    check(
        "generic house suffixes do not block the Serper Fragrantica URL",
        merged[0].frag_url == fg.frag_url
        and merged[0].resolver_source == "serper_fragrantica_search",
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
    test_api_strong_cache_precheck_skips_live_identity_hit()
    test_api_strong_cache_precheck_does_not_bypass_brand_only()
    test_api_bn_only_cache_hit_does_not_skip_live_search()
    test_api_identity_cache_rescues_bn_only_precheck()
    test_api_live_search_saturation_uses_cache_fallback()
    test_fragrantica_native_search_bypassed_by_default()
    test_pick_launch_year_ignores_house_founding_year()
    test_casamorati_tempio_catalog_identity_score()
    test_serper_disabled_without_env()
    test_serper_enabled_with_key_only()
    test_serper_rotates_after_exhausted_key()
    test_serper_parses_fragrantica_urls()
    test_serper_caches_responses()
    test_initio_prives_serper_url_survives_merge()
    test_strip_house_from_name()
    test_q_relevance_is_word_based()
    test_dolce_gabbana_identity_recovery_and_persistence()
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
