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


def test_stopword_names_are_not_rejected() -> None:
    # Regression: candidate_relevance_ok compared query tokens that KEEP
    # stopwords (eau/de/le/la/pour/homme/intense...) against target name tokens
    # that STRIP them, deflating fuzzy coverage so that even exact, score=1.0
    # identity matches were rejected. This nuked both the live filter and the DB
    # fallback for any fragrance whose name carries a stopword, e.g. the user
    # report of "Bleu de Chanel" returning zero rows.
    print("Stopword-name relevance checks:")
    exact_matches = [
        ("Bleu de Chanel", "Chanel", "Bleu De Chanel Eau De Parfum"),
        ("Eau Sauvage", "Dior", "Eau Sauvage"),
        ("L'Eau d'Issey", "Issey Miyake", "L'Eau d'Issey"),
        ("La Nuit de L'Homme", "Yves Saint Laurent", "La Nuit De L'Homme"),
    ]
    for query, brand, name in exact_matches:
        row = candidate(brand, name)
        names = [item.name for item in engine.filter_relevant_candidates(query, [row])]
        check(
            f"stopword name kept for {query!r}",
            names == [name],
            str(names),
        )

    # The sibling guard must still hold: a same-house row that drops the
    # distinctive non-stopword token is still rejected even after the alignment.
    sibling = candidate("Chanel", "Coco Mademoiselle")
    names = [item.name for item in engine.filter_relevant_candidates("Bleu de Chanel", [sibling])]
    check("non-matching same-house sibling still rejected", names == [], str(names))


def test_multi_token_brand_only_query_keeps_whole_house() -> None:
    # Regression: candidate_relevance_ok's distinctive-name-token coverage check
    # rejected multi-token *brand-only* queries. The query's name tokens collapse
    # to the full brand once the brand forms cancel, then get compared against the
    # candidate's name tokens (brand stripped) for ~0 coverage -- so every row of
    # the house was dropped. Single-token houses ("Xerjoff") slipped through only
    # via the len<2 early-out, while "Maison Francis Kurkdjian", "Imaginary
    # Authors", and the flagged "Thom Browne" returned zero rows even when the
    # house's fragrances were sitting in the DB. relevance_score already treats a
    # brand-only query as a 1.0 match; candidate_relevance_ok must agree.
    print("Multi-token brand-only relevance checks:")
    house_rows = [
        ("Maison Francis Kurkdjian", "Baccarat Rouge 540"),
        ("Imaginary Authors", "Memoirs of a Trespasser"),
        ("Thom Browne", "Eau de Parfum"),
    ]
    for brand, name in house_rows:
        row = candidate(brand, name)
        names = [item.name for item in engine.filter_relevant_candidates(brand, [row])]
        check(
            f"bare-brand query {brand!r} keeps its catalogue row",
            names == [name],
            str(names),
        )

    # A genuine brand+name query (not brand-only) must still demand distinctive
    # name coverage: the named row survives, a same-house sibling is rejected.
    named = candidate("Maison Francis Kurkdjian", "Grand Soir")
    sibling = candidate("Maison Francis Kurkdjian", "Baccarat Rouge 540")
    kept = [
        item.name
        for item in engine.filter_relevant_candidates(
            "Maison Francis Kurkdjian Grand Soir", [named, sibling]
        )
    ]
    check("brand+name query keeps the named row", "Grand Soir" in kept, str(kept))
    check("brand+name query still rejects non-matching sibling", "Baccarat Rouge 540" not in kept, str(kept))


def test_typed_concentration_keeps_exact_match() -> None:
    # Regression for the search screenshots: a user who types the concentration
    # ("Lancome Idole Eau de Toilette", "Dior Sauvage Eau de Toilette") kept the
    # eau/de/toilette tokens on the query side, while name_tokens() strips them
    # from the candidate -- so an otherwise exact, score=1.0 hit fell under the
    # coverage floor and was discarded. The stopword-symmetric fix in
    # candidate_relevance_ok aligns both sides; this guards the specific typed-
    # concentration case (distinct from the canonical stopword-name case in
    # test_stopword_names_are_not_rejected) without weakening the same-house guard.
    print("Typed-concentration relevance gate checks:")
    idole = engine.filter_relevant_candidates(
        "Lancome Idole Eau de Toilette",
        [candidate("Lancome", "Idole Eau de Toilette"),
         candidate("Lancome", "La Vie Est Belle")],
    )
    idole_names = [item.name for item in idole]
    check("typed concentration keeps the exact match",
          "Idole Eau de Toilette" in idole_names, str(idole_names))
    check("typed concentration still drops unrelated same-house row",
          "La Vie Est Belle" not in idole_names, str(idole_names))

    sauvage = engine.filter_relevant_candidates(
        "Dior Sauvage Eau de Toilette",
        [candidate("Dior", "Sauvage Eau de Toilette")],
    )
    check("Dior Sauvage Eau de Toilette keeps the exact match",
          [item.name for item in sauvage] == ["Sauvage Eau de Toilette"],
          str([item.name for item in sauvage]))


def test_concentration_phrase_is_not_a_required_name_marker() -> None:
    # Regression: required_name_markers() treated the "eau" inside the
    # concentration descriptor "Eau de Parfum/Toilette/Cologne" as a required
    # name marker. A bare base title ("No 5") therefore scored 0.00 against its
    # own EDP catalog entry ("No 5 Eau de Parfum") -- even though "No 5 Parfum"
    # scored 0.96 -- so the worker resolver rejected the correct Chanel No 5 page
    # and completed the job with fg_url=None, leaving the wardrobe row stuck on
    # junk (citrus/Comete) data. Stripping the concentration phrase before marker
    # extraction fixes the asymmetry while keeping genuine "Eau ..." name words
    # ("Eau Sauvage", "Eau de Cartier") distinct from their non-"eau" siblings.
    O = engine.Orchestrator
    print("Concentration-phrase marker checks:")

    def score(an: str, ab: str, bn: str, bb: str) -> float:
        a = engine.UnifiedFragrance(name=an, brand=ab, year="")
        b = engine.CatalogItem(name=bn, brand=bb, year="", url="x")
        return O.identity_score(a, b)

    # The bug: bare base title must now match its own concentration variant.
    for variant in ("No 5 Eau de Parfum", "Chanel No 5 Eau de Parfum",
                    "No 5 Eau de Toilette", "No 5 Eau de Cologne"):
        s = score("No 5", "Chanel", variant, "Chanel")
        check(f"'No 5' matches {variant!r}", s >= O.CATALOG_ACCEPT, f"score={s:.3f}")

    # A genuinely different flanker is NOT a concentration variant: stays low.
    s = score("No 5", "Chanel", "No 5 Eau Premiere", "Chanel")
    check("'No 5' still rejects 'No 5 Eau Premiere' flanker", s == 0.0, f"score={s:.3f}")

    # Regressions: a real "Eau ..." NAME word must stay distinct from its sibling.
    s = score("Sauvage", "Dior", "Eau Sauvage", "Dior")
    check("'Sauvage' stays distinct from 'Eau Sauvage'", s == 0.0, f"score={s:.3f}")
    s = score("Cartier", "Cartier", "Eau de Cartier", "Cartier")
    check("'Cartier' stays distinct from 'Eau de Cartier'", s == 0.0, f"score={s:.3f}")
    # ...but each of those still matches its OWN concentration variant.
    s = score("Eau Sauvage", "Dior", "Eau Sauvage Eau de Parfum", "Dior")
    check("'Eau Sauvage' matches its EDP variant", s >= O.CATALOG_ACCEPT, f"score={s:.3f}")

    # When removing the house leaves only stopwords, the fallback scorer must
    # strip the concentration phrase too. Otherwise these valid pairs score
    # 0.68, below CATALOG_ACCEPT, despite retaining their genuine leading "Eau".
    for base, brand, variant in (
        ("Eau de Cartier", "Cartier", "Eau de Cartier Eau de Parfum"),
        ("Eau de Rochas", "Rochas", "Eau de Rochas Eau de Toilette"),
    ):
        for left, right in ((base, variant), (variant, base)):
            s = score(left, brand, right, brand)
            check(
                f"{left!r} matches {right!r}",
                s >= O.CATALOG_ACCEPT,
                f"score={s:.3f}",
            )


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


def test_api_fact_question_answers_from_stored_record() -> None:
    print("API fact-question answer checks:")
    old_strong = api._strong_cache_search
    old_lookup = api.db.lookup_fragrance_record
    old_record_search = api.db.search_fragrance_records
    old_recover = api.db.recover_or_enqueue_job
    old_search_once = api.engine.search_once
    item = engine.UnifiedFragrance(
        name="Aventus",
        brand="Creed",
        year="2010",
        frag_url="https://www.fragrantica.com/perfume/Creed/Aventus-9828.html",
    )
    record = {
        "canonical_fg_url": item.frag_url,
        "bn_url": "",
        "name": "Aventus",
        "house": "Creed",
        "year": 2010,
        "gender": "Men",
        "fg_raw": {"concentration": "Eau de Parfum", "raw_identity": {"concentration": "Eau de Parfum"}},
        "bn_raw": {},
        "derived_metrics": None,
        "source_captured_at": "2099-01-01T00:00:00+00:00",
    }
    calls: dict[str, object] = {"recovered": False, "strong_query": ""}
    try:
        api._strong_cache_search = lambda query, limit, min_score=api._STRONG_CACHE_MIN_SCORE: (
            calls.update({"strong_query": query}) or ([item], "unit")
        )
        api.db.lookup_fragrance_record = lambda canonical_fg_url=None, bn_url=None: record
        api.db.search_fragrance_records = lambda query, limit=15: []
        api.db.recover_or_enqueue_job = lambda **kwargs: calls.update({"recovered": True}) or None
        api.engine.search_once = lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("live search should not run for strong fact-question hit")
        )
        response = api.search(q="what concentration is Creed Aventus?")
    finally:
        api._strong_cache_search = old_strong
        api.db.lookup_fragrance_record = old_lookup
        api.db.search_fragrance_records = old_record_search
        api.db.recover_or_enqueue_job = old_recover
        api.engine.search_once = old_search_once

    fact = response.get("fact_query") or {}
    result_fact = (response.get("results") or [{}])[0].get("requested_fact") or {}
    check("fact-question strips shell before search", calls["strong_query"] == "Creed Aventus", str(calls))
    check("fact-question response keeps original query", response.get("query") == "what concentration is Creed Aventus?", str(response))
    check("fact-question returns extracted search query", response.get("search_query") == "Creed Aventus", str(response))
    check("fact-question answers concentration from stored record", fact.get("value") == "Eau de Parfum", str(fact))
    check("result carries requested fact", result_fact.get("status") == "answered" and result_fact.get("field") == "concentration", str(result_fact))
    check("answered fact does not enqueue recovery", calls["recovered"] is False, str(calls))
    check("plain 'Year of ...' title is not treated as a fact query", api._parse_fact_query("Year of the Dragon") is None)


def test_api_fact_question_recovers_missing_fact() -> None:
    print("API fact-question recovery checks:")
    old_strong = api._strong_cache_search
    old_lookup = api.db.lookup_fragrance_record
    old_record_search = api.db.search_fragrance_records
    old_recover = api.db.recover_or_enqueue_job
    item = engine.UnifiedFragrance(
        name="Unmapped Test",
        brand="Creed",
        year="",
        frag_url="https://www.fragrantica.com/perfume/Creed/Unmapped-Test-999.html",
    )
    record = {
        "canonical_fg_url": item.frag_url,
        "bn_url": "",
        "name": "Unmapped Test",
        "house": "Creed",
        "year": None,
        "gender": "",
        "fg_raw": {},
        "bn_raw": {},
        "derived_metrics": None,
        "source_captured_at": "2099-01-01T00:00:00+00:00",
    }
    calls: dict[str, object] = {}
    try:
        api._strong_cache_search = lambda query, limit, min_score=api._STRONG_CACHE_MIN_SCORE: ([item], "unit")
        api.db.lookup_fragrance_record = lambda canonical_fg_url=None, bn_url=None: record
        api.db.search_fragrance_records = lambda query, limit=15: []

        def recover(**kwargs):
            calls.update(kwargs)
            return {"status": "pending", "requested_count": 2}

        api.db.recover_or_enqueue_job = recover
        response = api.search(q="what is the fragrance family for Creed Unmapped Test")
    finally:
        api._strong_cache_search = old_strong
        api.db.lookup_fragrance_record = old_lookup
        api.db.search_fragrance_records = old_record_search
        api.db.recover_or_enqueue_job = old_recover

    fact = response.get("fact_query") or {}
    result_fact = (response.get("results") or [{}])[0].get("requested_fact") or {}
    check("missing fact reports missing", fact.get("status") == "missing" and fact.get("value") is None, str(fact))
    check("missing fact queues recovery", fact.get("enrichment", {}).get("status") == "pending", str(fact))
    check("recovery uses user-question priority", calls.get("priority") == 10, str(calls))
    check("recovery carries original question for traceability", calls.get("query") == "what is the fragrance family for Creed Unmapped Test", str(calls))
    check("result carries missing requested fact", result_fact.get("field") == "family" and result_fact.get("status") == "missing", str(result_fact))


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


def test_multi_token_bare_brand_seeds_catalog_labels() -> None:
    print("Multi-token bare-brand catalog-seed checks:")
    # The bug: a *multi-token* house we don't alias ("Frederic Malle") fell
    # through search_once's single-token-only catalog seed with empty labels, so
    # it got zero designer-catalog breadth and returned zero rows whenever the
    # structured (Decodo) provider leg was slow/timed out -- even though its
    # Fragrantica designer page is crawlable. bare_brand_label() recovers it from
    # the dominant first-pass house, while leaving brand+fragrance queries alone.
    def rows(brand: str, count: int) -> list:
        return [engine.UnifiedFragrance(name=f"Frag {i}", brand=brand, year="") for i in range(count)]

    malle = rows("Frederic Malle", 8)
    check(
        "bare multi-token house resolves to its catalog label",
        engine.IdentityTools.bare_brand_label("Frederic Malle", malle) == "Frederic Malle",
        engine.IdentityTools.bare_brand_label("Frederic Malle", malle),
    )
    check(
        "recovered label feeds a non-empty catalog crawl",
        engine.IdentityTools.catalog_brand_keys(
            engine.IdentityTools.bare_brand_label("Frederic Malle", malle)
        ) == ["Frederic Malle"],
        str(engine.IdentityTools.catalog_brand_keys(
            engine.IdentityTools.bare_brand_label("Frederic Malle", malle)
        )),
    )
    # Brand+fragrance multi-token query: dominant house is "Tom Ford" but the
    # query carries leftover name tokens ("oud wood"), so it must NOT be treated
    # as a bare house (that would crawl a non-existent "Tom Ford Oud Wood" page).
    tf = rows("Tom Ford", 6)
    check(
        "brand+fragrance multi-token query is not treated as a bare house",
        engine.IdentityTools.bare_brand_label("Tom Ford Oud Wood", tf) == "",
        engine.IdentityTools.bare_brand_label("Tom Ford Oud Wood", tf),
    )
    # A single stray row matching the query must not trigger a crawl: the house
    # has to dominate the first-pass rows.
    mixed = rows("Some Other House", 7) + rows("Frederic Malle", 1)
    check(
        "a lone matching row does not trigger a bare-house crawl",
        engine.IdentityTools.bare_brand_label("Frederic Malle", mixed) == "",
        engine.IdentityTools.bare_brand_label("Frederic Malle", mixed),
    )
    check(
        "no first-pass rows yields no bare-house label",
        engine.IdentityTools.bare_brand_label("Frederic Malle", []) == "",
        engine.IdentityTools.bare_brand_label("Frederic Malle", []),
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
    "DECODO_ENABLE_BING_FALLBACK",
    "DECODO_ENABLE_AI_OVERVIEW",
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
    engine.DecodoScraperClient._designer_cache.clear()
    engine.DecodoScraperClient._brand_perfume_cache.clear()
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
    engine.DecodoScraperClient._designer_cache.clear()
    engine.DecodoScraperClient._brand_perfume_cache.clear()
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


def test_decodo_legacy_provider_env_does_not_block_credentials() -> None:
    print("Decodo legacy-provider selection checks:")
    saved = _set_decodo_env(enabled=True, provider="serper")
    try:
        check("Decodo stays enabled when stale Serper env is present", engine.DecodoScraperClient.enabled() is True, "")
        check(
            "stale Serper env selects Decodo when Decodo credentials exist",
            engine.structured_search_provider() is engine.DecodoScraperClient,
            str(engine.structured_search_provider()),
        )
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


def test_known_brand_query_skips_spell_repair() -> None:
    print("Known-brand spell-repair bypass checks:")
    saved = _set_decodo_env(enabled=True)
    old_get = engine.Http.get
    old_search_urls = engine.DecodoScraperClient.search_fragrantica_urls
    calls = {"http": 0, "decodo": 0}
    try:
        def fail_http(*args, **kwargs):
            calls["http"] += 1
            raise AssertionError("known brand query should not hit HTTP spell repair")

        def fail_decodo(cls, *args, **kwargs):
            calls["decodo"] += 1
            raise AssertionError("known brand query should not hit structured spell repair")

        engine.Http.get = staticmethod(fail_http)
        engine.DecodoScraperClient.search_fragrantica_urls = classmethod(fail_decodo)
        suggestion = engine.QueryRepair.suggest(None, "Thom Browne", seconds=2.0)
    finally:
        engine.Http.get = old_get
        engine.DecodoScraperClient.search_fragrantica_urls = old_search_urls
        _restore_decodo_env(saved)

    check("exact known brand query is not rewritten", suggestion == "", repr(suggestion))
    check("spell-repair network paths were skipped", calls == {"http": 0, "decodo": 0}, str(calls))


def test_ranking_tail_is_stripped_from_spell_repair() -> None:
    print("Listicle/ranking-tail spell-repair checks:")
    # Google autocomplete answers a bare house query ("frederic malle") with
    # listicle tails like "frederic malle ranked". The tail survived
    # clean_suggestion (CUT_AFTER_RE had best|top|list|rated but not the rank
    # family), so it was added as a direct candidate and won as a "repair" --
    # corrupting "Frederic Malle" -> "frederic malle ranked" and wasting the
    # spell-repair budget on a bare-brand miss. The tail must now be cut so the
    # candidate collapses back to the original query (a no-op, not a repair).
    QR = engine.QueryRepair
    for tail in ("ranked", "ranking", "rankings", "rank", "ranks"):
        cleaned = QR.clean_suggestion(f"frederic malle {tail}")
        check(
            f"'{tail}' listicle tail is stripped",
            QR._identity(cleaned) == QR._identity("frederic malle"),
            repr(cleaned),
        )
    # Word-boundary safety: names that merely contain the substring "rank"
    # ("frank", "franck") must be preserved.
    check(
        "ranking cut does not truncate 'frank'/'franck' names",
        QR.clean_suggestion("frank olivier black") == "frank olivier black"
        and QR.clean_suggestion("franck boclet cocaine") == "franck boclet cocaine",
        repr((QR.clean_suggestion("frank olivier black"), QR.clean_suggestion("franck boclet cocaine"))),
    )


def test_volume_tail_is_stripped_from_spell_repair() -> None:
    print("Bottle-size/volume-tail spell-repair checks:")
    # Google autocomplete answers a real brand+fragrance query ("frederic malle
    # carnal flower") with a retail tail carrying the bottle size ("... 50 ml").
    # A bare volume unit was not in CUT_AFTER_RE, so the "50 ml" tail survived
    # clean_suggestion, was added as a direct candidate and won as a "repair" --
    # corrupting the query and burning the spell-repair budget so the downstream
    # Decodo designer-discovery leg read-timed-out and the query returned zero
    # rows. The tail must now be cut so the candidate collapses back to the
    # original query (a no-op, not a repair). Same family as the rank-tail fix.
    QR = engine.QueryRepair
    for tail in ("50 ml", "100ml", "70 ml", "3.4 oz", "100 ml for men"):
        cleaned = QR.clean_suggestion(f"frederic malle carnal flower {tail}")
        check(
            f"'{tail}' volume tail is stripped",
            QR._identity(cleaned) == QR._identity("frederic malle carnal flower"),
            repr(cleaned),
        )
    # Safety: a leading digit is required, and real name tokens that merely
    # contain numbers or look unit-ish must be preserved in full.
    for name in ("baccarat rouge 540", "chanel no 5", "by kilian 461", "amber oud 24k gold"):
        check(
            f"volume cut preserves the name {name!r}",
            QR.clean_suggestion(name) == name,
            repr(QR.clean_suggestion(name)),
        )


def test_decodo_post_request_splits_connect_and_read_timeout() -> None:
    print("Decodo connect/read timeout-split checks:")
    # `requests` applies a single float timeout to the connect and read phases
    # *separately*, so a lone in-flight call started near the overall deadline
    # could run up to ~2x its budget and overshoot the wall-clock ceiling. The
    # client must pass a (connect, read) tuple: read bound to the clamped budget
    # and connect capped to the small CONNECT_TIMEOUT.
    saved = _set_decodo_env(enabled=True)
    old_post = engine.requests.post
    seen: dict[str, object] = {}
    try:
        def capturing_post(url, json=None, headers=None, timeout=None, **kwargs):
            seen["timeout"] = timeout
            return _FakeDecodoResponse(_DECODO_SAMPLE_PAYLOAD)

        engine.requests.post = capturing_post
        engine.DecodoScraperClient._post_google("xerjoff naxos", 10.0)
    finally:
        engine.requests.post = old_post
        _restore_decodo_env(saved)

    timeout = seen.get("timeout")
    cap = engine.DecodoScraperClient.CONNECT_TIMEOUT
    expected_read = min(10.0, engine.DecodoScraperClient.MAX_TIMEOUT)
    check(
        "Decodo POST timeout is a (connect, read) tuple",
        isinstance(timeout, tuple) and len(timeout) == 2,
        str(timeout),
    )
    check(
        "read phase is bound to the clamped budget",
        isinstance(timeout, tuple) and abs(timeout[1] - expected_read) < 1e-6,
        str(timeout),
    )
    check(
        "connect phase is capped to CONNECT_TIMEOUT",
        isinstance(timeout, tuple) and timeout[0] == min(cap, expected_read),
        str(timeout),
    )


def test_decodo_bing_fallback_recovers_when_google_empty() -> None:
    print("Decodo Bing-fallback recovery checks:")
    # Cold niche brand-only house: Google organic parsing returns zero
    # Fragrantica URLs. With DECODO_ENABLE_BING_FALLBACK set, the same
    # site-scoped query is retried on Bing, which recovers the perfume URLs.
    # The fallback is gated -- without the flag, no Bing call is made.
    google_empty = {
        "results": [{"content": {"results": {"organic": [
            {"url": "https://example.com/blog-about-bortnikoff"},
        ]}}}]
    }
    bing_hit = {
        "results": [{"content": {"results": {"organic": [
            {"url": "https://www.fragrantica.com/perfume/Bortnikoff/Sayat-Nova-12345.html"},
            {"url": "https://www.fragrantica.com/perfume/Bortnikoff/Oud-Maximus-67890.html"},
            {"url": "https://www.fragrantica.com/perfume/Bortnikoff/Sayat-Nova-12345.html?utm=dup"},
        ]}}}]
    }

    def run(flag: bool) -> tuple[list[str], list[str]]:
        saved = _set_decodo_env(enabled=True)
        if flag:
            os.environ["DECODO_ENABLE_BING_FALLBACK"] = "1"
        old_post = engine.requests.post
        targets: list[str] = []
        try:
            def routed_post(url, json=None, headers=None, timeout=None, **kwargs):
                body = json or {}
                target = str(body.get("target") or "")
                targets.append(target)
                if target == "bing_search":
                    return _FakeDecodoResponse(bing_hit)
                return _FakeDecodoResponse(google_empty)

            engine.requests.post = routed_post
            urls = engine.DecodoScraperClient.search_fragrantica_urls("Bortnikoff")
        finally:
            engine.requests.post = old_post
            _restore_decodo_env(saved)
        return urls, targets

    on_urls, on_targets = run(True)
    off_urls, off_targets = run(False)

    check(
        "Bing fallback recovers Fragrantica URLs Google missed",
        on_urls == [
            "https://www.fragrantica.com/perfume/Bortnikoff/Sayat-Nova-12345.html",
            "https://www.fragrantica.com/perfume/Bortnikoff/Oud-Maximus-67890.html",
        ],
        str(on_urls),
    )
    check("Bing fallback dedupes the recovered URLs", len(on_urls) == 2, str(on_urls))
    check("Bing search target is actually queried when enabled", "bing_search" in on_targets, str(on_targets))
    check("Bing fallback is gated off by default (no URLs)", off_urls == [], str(off_urls))
    check("Bing search is never called when the flag is unset", "bing_search" not in off_targets, str(off_targets))


def test_decodo_ai_overview_extracts_cited_urls() -> None:
    print("Decodo AI-Overview discovery checks:")
    # Last-ditch, opt-in path: mine Google's AI Overview source panel for cited
    # Fragrantica URLs. Gated behind DECODO_ENABLE_AI_OVERVIEW; only canonical
    # Fragrantica perfume links survive, and non-fragrance citations are dropped.
    ai_payload = {
        "results": [{"content": {"ai_overviews": [
            {"source_panel": {"items": [
                {"url": "https://www.fragrantica.com/perfume/Bortnikoff/Musk-Cologne-111.html"},
                {"url": "https://example.org/not-a-fragrance"},
            ]}},
        ]}}]
    }

    def run(flag: bool) -> tuple[list[str], int]:
        saved = _set_decodo_env(enabled=True)
        if flag:
            os.environ["DECODO_ENABLE_AI_OVERVIEW"] = "1"
        old_post = engine.requests.post
        calls = {"n": 0}
        try:
            def fake_post(url, json=None, headers=None, timeout=None, **kwargs):
                calls["n"] += 1
                return _FakeDecodoResponse(ai_payload)

            engine.requests.post = fake_post
            urls = engine.DecodoScraperClient.search_fragrantica_urls_via_ai_overview("Bortnikoff")
        finally:
            engine.requests.post = old_post
            _restore_decodo_env(saved)
        return urls, calls["n"]

    on_urls, on_calls = run(True)
    off_urls, off_calls = run(False)

    check(
        "AI Overview extracts the cited Fragrantica perfume URL",
        on_urls == ["https://www.fragrantica.com/perfume/Bortnikoff/Musk-Cologne-111.html"],
        str(on_urls),
    )
    check("AI Overview drops non-Fragrantica citations", all("fragrantica.com" in u for u in on_urls), str(on_urls))
    check("AI Overview is gated off by default (no URLs)", off_urls == [], str(off_urls))
    check("AI Overview makes no request when the flag is unset", off_calls == 0, str(off_calls))


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


def test_decodo_designer_fallback_resolves_thombrony() -> None:
    print("Decodo designer fallback checks:")
    saved = _set_decodo_env(enabled=True)
    old_designer = engine.DecodoScraperClient.search_fragrantica_designer_urls
    old_brand = engine.DecodoScraperClient.search_fragrantica_brand_urls
    try:
        check(
            "collapsed Thom Browne query maps to the canonical brand",
            engine.IdentityTools.canonical_brand_query("THOMBRONY") == "thom browne",
            engine.IdentityTools.canonical_brand_query("THOMBRONY"),
        )

        def fake_designer(query, deadline=None, timeout=8.0, max_results=5):
            del deadline, timeout, max_results
            return (
                ["https://www.fragrantica.com/designers/Thom-Browne.html"]
                if query == "THOMBRONY"
                else []
            )

        def fake_brand(brand, deadline=None, timeout=8.0, max_results=25):
            del deadline, timeout, max_results
            return (
                [
                    "https://www.fragrantica.com/perfume/Thom-Browne/Vetyver-And-Rose-57793.html",
                    "https://www.fragrantica.com/perfume/Thom-Browne/Vetyver-Absolute-57791.html",
                ]
                if engine.TextSanitizer.normalize_identity(brand) == "thom browne"
                else []
            )

        engine.DecodoScraperClient.search_fragrantica_designer_urls = classmethod(
            lambda cls, *args, **kwargs: fake_designer(*args, **kwargs)
        )
        engine.DecodoScraperClient.search_fragrantica_brand_urls = classmethod(
            lambda cls, *args, **kwargs: fake_brand(*args, **kwargs)
        )
        args = engine.build_parser().parse_args([])
        rows = engine._designer_provider_fallback_results("THOMBRONY", args)
        clean_rows = engine._designer_provider_fallback_results("Thom Browne", args)
    finally:
        engine.DecodoScraperClient.search_fragrantica_designer_urls = old_designer
        engine.DecodoScraperClient.search_fragrantica_brand_urls = old_brand
        _restore_decodo_env(saved)

    identities = [(row.brand, row.name, row.frag_url, row.resolver_source) for row in rows]
    check("THOMBRONY resolves to Thom Browne catalog rows", len(rows) == 2, str(identities))
    clean_identities = [(row.brand, row.name, row.frag_url, row.resolver_source) for row in clean_rows]
    check("Thom Browne resolves to Thom Browne catalog rows", len(clean_rows) == 2, str(clean_identities))
    check(
        "designer fallback emits Fragrantica perfume URLs",
        all(row.frag_url and row.brand == "Thom Browne" for row in rows),
        str(identities),
    )
    check(
        "designer fallback labels the resolver source",
        all(row.resolver_source == "decodo_fragrantica_designer_catalog" for row in rows),
        str(identities),
    )


def test_zero_candidates_with_raw_bn_rows_still_repairs() -> None:
    print("Zero-candidate repair gate checks:")
    raw_bn = [
        engine.UnifiedFragrance(
            name="Unrelated Browne",
            brand="Not Thom Browne",
            year="",
            query_score=0.95,
        )
    ]
    check(
        "raw BN rows do not suppress repair when no usable candidates survived",
        engine.QueryRepair.needs_repair(raw_bn, []) is True,
        "needs_repair returned False",
    )


def _norm(value: str) -> str:
    return engine.TextSanitizer.normalize_identity(value)


def test_query_expander_rewrites_known_shorthands_and_typos() -> None:
    # Regression for the search task list items 1 & 4: bare shorthands and common
    # misspellings of famous houses/flankers returned "No Olfactory Matches" because
    # nothing anchored them to a real designer. expand_query_aliases canonicalises
    # them before any provider call so discovery + ranking both see the right house.
    print("Query expander shorthand/typo checks:")
    expand = engine.IdentityTools.expand_query_aliases
    cases = {
        "monet blanco": "montblanc",
        "mont blanco": "montblanc",
        "ysev saint laurent": "yves saint laurent",
        "baccarat": "maison francis kurkdjian baccarat rouge 540",
        "layton": "parfums de marly layton",
        # trailing connector noise must be trimmed before the shorthand match
        "layton de": "parfums de marly layton",
    }
    for query, expected in cases.items():
        got = _norm(expand(query))
        check(f"{query!r} expands to {expected!r}", got == expected, got)

    # An unrecognised query must pass through byte-for-byte (no surprise rewrites).
    for untouched in ("dior sauvage", "creed aventus", "tom ford oud wood"):
        check(f"{untouched!r} is left unchanged", expand(untouched) == untouched, expand(untouched))


def test_query_expander_prepends_omitted_house_for_known_lines() -> None:
    # Items 1 & 4: a known product LINE typed without its house ("le male ...",
    # bare "bottled night") must be anchored to the parent designer, while a query
    # that already names the house is left alone (no double-prepend).
    print("Query expander line-house hint checks:")
    expand = engine.IdentityTools.expand_query_aliases

    long_jpg = expand("Le Male Elixir Absolu Parfum Intense")
    check(
        "long Le Male query gains the JPG house and keeps its tail",
        _norm(long_jpg).startswith("jean paul gaultier") and "elixir" in _norm(long_jpg),
        long_jpg,
    )

    bare_line = expand("le male")
    check("bare 'le male' gains the JPG house", _norm(bare_line) == "jean paul gaultier le male", bare_line)

    night = expand("bottled night")
    check("bare 'bottled night' gains the Hugo Boss house", _norm(night) == "hugo boss bottled night", night)

    # House already present -> no change (boss is an alias of Hugo Boss).
    check("'boss bottled night' is not re-prepended", expand("boss bottled night") == "boss bottled night", expand("boss bottled night"))
    check(
        "'Parfums de Marly Layton' is not re-prepended",
        expand("Parfums de Marly Layton") == "Parfums de Marly Layton",
        expand("Parfums de Marly Layton"),
    )


def test_filler_words_do_not_overpower_distinctive_anchor() -> None:
    # Task item 2: "layton de" surfaced Caron "Nuit De Noel", YSL "La Nuit De
    # L'Homme"/"... Bleu Electrique" and Armaf "Club De Nuit ... Man" because the
    # ranker rewarded the filler word "de" (and coincidental whole-string
    # similarity) instead of the missing anchor "layton". A query that carries a
    # distinctive token the candidate does not cover must fall below the floor.
    print("Filler-word ranking guard checks:")
    score = engine.IdentityTools.relevance_score
    floor = engine.QueryRepair.MIN_RESULT_SCORE
    junk = [
        candidate("Caron", "Nuit De Noel"),
        candidate("Yves Saint Laurent", "La Nuit De L'homme Eau De Toilette"),
        candidate("Yves Saint Laurent", "La Nuit De L Homme Bleu Electrique"),
        candidate("Armaf", "Club De Nuit Intense Man Limited Edition Parfum"),
    ]
    for row in junk:
        s = score("layton de", row)
        check(f"'layton de' does not match {row.brand} {row.name!r}", s < floor, f"{s:.3f}")

    # The real target, once present, must still rank strongly.
    layton = candidate("Parfums de Marly", "Layton")
    check("'layton de' still matches Parfums de Marly Layton", score("layton de", layton) >= floor, f"{score('layton de', layton):.3f}")

    # End-to-end through the filter: only the real house survives.
    survivors = [item.name for item in engine.filter_relevant_candidates("layton de", junk + [layton])]
    check("filter keeps only the Layton row for 'layton de'", survivors == ["Layton"], str(survivors))

    # The guard must NOT harm legitimate queries whose anchor IS covered.
    check(
        "'dior sauvage' still scores strongly",
        score("dior sauvage", candidate("Dior", "Sauvage")) >= 0.9,
        f"{score('dior sauvage', candidate('Dior', 'Sauvage')):.3f}",
    )
    check(
        "typo'd 'ysev saint laurent' still matches YSL via saint+laurent",
        score("ysev saint laurent", candidate("Yves Saint Laurent", "Libre")) >= floor,
        f"{score('ysev saint laurent', candidate('Yves Saint Laurent', 'Libre')):.3f}",
    )


def test_new_brand_aliases_are_brand_only_and_keep_house() -> None:
    # Task item 3: "le labo", "jo malone", "boss", "montblanc", "parfums de marly"
    # must register as whole-house (brand-only) searches that return the catalogue,
    # not a failed single-fragrance identification.
    print("New brand-alias brand-only checks:")
    brand_only = engine.IdentityTools.query_is_brand_only
    cases = [
        ("le labo", "Le Labo"),
        ("jo malone", "Jo Malone London"),
        ("boss", "Hugo Boss"),
        ("montblanc", "Montblanc"),
        ("pdm", "Parfums de Marly"),
        ("parfums de marly", "Parfums de Marly"),
    ]
    for query, brand in cases:
        check(f"{query!r} is brand-only for {brand!r}", brand_only(query, brand), "")
        check(
            f"{query!r} scores {brand!r} as a full house match",
            engine.IdentityTools.relevance_score(query, candidate(brand, "Some Fragrance")) >= 0.99,
            f"{engine.IdentityTools.relevance_score(query, candidate(brand, 'Some Fragrance')):.3f}",
        )

    # canonical_brand_query must resolve the aliases so the catalog crawl is seeded.
    check("'pdm' resolves to the Parfums de Marly house", _norm(engine.IdentityTools.canonical_brand_query("pdm")) == "parfums de marly", engine.IdentityTools.canonical_brand_query("pdm"))
    check("'boss' resolves to the Hugo Boss house", _norm(engine.IdentityTools.canonical_brand_query("boss")) == "hugo boss", engine.IdentityTools.canonical_brand_query("boss"))


def test_jo_malone_brand_query_keeps_both_house_spellings() -> None:
    # Task item 7 (backend half): "JO MALONE" and "JO MALONE LONDON" are the same
    # house, so a "jo malone" brand search must keep both spellings' rows (the SPA
    # can then merge the chips) and must not leak unrelated houses.
    print("Jo Malone house-spelling checks:")
    rows = [
        candidate("Jo Malone", "Lime Basil & Mandarin"),
        candidate("Jo Malone London", "Wood Sage & Sea Salt"),
        candidate("Dior", "Sauvage"),
    ]
    filtered = engine.filter_relevant_candidates("jo malone", rows)
    houses = {item.brand for item in filtered}
    check("both Jo Malone spellings survive", {"Jo Malone", "Jo Malone London"} <= houses, str(houses))
    check("unrelated house is dropped", "Dior" not in houses, str(houses))
    check(
        "both spellings are recognised as the same house",
        engine.IdentityTools.compatible_brand("Jo Malone", "Jo Malone London"),
        "",
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
    test_stopword_names_are_not_rejected()
    test_multi_token_brand_only_query_keeps_whole_house()
    test_typed_concentration_keeps_exact_match()
    test_concentration_phrase_is_not_a_required_name_marker()
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
    test_api_fact_question_answers_from_stored_record()
    test_api_fact_question_recovers_missing_fact()
    test_fragrantica_native_search_bypassed_by_default()
    test_pick_launch_year_ignores_house_founding_year()
    test_casamorati_tempio_catalog_identity_score()
    test_decodo_disabled_without_credentials()
    test_decodo_enabled_with_default_provider()
    test_decodo_legacy_provider_env_does_not_block_credentials()
    test_decodo_parses_fragrantica_urls()
    test_decodo_caches_responses()
    test_decodo_structured_spell_repair_evidence()
    test_known_brand_query_skips_spell_repair()
    test_ranking_tail_is_stripped_from_spell_repair()
    test_volume_tail_is_stripped_from_spell_repair()
    test_decodo_post_request_splits_connect_and_read_timeout()
    test_decodo_bing_fallback_recovers_when_google_empty()
    test_decodo_ai_overview_extracts_cited_urls()
    test_initio_prives_decodo_url_survives_merge()
    test_decodo_designer_fallback_resolves_thombrony()
    test_zero_candidates_with_raw_bn_rows_still_repairs()
    test_strip_house_from_name()
    test_q_relevance_is_word_based()
    test_brand_alias_query_keeps_house_catalog()
    test_query_expander_rewrites_known_shorthands_and_typos()
    test_query_expander_prepends_omitted_house_for_known_lines()
    test_filler_words_do_not_overpower_distinctive_anchor()
    test_new_brand_aliases_are_brand_only_and_keep_house()
    test_jo_malone_brand_query_keeps_both_house_spellings()
    test_dolce_gabbana_identity_recovery_and_persistence()
    test_house_echo_poison_is_suppressed()
    test_brand_only_query_gets_catalog_labels()
    test_multi_token_bare_brand_seeds_catalog_labels()
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
