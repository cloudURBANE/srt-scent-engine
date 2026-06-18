#!/usr/bin/env python3
"""Regression checks for offline wardrobe projection.

These stay DB-free and network-free: the bug was in the pure projection helper
that copies engine-cache facts into app wardrobe payloads.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, ROOT)

import heal_offline as heal


def _check(name: str, cond: bool) -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    return cond


def _complete_dm() -> dict:
    return {
        "performance_score": {"score_raw": 82, "label": "Strong"},
        "value_score": {"score_raw": 71, "label": "Fair"},
        "community_interest_score": {"score_raw": 64, "label": "Moderate"},
        "wear_profile": {
            "primary_seasons": ["Fall"],
            "primary_time": "Night",
        },
        "main_accords": {
            "top_accords": ["amber", "woody", "vanilla"],
            "source": "test",
        },
        "notes": {"top": ["bergamot"], "base": ["vanilla"]},
        "source_coverage": {
            "performance_score": True,
            "value_score": True,
            "community_interest_score": True,
            "wear_profile": True,
            "main_accords": True,
            "notes": True,
        },
    }


def main() -> int:
    ok = True

    dm = _complete_dm()
    payload = {"brand": "Christian Dior", "name": "Sauvage Elixir", "family": "Woody"}
    flags = heal.project_engine_facts(
        payload,
        {
            "derived_metrics": dm,
            "concentration": None,
            "year": None,
            "gender": None,
            "image_url": None,
        },
    )
    ok &= _check("projection reports derived_metrics fill", flags[4] is True)
    ok &= _check("blank wardrobe row receives derived_metrics", payload.get("derived_metrics") == dm)
    ok &= _check("blank wardrobe row receives clean accords copy", payload.get("accords") == ["amber", "woody", "vanilla"])
    ok &= _check("blank wardrobe row receives complete wear profile", payload.get("wear_profile", {}).get("primary_time") == "Night")
    ok &= _check("blank wardrobe row receives top-level notes", flags[5] is True and payload.get("notes") == ["bergamot", "vanilla"])
    ok &= _check("blank wardrobe row receives note pyramid", payload.get("pyramid", {}).get("top") == ["bergamot"])

    partial_payload = {
        "derived_metrics": {
            "performance_score": {"score_raw": 99, "label": "Keep me"},
            "source_coverage": {"performance_score": True},
        },
        "wear_profile": {"primary_seasons": ["Spring"], "primary_time": "Day"},
        "accords": ["amber", "woody", "vanilla"],
        "family": "Amber",
    }
    flags = heal.project_engine_facts(
        partial_payload,
        {
            "derived_metrics": dm,
            "concentration": None,
            "year": None,
            "gender": None,
            "image_url": None,
        },
    )
    merged = partial_payload["derived_metrics"]
    ok &= _check("partial derived_metrics are topped up", flags[4] is True)
    ok &= _check("existing populated score group is not overwritten", merged["performance_score"]["score_raw"] == 99)
    ok &= _check("missing score group is filled", merged["value_score"]["score_raw"] == 71)
    ok &= _check("source coverage is OR-merged", merged["source_coverage"]["value_score"] is True)
    ok &= _check("complete top-level wear profile is not downgraded", partial_payload["wear_profile"]["primary_time"] == "Day")
    ok &= _check("missing top-level notes are filled from existing derived metrics", partial_payload.get("notes") == ["bergamot", "vanilla"])

    complete_payload = {"derived_metrics": _complete_dm(), "accords": ["amber", "woody", "vanilla"], "family": "Amber"}
    before_dm = repr(complete_payload["derived_metrics"])
    flags = heal.project_engine_facts(
        complete_payload,
        {
            "derived_metrics": dm,
            "concentration": None,
            "year": None,
            "gender": None,
            "image_url": None,
        },
    )
    ok &= _check(
        "complete derived_metrics are left as a no-op",
        flags[4] is False and repr(complete_payload["derived_metrics"]) == before_dm,
    )

    # The "sponsored 100%" regression: projecting an engine blob whose scored
    # scent_vector still carries scraped junk must NOT copy that junk into the
    # wardrobe's derived_metrics (the SPA accord card renders its percentages
    # from scent_vector). The merge sanitizes a private copy.
    dirty_engine_dm = _complete_dm()
    dirty_engine_dm["main_accords"] = {
        "scent_vector": [
            {"accord": "sponsored", "score": 100.0},
            {"accord": "amber", "score": 90.0},
            {"accord": "woody", "score": 70.0},
        ],
        "top_accords": ["sponsored", "amber", "woody"],
        "source": "test",
    }
    blank_payload = {"brand": "House", "name": "Junk Vector", "family": "Woody"}
    heal.project_engine_facts(
        blank_payload,
        {
            "derived_metrics": dirty_engine_dm,
            "concentration": None,
            "year": None,
            "gender": None,
            "image_url": None,
        },
    )
    projected_vec = blank_payload.get("derived_metrics", {}).get("main_accords", {}).get("scent_vector", [])
    ok &= _check(
        "projection drops junk from the wardrobe scent_vector",
        [v["accord"] for v in projected_vec] == ["amber", "woody"],
    )
    ok &= _check(
        "projection drops junk from the wardrobe accords copy",
        blank_payload.get("accords") == ["amber", "woody"],
    )
    ok &= _check(
        "projection never mutates the shared engine scent_vector",
        [v["accord"] for v in dirty_engine_dm["main_accords"]["scent_vector"]]
        == ["sponsored", "amber", "woody"],
    )

    # An already-projected wardrobe row whose stored blob predates the junk label
    # converges on the next heal even when no score group needs filling.
    dirty_wardrobe = {
        "derived_metrics": {
            **_complete_dm(),
            "main_accords": {
                "scent_vector": [
                    {"accord": "sponsored", "score": 100.0},
                    {"accord": "amber", "score": 88.0},
                ],
                "top_accords": ["sponsored", "amber"],
                "source": "test",
            },
        },
        "accords": ["amber"],
        "wear_profile": {"primary_seasons": ["Fall"], "primary_time": "Night"},
        "family": "Amber",
    }
    clean_engine_dm = _complete_dm()  # nothing new to fill
    flags = heal.project_engine_facts(
        dirty_wardrobe,
        {
            "derived_metrics": clean_engine_dm,
            "concentration": None,
            "year": None,
            "gender": None,
            "image_url": None,
        },
    )
    healed_vec = dirty_wardrobe["derived_metrics"]["main_accords"]["scent_vector"]
    ok &= _check(
        "stale wardrobe scent_vector is sanitized on heal",
        [v["accord"] for v in healed_vec] == ["amber"] and flags[4] is True,
    )

    # The real "sponsored 100%" persistence: a wardrobe row whose abbreviated
    # name never matches an engine record (e.g. "Dylan Blue" vs the engine's
    # "Pour Homme Dylan Blue") never reaches the engine-projection sanitize path,
    # so its stored scent_vector kept the junk forever. _sanitize_wardrobe_blob
    # is the unconditional sweep that cleans it with no engine match at all.
    no_match_row = {
        "derived_metrics": {
            "main_accords": {
                "scent_vector": [
                    {"accord": "Amber", "score": 100.0},
                    {"accord": "Sponsored", "score": 100.0},
                    {"accord": "Citrus", "score": 91.65},
                ],
                "top_accords": ["Amber", "Sponsored", "Citrus"],
                "accord_summary": "A amber fragrance with sponsored and citrus facets.",
                "source": "test",
            },
        },
        "raw_engine_detail": {
            "derived_metrics": {
                "main_accords": {
                    "scent_vector": [
                        {"accord": "Amber", "score": 100.0},
                        {"accord": "Sponsored", "score": 100.0},
                        {"accord": "Citrus", "score": 91.65},
                    ],
                    "top_accords": ["Amber", "Sponsored", "Citrus"],
                    "accord_summary": "A amber fragrance with sponsored and citrus facets.",
                    "source": "test",
                },
            },
            "raw": {
                "frag_cards": {
                    "Main accords": [
                        {"label": "Amber", "display": "Amber", "pct": 100.0},
                        {"label": "Sponsored", "display": "Sponsored", "pct": 100.0},
                        {"label": "Citrus", "display": "Citrus", "pct": 91.65},
                    ],
                },
            },
        },
        "accords": ["Amber", "Sponsored", "Citrus"],
    }
    changed = heal._sanitize_wardrobe_blob(no_match_row)
    swept_vec = no_match_row["derived_metrics"]["main_accords"]["scent_vector"]
    swept_raw_vec = no_match_row["raw_engine_detail"]["derived_metrics"]["main_accords"]["scent_vector"]
    ok &= _check(
        "unmatched row's stale scent_vector is sanitized with no engine match",
        changed is True
        and [v["accord"] for v in swept_vec] == ["Amber", "Citrus"]
        and [v["accord"] for v in swept_raw_vec] == ["Amber", "Citrus"]
        and no_match_row["accords"] == ["Amber", "Citrus"],
    )
    ok &= _check(
        "sanitize sweep rebuilds accord summaries that mention junk",
        no_match_row["derived_metrics"]["main_accords"]["accord_summary"]
        == "A amber fragrance with citrus facets."
        and no_match_row["raw_engine_detail"]["derived_metrics"]["main_accords"]["accord_summary"]
        == "A amber fragrance with citrus facets.",
    )
    ok &= _check(
        "sanitize sweep drops junk from raw Main accords frag_cards",
        [
            row["label"]
            for row in no_match_row["raw_engine_detail"]["raw"]["frag_cards"]["Main accords"]
        ]
        == ["Amber", "Citrus"],
    )
    ok &= _check(
        "sanitize sweep is idempotent (clean row reports no change)",
        heal._sanitize_wardrobe_blob(no_match_row) is False,
    )

    # Taxonomy normalization backstop (dirty_data.normalize_row wired into the
    # sweep): lowercase accords/family -> Title Case, the Chypere typo, and a
    # numeric-string year -> int, all on a row with no derived_metrics junk at all.
    taxo_row = {
        "accords": ["citrus", "warm spicy", "musk"],
        "family": "fruity chypere",
        "year": "2007",
        "derived_metrics": {},
    }
    taxo_changed = heal._sanitize_wardrobe_blob(taxo_row)
    ok &= _check(
        "taxonomy backstop normalizes casing/typo/year with no engine match or DM junk",
        taxo_changed is True
        and taxo_row["accords"] == ["Citrus", "Warm Spicy", "Musk"]
        and taxo_row["family"] == "Fruity Chypre"
        and taxo_row["year"] == 2007,
    )
    ok &= _check(
        "taxonomy backstop is idempotent (already-clean row reports no change)",
        heal._sanitize_wardrobe_blob(taxo_row) is False,
    )
    # Deprecated 'Oriental' is a SEMANTIC remap -> flagged in the audit, never
    # auto-rewritten by the sweep (would silently move the family bucket).
    oriental_row = {"family": "Oriental", "accords": ["Amber"], "derived_metrics": {}}
    ok &= _check(
        "deprecated Oriental family is left untouched by the safe sweep",
        heal._sanitize_wardrobe_blob(oriental_row) is False
        and oriental_row["family"] == "Oriental",
    )

    # ------------------------------------------------------------------ #
    # Bucket-1: global_fragrances inherits owner-confirmed scalar facts
    # (gender / concentration / family / year) that the ENGINE lacks. The
    # owner-confirmed fold supplies these as entry scalars; project_engine_facts
    # must fill a blank global row from them. (a)
    # ------------------------------------------------------------------ #
    owner_entry = {
        "derived_metrics": None,  # engine had no metrics blob to derive family from
        "concentration": "Eau de Parfum",
        "year": "2018",
        "gender": "Feminine",
        "family": "Amber",
        "image_url": None,
    }
    blank_global = {"brand": "Tom Ford", "name": "Lost Cherry"}
    flags = heal.project_engine_facts(blank_global, owner_entry)
    # flag order: family, conc, accords, wear, dm, notes, year, gender, image
    ok &= _check(
        "global inherits owner-confirmed gender the engine lacked",
        flags[7] is True and blank_global.get("gender") == "Feminine",
    )
    ok &= _check(
        "global inherits owner-confirmed concentration the engine lacked",
        flags[1] is True and blank_global.get("concentration") == "Eau de Parfum",
    )
    ok &= _check(
        "global inherits owner-confirmed family (scalar fold, no derived_metrics)",
        flags[0] is True and blank_global.get("family") == "Amber",
    )
    ok &= _check(
        "global inherits owner-confirmed year the engine lacked",
        flags[6] is True and blank_global.get("year") == "2018",
    )

    # (b) gap-fill never OVERWRITES a value already present on the target.
    populated_global = {
        "brand": "Tom Ford",
        "name": "Lost Cherry",
        "gender": "Unisex",          # already set -> must survive
        "concentration": "Parfum",   # already set -> must survive
        "family": "Woody",           # already set -> must survive
        "year": "2017",              # already set -> must survive
    }
    flags = heal.project_engine_facts(populated_global, owner_entry)
    ok &= _check(
        "gap-fill never overwrites an existing gender",
        flags[7] is False and populated_global["gender"] == "Unisex",
    )
    ok &= _check(
        "gap-fill never overwrites an existing concentration",
        flags[1] is False and populated_global["concentration"] == "Parfum",
    )
    ok &= _check(
        "gap-fill never overwrites an existing family",
        flags[0] is False and populated_global["family"] == "Woody",
    )
    ok &= _check(
        "gap-fill never overwrites an existing year",
        flags[6] is False and populated_global["year"] == "2017",
    )

    # A scalar family fold must NOT clobber a family already derived from an
    # engine derived_metrics blob (the derived path runs first and wins).
    derived_family_entry = {**owner_entry, "derived_metrics": _complete_dm(), "family": "Citrus"}
    derived_payload = {"brand": "House", "name": "Derived Fam"}
    heal.project_engine_facts(derived_payload, derived_family_entry)
    ok &= _check(
        "derived-metrics family wins over the scalar family fold",
        heal.is_fact_complete(derived_payload.get("family"), "family")
        and derived_payload.get("family") != "Citrus",
    )

    # ------------------------------------------------------------------ #
    # Bucket-2: junk-purge predicate -- matches brand==name skeleton stubs
    # but NEVER a legit catalog row like "Chanel No 5". (c)
    # ------------------------------------------------------------------ #
    import purge_junk_globals as purge

    junk_stub = {
        "brand": "Xerjoff",
        "name": "Xerjoff",
        "concentration": "Unknown",
        "accords": ["Citrus", "Musk"],  # generic-default junk, but predicate keys
        "source_url": "",               # off the structural signals below
        "derived_metrics": {},
    }
    ok &= _check(
        "purge matches a brand==name skeleton stub",
        purge.is_junk_global_stub("Xerjoff", "Xerjoff", junk_stub) is True,
    )
    ok &= _check(
        "purge matches case-differing brand==name stub (Tom ford|Tom ford)",
        purge.is_junk_global_stub(
            "Tom ford", "Tom ford",
            {"concentration": "Unknown", "source_url": "", "accords": [], "derived_metrics": {}},
        )
        is True,
    )

    legit_numbered = {
        "brand": "Chanel",
        "name": "Chanel No 5",
        "concentration": "Unknown",
        "source_url": "",
        "accords": [],
        "derived_metrics": {},
    }
    ok &= _check(
        "purge does NOT match a legit 'Chanel No 5' (brand is a substring, not equal)",
        purge.is_junk_global_stub("Chanel", "Chanel No 5", legit_numbered) is False,
    )

    real_data_stub = {
        "brand": "Creed",
        "name": "Creed",
        "concentration": "Eau de Parfum",  # real concentration -> keep
        "source_url": "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html",
        "accords": ["Fruity", "Woody", "Smoky"],
        "derived_metrics": {"notes": {"has_pyramid": True}},
    }
    ok &= _check(
        "purge KEEPS a brand==name row that has real enrichment (pyramid/url/conc)",
        purge.is_junk_global_stub("Creed", "Creed", real_data_stub) is False,
    )
    ok &= _check(
        "purge keeps a brand==name row with a real FG source_url even if conc Unknown",
        purge.is_junk_global_stub(
            "House", "House",
            {
                "concentration": "Unknown",
                "source_url": "https://www.fragrantica.com/perfume/House/House-1.html",
                "accords": [],
                "derived_metrics": {},
            },
        )
        is False,
    )

    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
