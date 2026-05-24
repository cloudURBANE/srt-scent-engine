#!/usr/bin/env python3
"""Plain-script checks for Parfinity catalog integration."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import api
import fragrance_parser_full_rewrite_fixed as engine
import parfinity
from fastapi.testclient import TestClient

_FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        _FAILURES.append(label)


def test_url_and_lookup() -> None:
    print("Parfinity URL/cache checks:")
    url = "https://parfinity.com/de/perfume/l-entropiste-white-blood?utm=1"
    canonical = parfinity.canonical_parfinity_url(url)
    check(
        "canonical URL normalizes locale, host, and query",
        canonical == "https://www.parfinity.com/en/perfume/l-entropiste-white-blood",
        canonical,
    )
    entry = parfinity.lookup_detail(canonical)
    check("lookup_detail finds warmed cache entry", bool(entry), str(entry))
    if entry:
        check("lookup_detail preserves identity", entry.get("name") == "White Blood")
        check("lookup_detail marks source partial", entry.get("quality_status") == "partial")
    check(
        "lookup_concentration returns Parfinity Tier 0 label",
        parfinity.lookup_concentration("L'Entropiste", "White Blood") == "Eau de Parfum",
    )


def test_overlay() -> None:
    print("Parfinity overlay checks:")
    details = engine.UnifiedDetails(notes=engine.NotesList(), description="BN wins")
    entry = parfinity.lookup_detail("l-entropiste-white-blood") or {}
    applied = parfinity.overlay_onto_details(details, entry)
    check("overlay applies notes/concentration", applied)
    check("overlay does not overwrite existing BN description", details.description == "BN wins")
    check("overlay leaves frag_cards as not-FG signal", details.frag_cards == {})
    check("overlay adds flat notes", "Bergamot" in details.notes.flat, str(details.notes.flat))
    check(
        "overlay adds concentration",
        getattr(details, "concentration", None) == "Eau de Parfum",
        str(getattr(details, "concentration", None)),
    )


def test_api_candidate_guard() -> None:
    print("API Parfinity candidate checks:")
    selected = api._candidate_from_source_url(
        "https://www.parfinity.com/en/perfume/l-entropiste-white-blood"
    )
    check("candidate uses Parfinity URL as frag_url", parfinity.is_parfinity_url(selected.frag_url))
    check("candidate identity comes from cache", selected.name == "White Blood")
    check("candidate house comes from cache", selected.brand == "L'Entropiste")


def test_api_bn_parfinity_merge() -> None:
    print("API BN + Parfinity merge checks:")
    saved_get_scraper = engine.get_scraper
    saved_fetch_details = engine.BasenotesEngine.fetch_details

    def fake_fetch_details(_scraper, _url, details, deadline=None) -> None:
        details.description = "BN description wins"
        details.bn_consensus = {"pos": (9, 90), "neu": (1, 10), "neg": (0, 0)}
        details.reviews.append(engine.Review(text="A detailed Basenotes review with enough text.", source="Basenotes"))

    try:
        engine.get_scraper = lambda: object()
        engine.BasenotesEngine.fetch_details = staticmethod(fake_fetch_details)
        selected = engine.UnifiedFragrance(
            name="White Blood",
            brand="L'Entropiste",
            year="",
            bn_url="https://basenotes.com/fragrances/white-blood-by-lentropiste.0",
            frag_url="https://www.parfinity.com/en/perfume/l-entropiste-white-blood",
        )
        token = api._encode_id(selected)
        response = TestClient(api.app).post("/api/fragrances/details", json={"id": token})
    finally:
        engine.get_scraper = saved_get_scraper
        engine.BasenotesEngine.fetch_details = saved_fetch_details

    check("details endpoint returns 200", response.status_code == 200, str(response.status_code))
    payload = response.json()
    raw = payload.get("raw") or {}
    notes = raw.get("notes") or {}
    coverage = payload.get("source_coverage") or {}
    check("BN description is not overwritten", raw.get("description") == "BN description wins")
    check("Parfinity fills notes beside BN", "Bergamot" in (notes.get("flat") or []), str(notes))
    check("Parfinity fills concentration beside BN", payload.get("concentration") == "Eau de Parfum")
    check("coverage marks Basenotes", coverage.get("basenotes") is True, str(coverage))
    check("coverage marks Parfinity", coverage.get("parfinity") is True, str(coverage))
    check("coverage does not mark Fragrantica linked", coverage.get("fragrantica_linked") is False, str(coverage))
    check("catalog response does not require worker", (payload.get("enrichment") or {}).get("requires_worker") is False)


def main() -> int:
    test_url_and_lookup()
    test_overlay()
    test_api_candidate_guard()
    test_api_bn_parfinity_merge()
    if _FAILURES:
        print("\nFailures:")
        for failure in _FAILURES:
            print(f"  - {failure}")
        return 1
    print("\nAll Parfinity checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
