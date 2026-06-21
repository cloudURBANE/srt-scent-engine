#!/usr/bin/env python3
"""Checks for the read-only, no-egress POST /api/fragrances/enrichment-state.

The endpoint lets the web app poll a fragrance's CACHED completeness cheaply,
without ever triggering the billed live Decodo detail fetch. These checks prove:

  * a cached/complete fragrance reports level "full", complete true, cached true;
  * an uncached identity reports level "none", complete false, cached false, and
    NEVER reaches the live-fetch / enqueue path -- enforced by monkeypatching the
    live-fetch and job-enqueue functions to raise if they are ever called.

pytest-style (no __main__ guard): run via `python -m pytest`.
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import api
import fragrance_parser_full_rewrite_fixed as engine
from fastapi.testclient import TestClient


def _complete_details() -> engine.UnifiedDetails:
    """A cached bundle where all 4 FG status metric groups made it through."""
    details = engine.UnifiedDetails(notes=engine.NotesList())
    details.frag_cards = {"Community Interest": [{"label": "Have", "count": "9"}]}
    details.notes.top = ["Bergamot"]
    details.notes.base = ["Musk"]
    details.derived_metrics = {
        "source_coverage": {
            "performance_score": True,
            "value_score": True,
            "community_interest_score": True,
            "wear_profile": True,
        }
    }
    return details


def _fragrantica_request() -> dict[str, str]:
    selected = engine.UnifiedFragrance(
        name="Test Fragrance",
        brand="Test Brand",
        year="",
        bn_url="https://basenotes.com/fragrances/test-by-brand.0",
        frag_url="https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
    )
    return {"id": api._encode_id(selected)}


def _boom(*_args, **_kwargs):  # pragma: no cover - only called on regression
    raise AssertionError(
        "enrichment-state must never reach a live-fetch / enqueue path"
    )


def test_cached_complete_returns_full() -> None:
    saved_lookup = api._lookup_stored_detail
    try:
        api._lookup_stored_detail = lambda selected: _complete_details()
        client = TestClient(api.app)
        res = client.post(
            "/api/fragrances/enrichment-state", json=_fragrantica_request()
        )
        assert res.status_code == 200, res.text
        data = res.json()
        assert data["cached"] is True, data
        assert data["complete"] is True, data
        assert data["level"] == "full", data
        assert data["source_coverage"] is not None, data
        assert (
            data["source_coverage"]["fragrantica_metrics_complete"] is True
        ), data
        assert "derived_metrics" in data, data
        assert data["enrichment"]["status"] == "completed", data
        assert data["enrichment"]["requires_worker"] is False, data
    finally:
        api._lookup_stored_detail = saved_lookup


def test_uncached_returns_none_with_no_side_effects() -> None:
    saved_lookup = api._lookup_stored_detail
    saved_enqueue = api._enqueue_enrichment_job
    saved_fetch = engine.fetch_selected_details
    saved_get_scraper = engine.get_scraper
    try:
        # Cache miss.
        api._lookup_stored_detail = lambda selected: None
        # Any of these being called is a contract violation -> test fails loudly.
        api._enqueue_enrichment_job = _boom
        engine.fetch_selected_details = _boom
        engine.get_scraper = _boom

        client = TestClient(api.app)
        res = client.post(
            "/api/fragrances/enrichment-state", json=_fragrantica_request()
        )
        assert res.status_code == 200, res.text
        data = res.json()
        assert data["cached"] is False, data
        assert data["complete"] is False, data
        assert data["level"] == "none", data
        assert data["source_coverage"] is None, data
        assert data["enrichment"]["status"] == "unknown", data
        assert data["enrichment"]["requires_worker"] is True, data
    finally:
        api._lookup_stored_detail = saved_lookup
        api._enqueue_enrichment_job = saved_enqueue
        engine.fetch_selected_details = saved_fetch
        engine.get_scraper = saved_get_scraper


def test_no_source_url_is_400() -> None:
    client = TestClient(api.app)
    # An identity-only payload with no resolvable source URL -> 400, same as
    # /details, and still no live fetch.
    res = client.post("/api/fragrances/enrichment-state", json={})
    assert res.status_code == 400, res.text
