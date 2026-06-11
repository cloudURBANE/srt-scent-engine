#!/usr/bin/env python3
"""
test_enrichment.py

Lightweight verification for the Pass 1 + Pass 2 enrichment pipeline.

Runs as a plain script (no pytest required):

    python test_enrichment.py

Two tiers:

  * Always-on checks -- run without a database. They cover token protection
    and the graceful-degradation contract (worker endpoints 503 when storage
    or the worker token is unconfigured; the public status endpoint still
    answers). These exercise api.py only.

  * DB-backed checks -- run only when DATABASE_URL is set. They drive db.py
    directly through the full job lifecycle (enqueue -> list -> claim ->
    complete -> fg_detail_cache lookup) plus stale-claim reclaim and the
    empty-frag_cards rejection. They use a throwaway job_key and clean up
    after themselves, so they are safe against a real (even shared) database.

Exit code is non-zero if any check fails.
"""
from __future__ import annotations

import os
import sys
import threading
from http.cookies import SimpleCookie
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi.testclient import TestClient
from starlette.requests import Request
import requests

import api
import db
import mobile
import scripts.enrichment_worker as enrichment_worker

_FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        _FAILURES.append(label)


# ---------------------------------------------------------------------------
# Always-on checks (no database required)
# ---------------------------------------------------------------------------


def test_no_db_contract() -> None:
    print("Always-on checks (no DATABASE_URL required):")
    client = TestClient(api.app)

    # Public status endpoint answers even with storage disabled.
    r = client.get("/api/enrichment/status")
    check("public status endpoint responds 200", r.status_code == 200, str(r.status_code))

    # Worker endpoints reject a missing token. With ENRICHMENT_WORKER_TOKEN
    # unset the dependency returns 503 (unconfigured); with it set, 401.
    saved = api._ENRICHMENT_WORKER_TOKEN
    saved_public_diagnostics = api._PUBLIC_DIAGNOSTICS
    try:
        api._PUBLIC_DIAGNOSTICS = False
        api._ENRICHMENT_WORKER_TOKEN = ""
        r = client.get("/api/enrichment/jobs")
        check("worker endpoint 503 when token unconfigured", r.status_code == 503, str(r.status_code))

        r = client.get("/api/diagnostics/memory")
        check("diagnostics endpoint 503 when token unconfigured", r.status_code == 503, str(r.status_code))

        api._ENRICHMENT_WORKER_TOKEN = "test-secret-token"
        r = client.get("/api/enrichment/jobs")
        check("worker endpoint 401 without bearer token", r.status_code == 401, str(r.status_code))

        r = client.get("/api/diagnostics/memory")
        check("diagnostics endpoint 401 without bearer token", r.status_code == 401, str(r.status_code))

        r = client.get(
            "/api/enrichment/jobs",
            headers={"Authorization": "Bearer wrong-token"},
        )
        check("worker endpoint 401 with wrong token", r.status_code == 401, str(r.status_code))

        # Correct token passes auth; without a DB it then 503s on storage.
        r = client.get(
            "/api/enrichment/jobs",
            headers={"Authorization": "Bearer test-secret-token"},
        )
        expected = {200, 503}  # 503 when no DB, 200 when DB is configured
        check(
            "worker endpoint accepts valid token (then 200/503)",
            r.status_code in expected,
            str(r.status_code),
        )

        r = client.get(
            "/api/diagnostics/memory",
            headers={"Authorization": "Bearer test-secret-token"},
        )
        check("diagnostics endpoint accepts valid token", r.status_code == 200, str(r.status_code))

        api._ENRICHMENT_WORKER_TOKEN = ""
        api._PUBLIC_DIAGNOSTICS = True
        r = client.get("/api/diagnostics/memory")
        check("diagnostics endpoint supports explicit public override", r.status_code == 200, str(r.status_code))

        api._PUBLIC_DIAGNOSTICS = False
        api._ENRICHMENT_WORKER_TOKEN = "test-secret-token"
        r = client.patch(
            "/api/enrichment/jobs/does-not-exist",
            headers={"Authorization": "Bearer test-secret-token"},
            json={"fg_url": "https://www.fragrantica.com/perfume/Brand/Name-1.html"},
        )
        check(
            "patch job endpoint requires DB storage",
            r.status_code in expected,
            str(r.status_code),
        )

        r = client.post(
            "/api/fragrances/details/requeue",
            json={"source_url": _TEST_JOB_KEY},
        )
        check(
            "public requeue endpoint requires DB storage",
            r.status_code in expected,
            str(r.status_code),
        )
    finally:
        api._ENRICHMENT_WORKER_TOKEN = saved
        api._PUBLIC_DIAGNOSTICS = saved_public_diagnostics

    @dataclass
    class ParserLeak:
        amount: Decimal
        captured_at: datetime

    sanitized = api._json_for_db_blob(
        {
            "card": ParserLeak(Decimal("1.25"), datetime(2026, 1, 2, 3, 4, 5)),
            "notes": {"top": {"bergamot", "lemon"}},
        }
    )
    check(
        "db blob sanitizer accepts parser-native objects",
        sanitized["card"] == {"amount": 1.25, "captured_at": "2026-01-02T03:04:05"}
        and sorted(sanitized["notes"]["top"]) == ["bergamot", "lemon"],
    )

    response = requests.Response()
    response.status_code = 500
    response.headers["Content-Type"] = "application/json"
    response._content = b'{"detail":"complete_job failed (TypeError): bad payload"}'
    check(
        "worker extracts structured API error detail",
        enrichment_worker._safe_response_text(response) == "complete_job failed (TypeError): bad payload",
    )

    class FakeSession:
        def __init__(self) -> None:
            self.last_json: dict[str, object] | None = None

        def request(self, _method: str, _url: str, **kwargs: object) -> requests.Response:
            self.last_json = kwargs.get("json")  # type: ignore[assignment]
            ok = requests.Response()
            ok.status_code = 200
            ok._content = b"{}"
            return ok

    fake_session = FakeSession()
    worker_client = enrichment_worker.ApiClient("https://api.example.test", "token")
    worker_client.session = fake_session  # type: ignore[assignment]
    worker_client._request(
        "POST",
        "/api/enrichment/jobs/test/complete",
        json={
            "card": ParserLeak(Decimal("2.50"), datetime(2026, 2, 3, 4, 5, 6)),
            "tags": {"warm", "amber"},
        },
    )
    sent = fake_session.last_json or {}
    check(
        "worker normalizes outbound JSON payloads before requests",
        sent.get("card") == {"amount": 2.5, "captured_at": "2026-02-03T04:05:06"}
        and sorted(sent.get("tags") or []) == ["amber", "warm"],
    )

    saved_enqueue_state = db.enqueue_job_state
    try:
        db.enqueue_job_state = lambda **_kwargs: {  # type: ignore[assignment]
            "status": "completed",
            "requested_count": 82,
        }
        selected = api.engine.UnifiedFragrance(
            name="Completed Partial",
            brand="Test House",
            year="2026",
            frag_url=(
                "https://www.fragrantica.com/perfume/"
                "Test-House/Completed-Partial-82.html"
            ),
        )
        job_state = api._enqueue_enrichment_job(
            selected,
            api.DetailRequest(source_url=selected.frag_url),
        )
        status, requested_count = api._enrichment_status_from_job_state(job_state)
        check("duplicate completed queue row stays completed", status == "completed")
        check(
            "duplicate completed queue row keeps requested count",
            requested_count == 82,
        )
    finally:
        db.enqueue_job_state = saved_enqueue_state


def test_mobile_new_device_session_binding() -> None:
    print("Mobile auth checks (no DATABASE_URL required):")
    now = datetime.now(timezone.utc)
    account_row = {
        "id": "account-1",
        "email": "worker@example.test",
        "label": "Worker",
        "pin_hash": "stored",
        "disabled": False,
        "pin_strikes": 5,
        "created_at": now,
        "last_login_at": None,
    }
    expired = db._account_to_dict(
        {**account_row, "locked_until": now - timedelta(seconds=1)}
    )
    active = db._account_to_dict(
        {**account_row, "locked_until": now + timedelta(minutes=15)}
    )
    check("expired mobile lockout is inactive", expired["locked_until"] is None)
    check("active mobile lockout remains visible", active["locked_until"] is not None)

    saved = {
        "enabled": db.ENABLED,
        "consume_magic_link": db.consume_magic_link,
        "get_worker_account": db.get_worker_account,
        "reset_pin_strikes": db.reset_pin_strikes,
        "create_session": db.create_session,
        "verify_pin": mobile.verify_pin,
    }
    created: dict[str, str] = {}
    token = "test-magic-token"

    def fake_create_session(
        *, account_id: str, device_fingerprint: str, ttl_days: int
    ) -> str:
        created["account_id"] = account_id
        created["device"] = device_fingerprint
        created["ttl_days"] = str(ttl_days)
        return "session-1"

    try:
        db.ENABLED = True
        db.consume_magic_link = lambda token_hash: (
            {"account_id": "account-1"}
            if token_hash == mobile._hash_token(token)
            else None
        )
        db.get_worker_account = lambda account_id: {
            "id": account_id,
            "email": "worker@example.test",
            "pin_hash": "stored",
            "disabled": False,
            "locked_until": None,
        }
        db.reset_pin_strikes = lambda account_id: None
        db.create_session = fake_create_session
        mobile.verify_pin = lambda pin, stored: pin == "123456"

        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/m/verify",
                "headers": [],
                "query_string": b"",
                "client": ("127.0.0.1", 12345),
                "server": ("testserver", 443),
                "scheme": "https",
            }
        )
        response = mobile.m_verify(request, token=token, pin="123456")

        cookie = SimpleCookie()
        for header, value in response.raw_headers:
            if header.lower() == b"set-cookie":
                cookie.load(value.decode("latin-1"))

        check("mobile verify redirects after valid token and PIN", response.status_code == 302)
        check("mobile verify emits a device cookie", mobile.DEVICE_COOKIE in cookie)
        check("mobile verify emits a session cookie", mobile.SESSION_COOKIE in cookie)
        check(
            "new mobile session is bound to the emitted device cookie",
            created.get("device") == cookie[mobile.DEVICE_COOKIE].value,
        )
    finally:
        db.ENABLED = saved["enabled"]
        db.consume_magic_link = saved["consume_magic_link"]
        db.get_worker_account = saved["get_worker_account"]
        db.reset_pin_strikes = saved["reset_pin_strikes"]
        db.create_session = saved["create_session"]
        mobile.verify_pin = saved["verify_pin"]


def test_hydration_dedup() -> None:
    print("Detail hydration checks (no DATABASE_URL required):")

    def make_entry() -> dict[str, object]:
        return {
            "frag_cards": {
                "Community Interest": [{"label": "Have", "count": "12"}],
                "Performance": [{"label": "Longevity", "count": "8"}],
            },
            "notes": {
                "has_pyramid": True,
                "top": ["bergamot"],
                "heart": ["rose"],
                "base": ["musk"],
                "flat": [],
            },
            "pros_cons": ["long lasting", "great projection"],
            "reviews": [
                {"text": "A modern classic.", "source": "fragrantica"},
                {"text": "A touch sharp on me.", "source": "fragrantica"},
            ],
        }

    # Re-hydrating the same bundle twice must not accumulate duplicate rows --
    # _hydrate_details_from_entry can run more than once over one bundle.
    details = api.engine.UnifiedDetails(notes=api.engine.NotesList())
    first = api._hydrate_details_from_entry(details, make_entry())
    check("first hydration applies cached Fragrantica data", first is True)
    pros_after_first = list(details.pros_cons)
    reviews_after_first = len(details.reviews)
    api._hydrate_details_from_entry(details, make_entry())
    check(
        "re-hydration does not duplicate pros_cons",
        details.pros_cons == pros_after_first,
        f"{details.pros_cons}",
    )
    check(
        "re-hydration does not duplicate reviews",
        len(details.reviews) == reviews_after_first,
        f"{len(details.reviews)} review(s)",
    )

    # A complete cache entry backfills a partial bundle: live rows are kept
    # exactly once, and only genuinely new cached rows are appended.
    partial = api.engine.UnifiedDetails(
        notes=api.engine.NotesList(top=["lemon"]),
        pros_cons=["long lasting"],
    )
    partial.reviews.append(api.engine.Review(text="A modern classic.", source="live"))
    applied = api._hydrate_details_from_entry(partial, make_entry())
    check("hydration applies to a partial bundle", applied is True)
    check(
        "live pros_cons row kept exactly once after overlay",
        partial.pros_cons.count("long lasting") == 1,
        f"{partial.pros_cons}",
    )
    check(
        "new cached pros_cons row is appended",
        "great projection" in partial.pros_cons,
    )
    check(
        "complete cache entry backfills missing frag_cards",
        "Performance" in (partial.frag_cards or {}),
    )
    check(
        "same review text with a different source stays distinct",
        sum(1 for r in partial.reviews if r.text == "A modern classic.") == 2,
        f"{[(r.text, r.source) for r in partial.reviews]}",
    )


def test_concentration_pipeline() -> None:
    print("Concentration enrichment checks (no DATABASE_URL required):")

    # 1. The /complete payload model accepts a concentration field.
    payload = api.CompleteJobRequest(
        fg_url="https://www.fragrantica.com/perfume/Test-House/Test-1.html",
        frag_cards={"Community Interest": [{"label": "Have", "count": "1"}]},
        concentration="Eau de Parfum",
    )
    check(
        "complete payload model accepts concentration",
        payload.concentration == "Eau de Parfum",
    )

    # 2. Worker tier-1 resolution works without the SERP/browser tier.
    # Fictional names keep the Parfinity catalog tier from short-circuiting.
    resolved = enrichment_worker._resolve_concentration(
        "Testhouse", "Imaginary Extrait de Parfum", allow_serp=False
    )
    check(
        "worker resolves explicit concentration token",
        bool(resolved) and resolved["concentration"] == "Extrait",
        str(resolved),
    )
    resolved = enrichment_worker._resolve_concentration(
        "Xerjoff", "Totally Made Up Scent", allow_serp=False
    )
    check(
        "worker resolves brand-prior concentration",
        bool(resolved) and resolved["concentration"] == "Parfum",
        str(resolved),
    )
    check(
        "worker concentration meta carries source",
        bool(resolved) and resolved["concentration_meta"]["source"] == "tier1_brand_prior",
        str(resolved),
    )

    # 3. _apply_concentration injects into payload + raw_identity.
    worker_payload = {"raw_identity": {"name": "Imaginary Extrait de Parfum"}}
    enrichment_worker._apply_concentration(
        worker_payload, "Testhouse", "Imaginary Extrait de Parfum"
    )
    check(
        "worker payload carries top-level concentration",
        worker_payload.get("concentration") == "Extrait",
        str(worker_payload),
    )
    check(
        "worker raw_identity carries concentration + meta",
        worker_payload["raw_identity"].get("concentration") == "Extrait"
        and isinstance(worker_payload["raw_identity"].get("concentration_meta"), dict),
        str(worker_payload["raw_identity"]),
    )

    # 4. Cache hydration fills details.concentration (fill-only, live wins).
    details = api.engine.UnifiedDetails(notes=api.engine.NotesList())
    entry = {
        "frag_cards": {"Community Interest": [{"label": "Have", "count": "1"}]},
        "raw_identity": {"concentration": "Eau de Parfum"},
    }
    api._hydrate_details_from_entry(details, entry)
    check(
        "hydration fills concentration from cached raw_identity",
        getattr(details, "concentration", None) == "Eau de Parfum",
    )
    setattr(details, "concentration", "Extrait")
    api._hydrate_details_from_entry(details, entry)
    check(
        "hydration never overwrites an existing concentration",
        getattr(details, "concentration", None) == "Extrait",
    )

    # 5. Stored fragrance_records surface concentration from fg_raw.
    record = {
        "record_key": "fg:test",
        "canonical_fg_url": "https://www.fragrantica.com/perfume/Test-House/Test-1.html",
        "bn_url": None,
        "name": "Test",
        "house": "Test House",
        "year": 2026,
        "gender": "Unisex",
        "image_url": None,
        "search": {},
        "fg_raw": {
            "frag_cards": {"Community Interest": [{"label": "Have", "count": "1"}]},
            "concentration": "Extrait",
        },
        "bn_raw": {},
        "derived_metrics": None,
    }
    stored_details = api._details_from_fragrance_record(record)
    check(
        "stored record surfaces concentration onto details",
        getattr(stored_details, "concentration", None) == "Extrait",
    )

    # 6. The backlog sweeper flags rows missing base fields.
    import scripts.enrich_database_metrics as sweeper

    complete_dm = {
        "source_coverage": {
            "performance_score": True,
            "value_score": True,
            "community_interest_score": True,
            "wear_profile": True,
        }
    }
    full_row = {
        "derived_metrics": complete_dm,
        "gender": "Unisex",
        "year": 2026,
        "concentration": "Eau de Parfum",
    }
    check(
        "sweeper accepts a row with metrics and base fields",
        sweeper._stored_metrics_complete(full_row) is True,
    )
    for missing in ("gender", "year", "concentration"):
        partial_row = {k: v for k, v in full_row.items() if k != missing}
        check(
            f"sweeper flags a row missing {missing}",
            sweeper._stored_metrics_complete(partial_row) is False,
        )
    check(
        "sweeper treats explicit Unknown concentration as incomplete",
        sweeper._stored_metrics_complete({**full_row, "concentration": "Unknown"}) is False,
    )
    check(
        "sweeper treats default gender as incomplete",
        sweeper._stored_metrics_complete({**full_row, "gender": "Unisex / Unspecified"}) is False,
    )
    nested_row = {
        "derived_metrics": complete_dm,
        "concentration": "Eau de Parfum",
        "product": {"gender": "Unisex", "year": 2026},
    }
    check(
        "sweeper reads base fields nested under product",
        sweeper._stored_metrics_complete(nested_row) is True,
    )

    # 7. Backfill writes the missing base fields in place.
    row = {"concentration": ""}
    sweeper._backfill_base_fields(
        row, gender="Men", year=2020, brand="Testhouse", name="Imaginary Extrait de Parfum"
    )
    check(
        "backfill fills gender/year/concentration",
        row.get("gender") == "Men"
        and row.get("year") == 2020
        and row.get("concentration") == "Extrait",
        str(row),
    )

    # 8. Fact helpers derive app-facing family/wear fields from derived_metrics.
    import enrichment_facts

    derived = {
        "main_accords": {
            "top_accords": ["woody", "aromatic", "citrus"],
        },
        "wear_profile": {
            "primary_seasons": ["Spring", "Summer"],
            "primary_time": "Day",
        },
    }
    families = enrichment_facts.derive_families(derived, existing_family="Unknown Family")
    check("family derivation maps main accords", families["primary_family"] == "Woody", str(families))
    check(
        "default family is not complete",
        enrichment_facts.is_fact_complete("Unknown Family", "family") is False,
    )
    check(
        "unsupported Universal season is not complete",
        enrichment_facts.is_fact_complete("Universal", "season") is False,
    )

    payload = {}
    sweeper._apply_derived_facts(payload, derived)
    check(
        "sweeper projects derived family and season",
        payload.get("family") == "Woody" and payload.get("season") == "Spring",
        str(payload),
    )

    selected = api.engine.UnifiedFragrance(name="Test", brand="House", year="2026")
    detail_payload = api._details_to_dict(
        selected,
        api.engine.UnifiedDetails(
            notes=api.engine.NotesList(),
            derived_metrics=derived,
        ),
    )
    check(
        "details response projects family facts",
        detail_payload.get("family") == "Woody"
        and detail_payload.get("families") == ["Woody", "Aromatic", "Citrus"],
        str(detail_payload),
    )
    check(
        "details response projects wear profile",
        detail_payload.get("wear_profile", {}).get("primary_time") == "Day",
        str(detail_payload.get("wear_profile")),
    )

    merged_raw_identity = db._cache_row_raw_identity(
        {
            "raw_identity": {"name": "Test"},
            "concentration": "Eau de Parfum",
            "concentration_meta": {"source": "unit_test", "confidence": 99},
        }
    )
    check(
        "complete_job raw_identity merge keeps concentration durable",
        merged_raw_identity.get("concentration") == "Eau de Parfum"
        and merged_raw_identity.get("concentration_meta", {}).get("source") == "unit_test",
        str(merged_raw_identity),
    )


def test_stored_detail_completion_logic() -> None:
    print("Stored detail completion logic checks (no DATABASE_URL required):")

    saved_enabled = db.ENABLED
    saved_lookup_rec = db.lookup_fragrance_record
    saved_lookup_cache = db.lookup_detail_cache
    saved_enqueue = db.enqueue_job
    saved_recover = db.recover_or_enqueue_job
    saved_upsert = db.upsert_fragrance_details

    enqueue_calls = []
    recover_calls = []

    # A record representing a completed worker job but with missing metrics (wear_profile missing):
    mock_frag_record_partial = {
        "record_key": "fg:https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
        "canonical_fg_url": "https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
        "bn_url": None,
        "name": "Test Fragrance",
        "house": "Test Brand",
        "year": 2026,
        "gender": "Unisex",
        "image_url": None,
        "search": {},
        # No wear_profile
        "fg_raw": {
            "frag_cards": {
                "Community Interest": [{"label": "Have", "count": "10"}],
                "Performance": [{"label": "Longevity", "count": "5"}],
                "Price Value": [{"label": "Good Value", "count": "3"}],
            }
        },
        "bn_raw": {},
        "derived_metrics": None,
    }

    try:
        db.ENABLED = True

        def fake_lookup_fragrance_record(canonical_fg_url, bn_url):
            return mock_frag_record_partial

        def fake_lookup_detail_cache(canonical_fg_url):
            return {
                "canonical_fg_url": canonical_fg_url,
                "name": "Test Fragrance",
                "house": "Test Brand",
                "quality_status": "complete",
                "frag_cards": mock_frag_record_partial["fg_raw"]["frag_cards"],
            }

        def fake_enqueue_job(*args, **kwargs):
            enqueue_calls.append((args, kwargs))
            return 1

        def fake_recover_or_enqueue_job(*args, **kwargs):
            recover_calls.append((args, kwargs))
            return {"status": "pending", "requested_count": 83}

        def fake_upsert_fragrance_details(*args, **kwargs):
            pass

        db.lookup_fragrance_record = fake_lookup_fragrance_record
        db.lookup_detail_cache = fake_lookup_detail_cache
        db.enqueue_job = fake_enqueue_job
        db.recover_or_enqueue_job = fake_recover_or_enqueue_job
        db.upsert_fragrance_details = fake_upsert_fragrance_details

        client = TestClient(api.app)

        res1 = client.post(
            "/api/fragrances/details",
            json={"id": "source:https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html"}
        )
        check("details response 200 on first call", res1.status_code == 200)
        data1 = res1.json()

        check("enrichment block is present", "enrichment" in data1)
        if "enrichment" in data1:
            check("incomplete stored detail reopens as pending", data1["enrichment"]["status"] == "pending")
            check("incomplete stored detail requires worker", data1["enrichment"]["requires_worker"] is True)
            check("incomplete stored detail surfaces requested count", data1["enrichment"]["requested_count"] == 83)

        check("source_coverage is present", "source_coverage" in data1)
        if "source_coverage" in data1:
            check(
                "fragrantica_metrics_complete is false (since metrics are incomplete)",
                data1["source_coverage"]["fragrantica_metrics_complete"] is False
            )

        check("no enqueue job calls were made", len(enqueue_calls) == 0, f"calls={enqueue_calls}")
        check("recovery job was called by default", len(recover_calls) == 1, f"calls={recover_calls}")

        res2 = client.post(
            "/api/fragrances/details",
            json={
                "id": "source:https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
                "recover_incomplete": True,
            },
        )
        check("details response 200 with recovery flag", res2.status_code == 200)
        data2 = res2.json()
        check("incomplete completed detail reopens as pending", data2["enrichment"]["status"] == "pending")
        check("reopened detail requires worker", data2["enrichment"]["requires_worker"] is True)
        check("reopened detail surfaces requested count", data2["enrichment"]["requested_count"] == 83)
        check("recovery flag remains compatible", len(recover_calls) == 2, f"calls={recover_calls}")

        parfumo_url = "https://www.parfumo.com/Perfumes/Creed/Bayrhum_Vetiver"
        mock_parfumo_record = {
            "record_key": f"fg:{parfumo_url}",
            "canonical_fg_url": parfumo_url,
            "bn_url": "https://basenotes.com/fragrances/bayrhum-vetiver-by-creed.26120009",
            "name": "Bayrhum Vetiver",
            "house": "Creed",
            "year": 2006,
            "gender": "Unisex",
            "image_url": None,
            "search": {},
            "fg_raw": {
                "frag_cards": {},
                "notes": {
                    "has_pyramid": True,
                    "top": ["Bay Leaf", "Pepper"],
                    "heart": ["Vetiver"],
                    "base": ["Vetiver"],
                    "flat": [],
                },
                "raw_identity": {"parfumo_url": parfumo_url},
                "source": "parfumo",
                "quality_status": "partial",
            },
            "bn_raw": {},
            "derived_metrics": {
                "notes": {
                    "has_pyramid": True,
                    "top": ["Bay Leaf", "Pepper"],
                    "heart": ["Vetiver"],
                    "base": ["Vetiver"],
                    "flat": [],
                },
                "source_coverage": {
                    "performance_score": False,
                    "value_score": False,
                    "community_interest_score": False,
                    "wear_profile": False,
                    "notes": True,
                },
            },
        }

        def fake_lookup_parfumo_record(canonical_fg_url, bn_url):
            return mock_parfumo_record

        db.lookup_fragrance_record = fake_lookup_parfumo_record
        db.lookup_detail_cache = lambda canonical_fg_url: None
        selected = api.engine.UnifiedFragrance(
            name="Bayrhum Vetiver",
            brand="Creed",
            year="",
            bn_url="https://basenotes.com/fragrances/bayrhum-vetiver-by-creed.26120009",
            frag_url=parfumo_url,
        )
        res3 = client.post("/api/fragrances/details", json={"id": api._encode_id(selected)})
        check("parfumo stored detail response 200", res3.status_code == 200, str(res3.status_code))
        data3 = res3.json()
        check("parfumo fallback is surfaced as partial", data3["enrichment"]["status"] == "partial", str(data3.get("enrichment")))
        check("parfumo fallback is not marked worker-complete", data3["source_coverage"]["complete"] is False, str(data3.get("source_coverage")))
        check("parfumo fallback keeps parsed top notes", data3["raw"]["notes"]["top"] == ["Bay Leaf", "Pepper"], str(data3["raw"]["notes"]))

    finally:
        db.ENABLED = saved_enabled
        db.lookup_fragrance_record = saved_lookup_rec
        db.lookup_detail_cache = saved_lookup_cache
        db.enqueue_job = saved_enqueue
        db.recover_or_enqueue_job = saved_recover
        db.upsert_fragrance_details = saved_upsert


def test_detail_fetch_saturation_returns_retryable_503() -> None:
    print("Detail fetch saturation checks (no DATABASE_URL required):")
    old_gate = api._DETAIL_FETCH_SEMAPHORE
    old_timeout = api._DETAIL_FETCH_QUEUE_TIMEOUT
    old_fetch = api.engine.fetch_selected_details
    gate = threading.BoundedSemaphore(1)
    gate.acquire()
    called = {"fetch": 0}
    try:
        api._DETAIL_FETCH_SEMAPHORE = gate
        api._DETAIL_FETCH_QUEUE_TIMEOUT = 0.0

        def fail_fetch(*args, **kwargs):
            called["fetch"] += 1
            raise AssertionError("detail fetch should not run when the gate is saturated")

        api.engine.fetch_selected_details = fail_fetch
        client = TestClient(api.app)
        response = client.post(
            "/api/fragrances/details",
            json={
                "id": "source:https://www.fragrantica.com/perfume/Le-Labo/Santal-33-12201.html"
            },
        )
    finally:
        try:
            gate.release()
        except ValueError:
            pass
        api._DETAIL_FETCH_SEMAPHORE = old_gate
        api._DETAIL_FETCH_QUEUE_TIMEOUT = old_timeout
        api.engine.fetch_selected_details = old_fetch

    check("saturated detail fetch returns 503", response.status_code == 503, str(response.status_code))
    check("saturated detail fetch sets Retry-After", response.headers.get("retry-after") == "2", str(response.headers))
    check("saturated detail fetch does not call engine", called["fetch"] == 0, str(called))


# ---------------------------------------------------------------------------
# DB-backed checks (only when DATABASE_URL is set)
# ---------------------------------------------------------------------------

_TEST_JOB_KEY = "https://www.fragrantica.com/perfume/_test_/enrichment-selftest-0.html"


def _cleanup() -> None:
    ctx, conn = db._conn()
    try:
        conn.execute("DELETE FROM enrichment_jobs WHERE job_key = %s", (_TEST_JOB_KEY,))
        conn.execute(
            "DELETE FROM fg_detail_cache WHERE canonical_fg_url = %s", (_TEST_JOB_KEY,)
        )
        conn.execute(
            "DELETE FROM fragrance_records WHERE canonical_fg_url = %s", (_TEST_JOB_KEY,)
        )
    finally:
        ctx.__exit__(None, None, None)


def test_db_lifecycle() -> None:
    if not db.ENABLED:
        return
    print("DB-backed checks (DATABASE_URL is set):")
    db.init_db()
    _cleanup()
    try:
        # Enqueue, then enqueue again -> upsert, requested_count bumps to 2.
        enqueue_counts = [
            db.enqueue_job(
                job_key=_TEST_JOB_KEY,
                query="enrichment selftest",
                name="Enrichment Selftest",
                house="_test_",
                year=2026,
                bn_url=None,
                fg_url=_TEST_JOB_KEY,
            )
            for _ in range(2)
        ]
        check(
            "enqueue_job returns the incrementing requested_count",
            enqueue_counts == [1, 2],
            str(enqueue_counts),
        )
        jobs = [j for j in db.list_jobs("pending", 100) if j["job_key"] == _TEST_JOB_KEY]
        check("enqueue creates exactly one pending job", len(jobs) == 1)
        job = jobs[0]
        check("duplicate enqueue upserts (requested_count == 2)", job["requested_count"] == 2)

        # Claim it.
        claim = db.claim_job(job["id"], lease_seconds=900)
        check("pending job can be claimed", claim.get("claimed") is True)
        reclaim = db.claim_job(job["id"], lease_seconds=900)
        check("active claim is not re-claimable", reclaim.get("claimed") is False)

        # Stale-claim reclaim: expire the lease, then it is claimable again.
        ctx, conn = db._conn()
        try:
            conn.execute(
                "UPDATE enrichment_jobs SET claim_expires_at = now() - interval '1 hour' "
                "WHERE id = %s",
                (job["id"],),
            )
        finally:
            ctx.__exit__(None, None, None)
        stale = db.claim_job(job["id"], lease_seconds=900)
        check("stale processing job can be reclaimed", stale.get("claimed") is True)

        # Complete with valid frag_cards -> job completed + cache row written.
        cache_row = {
            "canonical_fg_url": _TEST_JOB_KEY,
            "name": "Enrichment Selftest",
            "house": "_test_",
            "year": 2026,
            "schema_version": 1,
            "source": "selftest",
            "captured_at": None,
            "frag_cards": {"Community Interest": [{"label": "Have", "count": "1"}]},
            "notes": {"top": ["bergamot"], "heart": [], "base": [], "flat": [], "has_pyramid": True},
            "pros_cons": ["smells good"],
            "reviews": [{"text": "nice", "source": "selftest"}],
            "raw_identity": {"name": "Enrichment Selftest"},
            "concentration": "Eau de Parfum",
            "concentration_meta": {"source": "selftest", "confidence": 99},
            "quality_status": "complete",
        }
        updated = db.complete_job(job["id"], cache_row)
        check("complete_job marks job completed", updated and updated["status"] == "completed")

        cached = db.lookup_detail_cache(_TEST_JOB_KEY)
        check("complete_job upserts fg_detail_cache", cached is not None)
        check(
            "cached entry carries non-empty frag_cards",
            bool(cached and cached.get("frag_cards")),
        )
        check(
            "cached entry quality_status is complete",
            bool(cached and cached.get("quality_status") == "complete"),
        )
        check(
            "cached raw_identity carries top-level concentration",
            bool(
                cached
                and cached.get("raw_identity", {}).get("concentration") == "Eau de Parfum"
                and cached.get("raw_identity", {}).get("concentration_meta", {}).get("source") == "selftest"
            ),
        )

        requeued = db.requeue_job(job["id"], priority=25)
        check("requeue_job resurrects completed jobs", requeued and requeued["status"] == "pending")
        check("requeue_job raises priority", bool(requeued and requeued["priority"] >= 25))

        refreshed = db.complete_job(
            job["id"],
            {
                **cache_row,
                "name": "Enrichment Selftest Fresh",
                "house": "_fresh_",
                "image_url": "https://img.example.test/fresh.jpg",
                "frag_cards": {"Community Interest": [{"label": "Have", "count": "2"}]},
            },
        )
        check("refreshed job completes", refreshed and refreshed["status"] == "completed")
        cached = db.lookup_detail_cache(_TEST_JOB_KEY)
        check(
            "complete_job overwrites cached name",
            bool(cached and cached.get("name") == "Enrichment Selftest Fresh"),
        )
        check(
            "complete_job overwrites cached image_url",
            bool(cached and cached.get("image_url") == "https://img.example.test/fresh.jpg"),
        )

        requeued = db.requeue_job(job["id"], priority=30)
        check("requeue_job resurrects refreshed jobs", requeued and requeued["status"] == "pending")
        original_aggregate_writer = db._upsert_completed_fragrance_record
        try:
            db._upsert_completed_fragrance_record = lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("simulated aggregate failure")
            )
            recovered = db.complete_job(
                job["id"],
                {
                    **cache_row,
                    "name": "Enrichment Selftest Recovered",
                    "house": "_recovered_",
                    "image_url": "https://img.example.test/recovered.jpg",
                    "frag_cards": {"Community Interest": [{"label": "Have", "count": "3"}]},
                },
            )
        finally:
            db._upsert_completed_fragrance_record = original_aggregate_writer

        check(
            "complete_job tolerates aggregate fragrance_records failures",
            recovered and recovered["status"] == "completed",
        )
        cached = db.lookup_detail_cache(_TEST_JOB_KEY)
        check(
            "aggregate failure still leaves fg_detail_cache updated",
            bool(cached and cached.get("name") == "Enrichment Selftest Recovered"),
        )

        # Legacy-complete decision: a cache_row that omits quality_status falls
        # back to 'complete' (matches the fg_detail_cache column DEFAULT). This
        # is the behaviour legacy rows -- written before the column existed --
        # inherit via the ADD COLUMN migration in db.py.
        legacy_row = {k: v for k, v in cache_row.items() if k != "quality_status"}
        legacy_row["name"] = "Enrichment Selftest Legacy"
        db.complete_job(job["id"], legacy_row)
        cached = db.lookup_detail_cache(_TEST_JOB_KEY)
        check(
            "cache_row without quality_status defaults to complete",
            bool(cached and cached.get("quality_status") == "complete"),
        )

        # patch_job: attach fg_url to a pending row missing one.
        patch_key = "https://www.fragrantica.com/perfume/_test_/enrichment-patch-selftest.html"
        _cleanup()
        db.enqueue_job(
            job_key="patch-selftest",
            query=None,
            name="For Tony Iommi",
            house="Xerjoff",
            year=2024,
            bn_url=None,
            fg_url=None,
        )
        patch_jobs = [j for j in db.list_jobs("pending", 100) if j.get("job_key") == "patch-selftest"]
        check("patch enqueue creates pending job", len(patch_jobs) == 1)
        patch_job_row = patch_jobs[0]
        patched = db.patch_job(patch_job_row["id"], fg_url=patch_key)
        check("patch_job sets fg_url", bool(patched and patched.get("fg_url") == patch_key))
        check(
            "patch_job deduped query builder input",
            enrichment_worker._build_query(patch_job_row) == "Xerjoff For Tony Iommi",
        )
        conn_ctx, conn = db._conn()
        try:
            conn.execute("DELETE FROM enrichment_jobs WHERE job_key = %s", ("patch-selftest",))
        finally:
            conn_ctx.__exit__(None, None, None)
    finally:
        _cleanup()


def main() -> int:
    test_no_db_contract()
    test_mobile_new_device_session_binding()
    test_hydration_dedup()
    test_concentration_pipeline()
    test_stored_detail_completion_logic()
    test_detail_fetch_saturation_returns_retryable_503()
    if db.ENABLED:
        test_db_lifecycle()
    else:
        print("DB-backed checks: SKIPPED (DATABASE_URL not set).")

    print()
    if _FAILURES:
        print(f"{len(_FAILURES)} check(s) FAILED: {', '.join(_FAILURES)}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
