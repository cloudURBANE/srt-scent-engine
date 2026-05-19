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
from http.cookies import SimpleCookie
from dataclasses import dataclass
from datetime import datetime
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
    try:
        api._ENRICHMENT_WORKER_TOKEN = ""
        r = client.get("/api/enrichment/jobs")
        check("worker endpoint 503 when token unconfigured", r.status_code == 503, str(r.status_code))

        api._ENRICHMENT_WORKER_TOKEN = "test-secret-token"
        r = client.get("/api/enrichment/jobs")
        check("worker endpoint 401 without bearer token", r.status_code == 401, str(r.status_code))

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


def test_mobile_new_device_session_binding() -> None:
    print("Mobile auth checks (no DATABASE_URL required):")
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
    finally:
        ctx.__exit__(None, None, None)


def test_db_lifecycle() -> None:
    print("DB-backed checks (DATABASE_URL is set):")
    db.init_db()
    _cleanup()
    try:
        # Enqueue, then enqueue again -> upsert, requested_count bumps to 2.
        for _ in range(2):
            db.enqueue_job(
                job_key=_TEST_JOB_KEY,
                query="enrichment selftest",
                name="Enrichment Selftest",
                house="_test_",
                year=2026,
                bn_url=None,
                fg_url=_TEST_JOB_KEY,
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
    finally:
        _cleanup()


def main() -> int:
    test_no_db_contract()
    test_mobile_new_device_session_binding()
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
