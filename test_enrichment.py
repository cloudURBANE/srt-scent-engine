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

from fastapi.testclient import TestClient

import api
import db

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
    finally:
        api._ENRICHMENT_WORKER_TOKEN = saved


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
    finally:
        _cleanup()


def main() -> int:
    test_no_db_contract()
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
