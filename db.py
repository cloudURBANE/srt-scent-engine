#!/usr/bin/env python3
"""
db.py

Durable Postgres storage for the enrichment pipeline.

This module is the *only* place that talks to the database. api.py imports the
functions here; nothing else does. It backs two things:

    enrichment_jobs   -- the Pass 1 work queue. A partial /details response
                         (Fragrantica URL linked, but no live/cached FG data)
                         enqueues a job here so an offline worker can fetch it.
    fg_detail_cache   -- the Pass 2 durable detail cache. A worker uploads
                         parsed Fragrantica detail output via the complete
                         endpoint; future /details requests hydrate from it.

Graceful degradation
--------------------
The whole module is inert when DATABASE_URL is unset (local dev without a
Postgres, or a misconfigured deploy). `ENABLED` reports this; api.py checks it
before enqueueing and the worker endpoints return 503 when it is False. The
JSON detail-cache overlay and the rest of the API keep working unchanged.

Schema management
-----------------
There is no prior migration framework in this repo, so `init_db()` runs an
idempotent `CREATE TABLE IF NOT EXISTS` bootstrap on startup. It is safe to run
on every boot and never drops or rewrites existing data.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

# psycopg / psycopg_pool are only imported when DATABASE_URL is present, so the
# API can still boot (and the JSON-cache path still works) if the driver is not
# installed in a given environment.
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ENABLED = bool(DATABASE_URL)

_pool: Any = None  # psycopg_pool.ConnectionPool, lazily created in init_db()

# Conservative limits for the worker list endpoint.
DEFAULT_JOB_LIMIT = 20
MAX_JOB_LIMIT = 100
# Lease window for a claimed job; a processing job past this is reclaimable.
DEFAULT_LEASE_SECONDS = 15 * 60

VALID_JOB_STATUSES = ("pending", "processing", "completed", "failed", "ignored")
VALID_QUALITY_STATUSES = ("complete", "partial", "bad_parse", "stale")


# ---------------------------------------------------------------------------
# Connection / schema bootstrap
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS enrichment_jobs (
    id                TEXT PRIMARY KEY,
    job_key           TEXT UNIQUE NOT NULL,
    query             TEXT,
    name              TEXT,
    house             TEXT,
    year              INTEGER,
    bn_url            TEXT,
    fg_url            TEXT,
    status            TEXT NOT NULL DEFAULT 'pending',
    priority          INTEGER NOT NULL DEFAULT 0,
    requested_count   INTEGER NOT NULL DEFAULT 1,
    failure_count     INTEGER NOT NULL DEFAULT 0,
    last_error        TEXT,
    metadata_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at        TIMESTAMPTZ,
    claim_expires_at  TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    failed_at         TIMESTAMPTZ,
    ignored_at        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_enrichment_jobs_status
    ON enrichment_jobs (status, priority DESC, last_requested_at);

CREATE TABLE IF NOT EXISTS fg_detail_cache (
    canonical_fg_url  TEXT PRIMARY KEY,
    name              TEXT,
    house             TEXT,
    year              INTEGER,
    schema_version    INTEGER NOT NULL DEFAULT 1,
    source            TEXT,
    captured_at       TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    frag_cards_json   JSONB NOT NULL,
    notes_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    pros_cons_json    JSONB NOT NULL DEFAULT '[]'::jsonb,
    reviews_json      JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_identity_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality_status    TEXT NOT NULL DEFAULT 'complete'
);
"""


def init_db() -> None:
    """Create the connection pool and bootstrap the schema. No-op when disabled.

    Safe to call once on startup. Raises only if DATABASE_URL is set but the
    database is genuinely unreachable -- that should fail the deploy loudly
    rather than silently running without durable storage.
    """
    global _pool
    if not ENABLED or _pool is not None:
        return
    from psycopg_pool import ConnectionPool

    _pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=5, kwargs={"autocommit": True})
    with _pool.connection() as conn:
        conn.execute(_SCHEMA)


def _conn():
    """Yield a pooled connection with dict rows. Caller uses it as a context mgr."""
    if _pool is None:
        raise RuntimeError("db.init_db() has not been called or DATABASE_URL is unset")
    from psycopg.rows import dict_row

    ctx = _pool.connection()
    conn = ctx.__enter__()
    conn.row_factory = dict_row
    return ctx, conn


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# enrichment_jobs -- Pass 1 queue
# ---------------------------------------------------------------------------


def enqueue_job(
    *,
    job_key: str,
    query: str | None,
    name: str | None,
    house: str | None,
    year: int | None,
    bn_url: str | None,
    fg_url: str | None,
) -> None:
    """Upsert a job by job_key. A duplicate request bumps counters, never dupes.

    On conflict the row's status is left untouched (a pending job stays pending,
    a completed job is not resurrected) except that an `ignored` job is never
    touched at all -- it was deliberately retired.
    """
    if not ENABLED:
        return
    ctx, conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO enrichment_jobs
                (id, job_key, query, name, house, year, bn_url, fg_url,
                 status, requested_count, created_at, last_requested_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s,
                 'pending', 1, now(), now())
            ON CONFLICT (job_key) DO UPDATE SET
                requested_count   = enrichment_jobs.requested_count + 1,
                last_requested_at = now(),
                query             = COALESCE(EXCLUDED.query, enrichment_jobs.query),
                name              = COALESCE(EXCLUDED.name, enrichment_jobs.name),
                house             = COALESCE(EXCLUDED.house, enrichment_jobs.house),
                year              = COALESCE(EXCLUDED.year, enrichment_jobs.year),
                bn_url            = COALESCE(EXCLUDED.bn_url, enrichment_jobs.bn_url)
            WHERE enrichment_jobs.status <> 'ignored'
            """,
            (str(uuid.uuid4()), job_key, query, name, house, year, bn_url, fg_url),
        )
    finally:
        ctx.__exit__(None, None, None)


def get_status_counts() -> dict[str, int]:
    """Public status signal: job counts grouped by status. Empty when disabled."""
    if not ENABLED:
        return {}
    ctx, conn = _conn()
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM enrichment_jobs GROUP BY status"
        ).fetchall()
        counts = {s: 0 for s in VALID_JOB_STATUSES}
        for row in rows:
            counts[row["status"]] = row["n"]
        return counts
    finally:
        ctx.__exit__(None, None, None)


def list_jobs(status: str = "pending", limit: int = DEFAULT_JOB_LIMIT) -> list[dict[str, Any]]:
    """Worker job list, newest-priority first. limit is clamped to MAX_JOB_LIMIT."""
    limit = max(1, min(int(limit or DEFAULT_JOB_LIMIT), MAX_JOB_LIMIT))
    ctx, conn = _conn()
    try:
        rows = conn.execute(
            """
            SELECT * FROM enrichment_jobs
            WHERE status = %s
            ORDER BY priority DESC, last_requested_at ASC, created_at ASC
            LIMIT %s
            """,
            (status, limit),
        ).fetchall()
        return [_job_to_dict(r) for r in rows]
    finally:
        ctx.__exit__(None, None, None)


def get_job(job_id: str) -> dict[str, Any] | None:
    ctx, conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM enrichment_jobs WHERE id = %s", (job_id,)
        ).fetchone()
        return _job_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def claim_job(job_id: str, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> dict[str, Any]:
    """Atomically claim a job.

    Claimable when status is 'pending', or 'processing' with an expired lease
    (the previous claimant died). Returns {"claimed": bool, ...}.
    """
    expires = _now() + timedelta(seconds=lease_seconds)
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            UPDATE enrichment_jobs
            SET status = 'processing', claimed_at = now(), claim_expires_at = %s
            WHERE id = %s
              AND (status = 'pending'
                   OR (status = 'processing' AND claim_expires_at < now()))
            RETURNING claim_expires_at
            """,
            (expires, job_id),
        ).fetchone()
        if row:
            return {"claimed": True, "claim_expires_at": _iso(row["claim_expires_at"])}
        # Not claimable -- report why.
        existing = conn.execute(
            "SELECT status FROM enrichment_jobs WHERE id = %s", (job_id,)
        ).fetchone()
        if not existing:
            return {"claimed": False, "reason": "not_found"}
        reason = (
            "already_processing"
            if existing["status"] == "processing"
            else f"status_{existing['status']}"
        )
        return {"claimed": False, "reason": reason}
    finally:
        ctx.__exit__(None, None, None)


def complete_job(job_id: str, cache_row: dict[str, Any]) -> dict[str, Any] | None:
    """Transactionally upsert fg_detail_cache and mark the job completed.

    `cache_row` must already be validated by the caller (non-empty frag_cards,
    canonical_fg_url present). Returns the updated job dict, or None if the job
    id does not exist. Both writes commit together or not at all.
    """
    from psycopg.types.json import Json

    ctx, conn = _conn()
    try:
        with conn.transaction():
            job = conn.execute(
                "SELECT * FROM enrichment_jobs WHERE id = %s FOR UPDATE", (job_id,)
            ).fetchone()
            if not job:
                return None
            conn.execute(
                """
                INSERT INTO fg_detail_cache
                    (canonical_fg_url, name, house, year, schema_version, source,
                     captured_at, updated_at, frag_cards_json, notes_json,
                     pros_cons_json, reviews_json, raw_identity_json, quality_status)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s, %s, %s)
                ON CONFLICT (canonical_fg_url) DO UPDATE SET
                    name              = EXCLUDED.name,
                    house             = EXCLUDED.house,
                    year              = EXCLUDED.year,
                    schema_version    = EXCLUDED.schema_version,
                    source            = EXCLUDED.source,
                    captured_at       = EXCLUDED.captured_at,
                    updated_at        = now(),
                    frag_cards_json   = EXCLUDED.frag_cards_json,
                    notes_json        = EXCLUDED.notes_json,
                    pros_cons_json    = EXCLUDED.pros_cons_json,
                    reviews_json      = EXCLUDED.reviews_json,
                    raw_identity_json = EXCLUDED.raw_identity_json,
                    quality_status    = EXCLUDED.quality_status
                """,
                (
                    cache_row["canonical_fg_url"],
                    cache_row.get("name"),
                    cache_row.get("house"),
                    cache_row.get("year"),
                    cache_row.get("schema_version", 1),
                    cache_row.get("source", "worker"),
                    cache_row.get("captured_at"),
                    Json(cache_row["frag_cards"]),
                    Json(cache_row.get("notes") or {}),
                    Json(cache_row.get("pros_cons") or []),
                    Json(cache_row.get("reviews") or []),
                    Json(cache_row.get("raw_identity") or {}),
                    cache_row.get("quality_status", "complete"),
                ),
            )
            updated = conn.execute(
                """
                UPDATE enrichment_jobs
                SET status = 'completed', completed_at = now(),
                    claim_expires_at = NULL, last_error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (job_id,),
            ).fetchone()
        return _job_to_dict(updated)
    finally:
        ctx.__exit__(None, None, None)


def fail_job(job_id: str, error: str, retryable: bool) -> dict[str, Any] | None:
    """Record a failure. Retryable -> back to 'pending'; otherwise -> 'failed'."""
    new_status = "pending" if retryable else "failed"
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            UPDATE enrichment_jobs
            SET status = %s,
                failure_count = failure_count + 1,
                last_error = %s,
                failed_at = now(),
                claim_expires_at = NULL
            WHERE id = %s
            RETURNING *
            """,
            (new_status, error, job_id),
        ).fetchone()
        return _job_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def ignore_job(job_id: str, note: str | None) -> dict[str, Any] | None:
    """Permanently retire a job (non-fragrance query, bad identity, dead URL)."""
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            UPDATE enrichment_jobs
            SET status = 'ignored',
                ignored_at = now(),
                last_error = COALESCE(%s, last_error),
                claim_expires_at = NULL
            WHERE id = %s
            RETURNING *
            """,
            (note, job_id),
        ).fetchone()
        return _job_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# fg_detail_cache -- Pass 2 durable detail cache
# ---------------------------------------------------------------------------


def lookup_detail_cache(canonical_fg_url: str) -> dict[str, Any] | None:
    """Fetch a cached detail entry by canonical URL. None when absent/disabled."""
    if not ENABLED or not canonical_fg_url:
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM fg_detail_cache WHERE canonical_fg_url = %s",
            (canonical_fg_url,),
        ).fetchone()
        if not row:
            return None
        return {
            "canonical_fg_url": row["canonical_fg_url"],
            "name": row["name"],
            "house": row["house"],
            "year": row["year"],
            "schema_version": row["schema_version"],
            "source": row["source"],
            "captured_at": _iso(row["captured_at"]),
            "updated_at": _iso(row["updated_at"]),
            "quality_status": row["quality_status"],
            # Shaped to match the JSON-overlay `entry` dict so api.py can reuse
            # one hydration helper for both the DB and JSON cache paths.
            "frag_cards": row["frag_cards_json"],
            "notes": row["notes_json"],
            "pros_cons": row["pros_cons_json"],
            "reviews": row["reviews_json"],
            "raw_identity": row["raw_identity_json"],
        }
    finally:
        ctx.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# Row serialization
# ---------------------------------------------------------------------------


def _iso(dt: Any) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(dt)


def _job_to_dict(row: Any) -> dict[str, Any]:
    """Serialize an enrichment_jobs row for the worker API. Never leaks secrets."""
    return {
        "id": row["id"],
        "job_key": row["job_key"],
        "query": row["query"],
        "name": row["name"],
        "house": row["house"],
        "year": row["year"],
        "bn_url": row["bn_url"],
        "fg_url": row["fg_url"],
        "status": row["status"],
        "priority": row["priority"],
        "requested_count": row["requested_count"],
        "failure_count": row["failure_count"],
        "last_error": row["last_error"],
        "created_at": _iso(row["created_at"]),
        "last_requested_at": _iso(row["last_requested_at"]),
        "claimed_at": _iso(row["claimed_at"]),
        "claim_expires_at": _iso(row["claim_expires_at"]),
        "completed_at": _iso(row["completed_at"]),
        "failed_at": _iso(row["failed_at"]),
        "ignored_at": _iso(row["ignored_at"]),
    }
