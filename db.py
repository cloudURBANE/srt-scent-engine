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

import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any

# psycopg / psycopg_pool are only imported when DATABASE_URL is present, so the
# API can still boot (and the JSON-cache path still works) if the driver is not
# installed in a given environment.
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ENABLED = bool(DATABASE_URL)
logger = logging.getLogger(__name__)

_pool: Any = None  # psycopg_pool.ConnectionPool, lazily created in init_db()

# Conservative limits for the worker list endpoint.
DEFAULT_JOB_LIMIT = 20
MAX_JOB_LIMIT = 100
DEFAULT_DB_POOL_MAX_SIZE = 40
# Lease window for a claimed job; a processing job past this is reclaimable.
DEFAULT_LEASE_SECONDS = 15 * 60
# Hard cap on retryable failures before a job is forcibly demoted to 'failed'.
# Without this, a row that hits the same parser error every tick (e.g. a stale
# Fragrantica URL returning 403) loops forever and starves the rest of the
# pending queue. fail_job() consults this whenever retryable=True.
MAX_RETRYABLE_FAILURES = 10

VALID_JOB_STATUSES = ("pending", "processing", "completed", "failed", "ignored")
VALID_QUALITY_STATUSES = ("complete", "partial", "bad_parse", "stale")


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


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

CREATE TABLE IF NOT EXISTS fragrance_records (
    record_key          TEXT PRIMARY KEY,
    canonical_fg_url    TEXT,
    bn_url              TEXT,
    name                TEXT,
    house               TEXT,
    year                INTEGER,
    gender              TEXT,
    image_url           TEXT,
    search_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    fg_raw_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    bn_raw_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
    derived_metrics_json JSONB,
    source_captured_at  TIMESTAMPTZ,
    metrics_computed_at TIMESTAMPTZ,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fragrance_records_fg_url
    ON fragrance_records (canonical_fg_url)
    WHERE canonical_fg_url IS NOT NULL AND canonical_fg_url <> '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_fragrance_records_bn_url
    ON fragrance_records (bn_url)
    WHERE bn_url IS NOT NULL AND bn_url <> '';
CREATE INDEX IF NOT EXISTS idx_fragrance_records_identity
    ON fragrance_records (house, name);

CREATE TABLE IF NOT EXISTS fg_detail_cache (
    canonical_fg_url  TEXT PRIMARY KEY,
    name              TEXT,
    house             TEXT,
    year              INTEGER,
    image_url         TEXT,
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
ALTER TABLE fg_detail_cache
    ADD COLUMN IF NOT EXISTS image_url TEXT;
-- quality_status was added to the CREATE TABLE above after the table already
-- existed in production, so `CREATE TABLE IF NOT EXISTS` never adds it to a
-- legacy database. Migrate it explicitly. Decision: rows that predate this
-- column are legacy-complete -- before the column existed every cached entry
-- was unconditionally trusted for hydration, so the NOT NULL DEFAULT 'complete'
-- backfills them to exactly that prior behaviour. Genuinely partial entries
-- only exist going forward, where the worker stamps quality_status explicitly.
ALTER TABLE fg_detail_cache
    ADD COLUMN IF NOT EXISTS quality_status TEXT NOT NULL DEFAULT 'complete';

CREATE TABLE IF NOT EXISTS worker_accounts (
    id              TEXT PRIMARY KEY,
    email           TEXT UNIQUE NOT NULL,
    label           TEXT,
    pin_hash        TEXT NOT NULL,
    disabled        BOOLEAN NOT NULL DEFAULT FALSE,
    pin_strikes     INTEGER NOT NULL DEFAULT 0,
    locked_until    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS mobile_sessions (
    id                 TEXT PRIMARY KEY,
    account_id         TEXT NOT NULL REFERENCES worker_accounts(id) ON DELETE CASCADE,
    device_fingerprint TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at         TIMESTAMPTZ NOT NULL,
    revoked_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_mobile_sessions_account
    ON mobile_sessions (account_id);

CREATE TABLE IF NOT EXISTS magic_links (
    id              TEXT PRIMARY KEY,
    account_id      TEXT NOT NULL REFERENCES worker_accounts(id) ON DELETE CASCADE,
    token_hash      TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    consumed_at     TIMESTAMPTZ,
    requesting_ip   TEXT
);
CREATE INDEX IF NOT EXISTS idx_magic_links_token_hash ON magic_links (token_hash);

CREATE TABLE IF NOT EXISTS worker_commands (
    id                    TEXT PRIMARY KEY,
    issued_by_account_id  TEXT REFERENCES worker_accounts(id) ON DELETE SET NULL,
    kind                  TEXT NOT NULL,
    payload_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                TEXT NOT NULL DEFAULT 'queued',
    result_text           TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_worker_commands_queued
    ON worker_commands (status, created_at);

CREATE TABLE IF NOT EXISTS worker_heartbeat (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT worker_heartbeat_singleton CHECK (id = 1)
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

    max_size = _env_int("DB_POOL_MAX_SIZE", DEFAULT_DB_POOL_MAX_SIZE)
    _pool = ConnectionPool(
        DATABASE_URL,
        min_size=1,
        max_size=max_size,
        kwargs={"autocommit": True},
    )
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


def _valid_uuid_text(value: Any) -> bool:
    try:
        uuid.UUID(str(value or ""))
    except (TypeError, ValueError, AttributeError):
        return False
    return True


# ---------------------------------------------------------------------------
# enrichment_jobs -- Pass 1 queue
# ---------------------------------------------------------------------------


def _upsert_completed_fragrance_record(
    conn: Any, job: dict[str, Any], cache_row: dict[str, Any]
) -> None:
    """Best-effort aggregate update for a completed enrichment payload."""
    from psycopg.types.json import Json

    record_key = _find_fragrance_record_key(
        conn,
        canonical_fg_url=_clean_url(cache_row["canonical_fg_url"]),
        bn_url=_clean_url(job.get("bn_url")),
        fallback_key=_fragrance_record_key(
            {
                "canonical_fg_url": cache_row["canonical_fg_url"],
                "bn_url": job.get("bn_url"),
                "name": cache_row.get("name"),
                "house": cache_row.get("house"),
                "year": cache_row.get("year"),
            }
        ),
    )
    fg_raw = {
        "frag_cards": cache_row["frag_cards"],
        "notes": cache_row.get("notes") or {},
        "pros_cons": cache_row.get("pros_cons") or [],
        "reviews": cache_row.get("reviews") or [],
        "raw_identity": cache_row.get("raw_identity") or {},
        "source": cache_row.get("source", "worker"),
        "quality_status": cache_row.get("quality_status", "complete"),
    }
    derived_metrics = cache_row.get("derived_metrics")
    conn.execute(
        """
        INSERT INTO fragrance_records
            (record_key, canonical_fg_url, bn_url, name, house, year,
             gender, image_url, fg_raw_json, derived_metrics_json,
             source_captured_at, metrics_computed_at,
             first_seen_at, last_seen_at, updated_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             now(), CASE WHEN %s THEN now() ELSE NULL END,
             now(), now(), now())
        ON CONFLICT (record_key) DO UPDATE SET
            canonical_fg_url = COALESCE(EXCLUDED.canonical_fg_url, fragrance_records.canonical_fg_url),
            bn_url           = COALESCE(EXCLUDED.bn_url, fragrance_records.bn_url),
            name             = COALESCE(EXCLUDED.name, fragrance_records.name),
            house            = COALESCE(EXCLUDED.house, fragrance_records.house),
            year             = COALESCE(EXCLUDED.year, fragrance_records.year),
            gender           = COALESCE(EXCLUDED.gender, fragrance_records.gender),
            image_url        = COALESCE(EXCLUDED.image_url, fragrance_records.image_url),
            fg_raw_json      = EXCLUDED.fg_raw_json,
            derived_metrics_json = EXCLUDED.derived_metrics_json,
            source_captured_at = now(),
            metrics_computed_at = CASE
                WHEN EXCLUDED.derived_metrics_json IS NOT NULL THEN now()
                ELSE NULL
            END,
            last_seen_at     = now(),
            updated_at       = now()
        """,
        (
            record_key,
            _clean_url(cache_row["canonical_fg_url"]),
            _clean_url(job.get("bn_url")),
            _clean_text(cache_row.get("name")),
            _clean_text(cache_row.get("house")),
            cache_row.get("year"),
            _clean_text(cache_row.get("gender")),
            _clean_text(cache_row.get("image_url")),
            Json(fg_raw),
            Json(derived_metrics) if derived_metrics is not None else None,
            derived_metrics is not None,
        ),
    )


def enqueue_job(
    *,
    job_key: str,
    query: str | None,
    name: str | None,
    house: str | None,
    year: int | None,
    bn_url: str | None,
    fg_url: str | None,
) -> int | None:
    """Upsert a job by job_key. A duplicate request bumps counters, never dupes.

    On conflict the row's status is left untouched (a pending job stays pending,
    a completed job is not resurrected) except that a `failed` job is returned
    to `pending`, and an `ignored` job is never touched at all -- it was
    deliberately retired.

    Returns the row's resulting `requested_count` -- 1 on first insert, then
    incremented once per duplicate request -- so callers can surface a
    bounded-retry signal to the client. Returns None when storage is disabled,
    or when the conflicting job is `ignored` (the upsert WHERE clause skips it,
    so no row is returned).
    """
    if not ENABLED:
        return None
    ctx, conn = _conn()
    try:
        with conn.transaction():
            row = conn.execute(
                """
                INSERT INTO enrichment_jobs
                    (id, job_key, query, name, house, year, bn_url, fg_url,
                     status, requested_count, created_at, last_requested_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s,
                     'pending', 1, now(), now())
                ON CONFLICT (job_key) DO UPDATE SET
                    status            = CASE
                        WHEN enrichment_jobs.status = 'failed' THEN 'pending'
                        ELSE enrichment_jobs.status
                    END,
                    failure_count     = CASE
                        WHEN enrichment_jobs.status = 'failed' THEN 0
                        ELSE enrichment_jobs.failure_count
                    END,
                    last_error        = CASE
                        WHEN enrichment_jobs.status = 'failed' THEN NULL
                        ELSE enrichment_jobs.last_error
                    END,
                    failed_at         = CASE
                        WHEN enrichment_jobs.status = 'failed' THEN NULL
                        ELSE enrichment_jobs.failed_at
                    END,
                    claimed_at        = CASE
                        WHEN enrichment_jobs.status = 'failed' THEN NULL
                        ELSE enrichment_jobs.claimed_at
                    END,
                    claim_expires_at  = CASE
                        WHEN enrichment_jobs.status = 'failed' THEN NULL
                        ELSE enrichment_jobs.claim_expires_at
                    END,
                    requested_count   = enrichment_jobs.requested_count + 1,
                    last_requested_at = now(),
                    query             = COALESCE(EXCLUDED.query, enrichment_jobs.query),
                    name              = COALESCE(EXCLUDED.name, enrichment_jobs.name),
                    house             = COALESCE(EXCLUDED.house, enrichment_jobs.house),
                    year              = COALESCE(EXCLUDED.year, enrichment_jobs.year),
                    bn_url            = COALESCE(EXCLUDED.bn_url, enrichment_jobs.bn_url),
                    fg_url            = COALESCE(EXCLUDED.fg_url, enrichment_jobs.fg_url)
                WHERE enrichment_jobs.status <> 'ignored'
                RETURNING requested_count
                """,
                (str(uuid.uuid4()), job_key, query, name, house, year, bn_url, fg_url),
            ).fetchone()
            return row["requested_count"] if row else None
    finally:
        ctx.__exit__(None, None, None)


def requeue_or_enqueue_job(
    *,
    job_key: str,
    query: str | None,
    name: str | None,
    house: str | None,
    year: int | None,
    bn_url: str | None,
    fg_url: str | None,
    priority: int = 10,
) -> dict[str, Any] | None:
    """Force a job back to pending, creating it when it does not exist.

    This is the manual refresh path: it intentionally resurrects completed,
    failed, processing, or ignored jobs so the worker can fetch fresh detail
    and overwrite fg_detail_cache on completion.
    """
    if not ENABLED:
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            INSERT INTO enrichment_jobs
                (id, job_key, query, name, house, year, bn_url, fg_url,
                 status, priority, requested_count, created_at, last_requested_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s,
                 'pending', %s, 1, now(), now())
            ON CONFLICT (job_key) DO UPDATE SET
                status            = 'pending',
                priority          = GREATEST(enrichment_jobs.priority, EXCLUDED.priority),
                requested_count   = enrichment_jobs.requested_count + 1,
                last_requested_at = now(),
                query             = COALESCE(EXCLUDED.query, enrichment_jobs.query),
                name              = COALESCE(EXCLUDED.name, enrichment_jobs.name),
                house             = COALESCE(EXCLUDED.house, enrichment_jobs.house),
                year              = COALESCE(EXCLUDED.year, enrichment_jobs.year),
                bn_url            = COALESCE(EXCLUDED.bn_url, enrichment_jobs.bn_url),
                fg_url            = COALESCE(EXCLUDED.fg_url, enrichment_jobs.fg_url),
                claimed_at        = NULL,
                claim_expires_at  = NULL,
                completed_at      = NULL,
                failed_at         = NULL,
                ignored_at        = NULL,
                last_error        = NULL
            RETURNING *
            """,
            (
                str(uuid.uuid4()),
                job_key,
                query,
                name,
                house,
                year,
                bn_url,
                fg_url,
                max(0, int(priority or 0)),
            ),
        ).fetchone()
        return _job_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def requeue_job(job_id: str, *, priority: int = 10) -> dict[str, Any] | None:
    """Force an existing job back to pending so a worker can refresh it."""
    if not ENABLED or not _valid_uuid_text(job_id):
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            UPDATE enrichment_jobs
            SET status = 'pending',
                priority = GREATEST(priority, %s),
                requested_count = requested_count + 1,
                failure_count = 0,
                last_requested_at = now(),
                claimed_at = NULL,
                claim_expires_at = NULL,
                completed_at = NULL,
                failed_at = NULL,
                ignored_at = NULL,
                last_error = NULL
            WHERE id = %s
            RETURNING *
            """,
            (max(0, int(priority or 0)), job_id),
        ).fetchone()
        return _job_to_dict(row) if row else None
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
    """Worker job list, newest-priority first. limit is clamped to MAX_JOB_LIMIT.

    Projects only the columns _job_to_dict surfaces (excludes the unused
    metadata_json blob) -- the mobile dashboard polls this every 4s, so the
    dropped column would otherwise be steady Supabase egress for nothing.
    """
    limit = max(1, min(int(limit or DEFAULT_JOB_LIMIT), MAX_JOB_LIMIT))
    ctx, conn = _conn()
    try:
        rows = conn.execute(
            f"""
            SELECT {_ENRICHMENT_JOB_COLUMNS} FROM enrichment_jobs
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
    if not _valid_uuid_text(job_id):
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            f"SELECT {_ENRICHMENT_JOB_COLUMNS} FROM enrichment_jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
        return _job_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def claim_job(job_id: str, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> dict[str, Any]:
    """Atomically claim a job.

    Claimable when status is 'pending', or 'processing' with an expired lease
    (the previous claimant died). Returns {"claimed": bool, ...}.
    """
    if not _valid_uuid_text(job_id):
        return {"claimed": False, "reason": "not_found"}
    lease_seconds = max(1, int(lease_seconds or DEFAULT_LEASE_SECONDS))
    ctx, conn = _conn()
    try:
        with conn.transaction():
            existing = conn.execute(
                """
                SELECT status, claim_expires_at, now() AS db_now
                FROM enrichment_jobs
                WHERE id = %s
                FOR UPDATE
                """,
                (job_id,),
            ).fetchone()
            if not existing:
                return {"claimed": False, "reason": "not_found"}

            status = existing["status"]
            claim_expires_at = existing["claim_expires_at"]
            db_now = existing["db_now"]
            lease_expired = (
                status == "processing"
                and (claim_expires_at is None or claim_expires_at <= db_now)
            )
            if status != "pending" and not lease_expired:
                reason = (
                    "already_processing"
                    if status == "processing"
                    else f"status_{status}"
                )
                return {
                    "claimed": False,
                    "reason": reason,
                    "claim_expires_at": _iso(claim_expires_at),
                }

            row = conn.execute(
                """
                UPDATE enrichment_jobs
                SET status = 'processing',
                    claimed_at = now(),
                    claim_expires_at = now() + (%s || ' seconds')::interval
                WHERE id = %s
                RETURNING claim_expires_at
                """,
                (str(lease_seconds), job_id),
            ).fetchone()
            return {"claimed": True, "claim_expires_at": _iso(row["claim_expires_at"])}
    finally:
        ctx.__exit__(None, None, None)


def complete_job(job_id: str, cache_row: dict[str, Any]) -> dict[str, Any] | None:
    """Transactionally upsert fg_detail_cache and mark the job completed.

    `cache_row` must already be validated by the caller (non-empty frag_cards,
    canonical_fg_url present). Returns the updated job dict, or None if the job
    id does not exist. Both writes commit together or not at all.
    """
    if not _valid_uuid_text(job_id):
        return None
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
                    (canonical_fg_url, name, house, year, image_url, schema_version, source,
                     captured_at, updated_at, frag_cards_json, notes_json,
                     pros_cons_json, reviews_json, raw_identity_json, quality_status)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s, %s, %s)
                ON CONFLICT (canonical_fg_url) DO UPDATE SET
                    name              = EXCLUDED.name,
                    house             = EXCLUDED.house,
                    year              = EXCLUDED.year,
                    image_url         = EXCLUDED.image_url,
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
                    cache_row.get("image_url"),
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
            try:
                # The aggregate record is an acceleration layer. Keep the
                # worker contract healthy even if that secondary write hits an
                # old schema, unique-index conflict, or malformed legacy row.
                with conn.transaction():
                    _upsert_completed_fragrance_record(conn, job, cache_row)
            except Exception:
                logger.exception(
                    "complete_job aggregate record write failed job_id=%s canonical=%s",
                    job_id,
                    cache_row["canonical_fg_url"],
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
    """Record a failure. Retryable -> back to 'pending' (until the retry cap is
    hit, at which point we force 'failed'); non-retryable -> 'failed' immediately.

    The status decision is made inside the UPDATE via a CASE expression so the
    threshold is evaluated atomically against the row's current failure_count
    (no read-then-write race with a concurrent worker). last_requested_at is
    refreshed so a chronically failing row rotates to the back of the pending
    queue instead of permanently occupying the head — see
    idx_enrichment_jobs_status which orders by (priority DESC, last_requested_at).
    """
    if not _valid_uuid_text(job_id):
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            UPDATE enrichment_jobs
            SET status = CASE
                    WHEN %s AND failure_count + 1 < %s THEN 'pending'
                    ELSE 'failed'
                END,
                failure_count = failure_count + 1,
                last_error = %s,
                failed_at = now(),
                last_requested_at = now(),
                claim_expires_at = NULL
            WHERE id = %s
            RETURNING *
            """,
            (retryable, MAX_RETRYABLE_FAILURES, error, job_id),
        ).fetchone()
        return _job_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def ignore_job(job_id: str, note: str | None) -> dict[str, Any] | None:
    """Permanently retire a job (non-fragrance query, bad identity, dead URL)."""
    if not _valid_uuid_text(job_id):
        return None
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


def patch_job(
    job_id: str,
    *,
    fg_url: str | None = None,
    query: str | None = None,
    name: str | None = None,
    house: str | None = None,
) -> dict[str, Any] | None:
    """Update editable identity fields on a pending or failed job.

    When fg_url is supplied on a failed row, the job is moved back to pending
    and its failure counter is cleared so the worker can retry immediately.
    Processing / completed / ignored rows are left untouched (returns None).
    """
    if not ENABLED or not _valid_uuid_text(job_id):
        return None
    cleaned_fg = _clean_url(fg_url) if fg_url is not None else None
    cleaned_query = _clean_text(query) if query is not None else None
    cleaned_name = _clean_text(name) if name is not None else None
    cleaned_house = _clean_text(house) if house is not None else None
    if fg_url is not None and not cleaned_fg:
        return None
    if not any(v is not None for v in (cleaned_fg, cleaned_query, cleaned_name, cleaned_house)):
        return get_job(job_id)

    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            UPDATE enrichment_jobs
            SET fg_url = COALESCE(%s, fg_url),
                query = COALESCE(%s, query),
                name = COALESCE(%s, name),
                house = COALESCE(%s, house),
                status = CASE
                    WHEN %s::text IS NOT NULL AND status = 'failed' THEN 'pending'
                    ELSE status
                END,
                failure_count = CASE
                    WHEN %s::text IS NOT NULL AND status = 'failed' THEN 0
                    ELSE failure_count
                END,
                last_error = CASE
                    WHEN %s::text IS NOT NULL AND status = 'failed' THEN NULL
                    ELSE last_error
                END,
                last_requested_at = now()
            WHERE id = %s
              AND status IN ('pending', 'failed')
            RETURNING *
            """,
            (
                cleaned_fg,
                cleaned_query,
                cleaned_name,
                cleaned_house,
                cleaned_fg,
                cleaned_fg,
                cleaned_fg,
                job_id,
            ),
        ).fetchone()
        return _job_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# fragrance_records -- write-through aggregate built by user traffic
# ---------------------------------------------------------------------------


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _clean_url(value: Any) -> str | None:
    text = str(value or "").strip().rstrip("/")
    if not text:
        return None
    if "://" in text:
        scheme, rest = text.split("://", 1)
        if "/" in rest:
            host, path = rest.split("/", 1)
            text = f"{scheme.lower()}://{host.lower()}/{path}"
        else:
            text = f"{scheme.lower()}://{rest.lower()}"
    return text


def _lock_fragrance_record_write(
    conn: Any, *, canonical_fg_url: str | None, bn_url: str | None, fallback_key: str
) -> None:
    """Serialize aggregate writes for the same known identity within a txn."""
    keys = {
        key
        for key in (
            f"fg:{canonical_fg_url}" if canonical_fg_url else "",
            f"bn:{bn_url}" if bn_url else "",
            f"record:{fallback_key}" if fallback_key else "",
        )
        if key
    }
    for key in sorted(keys):
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s::text))", (key,))


def _merge_fragrance_record_keys(conn: Any, target_key: str, duplicate_key: str) -> None:
    """Move duplicate aggregate data into target without violating URL indexes."""
    if not target_key or not duplicate_key or target_key == duplicate_key:
        return
    conn.execute(
        """
        WITH duplicate AS (
            DELETE FROM fragrance_records
            WHERE record_key = %s
            RETURNING *
        )
        UPDATE fragrance_records AS target
        SET
            canonical_fg_url = COALESCE(target.canonical_fg_url, duplicate.canonical_fg_url),
            bn_url           = COALESCE(target.bn_url, duplicate.bn_url),
            name             = COALESCE(target.name, duplicate.name),
            house            = COALESCE(target.house, duplicate.house),
            year             = COALESCE(target.year, duplicate.year),
            gender           = COALESCE(target.gender, duplicate.gender),
            image_url        = COALESCE(target.image_url, duplicate.image_url),
            search_json      = COALESCE(duplicate.search_json, '{}'::jsonb) || COALESCE(target.search_json, '{}'::jsonb),
            fg_raw_json      = COALESCE(duplicate.fg_raw_json, '{}'::jsonb) || COALESCE(target.fg_raw_json, '{}'::jsonb),
            bn_raw_json      = COALESCE(duplicate.bn_raw_json, '{}'::jsonb) || COALESCE(target.bn_raw_json, '{}'::jsonb),
            derived_metrics_json = COALESCE(target.derived_metrics_json, duplicate.derived_metrics_json),
            source_captured_at = CASE
                WHEN target.source_captured_at IS NULL THEN duplicate.source_captured_at
                WHEN duplicate.source_captured_at IS NULL THEN target.source_captured_at
                ELSE GREATEST(target.source_captured_at, duplicate.source_captured_at)
            END,
            metrics_computed_at = CASE
                WHEN target.metrics_computed_at IS NULL THEN duplicate.metrics_computed_at
                WHEN duplicate.metrics_computed_at IS NULL THEN target.metrics_computed_at
                ELSE GREATEST(target.metrics_computed_at, duplicate.metrics_computed_at)
            END,
            first_seen_at = LEAST(target.first_seen_at, duplicate.first_seen_at),
            last_seen_at = GREATEST(target.last_seen_at, duplicate.last_seen_at),
            updated_at = now()
        FROM duplicate
        WHERE target.record_key = %s
        """,
        (duplicate_key, target_key),
    )


def _identity_search_term(query: str) -> tuple[str, str]:
    """Return the SQL match mode and bound term for identity search.

    One- and two-character fragrance names are real, but substring ILIKE would
    make "Q" match "Acqua". For those tiny single-token queries, use a
    case-insensitive regex with non-alphanumeric boundaries.
    """
    text = (query or "").strip()
    if len(text) <= 2 and len(text.split()) == 1:
        return "regex", rf"(^|[^[:alnum:]]){re.escape(text)}([^[:alnum:]]|$)"
    return "ilike", f"%{text}%"


def _fragrance_record_key(row: dict[str, Any]) -> str:
    fg_url = _clean_url(row.get("canonical_fg_url"))
    bn_url = _clean_url(row.get("bn_url"))
    if fg_url:
        return f"fg:{fg_url}"
    if bn_url:
        return f"bn:{bn_url}"
    house = str(row.get("house") or "").strip().lower()
    name = str(row.get("name") or "").strip().lower()
    year = str(row.get("year") or "").strip()
    return f"id:{house}|{name}|{year}"


def _find_fragrance_record_key(
    conn: Any, *, canonical_fg_url: str | None, bn_url: str | None, fallback_key: str
) -> str:
    _lock_fragrance_record_write(
        conn,
        canonical_fg_url=canonical_fg_url,
        bn_url=bn_url,
        fallback_key=fallback_key,
    )
    rows = conn.execute(
        """
        SELECT record_key
        FROM fragrance_records
        WHERE (%s::text IS NOT NULL AND canonical_fg_url = %s::text)
           OR (%s::text IS NOT NULL AND bn_url = %s::text)
           OR record_key = %s::text
        ORDER BY
            CASE
                WHEN %s::text IS NOT NULL AND canonical_fg_url = %s::text THEN 0
                WHEN %s::text IS NOT NULL AND bn_url = %s::text THEN 1
                ELSE 2
            END
        FOR UPDATE
        """,
        (
            canonical_fg_url,
            canonical_fg_url,
            bn_url,
            bn_url,
            fallback_key,
            canonical_fg_url,
            canonical_fg_url,
            bn_url,
            bn_url,
        ),
    ).fetchall()
    if not rows:
        return fallback_key
    record_key = rows[0]["record_key"]
    for row in rows[1:]:
        duplicate_key = row["record_key"]
        if duplicate_key != record_key:
            _merge_fragrance_record_keys(conn, record_key, duplicate_key)
    return record_key


def upsert_fragrance_search(row: dict[str, Any]) -> None:
    """Light write-through upsert from /search. No extra fetches."""
    if not ENABLED:
        return
    from psycopg.types.json import Json

    canonical_fg_url = _clean_url(row.get("canonical_fg_url"))
    bn_url = _clean_url(row.get("bn_url"))
    fallback_key = _fragrance_record_key(
        {**row, "canonical_fg_url": canonical_fg_url, "bn_url": bn_url}
    )
    ctx, conn = _conn()
    try:
        with conn.transaction():
            record_key = _find_fragrance_record_key(
                conn,
                canonical_fg_url=canonical_fg_url,
                bn_url=bn_url,
                fallback_key=fallback_key,
            )
            conn.execute(
                """
                INSERT INTO fragrance_records
                    (record_key, canonical_fg_url, bn_url, name, house, year,
                     image_url, search_json, source_captured_at, first_seen_at,
                     last_seen_at, updated_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, now(), now(), now(), now())
                ON CONFLICT (record_key) DO UPDATE SET
                    canonical_fg_url = COALESCE(EXCLUDED.canonical_fg_url, fragrance_records.canonical_fg_url),
                    bn_url           = COALESCE(EXCLUDED.bn_url, fragrance_records.bn_url),
                    name             = COALESCE(EXCLUDED.name, fragrance_records.name),
                    house            = COALESCE(EXCLUDED.house, fragrance_records.house),
                    year             = COALESCE(EXCLUDED.year, fragrance_records.year),
                    image_url        = COALESCE(EXCLUDED.image_url, fragrance_records.image_url),
                    search_json      = fragrance_records.search_json || EXCLUDED.search_json,
                    source_captured_at = now(),
                    last_seen_at     = now(),
                    updated_at       = now()
                """,
                (
                    record_key,
                    canonical_fg_url,
                    bn_url,
                    _clean_text(row.get("name")),
                    _clean_text(row.get("house")),
                    row.get("year"),
                    _clean_text(row.get("image_url")),
                    Json(row.get("search") or {}),
                ),
            )
    finally:
        ctx.__exit__(None, None, None)


def upsert_fragrance_details(row: dict[str, Any]) -> None:
    """Full write-through upsert from /details, including raw source blobs."""
    if not ENABLED:
        return
    from psycopg.types.json import Json

    canonical_fg_url = _clean_url(row.get("canonical_fg_url"))
    bn_url = _clean_url(row.get("bn_url"))
    fallback_key = _fragrance_record_key(
        {**row, "canonical_fg_url": canonical_fg_url, "bn_url": bn_url}
    )
    fg_raw = row.get("fg_raw") or {}
    bn_raw = row.get("bn_raw") or {}
    derived_metrics = row.get("derived_metrics")
    ctx, conn = _conn()
    try:
        with conn.transaction():
            record_key = _find_fragrance_record_key(
                conn,
                canonical_fg_url=canonical_fg_url,
                bn_url=bn_url,
                fallback_key=fallback_key,
            )
            conn.execute(
                """
                INSERT INTO fragrance_records
                    (record_key, canonical_fg_url, bn_url, name, house, year,
                     gender, image_url, search_json, fg_raw_json, bn_raw_json,
                     derived_metrics_json, source_captured_at,
                     metrics_computed_at, first_seen_at, last_seen_at, updated_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     now(), CASE WHEN %s THEN now() ELSE NULL END, now(), now(), now())
                ON CONFLICT (record_key) DO UPDATE SET
                    canonical_fg_url = COALESCE(EXCLUDED.canonical_fg_url, fragrance_records.canonical_fg_url),
                    bn_url           = COALESCE(EXCLUDED.bn_url, fragrance_records.bn_url),
                    name             = COALESCE(EXCLUDED.name, fragrance_records.name),
                    house            = COALESCE(EXCLUDED.house, fragrance_records.house),
                    year             = COALESCE(EXCLUDED.year, fragrance_records.year),
                    gender           = COALESCE(EXCLUDED.gender, fragrance_records.gender),
                    image_url        = COALESCE(EXCLUDED.image_url, fragrance_records.image_url),
                    search_json      = fragrance_records.search_json || EXCLUDED.search_json,
                    fg_raw_json      = CASE
                        WHEN EXCLUDED.fg_raw_json <> '{}'::jsonb THEN EXCLUDED.fg_raw_json
                        ELSE fragrance_records.fg_raw_json
                    END,
                    bn_raw_json      = CASE
                        WHEN EXCLUDED.bn_raw_json <> '{}'::jsonb THEN EXCLUDED.bn_raw_json
                        ELSE fragrance_records.bn_raw_json
                    END,
                    derived_metrics_json = CASE
                        WHEN EXCLUDED.derived_metrics_json IS NOT NULL THEN EXCLUDED.derived_metrics_json
                        WHEN EXCLUDED.fg_raw_json <> '{}'::jsonb OR EXCLUDED.bn_raw_json <> '{}'::jsonb THEN NULL
                        ELSE fragrance_records.derived_metrics_json
                    END,
                    source_captured_at = now(),
                    metrics_computed_at = CASE
                        WHEN EXCLUDED.derived_metrics_json IS NOT NULL THEN now()
                        WHEN EXCLUDED.fg_raw_json <> '{}'::jsonb OR EXCLUDED.bn_raw_json <> '{}'::jsonb THEN NULL
                        ELSE fragrance_records.metrics_computed_at
                    END,
                    last_seen_at     = now(),
                    updated_at       = now()
                """,
                (
                    record_key,
                    canonical_fg_url,
                    bn_url,
                    _clean_text(row.get("name")),
                    _clean_text(row.get("house")),
                    row.get("year"),
                    _clean_text(row.get("gender")),
                    _clean_text(row.get("image_url")),
                    Json(row.get("search") or {}),
                    Json(fg_raw),
                    Json(bn_raw),
                    Json(derived_metrics) if derived_metrics is not None else None,
                    derived_metrics is not None,
                ),
            )
    finally:
        ctx.__exit__(None, None, None)


def lookup_fragrance_record(
    *, canonical_fg_url: str | None = None, bn_url: str | None = None
) -> dict[str, Any] | None:
    """Fetch one aggregate record by either source URL."""
    if not ENABLED:
        return None
    canonical_fg_url = _clean_url(canonical_fg_url)
    bn_url = _clean_url(bn_url)
    if not canonical_fg_url and not bn_url:
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            SELECT *
            FROM fragrance_records
            WHERE (%s::text IS NOT NULL AND canonical_fg_url = %s::text)
               OR (%s::text IS NOT NULL AND bn_url = %s::text)
            ORDER BY
                CASE
                    WHEN %s::text IS NOT NULL AND canonical_fg_url = %s::text THEN 0
                    WHEN %s::text IS NOT NULL AND bn_url = %s::text THEN 1
                    ELSE 2
                END
            LIMIT 1
            """,
            (
                canonical_fg_url,
                canonical_fg_url,
                bn_url,
                bn_url,
                canonical_fg_url,
                canonical_fg_url,
                bn_url,
                bn_url,
            ),
        ).fetchone()
        return _fragrance_record_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def search_fragrance_records(query: str, limit: int = 15) -> list[dict[str, Any]]:
    """Search aggregate identities. Empty when storage is absent/disabled.

    Selects only the light identity columns the search/candidate path actually
    reads. The heavy raw payload columns (fg_raw_json, bn_raw_json,
    derived_metrics_json) are deliberately excluded here: a cache-search sweep
    returns up to 50 rows and the caller discards those blobs, so fetching them
    is pure Supabase egress. Per-URL hydration uses lookup_fragrance_record,
    which still does SELECT * for the full record.
    """
    if not ENABLED:
        return []
    text = (query or "").strip()
    if not text:
        return []
    limit = max(1, min(int(limit or 15), 50))
    mode, term = _identity_search_term(text)
    ctx, conn = _conn()
    try:
        if mode == "regex":
            rows = conn.execute(
                f"""
                SELECT {_FRAGRANCE_RECORD_SEARCH_COLUMNS}
                FROM fragrance_records
                WHERE name ~* %s
                   OR house ~* %s
                   OR concat_ws(' ', house, name) ~* %s
                   OR concat_ws(' ', name, house) ~* %s
                ORDER BY
                    CASE
                        WHEN concat_ws(' ', house, name) ~* %s THEN 0
                        WHEN name ~* %s THEN 1
                        WHEN house ~* %s THEN 2
                        ELSE 3
                    END,
                    last_seen_at DESC
                LIMIT %s
                """,
                (term, term, term, term, term, term, term, limit),
            ).fetchall()
            return [_fragrance_record_search_to_dict(row) for row in rows]
        rows = conn.execute(
            f"""
            SELECT {_FRAGRANCE_RECORD_SEARCH_COLUMNS}
            FROM fragrance_records
            WHERE name ILIKE %s
               OR house ILIKE %s
               OR concat_ws(' ', house, name) ILIKE %s
               OR concat_ws(' ', name, house) ILIKE %s
            ORDER BY
                CASE
                    WHEN concat_ws(' ', house, name) ILIKE %s THEN 0
                    WHEN name ILIKE %s THEN 1
                    WHEN house ILIKE %s THEN 2
                    ELSE 3
                END,
                last_seen_at DESC
            LIMIT %s
            """,
            (term, term, term, term, term, term, term, limit),
        ).fetchall()
        return [_fragrance_record_search_to_dict(row) for row in rows]
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
            "image_url": row["image_url"],
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


def search_detail_cache(query: str, limit: int = 15) -> list[dict[str, Any]]:
    """Search completed detail-cache identities. Empty when absent/disabled."""
    if not ENABLED:
        return []
    text = (query or "").strip()
    if not text:
        return []
    limit = max(1, min(int(limit or 15), 50))
    mode, term = _identity_search_term(text)
    ctx, conn = _conn()
    try:
        if mode == "regex":
            rows = conn.execute(
                f"""
                SELECT {_DETAIL_CACHE_SEARCH_COLUMNS} FROM fg_detail_cache
                WHERE quality_status = 'complete'
                  AND (
                      name ~* %s
                      OR house ~* %s
                      OR concat_ws(' ', house, name) ~* %s
                      OR concat_ws(' ', name, house) ~* %s
                  )
                ORDER BY
                    CASE
                        WHEN concat_ws(' ', house, name) ~* %s THEN 0
                        WHEN name ~* %s THEN 1
                        WHEN house ~* %s THEN 2
                        ELSE 3
                    END,
                    updated_at DESC
                LIMIT %s
                """,
                (term, term, term, term, term, term, term, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT {_DETAIL_CACHE_SEARCH_COLUMNS} FROM fg_detail_cache
                WHERE quality_status = 'complete'
                  AND (
                      name ILIKE %s
                      OR house ILIKE %s
                      OR concat_ws(' ', house, name) ILIKE %s
                      OR concat_ws(' ', name, house) ILIKE %s
                  )
                ORDER BY
                    CASE
                        WHEN concat_ws(' ', house, name) ILIKE %s THEN 0
                        WHEN name ILIKE %s THEN 1
                        WHEN house ILIKE %s THEN 2
                        ELSE 3
                    END,
                    updated_at DESC
                LIMIT %s
                """,
                (term, term, term, term, term, term, term, limit),
            ).fetchall()
        return [_detail_cache_search_to_dict(row) for row in rows]
    finally:
        ctx.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# Row serialization
# ---------------------------------------------------------------------------


# Light projections for the cache-search hot path. These exclude the heavy raw
# payload columns the search/candidate builders never read, so a search sweep
# (up to 50 rows, run twice per request) stops egressing full scraped blobs out
# of Supabase. Per-URL hydration (lookup_*) still selects everything.
_FRAGRANCE_RECORD_SEARCH_COLUMNS = (
    "canonical_fg_url, bn_url, name, house, year, image_url, "
    "search_json, source_captured_at, updated_at"
)
_DETAIL_CACHE_SEARCH_COLUMNS = (
    "canonical_fg_url, name, house, year, image_url, raw_identity_json"
)


def _fragrance_record_search_to_dict(row: Any) -> dict[str, Any]:
    """Light serializer matching _FRAGRANCE_RECORD_SEARCH_COLUMNS."""
    if not row:
        return {}
    return {
        "canonical_fg_url": row["canonical_fg_url"],
        "bn_url": row["bn_url"],
        "name": row["name"],
        "house": row["house"],
        "year": row["year"],
        "image_url": row["image_url"],
        "search": row["search_json"] or {},
        "source_captured_at": _iso(row["source_captured_at"]),
        "updated_at": _iso(row["updated_at"]),
    }


def _detail_cache_search_to_dict(row: Any) -> dict[str, Any]:
    """Light serializer matching _DETAIL_CACHE_SEARCH_COLUMNS.

    raw_identity is kept because _cache_entry_identity / _cache_entry_image_url
    fall back to it for name/house/year/image; the bulk columns (reviews_json,
    notes_json, frag_cards_json, pros_cons_json) are dropped.
    """
    if not row:
        return {}
    return {
        "canonical_fg_url": row["canonical_fg_url"],
        "name": row["name"],
        "house": row["house"],
        "year": row["year"],
        "image_url": row["image_url"],
        "raw_identity": row["raw_identity_json"],
    }


def _fragrance_record_to_dict(row: Any) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "record_key": row["record_key"],
        "canonical_fg_url": row["canonical_fg_url"],
        "bn_url": row["bn_url"],
        "name": row["name"],
        "house": row["house"],
        "year": row["year"],
        "gender": row["gender"],
        "image_url": row["image_url"],
        "search": row["search_json"] or {},
        "fg_raw": row["fg_raw_json"] or {},
        "bn_raw": row["bn_raw_json"] or {},
        "derived_metrics": row["derived_metrics_json"],
        "source_captured_at": _iso(row["source_captured_at"]),
        "metrics_computed_at": _iso(row["metrics_computed_at"]),
        "first_seen_at": _iso(row["first_seen_at"]),
        "last_seen_at": _iso(row["last_seen_at"]),
        "updated_at": _iso(row["updated_at"]),
    }


def _iso(dt: Any) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(dt)


# ---------------------------------------------------------------------------
# worker_accounts / mobile_sessions / magic_links -- phone-side auth
# ---------------------------------------------------------------------------
#
# All four mobile tables are accessed only from mobile.py. Like the rest of
# db.py, every function is inert (returns None / []) when ENABLED is False, so
# the mobile surface can be probed for capability without raising.

VALID_COMMAND_STATUSES = ("queued", "claimed", "completed", "failed")
VALID_COMMAND_KINDS = (
    "toggle_auto_approve",
    "set_auto_approve",
    "process_pending",
    "retry_failed",
)


def create_worker_account(
    *, email: str, label: str | None, pin_hash: str
) -> dict[str, Any]:
    """Insert a new account. Raises on duplicate email."""
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            INSERT INTO worker_accounts (id, email, label, pin_hash)
            VALUES (%s, %s, %s, %s)
            RETURNING *
            """,
            (str(uuid.uuid4()), email.strip().lower(), label, pin_hash),
        ).fetchone()
        return _account_to_dict(row)
    finally:
        ctx.__exit__(None, None, None)


def get_worker_account_by_email(email: str) -> dict[str, Any] | None:
    if not ENABLED:
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM worker_accounts WHERE email = %s",
            (email.strip().lower(),),
        ).fetchone()
        return _account_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def get_worker_account(account_id: str) -> dict[str, Any] | None:
    if not ENABLED:
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM worker_accounts WHERE id = %s", (account_id,)
        ).fetchone()
        return _account_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def list_worker_accounts() -> list[dict[str, Any]]:
    if not ENABLED:
        return []
    ctx, conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM worker_accounts ORDER BY created_at"
        ).fetchall()
        return [_account_to_dict(r) for r in rows]
    finally:
        ctx.__exit__(None, None, None)


def update_account_pin(account_id: str, pin_hash: str) -> None:
    ctx, conn = _conn()
    try:
        conn.execute(
            "UPDATE worker_accounts SET pin_hash = %s, pin_strikes = 0, locked_until = NULL WHERE id = %s",
            (pin_hash, account_id),
        )
    finally:
        ctx.__exit__(None, None, None)


def set_account_disabled(account_id: str, disabled: bool) -> None:
    ctx, conn = _conn()
    try:
        conn.execute(
            "UPDATE worker_accounts SET disabled = %s WHERE id = %s",
            (disabled, account_id),
        )
    finally:
        ctx.__exit__(None, None, None)


def record_pin_strike(account_id: str, lock_minutes: int) -> dict[str, Any] | None:
    """Increment strikes; lock account at 5 strikes for lock_minutes."""
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            UPDATE worker_accounts
            SET pin_strikes = pin_strikes + 1,
                locked_until = CASE
                    WHEN pin_strikes + 1 >= 5 THEN now() + (%s || ' minutes')::interval
                    ELSE locked_until
                END
            WHERE id = %s
            RETURNING *
            """,
            (str(lock_minutes), account_id),
        ).fetchone()
        return _account_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def reset_pin_strikes(account_id: str) -> None:
    ctx, conn = _conn()
    try:
        conn.execute(
            """
            UPDATE worker_accounts
            SET pin_strikes = 0, locked_until = NULL, last_login_at = now()
            WHERE id = %s
            """,
            (account_id,),
        )
    finally:
        ctx.__exit__(None, None, None)


def insert_magic_link(
    *, account_id: str, token_hash: str, ttl_minutes: int, requesting_ip: str | None
) -> str:
    ctx, conn = _conn()
    try:
        link_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO magic_links (id, account_id, token_hash, expires_at, requesting_ip)
            VALUES (%s, %s, %s, now() + (%s || ' minutes')::interval, %s)
            """,
            (link_id, account_id, token_hash, str(ttl_minutes), requesting_ip),
        )
        return link_id
    finally:
        ctx.__exit__(None, None, None)


def consume_magic_link(token_hash: str) -> dict[str, Any] | None:
    """Atomically consume a fresh, unexpired link. Returns the row or None."""
    ctx, conn = _conn()
    try:
        with conn.transaction():
            link = conn.execute(
                """
                SELECT id
                FROM magic_links
                WHERE token_hash = %s
                  AND consumed_at IS NULL
                  AND expires_at > now()
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE
                """,
                (token_hash,),
            ).fetchone()
            if not link:
                return None
            row = conn.execute(
                """
                UPDATE magic_links
                SET consumed_at = now()
                WHERE id = %s
                  AND consumed_at IS NULL
                RETURNING *
                """,
                (link["id"],),
            ).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "account_id": row["account_id"],
            "consumed_at": _iso(row["consumed_at"]),
        }
    finally:
        ctx.__exit__(None, None, None)


def count_recent_magic_links(*, account_id: str | None, ip: str | None, minutes: int) -> int:
    ctx, conn = _conn()
    try:
        if account_id is not None:
            row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM magic_links
                WHERE account_id = %s AND created_at > now() - (%s || ' minutes')::interval
                """,
                (account_id, str(minutes)),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM magic_links
                WHERE requesting_ip = %s AND created_at > now() - (%s || ' minutes')::interval
                """,
                (ip, str(minutes)),
            ).fetchone()
        return int(row["n"] if row else 0)
    finally:
        ctx.__exit__(None, None, None)


def create_session(
    *, account_id: str, device_fingerprint: str, ttl_days: int
) -> str:
    ctx, conn = _conn()
    try:
        session_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO mobile_sessions (id, account_id, device_fingerprint, expires_at)
            VALUES (%s, %s, %s, now() + (%s || ' days')::interval)
            """,
            (session_id, account_id, device_fingerprint, str(ttl_days)),
        )
        return session_id
    finally:
        ctx.__exit__(None, None, None)


def get_active_session(session_id: str, device_fingerprint: str) -> dict[str, Any] | None:
    if not ENABLED or not session_id:
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            SELECT * FROM mobile_sessions
            WHERE id = %s
              AND device_fingerprint = %s
              AND revoked_at IS NULL
              AND expires_at > now()
            """,
            (session_id, device_fingerprint),
        ).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "account_id": row["account_id"],
            "expires_at": _iso(row["expires_at"]),
        }
    finally:
        ctx.__exit__(None, None, None)


def touch_session(session_id: str, renew_days: int) -> None:
    ctx, conn = _conn()
    try:
        conn.execute(
            """
            UPDATE mobile_sessions
            SET last_used_at = now(),
                expires_at = GREATEST(expires_at, now() + (%s || ' days')::interval)
            WHERE id = %s
            """,
            (str(renew_days), session_id),
        )
    finally:
        ctx.__exit__(None, None, None)


def revoke_session(session_id: str) -> None:
    ctx, conn = _conn()
    try:
        conn.execute(
            "UPDATE mobile_sessions SET revoked_at = now() WHERE id = %s AND revoked_at IS NULL",
            (session_id,),
        )
    finally:
        ctx.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# worker_commands -- phone -> desktop intent queue
# ---------------------------------------------------------------------------


def enqueue_command(
    *, kind: str, payload: dict[str, Any], issued_by_account_id: str | None
) -> dict[str, Any]:
    from psycopg.types.json import Json

    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            INSERT INTO worker_commands (id, issued_by_account_id, kind, payload_json)
            VALUES (%s, %s, %s, %s)
            RETURNING *
            """,
            (str(uuid.uuid4()), issued_by_account_id, kind, Json(payload or {})),
        ).fetchone()
        return _command_to_dict(row)
    finally:
        ctx.__exit__(None, None, None)


def list_commands(*, status: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
    if not ENABLED:
        return []
    limit = max(1, min(int(limit or 25), 100))
    ctx, conn = _conn()
    try:
        if status:
            rows = conn.execute(
                """
                SELECT * FROM worker_commands
                WHERE status = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM worker_commands
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [_command_to_dict(r) for r in rows]
    finally:
        ctx.__exit__(None, None, None)


def claim_next_command() -> dict[str, Any] | None:
    """Desktop worker pops one queued command, marks it claimed atomically."""
    ctx, conn = _conn()
    try:
        row = conn.execute(
            """
            UPDATE worker_commands
            SET status = 'claimed', claimed_at = now()
            WHERE id = (
                SELECT id FROM worker_commands
                WHERE status = 'queued'
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
            """
        ).fetchone()
        return _command_to_dict(row) if row else None
    finally:
        ctx.__exit__(None, None, None)


def finish_command(command_id: str, *, ok: bool, result_text: str | None) -> None:
    ctx, conn = _conn()
    try:
        conn.execute(
            """
            UPDATE worker_commands
            SET status = %s, completed_at = now(), result_text = %s
            WHERE id = %s
            """,
            ("completed" if ok else "failed", result_text, command_id),
        )
    finally:
        ctx.__exit__(None, None, None)


def stamp_worker_heartbeat() -> None:
    """Called by the desktop worker on every poll tick. Singleton row upsert."""
    ctx, conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO worker_heartbeat (id, last_seen_at)
            VALUES (1, now())
            ON CONFLICT (id) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
            """
        )
    finally:
        ctx.__exit__(None, None, None)


def latest_worker_heartbeat_seconds() -> float | None:
    """Seconds since the desktop worker last pinged. None when never seen."""
    if not ENABLED:
        return None
    ctx, conn = _conn()
    try:
        row = conn.execute(
            "SELECT EXTRACT(EPOCH FROM (now() - last_seen_at)) AS secs FROM worker_heartbeat WHERE id = 1"
        ).fetchone()
        if not row or row.get("secs") is None:
            return None
        try:
            return float(row["secs"])
        except (TypeError, ValueError):
            return None
    finally:
        ctx.__exit__(None, None, None)


def _account_to_dict(row: Any) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "id": row["id"],
        "email": row["email"],
        "label": row["label"],
        "pin_hash": row["pin_hash"],
        "disabled": bool(row["disabled"]),
        "pin_strikes": row["pin_strikes"],
        "locked_until": _iso(row["locked_until"]),
        "created_at": _iso(row["created_at"]),
        "last_login_at": _iso(row["last_login_at"]),
    }


def _command_to_dict(row: Any) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "id": row["id"],
        "issued_by_account_id": row["issued_by_account_id"],
        "kind": row["kind"],
        "payload": row["payload_json"] or {},
        "status": row["status"],
        "result_text": row["result_text"],
        "created_at": _iso(row["created_at"]),
        "claimed_at": _iso(row["claimed_at"]),
        "completed_at": _iso(row["completed_at"]),
    }


# Exactly the columns _job_to_dict reads -- excludes metadata_json so the
# 4s-interval dashboard poll (list_jobs) stops egressing an unused JSONB blob.
_ENRICHMENT_JOB_COLUMNS = (
    "id, job_key, query, name, house, year, bn_url, fg_url, status, priority, "
    "requested_count, failure_count, last_error, created_at, last_requested_at, "
    "claimed_at, claim_expires_at, completed_at, failed_at, ignored_at"
)


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
