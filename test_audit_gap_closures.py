#!/usr/bin/env python3
"""Guards for the 2026-07 audit Part B closures (CODEBASE_BUG_AUDIT_2026-07-02.md).

Covers, fully offline (no DB, no network):

  * B1 -- recover_or_enqueue_job reopens an orphaned `processing` row whose
    claim lease is expired/NULL, instead of preserving it forever;
  * B2 -- a raised DB error during job recovery surfaces enrichment_status
    "unavailable" (SPA keeps retrying) and is never rewritten to "completed";
    the legitimate no-row case still degrades to "completed";
  * B3 -- psycopg_pool.PoolTimeout raised inside a route maps to a retryable
    503 (Retry-After), mirroring the scrape gates, instead of a 30s-hang 500;
  * B4 -- derived-metrics locks are striped to a fixed cardinality (no
    per-record monotonic growth) and stable per record;
  * B6 -- diagnostics mint attempts are gated to one in-flight mint per site
    on a daemon thread: overlap gets an immediate honest "busy" result instead
    of stranding a fresh thread per call or silently queueing behind a stuck
    mint;
  * B1 sweep -- list_jobs(status="pending") flips expired/NULL-lease
    `processing` orphans back to pending first, so plain --process-pending
    drains can see them.

pytest-style (no __main__ guard): run via `python -m pytest`.
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import api
import db
import fragrance_parser_full_rewrite_fixed as engine
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _fragrantica_selected() -> engine.UnifiedFragrance:
    return engine.UnifiedFragrance(
        name="Test Fragrance",
        brand="Test Brand",
        year="",
        bn_url="https://basenotes.com/fragrances/test-by-brand.0",
        frag_url="https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
    )


def _details_request_payload() -> dict[str, str]:
    return {"id": api._encode_id(_fragrantica_selected())}


def _incomplete_stored_details() -> engine.UnifiedDetails:
    """A cached FG bundle where the 4 status metric groups are NOT all present."""
    details = engine.UnifiedDetails(notes=engine.NotesList())
    details.frag_cards = {"Community Interest": [{"label": "Have", "count": "9"}]}
    details.notes.top = ["Bergamot"]
    details.derived_metrics = {
        "source_coverage": {
            "performance_score": False,
            "value_score": False,
            "community_interest_score": True,
            "wear_profile": False,
        }
    }
    # Metrics blob exists (incomplete), so /details must not re-persist.
    details._had_stored_derived_metrics = True
    return details


# ---------------------------------------------------------------------------
# B1 -- expired-lease processing rows must reopen on recovery
# ---------------------------------------------------------------------------


class _RecordingCursor:
    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _RecordingConn:
    def __init__(self, log: list) -> None:
        self._log = log

    def execute(self, sql, params=None):
        self._log.append((sql, params))
        return _RecordingCursor()


class _NullCtx:
    def __exit__(self, *exc):
        return False


def test_recover_sql_reopens_expired_lease_processing() -> None:
    executed: list = []
    saved_enabled = db.ENABLED
    saved_conn = db._conn
    try:
        db.ENABLED = True
        db._conn = lambda: (_NullCtx(), _RecordingConn(executed))
        out = db.recover_or_enqueue_job(
            job_key="fg:test",
            query=None,
            name="Test Fragrance",
            house="Test Brand",
            year=None,
            bn_url=None,
            fg_url="https://www.fragrantica.com/perfume/Test-Brand/Test-Fragrance-9999.html",
        )
    finally:
        db.ENABLED = saved_enabled
        db._conn = saved_conn

    assert out is None  # fake cursor returns no row
    assert len(executed) == 1, executed
    sql, params = executed[0]
    # A processing row is preserved ONLY under a live (non-NULL, unexpired)
    # claim lease; the old blanket preserve-processing predicate must be gone.
    assert "claim_expires_at > now()" in sql, sql
    assert "claim_expires_at IS NOT NULL" in sql, sql
    assert "status IN ('pending', 'processing')" not in sql, sql
    # Ignored rows stay retired.
    assert "status <> 'ignored'" in sql, sql
    assert "fg:test" in params, params


def _list_jobs_sql(status: str) -> list:
    executed: list = []
    saved_enabled = db.ENABLED
    saved_conn = db._conn
    try:
        db.ENABLED = True
        db._conn = lambda: (_NullCtx(), _RecordingConn(executed))
        db.list_jobs(status=status)
    finally:
        db.ENABLED = saved_enabled
        db._conn = saved_conn
    return executed


def test_pending_list_reopens_expired_lease_orphans_first() -> None:
    executed = _list_jobs_sql("pending")
    # Orphan flip runs before the SELECT, and only for the pending listing.
    assert len(executed) == 2, [sql for sql, _ in executed]
    flip_sql = executed[0][0]
    assert "SET status = 'pending'" in flip_sql, flip_sql
    assert "status = 'processing'" in flip_sql, flip_sql
    assert "claim_expires_at IS NULL OR claim_expires_at <= now()" in flip_sql, flip_sql
    assert "SELECT" in executed[1][0], executed[1][0]

    # Non-pending listings (mobile dashboard tabs) never mutate.
    executed = _list_jobs_sql("processing")
    assert len(executed) == 1, [sql for sql, _ in executed]
    assert "UPDATE" not in executed[0][0], executed[0][0]


# ---------------------------------------------------------------------------
# B2 -- raised recovery errors must not be masked as "completed"
# ---------------------------------------------------------------------------


def test_recover_helper_distinguishes_raise_from_no_row() -> None:
    saved = db.recover_or_enqueue_job
    try:
        db.recover_or_enqueue_job = lambda **kw: (_ for _ in ()).throw(
            RuntimeError("transient supabase error")
        )
        job_state = api._recover_incomplete_enrichment_job(
            _fragrantica_selected(), api.DetailRequest(id="x")
        )
    finally:
        db.recover_or_enqueue_job = saved
    # Raised -> non-None error state that normalizes to "unavailable".
    assert job_state is not None
    assert api._enrichment_status_from_job_state(job_state) == ("unavailable", None)


def _post_details_with_recovery(recover_fn) -> dict:
    saved_lookup = api._lookup_stored_detail
    saved_recover = db.recover_or_enqueue_job
    saved_price = api._attach_price
    try:
        api._lookup_stored_detail = lambda selected: _incomplete_stored_details()
        db.recover_or_enqueue_job = recover_fn
        api._attach_price = lambda payload, req, selected: payload
        client = TestClient(api.app)
        res = client.post("/api/fragrances/details", json=_details_request_payload())
        assert res.status_code == 200, res.text
        return res.json()
    finally:
        api._lookup_stored_detail = saved_lookup
        db.recover_or_enqueue_job = saved_recover
        api._attach_price = saved_price


def test_details_surfaces_unavailable_when_recovery_raises() -> None:
    def _raise(**kw):
        raise RuntimeError("transient supabase error")

    data = _post_details_with_recovery(_raise)
    # The SPA must keep retrying: a partial tile is NOT reported complete.
    assert data["enrichment"]["status"] == "unavailable", data["enrichment"]


def test_details_still_degrades_to_completed_when_no_row() -> None:
    data = _post_details_with_recovery(lambda **kw: None)
    # Legit no-row recovery (DB disabled / ignored row) keeps today's contract.
    assert data["enrichment"]["status"] == "completed", data["enrichment"]


# ---------------------------------------------------------------------------
# B3 -- DB pool saturation degrades to a retryable 503
# ---------------------------------------------------------------------------


def test_pool_timeout_maps_to_503_with_retry_after() -> None:
    from psycopg_pool import PoolTimeout

    saved_lookup = api._lookup_stored_detail

    def _saturated(selected):
        raise PoolTimeout("couldn't get a connection after 5.0 sec")

    try:
        api._lookup_stored_detail = _saturated
        client = TestClient(api.app, raise_server_exceptions=False)
        res = client.post(
            "/api/fragrances/enrichment-state", json=_details_request_payload()
        )
    finally:
        api._lookup_stored_detail = saved_lookup

    assert res.status_code == 503, (res.status_code, res.text)
    assert res.headers.get("retry-after") == "2", dict(res.headers)
    assert "retry" in res.json()["detail"].lower(), res.text


# ---------------------------------------------------------------------------
# B4 -- derived-metrics locks are bounded and stable
# ---------------------------------------------------------------------------


def test_derived_metrics_locks_are_striped_and_bounded() -> None:
    stripes = api._DERIVED_METRICS_LOCK_STRIPES
    assert len(api._DERIVED_METRICS_LOCKS) == stripes
    before = api._DERIVED_METRICS_LOCKS
    seen = set()
    for i in range(10_000):
        lock = api._derived_metrics_lock_for({"record_key": f"record-{i}"})
        assert isinstance(lock, type(threading.Lock()))
        seen.add(id(lock))
    # Container never grew: locks come from the fixed stripe set only.
    assert api._DERIVED_METRICS_LOCKS is before
    assert len(api._DERIVED_METRICS_LOCKS) == stripes
    assert len(seen) <= stripes
    # Same record -> same lock (stable across calls).
    a = api._derived_metrics_lock_for({"record_key": "record-42"})
    b = api._derived_metrics_lock_for({"record_key": "record-42"})
    assert a is b


# ---------------------------------------------------------------------------
# B6 -- diagnostics mint attempts are gated, daemon-threaded, and honest
# ---------------------------------------------------------------------------


def test_mint_diagnostics_gated_daemon_and_reusable() -> None:
    assert api._BN_MINT_DIAG_GATE is not api._FG_MINT_DIAG_GATE

    seen: list[tuple[str, bool]] = []
    saved_mint = engine._mint_basenotes_clearance

    def _fake_mint():
        t = threading.current_thread()
        seen.append((t.name, t.daemon))
        return None

    try:
        engine._mint_basenotes_clearance = _fake_mint
        first = api._bn_diag_mint_attempt(True)
        second = api._bn_diag_mint_attempt(True)
    finally:
        engine._mint_basenotes_clearance = saved_mint

    # Gate released after each completed mint -> both attempts actually ran,
    # on named daemon threads (a stuck mint must not block interpreter exit).
    assert first["success"] is False and second["success"] is False
    assert first["error"] is None and second["error"] is None, (first, second)
    assert len(seen) == 2, seen
    assert all(name.startswith("bn-mint-diag") for name, _ in seen), seen
    assert all(daemon for _, daemon in seen), seen


def test_mint_diagnostics_overlap_reports_busy_immediately() -> None:
    saved_mint = engine._mint_basenotes_clearance
    engine._mint_basenotes_clearance = lambda: None
    # Simulate an in-flight mint by holding the gate ourselves.
    assert api._BN_MINT_DIAG_GATE.acquire(blocking=False)
    try:
        started = time.monotonic()
        result = api._bn_diag_mint_attempt(True)
        elapsed = time.monotonic() - started
    finally:
        api._BN_MINT_DIAG_GATE.release()
        engine._mint_basenotes_clearance = saved_mint

    # Overlap is rejected immediately with an honest error -- it must not
    # queue behind the running mint and burn the 90s budget.
    assert result["success"] is False
    assert result["error"] is not None and result["error"].startswith("busy"), result
    assert elapsed < 5.0, elapsed
