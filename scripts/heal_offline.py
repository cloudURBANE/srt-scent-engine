#!/usr/bin/env python3
"""heal_offline.py -- strict-offline, idempotent backfill for "Unknown" family
and concentration across the engine cache AND the app wardrobe tables.

Why this exists
---------------
A raw upsert deliberately NULLs ``fragrance_records.derived_metrics_json`` so a
stale metrics blob is never served (db.py CASE). Family is *derived* from that
blob (``enrichment_facts.derive_families`` over the main accords), so a NULL blob
reads as "Unknown" even though every accord already sits in ``fg_raw_json``. The
only code paths that refill the blob are (a) a live worker ``/complete`` and (b) a
read-time ``/details`` hydrate -- so the bulk of the cache never recovers on its
own, and the wardrobe tables (which keep a *static copy* of family/concentration)
never recover at all until a user re-opens each fragrance.

This script heals all of that **without any network**, reusing the engine's own
canonical reconstruction and resolvers so the numbers match exactly what a
``/details`` hydrate would compute:

  1. metrics        -- recompute derived_metrics_json from stored raw
                       (mirrors POST /api/enrichment/recompute-metrics).
  2. concentration  -- strict-offline concentration backfill on the engine cache
                       (reuses scripts/enrich_concentration.py --engine-gap).
  3. wardrobe       -- project the healed family + concentration from
                       fragrance_records into user_fragrances / global_fragrances
                       by exact normalized brand+name (skips ambiguous rows).

Every write is additive + merge-safe: metrics fill only NULLs, concentration
never overwrites an existing value, and the wardrobe projection only fills facts
that are currently Unknown. Re-running is a no-op. ``--dry-run`` writes nothing.

Usage (from search_engine/, Windows venv interpreter):
    .venv\\Scripts\\python.exe scripts\\heal_offline.py --dry-run
    .venv\\Scripts\\python.exe scripts\\heal_offline.py --record-key KEY1 --record-key KEY2   # scoped test
    .venv\\Scripts\\python.exe scripts\\heal_offline.py                                       # full bulk
    .venv\\Scripts\\python.exe scripts\\heal_offline.py --steps metrics,concentration         # subset

DATABASE_URL is read from the environment, else loaded from huge_monorepo/.env.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_database_url() -> None:
    """Populate DATABASE_URL from the monorepo .env if it is not already set.

    Never overrides a value already present in the shell, mirroring the worker's
    load_local_env precedence."""
    if os.environ.get("DATABASE_URL"):
        return
    for candidate in (
        _REPO_ROOT / ".env",
        _REPO_ROOT.parent / "huge_monorepo" / ".env",
    ):
        if not candidate.exists():
            continue
        for line in candidate.read_text(encoding="utf-8", errors="ignore").splitlines():
            m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", line)
            if not m:
                continue
            key, val = m.group(1), m.group(2).strip().strip('"').strip("'")
            if key == "DATABASE_URL" and key not in os.environ:
                os.environ[key] = val
                return


_load_database_url()

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Json  # noqa: E402

import db  # noqa: E402
import api  # noqa: E402  (reuse _details_from_fragrance_record -- single source of truth)
from derived_metrics_adapter import build_derived_metrics  # noqa: E402
from enrichment_facts import derive_families, is_fact_complete  # noqa: E402
import enrich_concentration as conc  # noqa: E402
from enrich_database_metrics import _apply_derived_facts  # noqa: E402


# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------
def _norm_key(brand: str, name: str) -> str:
    """Exact-match identity key: lowercase alphanumerics of brand + name.

    Conservative on purpose -- a wardrobe row only inherits engine facts when its
    brand+name normalizes identically to a fragrance_records row, so we never
    project the wrong family/concentration into a user's saved data."""
    raw = f"{brand or ''} {name or ''}"
    return re.sub(r"[^a-z0-9]+", "", raw.lower())


def _record_has_raw(record: dict[str, Any]) -> bool:
    fg_raw = record.get("fg_raw") or {}
    bn_raw = record.get("bn_raw") or {}
    return bool(fg_raw) or bool(bn_raw)


def _record_concentration(record: dict[str, Any]) -> str | None:
    fg_raw = record.get("fg_raw") or {}
    raw_identity = fg_raw.get("raw_identity") or {}
    for value in (fg_raw.get("concentration"), raw_identity.get("concentration")):
        text = str(value or "").strip()
        if text and text.lower() != "unknown":
            return text
    return None


def _all_records() -> list[dict[str, Any]]:
    """Every fragrance_records row, paged through list_fragrance_records."""
    out: list[dict[str, Any]] = []
    offset = 0
    page = 500
    while True:
        rows = db.list_fragrance_records(limit=page, offset=offset)
        if not rows:
            break
        out.extend(rows)
        if len(rows) < page:
            break
        offset += page
    return out


# --------------------------------------------------------------------------
# Step 1 -- metrics (family)
# --------------------------------------------------------------------------
def heal_metrics(records: list[dict[str, Any]], *, dry_run: bool) -> dict[str, int]:
    scanned = already = no_raw = recomputed = persisted = fam_ok = errors = 0
    for record in records:
        scanned += 1
        if record.get("derived_metrics") is not None:
            already += 1
            continue
        if not _record_has_raw(record):
            no_raw += 1
            continue
        try:
            details = api._details_from_fragrance_record(record)
            dm = build_derived_metrics(details)
        except Exception as exc:  # noqa: BLE001
            errors += 1
            print(f"  [metrics] build failed {record.get('record_key')}: {exc}")
            continue
        if not dm:
            errors += 1
            continue
        recomputed += 1
        fam = derive_families({"derived_metrics": dm}, existing_family=None)
        if fam.get("primary_family"):
            fam_ok += 1
        if dry_run:
            continue
        try:
            if db.set_record_derived_metrics(record.get("record_key"), dm):
                persisted += 1
        except Exception as exc:  # noqa: BLE001
            errors += 1
            print(f"  [metrics] persist failed {record.get('record_key')}: {exc}")
    print(
        f"metrics: scanned={scanned} already_present={already} no_raw={no_raw} "
        f"recomputed={recomputed} family_resolved={fam_ok} persisted={persisted} "
        f"errors={errors} dry_run={dry_run}"
    )
    return {"persisted": persisted, "family_resolved": fam_ok}


# --------------------------------------------------------------------------
# Step 2 -- concentration (engine cache)
# --------------------------------------------------------------------------
def heal_concentration(
    records: list[dict[str, Any]] | None, *, dry_run: bool
) -> dict[str, int]:
    """Strict-offline concentration backfill on fragrance_records / fg_detail_cache.

    When ``records`` is None this delegates to the existing --engine-gap sweep
    (full table). When a scoped record subset is given (the 5-fragrance test) it
    resolves just those identities so the test stays cheap and targeted."""
    if records is None:
        ns = argparse.Namespace(dry_run=dry_run, limit=None)
        conc.run_engine_gap(ns)
        return {}

    resolved = skipped = written = 0
    dsn = os.environ["DATABASE_URL"]
    seen: set[tuple[str, str]] = set()
    for record in records:
        if _record_concentration(record):
            continue  # already has a concentration -- merge-safe skip
        house = str(record.get("house") or "").strip()
        name = str(record.get("name") or "").strip()
        key = (house.lower(), name.lower())
        if not house or not name or key in seen:
            continue
        seen.add(key)
        res = conc.resolve_concentration_strict(house, name)
        if not res:
            skipped += 1
            continue
        resolved += 1
        print(
            f"  [concentration] {house} | {name} -> {res['concentration']} "
            f"(src={res['concentration_meta']['source']})"
        )
        if dry_run:
            continue
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                d, r = conc.update_engine_cache_rows(
                    cur, house, name, res["concentration"], res["concentration_meta"]
                )
            conn.commit()
        written += d + r
    print(
        f"concentration(scoped): resolved={resolved} skipped={skipped} "
        f"cache_rows_written={written} dry_run={dry_run}"
    )
    return {"resolved": resolved}


# --------------------------------------------------------------------------
# Step 3 -- wardrobe projection (family + concentration into user data)
# --------------------------------------------------------------------------
def _build_engine_index(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Exact-match index: normalized brand+name -> {derived_metrics, concentration}.

    A re-read of fragrance_records is intentional: the metrics/concentration steps
    may have just filled values that ``records`` (read before the write) does not
    carry, so we hydrate from the freshest engine state."""
    index: dict[str, dict[str, Any]] = {}
    for record in _all_records():
        key = _norm_key(str(record.get("house") or ""), str(record.get("name") or ""))
        if not key:
            continue
        entry = index.setdefault(key, {"derived_metrics": None, "concentration": None})
        if entry["derived_metrics"] is None and record.get("derived_metrics") is not None:
            entry["derived_metrics"] = record["derived_metrics"]
        if entry["concentration"] is None:
            entry["concentration"] = _record_concentration(record)
    return index


def heal_wardrobe(
    index: dict[str, dict[str, Any]],
    *,
    only_keys: set[str] | None,
    dry_run: bool,
) -> dict[str, int]:
    dsn = os.environ["DATABASE_URL"]
    stats = {"uf_family": 0, "uf_conc": 0, "gf_family": 0, "gf_conc": 0, "no_match": 0}

    def _project(payload: dict[str, Any], norm: str) -> tuple[bool, bool]:
        """Returns (family_filled, concentration_filled)."""
        entry = index.get(norm)
        if not entry:
            stats["no_match"] += 1
            return False, False
        fam_filled = conc_filled = False
        family_unknown = not is_fact_complete(payload.get("family"), "family")
        if family_unknown and entry["derived_metrics"] is not None:
            before = payload.get("family")
            _apply_derived_facts(payload, entry["derived_metrics"])
            fam_filled = payload.get("family") != before and is_fact_complete(
                payload.get("family"), "family"
            )
        if not is_fact_complete(payload.get("concentration"), "concentration") and entry["concentration"]:
            payload["concentration"] = entry["concentration"]
            payload.setdefault("concentration_meta", {"source": "engine_cache_projection"})
            conc_filled = True
        return fam_filled, conc_filled

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        # user_fragrances
        with conn.cursor() as cur:
            cur.execute("SELECT id, fragrance_data FROM user_fragrances")
            uf_rows = cur.fetchall()
        for row in uf_rows:
            fdata = row["fragrance_data"] or {}
            if not isinstance(fdata, dict):
                continue
            norm = _norm_key(str(fdata.get("brand") or ""), str(fdata.get("name") or ""))
            if only_keys is not None and norm not in only_keys:
                continue
            fam, cc = _project(fdata, norm)
            if fam:
                stats["uf_family"] += 1
            if cc:
                stats["uf_conc"] += 1
            if (fam or cc) and not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE user_fragrances SET fragrance_data = %s WHERE id = %s",
                        (Json(fdata), row["id"]),
                    )

        # global_fragrances
        with conn.cursor() as cur:
            cur.execute("SELECT id, brand, name, profile_data FROM global_fragrances")
            gf_rows = cur.fetchall()
        for row in gf_rows:
            pdata = row["profile_data"] or {}
            if not isinstance(pdata, dict):
                continue
            brand = row.get("brand") or pdata.get("brand") or ""
            name = row.get("name") or pdata.get("name") or ""
            norm = _norm_key(str(brand), str(name))
            if only_keys is not None and norm not in only_keys:
                continue
            fam, cc = _project(pdata, norm)
            if fam:
                stats["gf_family"] += 1
            if cc:
                stats["gf_conc"] += 1
            if (fam or cc) and not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE global_fragrances SET profile_data = %s WHERE id = %s",
                        (Json(pdata), row["id"]),
                    )
        if not dry_run:
            conn.commit()
    print(
        f"wardrobe: user_fragrances[family+{stats['uf_family']} conc+{stats['uf_conc']}] "
        f"global_fragrances[family+{stats['gf_family']} conc+{stats['gf_conc']}] "
        f"no_engine_match={stats['no_match']} dry_run={dry_run}"
    )
    return stats


# --------------------------------------------------------------------------
# Step 4 -- seed (queue wardrobe fragrances absent from the engine cache)
# --------------------------------------------------------------------------
def seed_missing_wardrobe(
    index: dict[str, dict[str, Any]], *, dry_run: bool
) -> dict[str, int]:
    """Enqueue an enrichment job for every wardrobe fragrance that has NO engine
    record yet, so the live worker (residential IP) can scrape it.

    Offline heal + projection can only fix wardrobe rows that already exist in
    fragrance_records. The rest (e.g. Creed Royal Oud, Chanel Coco) were never
    scraped, so projection can't help -- they must be resolved + parsed by the
    worker, which on /complete persists family + concentration. This step keys
    each job exactly like api._enqueue_enrichment_job (brand|name identity slug)
    so it dedupes with the queue the SPA's /details path writes."""
    dsn = os.environ["DATABASE_URL"]
    candidates: dict[str, tuple[str, str]] = {}
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fragrance_data->>'brand' b, fragrance_data->>'name' n FROM user_fragrances")
            rows = cur.fetchall()
            cur.execute(
                "SELECT COALESCE(brand, profile_data->>'brand') b,"
                " COALESCE(name, profile_data->>'name') n FROM global_fragrances"
            )
            rows += cur.fetchall()
    for row in rows:
        brand = str(row.get("b") or "").strip()
        name = str(row.get("n") or "").strip()
        if not name:
            continue
        if _norm_key(brand, name) in index:
            continue  # already in the engine cache -- offline projection covers it
        selected = api.engine.UnifiedFragrance(name=name, brand=brand, year="")
        job_key = api._identity_job_key(selected)
        if job_key:
            candidates.setdefault(job_key, (brand, name))
    print(f"seed: {len(candidates)} wardrobe fragrances missing from engine cache")
    for job_key, (brand, name) in list(candidates.items())[:8]:
        print(f"  [seed] {brand} | {name}  (job_key={job_key})")
    if dry_run:
        print(f"seed: dry_run -- would enqueue {len(candidates)} jobs")
        return {"candidates": len(candidates)}
    enqueued = 0
    for job_key, (brand, name) in candidates.items():
        state = db.enqueue_job_state(
            job_key=job_key,
            query=f"{brand} {name}".strip(),
            name=name or None,
            house=brand or None,
            year=None,
            bn_url=None,
            fg_url=None,
        )
        if state:
            enqueued += 1
    print(f"seed: enqueued={enqueued} dry_run={dry_run}")
    return {"enqueued": enqueued}


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="compute + report, write nothing")
    parser.add_argument(
        "--steps",
        default="metrics,concentration,wardrobe",
        help="comma list subset of: metrics,concentration,wardrobe,seed "
        "(seed is opt-in: it queues wardrobe fragrances absent from the engine "
        "cache for the live worker, and is NOT in the default set)",
    )
    parser.add_argument(
        "--record-key",
        action="append",
        default=[],
        help="scope the engine-cache steps to these fragrance_records keys (repeatable). "
        "Wardrobe projection is restricted to rows matching these records' identities.",
    )
    args = parser.parse_args()

    db.init_db()
    if not db.ENABLED:
        print("DATABASE_URL not configured; nothing to do.", file=sys.stderr)
        return 2

    steps = {s.strip() for s in args.steps.split(",") if s.strip()}
    record_keys = set(args.record_key) or None
    scoped = record_keys is not None

    all_records = _all_records()
    subset = (
        [r for r in all_records if r.get("record_key") in record_keys]
        if scoped
        else all_records
    )
    if scoped:
        found = {r.get("record_key") for r in subset}
        missing = record_keys - found
        print(f"Scoped run: {len(subset)} of {len(record_keys)} record-keys found.")
        for key in sorted(missing):
            print(f"  [warn] record_key not found: {key}")

    print(f"=== heal_offline (dry_run={args.dry_run}) steps={sorted(steps)} ===")
    if "metrics" in steps:
        heal_metrics(subset, dry_run=args.dry_run)
    if "concentration" in steps:
        heal_concentration(subset if scoped else None, dry_run=args.dry_run)
    if "wardrobe" in steps:
        index = _build_engine_index(subset if scoped else all_records)
        only_keys = (
            {_norm_key(str(r.get("house") or ""), str(r.get("name") or "")) for r in subset}
            if scoped
            else None
        )
        heal_wardrobe(index, only_keys=only_keys, dry_run=args.dry_run)
    if "seed" in steps:
        if scoped:
            print("seed: skipped (not supported with --record-key scoping)")
        else:
            seed_missing_wardrobe(_build_engine_index(all_records), dry_run=args.dry_run)
    print("=== done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
