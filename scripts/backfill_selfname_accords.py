#!/usr/bin/env python3
"""One-shot backfill: converge already-stored self-name / meta accord pollution.

Why this exists (gap the read-time backstop cannot close on its own):

  * The vote-count / rating / season / ad / meta-heading classes self-heal on
    READ: :func:`enrichment_facts.sanitize_derived_metrics` and
    :func:`top_accords_from` re-filter every stored blob label-only, so a legacy
    row carrying "14.9K" or "Fragrance Composition" reads clean and projects
    clean without any recompute. (test_enrichment covers this.)
  * The SELF-NAME class canonically CANNOT self-heal that way: deciding that
    "Bal a Versailles" is junk requires knowing the fragrance's own name, and the
    read-time backstops are called label-only. A blob stored before the identity
    fix (commit that threaded name/brand into is_junk_accord_label) keeps its own
    name baked into main_accords forever unless it is re-extracted.

This script re-filters stored ``main_accords`` (scent_vector / top_accords /
accord_summary) and raw ``frag_cards`` WITH each row's house+name identity, using
the same single-authority predicate. It is deterministic and idempotent -- a
second run is a no-op -- and DRY-RUN by default; nothing is written without
``--commit``.

Targets (choose one or both; each needs the matching DB):
  --engine    fragrance_records.derived_metrics_json   (engine viaduct DB via db.py)
  --wardrobe  user_fragrances.fragrance_data + global_fragrances.profile_data
              (Supabase wardrobe DB; DSN from DATABASE_URL or huge_monorepo/.env)

Offline check with no DB creds:
  --demo      run the identity-aware sanitizer on a synthetic polluted blob and
              print the before/after, proving the repair + idempotency locally.
"""
from __future__ import annotations

import argparse
import copy
import os
import re
import sys
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from enrichment_facts import sanitize_derived_metrics, sanitize_frag_cards


def _sanitize_blob_with_identity(blob: dict[str, Any], name: str, brand: str) -> bool:
    """Identity-aware in-place sanitize of a stored derived_metrics + frag_cards blob."""
    changed = False
    dm = blob.get("derived_metrics")
    if isinstance(dm, dict):
        if sanitize_derived_metrics(dm, fragrance_name=name, brand=brand):
            changed = True
    # Some wardrobe rows nest derived metrics under fragrance_data directly.
    if isinstance(blob.get("main_accords"), dict):
        if sanitize_derived_metrics(blob, fragrance_name=name, brand=brand):
            changed = True
    fc = blob.get("frag_cards")
    if isinstance(fc, dict):
        if sanitize_frag_cards(fc, fragrance_name=name, brand=brand):
            changed = True
    return changed


# --------------------------------------------------------------------------
# Engine viaduct: fragrance_records.derived_metrics_json
# --------------------------------------------------------------------------
def backfill_engine(commit: bool) -> int:
    import db

    if not db.ENABLED:
        print("[engine] db not ENABLED (no DATABASE_URL for the engine cache) -> skipped")
        return 0
    scanned = repaired = persisted = 0
    offset, page = 0, 500
    while True:
        rows = db.list_fragrance_records(limit=page, offset=offset)
        if not rows:
            break
        for rec in rows:
            scanned += 1
            dm = rec.get("derived_metrics")
            if not isinstance(dm, dict):
                continue
            name = rec.get("name") or ""
            house = rec.get("house") or ""
            dm_copy = copy.deepcopy(dm)
            if sanitize_derived_metrics(dm_copy, fragrance_name=name, brand=house):
                repaired += 1
                print(f"  [engine] {rec.get('record_key')} :: {house} / {name} -> self-name/meta stripped")
                if commit:
                    # force=True: the blob is non-NULL but corrupted; the additive
                    # NULL-guarded path can never reach it (see set_record_derived_metrics).
                    if db.set_record_derived_metrics(rec.get("record_key"), dm_copy, force=True):
                        persisted += 1
        if len(rows) < page:
            break
        offset += page
    print(f"[engine] scanned={scanned} repaired={repaired} persisted={persisted} committed={commit}")
    return repaired


# --------------------------------------------------------------------------
# Supabase wardrobe: user_fragrances / global_fragrances
# --------------------------------------------------------------------------
def _wardrobe_dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return dsn.strip()
    env_path = os.path.join(ROOT, "..", "huge_monorepo", ".env")
    try:
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                m = re.match(r"\s*DATABASE_URL\s*=\s*(.*)\s*$", line)
                if m:
                    return m.group(1).strip()
    except OSError:
        pass
    raise SystemExit("no DATABASE_URL in env or huge_monorepo/.env")


def backfill_wardrobe(commit: bool) -> int:
    try:
        import psycopg
        from psycopg.types.json import Json
    except ImportError:
        print("[wardrobe] psycopg not installed -> skipped")
        return 0
    dsn = _wardrobe_dsn()
    total = 0
    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            for table, col in (("user_fragrances", "fragrance_data"), ("global_fragrances", "profile_data")):
                cur.execute(f"SELECT id, {col} FROM {table}")
                rows = cur.fetchall()
                cleaned = 0
                for rid, blob in rows:
                    if not isinstance(blob, dict):
                        continue
                    name = blob.get("name") or ""
                    brand = blob.get("brand") or blob.get("house") or ""
                    if _sanitize_blob_with_identity(blob, name, brand):
                        cleaned += 1
                        print(f"  [{table}] {rid} :: {brand} / {name} -> self-name/meta stripped")
                        if commit:
                            cur.execute(
                                f"UPDATE {table} SET {col} = %s, updated_at = now() WHERE id = %s",
                                (Json(blob), rid),
                            )
                print(f"[wardrobe] {table}.{col}: scanned={len(rows)} cleaned={cleaned} committed={commit}")
                total += cleaned
        if commit:
            conn.commit()
    finally:
        conn.close()
    return total


# --------------------------------------------------------------------------
# Offline demo (no DB): prove the identity-aware repair + idempotency
# --------------------------------------------------------------------------
def demo() -> int:
    name, brand = "Bal a Versailles", "Jean Desprez"
    blob = {
        "derived_metrics": {
            "main_accords": {
                "scent_vector": [
                    {"accord": "Bal a Versailles", "score": 100.0},
                    {"accord": "Fragrance Composition", "score": 99.0},
                    {"accord": "Amber", "score": 90.0},
                    {"accord": "Powdery", "score": 82.0},
                    {"accord": "Woody", "score": 71.0},
                ],
                "top_accords": ["Bal a Versailles", "Fragrance Composition", "Amber", "Powdery", "Woody"],
                "accord_summary": "A bal a versailles fragrance with fragrance composition and amber facets.",
            }
        }
    }
    print("BEFORE:", blob["derived_metrics"]["main_accords"]["top_accords"])
    changed = _sanitize_blob_with_identity(blob, name, brand)
    top = blob["derived_metrics"]["main_accords"]["top_accords"]
    print("AFTER :", top)
    print("summary:", blob["derived_metrics"]["main_accords"]["accord_summary"])
    print("changed:", changed)
    # Idempotency: a second pass must be a no-op.
    again = _sanitize_blob_with_identity(blob, name, brand)
    print("second pass changed (must be False):", again)
    ok = (
        changed
        and not again
        and top == ["Amber", "Powdery", "Woody"]
    )
    print("DEMO", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", action="store_true", help="repair fragrance_records.derived_metrics_json")
    ap.add_argument("--wardrobe", action="store_true", help="repair user_fragrances/global_fragrances")
    ap.add_argument("--demo", action="store_true", help="offline synthetic repair proof (no DB)")
    ap.add_argument("--commit", action="store_true", help="persist changes (default: dry-run)")
    args = ap.parse_args()

    if args.demo:
        return demo()
    if not (args.engine or args.wardrobe):
        ap.error("choose at least one of --engine / --wardrobe (or --demo)")

    print(f"backfill self-name/meta accords  commit={args.commit} (dry-run unless --commit)\n")
    total = 0
    if args.engine:
        total += backfill_engine(args.commit)
    if args.wardrobe:
        total += backfill_wardrobe(args.commit)
    print(f"\nTOTAL rows needing repair={total} committed={args.commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
