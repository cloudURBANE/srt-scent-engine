#!/usr/bin/env python3
"""Purge skeletal junk stub rows from ``global_fragrances`` (catalog/search index).

Some ``global_fragrances`` rows are not real fragrances at all -- they are
brand==name skeletons with no Fragrantica page (``gucci|gucci``,
``Xerjoff|Xerjoff``, ``Tom ford|Tom ford``, ``Creed|Creed`` ...). They pollute
the dirty-data audit and the search index, and the phone never renders them
because they carry no enrichment. This removes ONLY those.

Conservative predicate (ALL must hold for a row to be purged):
  * normalized brand == normalized name  (EXACT identity, not substring -- so a
    legit "Chanel No 5" / "Chanel" pair is never matched), AND
  * no real concentration (``Unknown``/blank), AND
  * no note pyramid (``derived_metrics.notes.has_pyramid`` is false), AND
  * no source Fragrantica page (``source_url`` blank).

These three structural signals together prove the row was never enriched from a
real Fragrantica page: every genuine catalog fragrance has at least a source_url
(and almost always a pyramid + concentration). The 1-2 generic accords these
stubs sometimes carry are auto-generated placeholders, so the accord list is NOT
used as a keep-gate -- a real fragrance is distinguished by the structural trio,
not by whether its accords happen to be valid words. A row that has ANY of a
pyramid, a known concentration, or a real FG URL is KEPT even if brand==name.
``user_fragrances`` is NEVER touched -- only the ``global_fragrances`` catalog.

Dry-run by default: prints the exact rows that WOULD be deleted. Pass ``--commit``
to actually delete. Verify the printed list against ``db_audit_report.json``
before committing.

DSN: reads DATABASE_URL from the environment, else from huge_monorepo/.env (the
app/wardrobe DB the phone reads). No Railway token needed -- Supabase is
off-network.

Usage:
    python scripts/purge_junk_globals.py            # dry-run (default)
    python scripts/purge_junk_globals.py --commit    # actually delete
"""
from __future__ import annotations

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, ROOT)

import psycopg
from psycopg.rows import dict_row

from enrichment_facts import is_fact_complete

# Reuse the file's canonical identity key so the brand==name test matches exactly
# how the rest of the pipeline normalizes (lowercased alphanumerics).
from heal_offline import _norm_key


def _resolve_dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return dsn.strip()
    env_path = os.path.join(ROOT, "..", "huge_monorepo", ".env")
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            m = re.match(r"\s*DATABASE_URL\s*=\s*(.*)\s*$", line)
            if m:
                return m.group(1).strip().strip('"').strip("'")
    raise SystemExit("no DATABASE_URL in env or huge_monorepo/.env")


def _has_pyramid(blob: dict) -> bool:
    dm = blob.get("derived_metrics")
    if not isinstance(dm, dict):
        return False
    notes = dm.get("notes")
    return bool(isinstance(notes, dict) and notes.get("has_pyramid"))


def is_junk_global_stub(brand: str, name: str, blob: dict) -> bool:
    """Conservative junk predicate. ALL conditions must hold (see module docstring).

    Pure + DB-free so it is unit-testable. A row with any real enrichment is kept.
    """
    if not isinstance(blob, dict):
        return False
    b = (brand or blob.get("brand") or "").strip()
    n = (name or blob.get("name") or "").strip()
    if not b or not n:
        return False
    # EXACT normalized identity -- never a substring match. "Chanel No 5" vs
    # "Chanel" normalize to different keys, so a legit catalog row is safe.
    if _norm_key(b, "") != _norm_key(n, ""):
        return False
    if is_fact_complete(blob.get("concentration"), "concentration"):
        return False
    if _has_pyramid(blob):
        return False
    if (blob.get("source_url") or "").strip():
        return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--commit", action="store_true", help="actually delete (default: dry-run)")
    args = ap.parse_args()

    dsn = _resolve_dsn()
    print(f"DB: {re.sub(r'//[^@]*@', '//***@', dsn).split('?')[0]}")
    conn = psycopg.connect(dsn, row_factory=dict_row)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, brand, name, profile_data FROM global_fragrances")
            rows = cur.fetchall()
            victims = []
            for row in rows:
                blob = row["profile_data"] if isinstance(row["profile_data"], dict) else {}
                brand = row.get("brand") or blob.get("brand") or ""
                name = row.get("name") or blob.get("name") or ""
                if is_junk_global_stub(str(brand), str(name), blob):
                    victims.append((row["id"], str(brand), str(name)))

            print(f"\nscanned {len(rows)} global_fragrances rows")
            print(f"{'WOULD DELETE' if not args.commit else 'DELETING'} {len(victims)} junk stub(s):")
            for rid, brand, name in victims:
                print(f"  - {brand!r} | {name!r}  (id={rid})")

            if args.commit and victims:
                cur.executemany(
                    "DELETE FROM global_fragrances WHERE id = %s",
                    [(rid,) for rid, _, _ in victims],
                )
                conn.commit()
                print(f"\nDELETED {len(victims)} rows.")
            elif not args.commit:
                print("\n(dry-run -- nothing deleted; re-run with --commit to apply)")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
