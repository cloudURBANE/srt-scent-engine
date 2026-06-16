#!/usr/bin/env python3
"""Purge non-perfume / test-junk rows from the ENGINE cache (viaduct DB).

The app is a fragrance app; body-care products and leaked test fixtures should
not sit in the enrichment cache. This reuses the SAME high-precision classifier
as the ingest gate (`enrichment_facts.non_perfume_signal`) so cleanup and
prevention never diverge -- a genuine perfume is never selected.

It deletes the matched identity from all three engine tables that key on it:
fragrance_records (record_key), fg_detail_cache (canonical_fg_url) and
enrichment_jobs (job_key).

SAFETY:
  * Dry-run by default -- prints exactly what WOULD be deleted, writes nothing.
  * `--apply` is required to actually delete; runs in a single transaction.
  * "Review" brands (mixed catalogs like Victoria's Secret / Fresh that DO make
    some real perfumes) are listed but NEVER auto-deleted -- pass their exact
    record_keys with `--also-key` after eyeballing the dry-run.

Run against the engine DB (so DATABASE_URL = viaduct):
    railway run -s lively-adaptation -- python scripts/purge_non_perfumes.py            # dry-run
    railway run -s lively-adaptation -- python scripts/purge_non_perfumes.py --apply    # delete
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import psycopg  # noqa: E402

from enrichment_facts import non_perfume_signal  # noqa: E402

# Mixed brands that also make genuine perfumes -- surfaced for review, never
# auto-deleted. (Kept here, not in the classifier, so the gate stays precise.)
REVIEW_BRANDS = {"victoria s secret", "victorias secret", "fresh"}


def _norm_house(house: str) -> str:
    import re

    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", (house or "").lower())).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Actually delete (default: dry-run).")
    ap.add_argument(
        "--also-key",
        action="append",
        default=[],
        help="Extra record_key to delete (e.g. an approved review-brand row). Repeatable.",
    )
    args = ap.parse_args()

    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL unset. Run under `railway run -s lively-adaptation -- ...`.")
    print(f"DB host: {url.split('@')[-1].split('/')[0]}")

    with psycopg.connect(url) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT record_key, house, name, canonical_fg_url, bn_url FROM fragrance_records"
        )
        rows = cur.fetchall()

        to_delete: list[tuple] = []
        review: list[tuple] = []
        for rk, house, name, fg, bn in rows:
            flagged, why = non_perfume_signal(name, house, fg_url=fg, bn_url=bn)
            if flagged:
                to_delete.append((rk, house, name, fg, bn, why))
            elif _norm_house(house) in REVIEW_BRANDS:
                review.append((rk, house, name, fg, bn, "review brand (mixed catalog)"))

        extra_keys = set(args.also_key)
        if extra_keys:
            for rk, house, name, fg, bn in rows:
                if rk in extra_keys and rk not in {d[0] for d in to_delete}:
                    to_delete.append((rk, house, name, fg, bn, "explicit --also-key"))

        print(f"\n=== WOULD DELETE: {len(to_delete)} record(s) ===")
        for rk, house, name, fg, bn, why in to_delete:
            print(f"  {house} | {name}   [{why}]")
        print(f"\n=== REVIEW (NOT auto-deleted): {len(review)} ===")
        for rk, house, name, fg, bn, why in review:
            print(f"  {house} | {name}   record_key={rk}")

        if not args.apply:
            print("\nDRY-RUN. Re-run with --apply to delete (and --also-key KEY for review rows).")
            return 0

        deleted = {"fragrance_records": 0, "fg_detail_cache": 0, "enrichment_jobs": 0}
        for rk, house, name, fg, bn, why in to_delete:
            cur.execute("DELETE FROM fragrance_records WHERE record_key = %s", (rk,))
            deleted["fragrance_records"] += cur.rowcount
            for u in {fg, bn}:
                if u:
                    cur.execute("DELETE FROM fg_detail_cache WHERE canonical_fg_url = %s", (u,))
                    deleted["fg_detail_cache"] += cur.rowcount
                    cur.execute("DELETE FROM enrichment_jobs WHERE job_key = %s", (u,))
                    deleted["enrichment_jobs"] += cur.rowcount
        conn.commit()
        print(f"\nDELETED: {deleted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
