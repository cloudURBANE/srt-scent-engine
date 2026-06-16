#!/usr/bin/env python3
"""Read-only audit of non-perfume rows in the WARDROBE DB (Supabase).

The engine purge (scripts/purge_non_perfumes.py) cleans viaduct; the lotions a
user actually sees in the app live in the separate Supabase wardrobe tables
(user_fragrances, global_fragrances). This lists non-perfume candidates there
using the SAME classifier as the gate, tiered so mixed brands are reviewed not
auto-flagged. Read-only -- writes nothing.

DATABASE_URL is taken from the env, else loaded from huge_monorepo/.env (the
Supabase wardrobe), mirroring heal_offline. Run LOCALLY (not under railway run,
which would point DATABASE_URL at viaduct):
    .venv\\Scripts\\python.exe scripts/audit_wardrobe_non_perfumes.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "scripts"))

import psycopg  # noqa: E402

import heal_offline  # noqa: E402  (reuse its DATABASE_URL loader)
from enrichment_facts import non_perfume_signal  # noqa: E402

REVIEW_BRANDS = {"victoria s secret", "victorias secret", "fresh"}


def _norm(s: str) -> str:
    import re

    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())).strip()


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Delete flagged rows (default: dry-run).")
    args = ap.parse_args()

    heal_offline._load_database_url()
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL unset and no huge_monorepo/.env found.")
    print(f"Wardrobe DB host: {url.split('@')[-1].split('/')[0]}")

    flagged: list[tuple[str, str, str, str, str]] = []  # table, id, brand, name, reason
    review: list[tuple[str, str, str]] = []

    with psycopg.connect(url) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, fragrance_data->>'brand', fragrance_data->>'name' FROM user_fragrances"
        )
        uf = cur.fetchall()
        cur.execute(
            "SELECT id, COALESCE(brand, profile_data->>'brand'),"
            " COALESCE(name, profile_data->>'name') FROM global_fragrances"
        )
        gf = cur.fetchall()

        for table, rows in (("user_fragrances", uf), ("global_fragrances", gf)):
            for _id, brand, name in rows:
                is_np, why = non_perfume_signal(name, brand)
                if is_np:
                    flagged.append((table, str(_id), brand or "?", name or "?", why))
                elif _norm(brand) in REVIEW_BRANDS:
                    review.append((table, brand or "?", name or "?"))

        print(f"\nuser_fragrances rows: {len(uf)}   global_fragrances rows: {len(gf)}")
        print(f"\n=== NON-PERFUME CANDIDATES: {len(flagged)} ===")
        for table, _id, brand, name, why in flagged:
            print(f"  [{table}] {brand} | {name}   [{why}]")
        print(f"\n=== REVIEW (mixed brand, NOT auto): {len(review)} ===")
        for table, brand, name in review:
            print(f"  [{table}] {brand} | {name}")

        if not args.apply:
            print("\nDRY-RUN. Re-run with --apply to delete the flagged rows.")
            return 0

        deleted = {"user_fragrances": 0, "global_fragrances": 0}
        for table, _id, brand, name, why in flagged:
            cur.execute(f"DELETE FROM {table} WHERE id = %s", (_id,))
            deleted[table] += cur.rowcount
        conn.commit()
        print(f"\nDELETED: {deleted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
