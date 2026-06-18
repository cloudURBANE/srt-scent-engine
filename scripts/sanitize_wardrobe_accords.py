#!/usr/bin/env python3
"""Strip scraped-junk accord labels (e.g. "sponsored 100%") from the live wardrobe.

The engine-projection heal only converges a stale ``scent_vector`` when an engine
record matches the row; an abbreviated wardrobe name that never matches (e.g.
"Dylan Blue" vs the engine's "Pour Homme Dylan Blue") keeps the junk forever.
This is the standalone, engine-free sweep -- it runs ``_sanitize_wardrobe_blob``
(the same in-place junk filter wired into ``heal_wardrobe``) over every row of
both wardrobe tables. Dry-run by default; pass ``--commit`` to persist.

DSN: reads DATABASE_URL from the environment, else from huge_monorepo/.env.
"""
from __future__ import annotations

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, ROOT)

import psycopg2
from psycopg2.extras import Json

from heal_offline import _sanitize_wardrobe_blob


def _resolve_dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return dsn.strip()
    env_path = os.path.join(ROOT, "..", "huge_monorepo", ".env")
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            m = re.match(r"\s*DATABASE_URL\s*=\s*(.*)\s*$", line)
            if m:
                return m.group(1).strip()
    raise SystemExit("no DATABASE_URL in env or huge_monorepo/.env")


def _sweep(cur, table: str, col: str, commit: bool) -> int:
    cur.execute(f"SELECT id, {col} FROM {table}")
    rows = cur.fetchall()
    cleaned = 0
    for rid, blob in rows:
        if not isinstance(blob, dict):
            continue
        if _sanitize_wardrobe_blob(blob):
            cleaned += 1
            brand = blob.get("brand")
            name = blob.get("name")
            print(f"  [{table}] {rid} {brand} {name} -> sanitized")
            if commit:
                cur.execute(
                    f"UPDATE {table} SET {col} = %s WHERE id = %s", (Json(blob), rid)
                )
    print(f"{table}.{col}: scanned={len(rows)} cleaned={cleaned} committed={commit}")
    return cleaned


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true", help="persist changes (default: dry-run)")
    args = ap.parse_args()

    conn = psycopg2.connect(_resolve_dsn())
    try:
        with conn.cursor() as cur:
            total = 0
            total += _sweep(cur, "user_fragrances", "fragrance_data", args.commit)
            total += _sweep(cur, "global_fragrances", "profile_data", args.commit)
        if args.commit:
            conn.commit()
        print(f"\nTOTAL cleaned={total} committed={args.commit}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
