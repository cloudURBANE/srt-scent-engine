#!/usr/bin/env python3
"""Heal cached identities where the name still includes the house prefix.

Examples: house=Hermes, name="Hermès Rocabar" -> name="Rocabar".

Targets:
  - Postgres: fg_detail_cache, fragrance_records, enrichment_jobs
  - Local JSON: fg_cache/fg_detail_cache_v1.json (when present)

Default is dry-run; pass --apply to write changes.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Load DATABASE_URL from nearby env files when unset.
for env_path in (_REPO_ROOT / ".env", _REPO_ROOT.parent / "huge_monorepo" / ".env"):
    if not env_path.is_file() or os.environ.get("DATABASE_URL"):
        break
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, val = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

import db
import fragrance_parser_full_rewrite_fixed as engine
from psycopg.types.json import Json

strip_house = engine.IdentityTools.strip_house_from_name


def _heal_name(name: str | None, house: str | None) -> tuple[str | None, bool]:
    cleaned_name = str(name or "").strip()
    cleaned_house = str(house or "").strip()
    if not cleaned_name or not cleaned_house:
        return (cleaned_name or None), False
    fixed = strip_house(cleaned_name, cleaned_house)
    return (fixed or None), fixed != cleaned_name


def _heal_raw_identity(raw: Any) -> tuple[dict[str, Any], bool]:
    if not isinstance(raw, dict):
        return {}, False
    name = str(raw.get("name") or "")
    house = str(raw.get("house") or "")
    fixed_name, changed = _heal_name(name, house)
    if not changed:
        return raw, False
    out = dict(raw)
    out["name"] = fixed_name
    return out, True


def _heal_query(query: str | None, house: str | None, old_name: str | None, new_name: str | None) -> tuple[str | None, bool]:
    text = str(query or "").strip()
    if not text or not old_name or not new_name or old_name == new_name:
        return (text or None), False
    old_full = " ".join(part for part in [house, old_name] if part).strip()
    new_full = " ".join(part for part in [house, new_name] if part).strip()
    if text == old_full:
        return new_full, True
    if text.lower() == old_name.lower() and new_name:
        return new_full, True
    return text, False


def heal_postgres(*, apply: bool) -> dict[str, int]:
    stats = {
        "fg_detail_cache": 0,
        "fragrance_records": 0,
        "fragrance_records_key": 0,
        "enrichment_jobs": 0,
        "skipped_conflicts": 0,
    }
    db.init_db()
    if not db.ENABLED:
        print("Postgres: DATABASE_URL not configured — skipped.")
        return stats

    ctx, conn = db._conn()
    try:
        with conn.transaction():
            cache_rows = conn.execute(
                "SELECT canonical_fg_url, name, house, raw_identity_json FROM fg_detail_cache"
            ).fetchall()
            for row in cache_rows:
                old_name = row["name"]
                house = row["house"]
                new_name, changed = _heal_name(old_name, house)
                raw_identity, raw_changed = _heal_raw_identity(row["raw_identity_json"])
                if not changed and not raw_changed:
                    continue
                stats["fg_detail_cache"] += 1
                print(
                    f"  fg_detail_cache  {house} | {old_name!r} -> {new_name!r}  "
                    f"({row['canonical_fg_url']})"
                )
                if apply:
                    conn.execute(
                        """
                        UPDATE fg_detail_cache
                        SET name = %s,
                            raw_identity_json = %s,
                            updated_at = now()
                        WHERE canonical_fg_url = %s
                        """,
                        (new_name, Json(raw_identity), row["canonical_fg_url"]),
                    )

            record_rows = conn.execute(
                """
                SELECT record_key, canonical_fg_url, bn_url, name, house, year
                FROM fragrance_records
                """
            ).fetchall()
            for row in record_rows:
                old_name = row["name"]
                house = row["house"]
                new_name, changed = _heal_name(old_name, house)
                if not changed:
                    continue
                old_key = row["record_key"]
                new_key = db._fragrance_record_key(
                    {
                        "canonical_fg_url": row["canonical_fg_url"],
                        "bn_url": row["bn_url"],
                        "house": house,
                        "name": new_name,
                        "year": row["year"],
                    }
                )
                stats["fragrance_records"] += 1
                print(
                    f"  fragrance_records {house} | {old_name!r} -> {new_name!r}  "
                    f"(key={old_key})"
                )
                if not apply:
                    continue
                if new_key != old_key:
                    conflict = conn.execute(
                        "SELECT 1 FROM fragrance_records WHERE record_key = %s",
                        (new_key,),
                    ).fetchone()
                    if conflict:
                        stats["skipped_conflicts"] += 1
                        print(f"    SKIP key conflict: {new_key} already exists")
                        conn.execute(
                            "UPDATE fragrance_records SET name = %s, updated_at = now() WHERE record_key = %s",
                            (new_name, old_key),
                        )
                        continue
                    conn.execute(
                        """
                        UPDATE fragrance_records
                        SET record_key = %s, name = %s, updated_at = now()
                        WHERE record_key = %s
                        """,
                        (new_key, new_name, old_key),
                    )
                    stats["fragrance_records_key"] += 1
                else:
                    conn.execute(
                        "UPDATE fragrance_records SET name = %s, updated_at = now() WHERE record_key = %s",
                        (new_name, old_key),
                    )

            job_rows = conn.execute(
                "SELECT id, job_key, query, name, house FROM enrichment_jobs"
            ).fetchall()
            for row in job_rows:
                old_name = row["name"]
                house = row["house"]
                new_name, name_changed = _heal_name(old_name, house)
                new_query, query_changed = _heal_query(row["query"], house, old_name, new_name)
                if not name_changed and not query_changed:
                    continue
                stats["enrichment_jobs"] += 1
                msg = f"  enrichment_jobs  {house} | {old_name!r} -> {new_name!r}"
                if query_changed:
                    msg += f"  query={row['query']!r} -> {new_query!r}"
                print(msg)
                if apply:
                    conn.execute(
                        """
                        UPDATE enrichment_jobs
                        SET name = %s,
                            query = COALESCE(%s, query)
                        WHERE id = %s
                        """,
                        (new_name, new_query if query_changed else None, row["id"]),
                    )
    finally:
        ctx.__exit__(None, None, None)

    return stats


def heal_json_detail_cache(path: Path, *, apply: bool) -> int:
    if not path.is_file():
        print(f"JSON cache not found: {path}")
        return 0

    original_text = path.read_text(encoding="utf-8")
    payload = json.loads(original_text)
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, dict):
        print(f"JSON cache has no entries dict: {path}")
        return 0

    changed = 0
    for url, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        old_name = entry.get("name")
        house = entry.get("house")
        new_name, name_changed = _heal_name(
            str(old_name) if old_name is not None else "",
            str(house) if house is not None else "",
        )
        raw_identity, raw_changed = _heal_raw_identity(entry.get("raw_identity"))
        if not name_changed and not raw_changed:
            continue
        changed += 1
        print(f"  json  {house} | {old_name!r} -> {new_name!r}  ({url})")
        if apply:
            if name_changed:
                entry["name"] = new_name
            if raw_changed:
                entry["raw_identity"] = raw_identity

    if apply and changed:
        backup = path.with_suffix(path.suffix + f".bak-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
        backup.write_text(original_text, encoding="utf-8")
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"  wrote {path} (backup: {backup.name})")

    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write healed identities (default: dry-run only).",
    )
    parser.add_argument(
        "--json-path",
        default=str(_REPO_ROOT / "fg_cache" / "fg_detail_cache_v1.json"),
        help="Path to bundled fg_detail_cache_v1.json",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip Postgres tables.",
    )
    parser.add_argument(
        "--skip-json",
        action="store_true",
        help="Skip local JSON cache file.",
    )
    args = parser.parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"Healing duplicate house prefixes in cached identities [{mode}]")

    total = 0
    if not args.skip_db:
        print("\nPostgres:")
        stats = heal_postgres(apply=args.apply)
        total += sum(
            stats[k]
            for k in ("fg_detail_cache", "fragrance_records", "enrichment_jobs")
        )
        if stats["skipped_conflicts"]:
            print(f"  key conflicts (name only updated): {stats['skipped_conflicts']}")

    if not args.skip_json:
        print("\nJSON detail cache:")
        total += heal_json_detail_cache(Path(args.json_path), apply=args.apply)

    print(f"\nDone. Rows needing heal: {total}")
    if not args.apply and total:
        print("Re-run with --apply to persist fixes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
