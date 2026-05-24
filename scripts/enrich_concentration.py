#!/usr/bin/env python3
"""
Batch backfill concentration for user_fragrances / global_fragrances rows
where concentration is null, empty, or "Unknown".

Usage:
    python scripts/enrich_concentration.py --dry-run
    python scripts/enrich_concentration.py --limit 10
    python scripts/enrich_concentration.py --all-unknown
    python scripts/enrich_concentration.py --brand Creed --name Aventus
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

env_path = _REPO_ROOT.parent / "huge_monorepo" / ".env"
if env_path.exists():
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

import db
from concentration_grabber import SemanticScentEngine, to_scentcast_concentration
from psycopg.types.json import Json
import psycopg
from psycopg.rows import dict_row

# Mirror of scentVectorizer.ts CONCENTRATION_BOOSTS — zero cross-runtime dependency
CONCENTRATION_BOOSTS = {
    "Extrait":         {"longevity": 3, "sillage": 2},
    "Parfum":          {"longevity": 2, "sillage": 1},
    "Eau de Parfum":   {"longevity": 1, "sillage": 1},
    "Eau de Toilette": {"longevity": 0, "sillage": 0},
    "Eau de Cologne":  {"longevity": -1, "sillage": -1},
    "Body Spray":      {"longevity": -2, "sillage": -1},
    "Unknown":         {"longevity": 0, "sillage": 0},
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def resolve_concentration(brand: str, name: str, use_browser: bool = True) -> dict | None:
    """
    Run Tier-1 (lexical, instant) then optionally Tier-2 (SERP, ~58s).
    Returns a dict with keys 'concentration' and 'concentration_meta', or None.
    """
    query = f"{brand} {name}".strip()

    # Tier 0 - Parfinity catalog (instant, no browser).
    try:
        from parfinity import lookup_concentration as _pf_lookup

        hit = _pf_lookup(brand, name)
    except Exception:
        hit = None
    if hit:
        return {
            "concentration": hit,
            "concentration_meta": {
                "confidence": 95,
                "source": "parfinity_catalog",
                "resolved_at": _now_iso(),
                "engine_label": hit,
            },
        }

    # Tier 1a — explicit in name/query
    explicit = SemanticScentEngine._explicit_intent(query)
    if explicit:
        scentcast = to_scentcast_concentration(explicit)
        if scentcast != "Unknown":
            return {
                "concentration": scentcast,
                "concentration_meta": {
                    "confidence": 100,
                    "source": "tier1_explicit",
                    "resolved_at": _now_iso(),
                    "engine_label": explicit,
                },
            }

    # Tier 1b — brand prior
    brand_hit, brand_conc = SemanticScentEngine._brand_prior(query)
    if brand_hit and brand_conc:
        scentcast = to_scentcast_concentration(brand_conc)
        if scentcast != "Unknown":
            return {
                "concentration": scentcast,
                "concentration_meta": {
                    "confidence": 70,
                    "source": "tier1_brand_prior",
                    "resolved_at": _now_iso(),
                    "engine_label": brand_conc,
                },
            }

    # Tier 1c — pillar prior
    pillar = SemanticScentEngine._query_pillar_prior(query)
    if pillar:
        scentcast = to_scentcast_concentration(pillar)
        if scentcast != "Unknown":
            return {
                "concentration": scentcast,
                "concentration_meta": {
                    "confidence": 80,
                    "source": "tier1_pillar",
                    "resolved_at": _now_iso(),
                    "engine_label": pillar,
                },
            }

    if not use_browser:
        return None

    # Tier 2 — SERP engine (~58s, uses DrissionPage + DuckDuckGo)
    try:
        profile = SemanticScentEngine.analyze(query)
    except Exception as exc:
        print(f"  [WARN] Tier-2 analyze failed: {exc}")
        return None

    if profile and profile.primary_confidence >= 50:
        scentcast = to_scentcast_concentration(profile.primary_concentration)
        return {
            "concentration": scentcast,
            "concentration_meta": {
                "confidence": profile.primary_confidence,
                "source": "semantic_engine_v6",
                "resolved_at": _now_iso(),
                "engine_label": profile.primary_concentration,
            },
        }

    return None


def recalc_performance(existing: dict, old_conc: str, new_conc: str) -> dict:
    """Adjust longevity/sillage by the delta between old and new concentration boosts."""
    old_b = CONCENTRATION_BOOSTS.get(old_conc, {"longevity": 0, "sillage": 0})
    new_b = CONCENTRATION_BOOSTS.get(new_conc, {"longevity": 0, "sillage": 0})
    perf = dict(existing)
    perf["longevity"] = perf.get("longevity", 5) - old_b["longevity"] + new_b["longevity"]
    perf["sillage"] = perf.get("sillage", 5) - old_b["sillage"] + new_b["sillage"]
    return perf


def _is_unknown(val) -> bool:
    return not val or str(val).strip() in ("", "Unknown")


def fetch_rows(cur, brand: str | None, name: str | None, limit: int | None) -> list[dict]:
    if brand and name:
        cur.execute(
            "SELECT id, fragrance_data FROM user_fragrances "
            "WHERE LOWER(fragrance_data->>'brand') = LOWER(%s) "
            "AND LOWER(fragrance_data->>'name') = LOWER(%s)",
            (brand, name),
        )
    else:
        cur.execute(
            "SELECT id, fragrance_data FROM user_fragrances "
            "WHERE fragrance_data->>'concentration' IS NULL "
            "   OR fragrance_data->>'concentration' = '' "
            "   OR fragrance_data->>'concentration' = 'Unknown' "
            "ORDER BY id"
            + (f" LIMIT {int(limit)}" if limit else "")
        )
    return cur.fetchall()


def find_global_row(cur, brand: str, name: str) -> dict | None:
    cur.execute(
        "SELECT id, profile_data FROM global_fragrances "
        "WHERE LOWER(profile_data->>'name') = LOWER(%s) "
        "AND LOWER(profile_data->>'brand') = LOWER(%s) "
        "LIMIT 1",
        (name, brand),
    )
    return cur.fetchone()


def run(args: argparse.Namespace) -> None:
    if args.brand and args.name and not args.all_unknown and args.limit is None:
        print(f"[direct] {args.brand} - {args.name}")
        resolved = resolve_concentration(
            args.brand,
            args.name,
            use_browser=not args.tier1_only,
        )
        if not resolved:
            print("  -> Unresolved")
            return
        meta = resolved["concentration_meta"]
        print(
            f"  -> {resolved['concentration']}  "
            f"(conf={meta['confidence']}, src={meta['source']})"
        )
        return
    db.init_db()
    if not db.ENABLED:
        raise RuntimeError("DATABASE_URL not configured.")

    DATABASE_URL = os.environ.get("DATABASE_URL")
    dry_run = args.dry_run
    use_browser = not args.tier1_only

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            rows = fetch_rows(cur, args.brand, args.name, args.limit)

    print(f"Found {len(rows)} row(s) to process.")

    resolved_count = 0
    skipped_count = 0

    for row in rows:
        fdata = dict(row["fragrance_data"])
        brand = fdata.get("brand", "")
        name = fdata.get("name", "")
        old_conc = fdata.get("concentration") or "Unknown"
        uf_id = row["id"]

        print(f"\n[{uf_id}] {brand} - {name}  (was: {old_conc})")

        resolved = resolve_concentration(brand, name, use_browser=use_browser)

        if not resolved:
            print(f"  -> Unresolved (skipping)")
            skipped_count += 1
            continue

        new_conc = resolved["concentration"]
        meta = resolved["concentration_meta"]
        print(f"  -> {new_conc}  (conf={meta['confidence']}, src={meta['source']})")

        if dry_run:
            print(f"  [DRY RUN] would write concentration={new_conc} + recalc performance")
            resolved_count += 1
            continue

        # Recalculate performance if concentration actually changed
        if "performance" in fdata and new_conc != old_conc:
            fdata["performance"] = recalc_performance(fdata["performance"], old_conc, new_conc)

        fdata["concentration"] = new_conc
        fdata["concentration_meta"] = meta

        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE user_fragrances SET fragrance_data = %s WHERE id = %s",
                    (Json(fdata), uf_id),
                )

                gf_row = find_global_row(cur, brand, name)
                if gf_row:
                    pdata = dict(gf_row["profile_data"])
                    pdata["concentration"] = new_conc
                    pdata["concentration_meta"] = meta
                    if "performance" in pdata and new_conc != old_conc:
                        pdata["performance"] = recalc_performance(
                            pdata["performance"], old_conc, new_conc
                        )
                    cur.execute(
                        "UPDATE global_fragrances SET profile_data = %s WHERE id = %s",
                        (Json(pdata), gf_row["id"]),
                    )
                    print(f"  Updated global_fragrances id={gf_row['id']}")

            conn.commit()
        print(f"  Wrote user_fragrances id={uf_id}")
        resolved_count += 1

    print(f"\nDone. resolved={resolved_count}  skipped={skipped_count}  total={len(rows)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill concentration for unknown wardrobe items.")
    parser.add_argument("--dry-run", action="store_true", help="Print proposed changes without writing")
    parser.add_argument("--limit", type=int, default=None, metavar="N", help="Process at most N rows")
    parser.add_argument("--all-unknown", action="store_true", help="Process all unknown rows (no limit)")
    parser.add_argument("--brand", type=str, default=None, help="Target a specific brand")
    parser.add_argument("--name", type=str, default=None, help="Target a specific fragrance name")
    parser.add_argument("--tier1-only", action="store_true", help="Skip Tier-2 SERP (no browser)")
    args = parser.parse_args()

    if not args.all_unknown and not args.brand and args.limit is None:
        parser.error("Specify --all-unknown, --limit N, or --brand / --name to select rows.")

    run(args)


if __name__ == "__main__":
    main()
