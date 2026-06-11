#!/usr/bin/env python3
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Insert repository root in sys.path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Parse .env from huge_monorepo to get env vars
env_path = _REPO_ROOT.parent / "huge_monorepo" / ".env"
if env_path.exists():
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

os.environ["SERP_API_PROVIDER"] = "serper"

import db
import fragrance_parser_full_rewrite_fixed as engine
from derived_metrics_adapter import build_derived_metrics
from enrichment_facts import (
    derive_families,
    derive_wear_profile,
    is_fact_complete,
    primary_season_from_wear_profile,
)
from psycopg.types.json import Json
import psycopg
from psycopg.rows import dict_row

_FG_METRIC_GROUPS = (
    "performance_score",
    "value_score",
    "community_interest_score",
    "wear_profile",
)

def init_connections():
    db.init_db()
    if not db.ENABLED:
        raise RuntimeError("DATABASE_URL not configured.")

def _fg_metrics_complete(details: Any) -> bool:
    try:
        dm = build_derived_metrics(details)
        if not isinstance(dm, dict):
            return False
        cov = dm.get("source_coverage") or {}
        return all(bool(cov.get(g)) for g in _FG_METRIC_GROUPS)
    except Exception:
        return False

def _derived_metrics_complete(dm: Any) -> bool:
    if not isinstance(dm, dict):
        return False
    cov = dm.get("source_coverage")
    if isinstance(cov, dict):
        return all(bool(cov.get(g)) for g in _FG_METRIC_GROUPS)
    return all(dm.get(g) is not None for g in _FG_METRIC_GROUPS)

_BASE_FIELDS = ("gender", "year", "concentration")

def _base_field_value(payload: dict[str, Any], key: str) -> Any:
    """Read a base field from a stored payload, flat or product-nested.

    user_fragrances.fragrance_data keeps these flat; global_fragrances
    .profile_data nests identity under `product`.
    """
    value = payload.get(key)
    if value not in (None, "", 0):
        return value
    product = payload.get("product")
    if isinstance(product, dict):
        return product.get(key)
    return value

def _base_fields_complete(payload: Any) -> bool:
    """True when gender, year, and concentration are real facts."""
    if not isinstance(payload, dict):
        return False
    for key in _BASE_FIELDS:
        value = _base_field_value(payload, key)
        if not is_fact_complete(value, key):
            return False
    return True

def _stored_metrics_complete(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    return _derived_metrics_complete(payload.get("derived_metrics")) and _base_fields_complete(payload)

def _cheap_concentration(brand: str, name: str) -> str:
    """Tier-1 concentration resolution (no browser): Parfinity catalog,
    explicit token, brand prior, pillar prior. Returns "Unknown" when
    unresolved so the row records an attempt instead of being re-queued
    forever by the completeness sweep."""
    query = f"{brand} {name}".strip()
    try:
        from parfinity import lookup_concentration as _pf_lookup

        hit = _pf_lookup(brand, name)
        if hit:
            return hit
    except Exception:
        pass
    try:
        from concentration_grabber import SemanticScentEngine, to_scentcast_concentration

        for label in (
            SemanticScentEngine._explicit_intent(query),
            SemanticScentEngine._brand_prior(query)[1],
            SemanticScentEngine._query_pillar_prior(query),
        ):
            if not label:
                continue
            scentcast = to_scentcast_concentration(label)
            if scentcast != "Unknown":
                return scentcast
    except Exception:
        pass
    return "Unknown"

def _backfill_base_fields(payload: dict[str, Any], *, gender: Any, year: Any, brand: str, name: str) -> None:
    """Fill missing gender/year/concentration on a stored row, in place.

    Without this the new _base_fields_complete check would flag the same rows
    incomplete again on every sweep, because the metric writes below never
    touched these fields."""
    if not is_fact_complete(payload.get("gender"), "gender") and is_fact_complete(gender, "gender"):
        payload["gender"] = gender
    if not is_fact_complete(payload.get("year"), "year") and is_fact_complete(year, "year"):
        payload["year"] = year
    if not is_fact_complete(payload.get("concentration"), "concentration"):
        concentration = _cheap_concentration(brand, name)
        if is_fact_complete(concentration, "concentration"):
            payload["concentration"] = concentration


def _apply_derived_facts(payload: dict[str, Any], derived_metrics: dict[str, Any]) -> None:
    """Project canonical engine-derived facts into app/profile payloads."""
    families = derive_families(
        derived_metrics,
        existing_family=payload.get("family") or payload.get("primary_family"),
    )
    if families.get("primary_family"):
        payload["family"] = families["primary_family"]
        payload["primary_family"] = families["primary_family"]
        payload["families"] = families["families"]
        payload["accords"] = families["accords"]
        payload["family_meta"] = families["provenance"]

    wear_profile = derive_wear_profile(derived_metrics)
    if wear_profile:
        payload["wear_profile"] = wear_profile
        context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
        context["wear_profile"] = wear_profile
        payload["context"] = context
        primary_season = primary_season_from_wear_profile(wear_profile)
        if primary_season:
            payload["season"] = primary_season

def _global_profile_identity(row: dict[str, Any]) -> tuple[str, str]:
    pdata = row.get("profile_data") or {}
    if not isinstance(pdata, dict):
        pdata = {}
    product = pdata.get("product") or {}
    if not isinstance(product, dict):
        product = {}
    brand = row.get("brand") or product.get("brand") or pdata.get("brand") or ""
    name = row.get("name") or product.get("name") or pdata.get("name") or ""
    return str(brand or "").strip(), str(name or "").strip()

def _source_urls_from_payload(payload: Any) -> tuple[str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None
    product = payload.get("product") or {}
    raw_identity = payload.get("raw_identity") or {}
    candidates = [
        payload.get("source_url"),
        payload.get("frag_url"),
        payload.get("bn_url"),
        product.get("source_url") if isinstance(product, dict) else None,
        product.get("frag_url") if isinstance(product, dict) else None,
        product.get("bn_url") if isinstance(product, dict) else None,
        raw_identity.get("fg_url") if isinstance(raw_identity, dict) else None,
        raw_identity.get("bn_url") if isinstance(raw_identity, dict) else None,
    ]
    fg_url = None
    bn_url = None
    for raw in candidates:
        url = str(raw or "").strip()
        if not url:
            continue
        if "fragrantica.com" in url and not fg_url:
            fg_url = url
        elif "basenotes.com" in url and not bn_url:
            bn_url = url
    return fg_url, bn_url

def enrich_fragrance(scraper, args, brand, name, fg_url=None, bn_url=None, user_fragrance_id=None, global_fragrance_id=None):
    print(f"\n>>> Enriching: {brand} - {name}...")

    # Check if already complete in database (saves time and requests)
    DATABASE_URL = os.environ.get("DATABASE_URL")
    already_complete = False
    try:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                if user_fragrance_id:
                    cur.execute("SELECT fragrance_data FROM user_fragrances WHERE id = %s", (user_fragrance_id,))
                    row = cur.fetchone()
                    if row:
                        if _stored_metrics_complete(row["fragrance_data"]):
                            already_complete = True
                elif global_fragrance_id:
                    cur.execute("SELECT profile_data FROM global_fragrances WHERE id = %s", (global_fragrance_id,))
                    row = cur.fetchone()
                    if row:
                        if _stored_metrics_complete(row["profile_data"]):
                            already_complete = True
                else:
                    user_complete = True
                    cur.execute("SELECT fragrance_data FROM user_fragrances")
                    for r in cur.fetchall():
                        fdata = r["fragrance_data"]
                        if fdata.get("brand", "").lower() == brand.lower() and fdata.get("name", "").lower() == name.lower():
                            if not _stored_metrics_complete(fdata):
                                user_complete = False
                                break

                    global_complete = True
                    cur.execute("SELECT id, brand, name, profile_data FROM global_fragrances")
                    for r in cur.fetchall():
                        pdata = r["profile_data"]
                        gb, gn = _global_profile_identity(r)
                        if gb.lower() == brand.lower() and gn.lower() == name.lower():
                            if not _stored_metrics_complete(pdata):
                                global_complete = False
                                break

                    cur.execute("SELECT quality_status FROM fg_detail_cache WHERE house ILIKE %s AND name ILIKE %s", (brand, name))
                    cache_row = cur.fetchone()
                    cache_complete = cache_row and cache_row["quality_status"] == "complete"

                    if user_complete and global_complete and cache_complete:
                        already_complete = True
    except Exception as exc:
        print(f"  Warning: error checking completeness: {exc}")

    if already_complete:
        print(f"  Already complete in DB. Skipping.")
        return True

    # 1. Resolve URLs if missing
    resolved_fg_url = fg_url
    resolved_bn_url = bn_url

    if not resolved_fg_url:
        query = f"{brand} {name}"
        print(f"  Resolving URLs for '{query}'...")
        try:
            results = engine.search_once(scraper, query, args)
            def _clean(s):
                return "".join(c for c in s.lower() if c.isalnum())
            best_match = None
            for item in results:
                b_match = _clean(brand) in _clean(item.brand) or _clean(item.brand) in _clean(brand)
                if not b_match:
                    continue
                n_target = _clean(name)
                n_candidate = _clean(item.name)
                if n_target in n_candidate or n_candidate in n_target:
                    best_match = item
                    break
            if not best_match:
                for item in results:
                    if brand.lower() in item.brand.lower() or item.brand.lower() in brand.lower():
                        best_match = item
                        break
            if best_match and best_match.frag_url:
                resolved_fg_url = best_match.frag_url
                resolved_bn_url = best_match.bn_url or resolved_bn_url
                print(f"  Resolved Fragrantica URL: {resolved_fg_url}")
                if resolved_bn_url:
                    print(f"  Resolved Basenotes URL: {resolved_bn_url}")
            else:
                print(f"  Could not resolve a Fragrantica URL for '{query}'. Skipping.")
                return False
        except Exception as e:
            print(f"  Resolver failed for '{query}': {e}")
            return False

    # 2. Requeue or Enqueue the job in Postgres to align state
    canonical_fg_url = db._clean_url(resolved_fg_url)
    job = db.requeue_or_enqueue_job(
        job_key=canonical_fg_url,
        query=resolved_fg_url,
        name=name,
        house=brand,
        year=None,
        bn_url=resolved_bn_url,
        fg_url=resolved_fg_url,
        priority=20
    )
    if not job:
        print("  Failed to create/requeue enrichment job.")
        return False

    # 3. Lookup existing year and image_url from database
    DATABASE_URL = os.environ.get("DATABASE_URL")
    existing_year = None
    existing_image_url = None
    try:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT profile_data->'product'->>'year' as yr, profile_data->'product'->>'image_url' as img
                    FROM global_fragrances
                    WHERE brand ILIKE %s AND name ILIKE %s
                    LIMIT 1
                """, (brand, name))
                row = cur.fetchone()
                if row:
                    existing_year = row["yr"]
                    existing_image_url = row["img"]

                if not existing_year or not existing_image_url:
                    cur.execute("""
                        SELECT fragrance_data->>'year' as yr, fragrance_data->>'image_url' as img
                        FROM user_fragrances
                        WHERE fragrance_data->>'brand' ILIKE %s AND fragrance_data->>'name' ILIKE %s
                        LIMIT 1
                    """, (brand, name))
                    row = cur.fetchone()
                    if row:
                        if not existing_year:
                            existing_year = row["yr"]
                        if not existing_image_url:
                            existing_image_url = row["img"]
    except Exception as exc:
        print(f"  Warning: could not lookup existing year/image: {exc}")

    # 4. Fetch details
    print(f"  Fetching details from Fragrantica: {resolved_fg_url}")
    candidate = engine.UnifiedFragrance(
        name=name,
        brand=brand,
        year=str(existing_year) if existing_year else "",
        bn_url=resolved_bn_url or "",
        frag_url=resolved_fg_url
    )

    try:
        details = engine.fetch_selected_details(scraper, candidate, 15.0)
        if not details.frag_cards:
            print("  Fetched details carry no cards. Skipping.")
            return False
    except Exception as e:
        print(f"  Detail fetch failed: {e}")
        return False

    # Heal gender/year from the fetched cards, exactly like the worker does,
    # so the base-field backfill below has real values to write.
    engine.heal_missing_gender_and_year(candidate, details)
    healed_year = existing_year or candidate.year
    record_year = int(healed_year) if (healed_year and str(healed_year).isdigit()) else None
    record_concentration = _cheap_concentration(brand, name)

    # 5. Construct cache row
    raw_identity = {"name": name, "house": brand, "fg_url": resolved_fg_url, "bn_url": resolved_bn_url}
    if record_concentration != "Unknown":
        raw_identity["concentration"] = record_concentration

    cache_row = {
        "canonical_fg_url": canonical_fg_url,
        "name": name,
        "house": brand,
        "year": record_year,
        "gender": details.gender,
        "concentration": record_concentration if record_concentration != "Unknown" else None,
        "image_url": existing_image_url if existing_image_url else None,
        "schema_version": 1,
        "source": "manual_enrichment_script",
        "captured_at": db._now().isoformat(),
        "frag_cards": details.frag_cards,
        "notes": {
            "has_pyramid": details.notes.has_pyramid if details.notes else False,
            "top": list(details.notes.top) if details.notes and details.notes.top else [],
            "heart": list(details.notes.heart) if details.notes and details.notes.heart else [],
            "base": list(details.notes.base) if details.notes and details.notes.base else [],
            "flat": list(details.notes.flat) if details.notes and details.notes.flat else [],
        },
        "pros_cons": list(details.pros_cons) if details.pros_cons else [],
        "reviews": [{"text": r.text, "source": r.source} for r in details.reviews or []],
        "raw_identity": raw_identity,
        "quality_status": "complete" if _fg_metrics_complete(details) else "partial",
    }

    # 5. Complete the job (writes to fg_detail_cache & fragrance_records)
    print("  Completing job and saving cache records...")
    db.complete_job(job["id"], cache_row)

    # 6. Calculate derived metrics
    print("  Calculating derived metrics...")
    try:
        dm = build_derived_metrics(details)
    except Exception as e:
        print(f"  Derived metrics calculation failed: {e}")
        return False

    # 7. Write directly to user_fragrances / global_fragrances
    print("  Writing metrics directly to user_fragrances / global_fragrances...")
    DATABASE_URL = os.environ.get("DATABASE_URL")
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # Update user_fragrances
            if user_fragrance_id:
                cur.execute("SELECT fragrance_data FROM user_fragrances WHERE id = %s", (user_fragrance_id,))
                row = cur.fetchone()
                if row:
                    fdata = row["fragrance_data"]
                    fdata["derived_metrics"] = dm
                    fdata["source_url"] = resolved_fg_url
                    # Also update notes and accords to match completed profile
                    fdata["notes"] = cache_row["notes"]["flat"]
                    fdata["pyramid"] = cache_row["notes"]
                    _apply_derived_facts(fdata, dm)
                    _backfill_base_fields(fdata, gender=details.gender, year=record_year, brand=brand, name=name)
                    cur.execute("UPDATE user_fragrances SET fragrance_data = %s WHERE id = %s", (Json(fdata), user_fragrance_id))
                    print(f"  Updated user_fragrances row ID {user_fragrance_id}")
            else:
                # Update all user_fragrances matching brand/name
                cur.execute("SELECT id, fragrance_data FROM user_fragrances")
                rows = cur.fetchall()
                for r in rows:
                    fdata = r["fragrance_data"]
                    fb = fdata.get("brand", "")
                    fn = fdata.get("name", "")
                    if fb.lower() == brand.lower() and fn.lower() == name.lower():
                        fdata["derived_metrics"] = dm
                        fdata["source_url"] = resolved_fg_url
                        fdata["notes"] = cache_row["notes"]["flat"]
                        fdata["pyramid"] = cache_row["notes"]
                        _apply_derived_facts(fdata, dm)
                        _backfill_base_fields(fdata, gender=details.gender, year=record_year, brand=brand, name=name)
                        cur.execute("UPDATE user_fragrances SET fragrance_data = %s WHERE id = %s", (Json(fdata), r["id"]))
                        print(f"  Updated user_fragrances row ID {r['id']}")

            # Update global_fragrances
            if global_fragrance_id:
                cur.execute("SELECT profile_data FROM global_fragrances WHERE id = %s", (global_fragrance_id,))
                row = cur.fetchone()
                if row:
                    pdata = row["profile_data"]
                    pdata["derived_metrics"] = dm
                    pdata["source_url"] = resolved_fg_url
                    pdata["notes"] = cache_row["notes"]["flat"]
                    pdata["pyramid"] = cache_row["notes"]
                    _apply_derived_facts(pdata, dm)
                    _backfill_base_fields(pdata, gender=details.gender, year=record_year, brand=brand, name=name)
                    cur.execute("UPDATE global_fragrances SET profile_data = %s WHERE id = %s", (Json(pdata), global_fragrance_id))
                    print(f"  Updated global_fragrances row ID {global_fragrance_id}")
            else:
                # Update all global_fragrances matching brand/name
                cur.execute("SELECT id, profile_data FROM global_fragrances")
                rows = cur.fetchall()
                for r in rows:
                    pdata = r["profile_data"]
                    prod = pdata.get("product") or {}
                    gb = prod.get("brand", "")
                    gn = prod.get("name", "")
                    if gb.lower() == brand.lower() and gn.lower() == name.lower():
                        pdata["derived_metrics"] = dm
                        pdata["source_url"] = resolved_fg_url
                        pdata["notes"] = cache_row["notes"]["flat"]
                        pdata["pyramid"] = cache_row["notes"]
                        _apply_derived_facts(pdata, dm)
                        _backfill_base_fields(pdata, gender=details.gender, year=record_year, brand=brand, name=name)
                        cur.execute("UPDATE global_fragrances SET profile_data = %s WHERE id = %s", (Json(pdata), r["id"]))
                        print(f"  Updated global_fragrances row ID {r['id']}")
            conn.commit()

    print("  Done enriching.")
    return True

def main():
    init_connections()
    scraper = engine.get_scraper()
    args = engine.build_parser().parse_args([])
    if hasattr(args, "external_search"):
        args.external_search = False

    # Targeted enrichment list
    targets = [
        {
            "brand": "Creed",
            "name": "Bois Du Portugal",
            "fg_url": "https://www.fragrantica.com/perfume/Creed/Bois-du-Portugal-480.html",
            "bn_url": "https://basenotes.com/fragrances/bois-du-portugal-by-creed.26120233"
        },
        {
            "brand": "Tom Ford",
            "name": "Fucking Fabulous",
            "fg_url": "https://www.fragrantica.com/perfume/Tom-Ford/Fucking-Fabulous-46118.html",
            "bn_url": "https://basenotes.com/fragrances/fucking-fabulous-by-tom-ford.26153676"
        },
        {
            "brand": "Chanel",
            "name": "Les Exclusifs De Chanel N22",
            "fg_url": "https://www.fragrantica.com/perfume/Chanel/Les-Exclusifs-de-Chanel-No-22-Eau-de-Parfum-41775.html",
            "bn_url": "https://basenotes.com/fragrances/no-22-by-chanel.26120220"
        },
        {
            "brand": "Chanel",
            "name": "Coromandel",
            "fg_url": "https://www.fragrantica.com/perfume/Chanel/Coromandel-Eau-de-Parfum-41771.html",
            "bn_url": "https://basenotes.com/fragrances/coromandel-eau-de-parfum-by-chanel.26149451"
        }
    ]

    print("=== TARGETED ENRICHMENT START ===")
    for t in targets:
        try:
            enrich_fragrance(scraper, args, t["brand"], t["name"], t["fg_url"], t["bn_url"])
            time.sleep(2.0)
        except Exception as e:
            print(f"Error enriching targeted {t['brand']} - {t['name']}: {e}")

    # Verify La Depeche pour Homme
    print("\n=== GRACEFUL HANDLING OF FICTIONAL FRAGRANCE ===")
    try:
        resolved = enrich_fragrance(scraper, args, "La Depeche", "pour Homme")
        print(f"Enrichment result for La Depeche pour Homme: {resolved}")
    except Exception as e:
        print(f"Error handling La Depeche pour Homme: {e}")

    # General Scan: Check and fix the rest of the database
    print("\n=== GENERAL SCAN START ===")
    DATABASE_URL = os.environ.get("DATABASE_URL")
    incomplete = []

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # Scan user_fragrances
            cur.execute("SELECT id, fragrance_data FROM user_fragrances")
            rows = cur.fetchall()
            for r in rows:
                fdata = r["fragrance_data"]
                brand = fdata.get("brand")
                name = fdata.get("name")

                if brand and name and not _stored_metrics_complete(fdata):
                    fg_url, bn_url = _source_urls_from_payload(fdata)
                    incomplete.append({
                        "type": "user",
                        "id": r["id"],
                        "brand": brand,
                        "name": name,
                        "fg_url": fg_url,
                        "bn_url": bn_url
                    })

            # Scan global_fragrances catalog
            cur.execute("SELECT id, brand, name, profile_data FROM global_fragrances")
            rows = cur.fetchall()
            for r in rows:
                pdata = r["profile_data"]
                brand, name = _global_profile_identity(r)

                if brand and name and not _stored_metrics_complete(pdata):
                    fg_url, bn_url = _source_urls_from_payload(pdata)
                    incomplete.append({
                        "type": "global",
                        "id": r["id"],
                        "brand": brand,
                        "name": name,
                        "fg_url": fg_url,
                        "bn_url": bn_url
                    })

    print(f"Found {len(incomplete)} incomplete entries in wardrobe/catalog database.")

    # Process general scan with larger delays to avoid Cloudflare rate limiting
    for idx, inc in enumerate(incomplete, 1):
        print(f"[{idx}/{len(incomplete)}] processing general scan item: {inc['brand']} - {inc['name']}")
        try:
            enrich_fragrance(
                scraper, args,
                brand=inc["brand"],
                name=inc["name"],
                fg_url=inc["fg_url"],
                bn_url=inc["bn_url"],
                user_fragrance_id=inc["id"] if inc["type"] == "user" else None,
                global_fragrance_id=inc["id"] if inc["type"] == "global" else None
            )
            time.sleep(45.0)
        except Exception as e:
            print(f"Error in general scan item: {e}")

if __name__ == "__main__":
    main()
