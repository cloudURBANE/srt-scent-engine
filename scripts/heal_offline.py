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
                       by exact normalized brand+name (skips ambiguous rows), and
                       refresh the junk-filtered accords copy so the SPA card
                       drops scrape-leak accords even on rows already familied.

Every write is additive + merge-safe: metrics fill only NULLs, concentration
never overwrites an existing value, and the wardrobe family/concentration
projection only fills facts that are currently Unknown. The accords copy is the
one exception -- it is re-projected from the canonical engine top accords
whenever it differs (that is the scrape-leak repair), so it converges on the
clean list rather than only filling blanks. Re-running is a no-op once clean.
``--dry-run`` writes nothing.

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


def _env_file_database_url() -> str | None:
    """Return the DATABASE_URL configured in a checked-out .env, or None.

    This is the APP wardrobe DSN (the monorepo Supabase DB the phone reads). It
    is read WITHOUT mutating os.environ, so it can be compared against whatever
    DATABASE_URL the process was launched with (e.g. the viaduct engine DSN that
    `railway run -s lively-adaptation` injects)."""
    for candidate in (_REPO_ROOT / ".env", _REPO_ROOT.parent / "huge_monorepo" / ".env"):
        if not candidate.exists():
            continue
        for line in candidate.read_text(encoding="utf-8", errors="ignore").splitlines():
            m = re.match(r"\s*DATABASE_URL\s*=\s*(.*)\s*$", line)
            if m:
                val = m.group(1).strip().strip('"').strip("'")
                if val:
                    return val
    return None


def _mask_dsn(dsn: str) -> str:
    return re.sub(r"://[^@]+@", "://****@", dsn or "")[:72]


_ENGINE_DSN_PROBE: dict[str, bool] = {}


def _dsn_is_engine_cache(dsn: str) -> bool:
    """True iff ``dsn`` points at the ENGINE cache -- detected by the presence of
    a ``fragrance_records`` table, which the app wardrobe DB never has. Result is
    cached per-DSN. Fail-safe: any connection/probe error returns False so we
    never redirect on uncertainty (preserves the legacy 'write DATABASE_URL'
    behavior when we cannot prove the trap)."""
    if dsn in _ENGINE_DSN_PROBE:
        return _ENGINE_DSN_PROBE[dsn]
    result = False
    try:
        import psycopg as _pg  # local import: this runs well after module import
        with _pg.connect(dsn, connect_timeout=8) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.fragrance_records') IS NOT NULL")
                row = cur.fetchone()
                result = bool(row and row[0])
    except Exception as exc:  # noqa: BLE001
        print(f"  [wardrobe] engine-cache probe skipped: {exc}")
        result = False
    _ENGINE_DSN_PROBE[dsn] = result
    return result


_WARDROBE_DSN_CACHE: str | None = None


def _wardrobe_dsn() -> str:
    """DSN of the DB that holds the wardrobe tables (user_fragrances /
    global_fragrances) -- the APP database the phone reads.

    Defaults to DATABASE_URL, but SELF-CORRECTS the dual-DB trap: when a drain is
    launched via ``railway run -s lively-adaptation``, Railway injects the ENGINE
    service's DSN as DATABASE_URL (the viaduct cache, which carries its own STALE
    user_fragrances copy). Writing the wardrobe there silently no-ops for the app.
    So when DATABASE_URL is detected as the engine cache AND a distinct app DSN is
    configured (WARDROBE_DATABASE_URL, or DATABASE_URL in a checked-out .env), we
    route wardrobe writes to the app DB instead -- automatically, so the standard
    drain heals the phone with no operator ceremony. Engine reads/writes still
    target DATABASE_URL via the db module, unchanged. Cached after first resolve."""
    global _WARDROBE_DSN_CACHE
    if _WARDROBE_DSN_CACHE is not None:
        return _WARDROBE_DSN_CACHE

    current = os.environ["DATABASE_URL"]
    override = (os.environ.get("WARDROBE_DATABASE_URL") or "").strip()
    app_dsn = override or _env_file_database_url()

    resolved = current
    # Redirect ONLY in the unambiguous trap: a distinct app DSN exists and the
    # injected DATABASE_URL is provably the engine cache. Same DSN -> no-op (the
    # common local / heal_app_wardrobe case); probe failure -> keep DATABASE_URL.
    if app_dsn and app_dsn != current and _dsn_is_engine_cache(current):
        print(
            "  [wardrobe] DATABASE_URL is the engine cache; routing wardrobe writes to "
            f"the app DB {_mask_dsn(app_dsn)} (engine still read from DATABASE_URL)"
        )
        resolved = app_dsn
        # No need to set ENGINE_DATABASE_URL: DATABASE_URL is unchanged (still the
        # engine), so _all_records() already folds the live engine cache. Adding
        # the explicit fold path here would only re-scan the same viaduct rows.

    _WARDROBE_DSN_CACHE = resolved
    return resolved


import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Json  # noqa: E402

import db  # noqa: E402
import api  # noqa: E402  (reuse _details_from_fragrance_record -- single source of truth)
from derived_metrics_adapter import build_derived_metrics  # noqa: E402
from enrichment_facts import (  # noqa: E402
    derive_families,
    derive_wear_profile,
    is_fact_complete,
    primary_season_from_wear_profile,
    top_accords_from,
)
import enrich_concentration as conc  # noqa: E402

# enrich_database_metrics runs an import-time loader that UNCONDITIONALLY clobbers
# os.environ from huge_monorepo/.env -- intentional for its OWN standalone run
# (it writes user_fragrances metrics and must target the app DB), but a hidden
# side effect we must NOT inherit here. If it silently flips DATABASE_URL out from
# under us, db (already imported above, frozen on the injected engine DSN) and the
# wardrobe writer would point at different DBs by accident of import order. We pull
# in only the pure _apply_derived_facts helper, snapshotting/restoring DATABASE_URL
# so this module's DSN routing stays EXPLICIT (via _wardrobe_dsn, not a side effect).
_DSN_BEFORE_EDM_IMPORT = os.environ.get("DATABASE_URL")
from enrich_database_metrics import _apply_derived_facts  # noqa: E402
if _DSN_BEFORE_EDM_IMPORT is not None and os.environ.get("DATABASE_URL") != _DSN_BEFORE_EDM_IMPORT:
    os.environ["DATABASE_URL"] = _DSN_BEFORE_EDM_IMPORT


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


_GENERIC_BRAND_TOKENS = {
    "perfume", "perfumes", "parfum", "parfums", "fragrance", "fragrances",
    "beauty", "cosmetics", "industries", "industry", "llc", "ltd", "inc",
}


def _distinctive_brand_tokens(brand: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", str(brand or "").lower())
        if len(token) > 1 and token not in _GENERIC_BRAND_TOKENS
    }


def _strip_brand_tokens_from_name(brand: str, name: str) -> str:
    """Remove duplicated house tokens from a source name.

    Some fallback sources embed the house inside the perfume name, e.g.
    Parfumo's ``Ramz Lattafa (Gold)`` becomes ``Ramz Lattafa Gold`` while the
    wardrobe row is ``Lattafa | Ramz Gold``. This strips only distinctive whole
    brand tokens and refuses to return an empty/unchanged alias, keeping the
    projection match much narrower than fuzzy search scoring.
    """
    kill = _distinctive_brand_tokens(brand)
    if not kill:
        return ""
    kept = [
        token
        for token in re.findall(r"[a-z0-9]+", str(name or "").lower())
        if token not in kill
    ]
    if not kept:
        return ""
    alias = " ".join(kept)
    original = " ".join(re.findall(r"[a-z0-9]+", str(name or "").lower()))
    return alias if alias and alias != original else ""


# Numbered fragrances ("Chanel No 5", "Les Exclusifs de Chanel No 22") are spelled
# inconsistently across sources: the engine canonicalises "No 22" while a wardrobe
# row may store "N22" or "N°22". Those normalise to different exact keys
# ("...chaneln22" vs "...chanelno22") so the row never inherits the engine year.
# This collapses every "no/n/n°/nº/number <digits>" spelling to a single "no<digits>"
# token, on BOTH sides, so the variants resolve to one identity. Leading-only on the
# number word (a bare trailing digit like "212" is untouched), and the brand is still
# part of the key, so it can never merge two genuinely different fragrances.
_NUMBER_TOKEN_RE = re.compile(r"\bn(?:o|[°º]|umber)?\.?\s*(\d+)\b", re.IGNORECASE)


def _number_canonical(text: str) -> str:
    return _NUMBER_TOKEN_RE.sub(lambda m: f"no{m.group(1)}", str(text or ""))


def _number_norm_key(brand: str, name: str) -> str:
    """``_norm_key`` after collapsing number-word spellings (see _NUMBER_TOKEN_RE).

    Returns "" when it is identical to the plain ``_norm_key`` (no number token in
    the identity), so callers only ever register/look up a *distinct* bridging key."""
    plain = _norm_key(brand, name)
    bridged = _norm_key(_number_canonical(brand), _number_canonical(name))
    return bridged if bridged != plain else ""


def _identity_keys(brand: str, name: str) -> list[str]:
    keys = [_norm_key(brand, name)]
    alias = _strip_brand_tokens_from_name(brand, name)
    if alias:
        keys.append(_norm_key(brand, alias))
    number_key = _number_norm_key(brand, name)
    if number_key:
        keys.append(number_key)
        if alias:
            alias_number = _number_norm_key(brand, alias)
            if alias_number:
                keys.append(alias_number)
    return [key for key in dict.fromkeys(keys) if key]


# Gender-label phrases that some Fragrantica pages GLUE onto a name as a LEADING
# token: the men's "Versace Dylan Blue" is cached as "Pour Homme Dylan Blue".
# Only a *leading* occurrence is a label artifact -- a TRAILING one ("Eros Pour
# Femme") denotes a genuine gender flanker, a distinct product, and must never be
# stripped or the men's and women's editions would collide. Ordered longest-first
# so "pour homme" matches before a bare "homme". The implied gender is captured
# so the wardrobe row can inherit it too.
_LEADING_GENDER_LABELS: tuple[tuple[str, str], ...] = (
    ("pour homme", "Men"),
    ("pour femme", "Women"),
    ("for men", "Men"),
    ("for women", "Women"),
    ("for him", "Men"),
    ("for her", "Women"),
    ("homme", "Men"),
    ("femme", "Women"),
)


def _strip_leading_gender(name: str) -> tuple[str, str] | None:
    """If ``name`` starts with a glued gender label, return ``(rest, gender)``;
    else ``None``. Leading-only on purpose (see _LEADING_GENDER_LABELS): this is
    what lets the abbreviated wardrobe "Dylan Blue" inherit the canonical engine
    record cached as "Pour Homme Dylan Blue" *without* ever collapsing a real
    trailing-gender flanker into its base."""
    low = re.sub(r"\s+", " ", str(name or "").strip().lower())
    for label, gender in _LEADING_GENDER_LABELS:
        if low.startswith(label + " "):
            rest = low[len(label):].strip()
            if rest:
                return rest, gender
    return None


def _core_match(
    core_index: dict[str, list[dict[str, Any]]], lookup_key: str
) -> dict[str, Any] | None:
    """Resolve a wardrobe identity to its canonical engine entry via the
    gender-label-stripped core index, but ONLY when the match is unambiguous.

    ``core_index`` maps a stripped-core norm key -> the engine entries whose name
    carried a leading gender label. If two distinct fragrances share a core (e.g.
    a record cached as "Pour Homme X" *and* one as "Pour Femme X"), we refuse to
    guess and return None -- so a heal never projects the wrong gender/year."""
    cands = core_index.get(lookup_key) or []
    unique = {id(e): e for e in cands}
    return next(iter(unique.values())) if len(unique) == 1 else None


def _str_accords(value: Any) -> list[str]:
    """Normalize a stored accords copy to a flat list of trimmed strings.

    The wardrobe accords copy can arrive as a list of plain strings (engine
    projection) or, on older rows, a list of ``{"accord": ...}`` dicts. Coercing
    both to the same shape lets us compare against the clean engine top accords
    and skip a no-op write when nothing actually changed."""
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, dict):
            item = item.get("accord") or item.get("name") or ""
        s = str(item or "").strip()
        if s:
            out.append(s)
    return out


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


def _record_year(record: dict[str, Any]) -> str | None:
    """First engine-complete year for a record (column, then fg raw_identity)."""
    fg_raw = record.get("fg_raw") or {}
    raw_identity = fg_raw.get("raw_identity") or {}
    for value in (record.get("year"), raw_identity.get("year")):
        text = str(value or "").strip()
        if is_fact_complete(text, "year"):
            return text
    return None


def _record_gender(record: dict[str, Any]) -> str | None:
    """First engine-complete gender for a record (column, then fg raw_identity)."""
    fg_raw = record.get("fg_raw") or {}
    raw_identity = fg_raw.get("raw_identity") or {}
    for value in (record.get("gender"), raw_identity.get("gender")):
        text = str(value or "").strip()
        if is_fact_complete(text, "gender"):
            return text
    return None


def _clean_image_url(value: Any) -> str | None:
    text = str(value or "").strip()
    return text if text.startswith(("http://", "https://")) else None


def _payload_image_url(payload: dict[str, Any]) -> str | None:
    product = payload.get("product") if isinstance(payload.get("product"), dict) else {}
    for value in (
        payload.get("image_url"),
        payload.get("imageUrl"),
        product.get("image_url"),
        product.get("imageUrl"),
    ):
        if cleaned := _clean_image_url(value):
            return cleaned
    return None


def _record_image_url(record: dict[str, Any]) -> str | None:
    """First usable bottle image for a fragrance_records-shaped row."""
    fg_raw = record.get("fg_raw") or {}
    raw_identity = fg_raw.get("raw_identity") or {}
    for value in (
        record.get("image_url"),
        raw_identity.get("image_url"),
        raw_identity.get("imageUrl"),
        fg_raw.get("image_url"),
        fg_raw.get("imageUrl"),
    ):
        if cleaned := _clean_image_url(value):
            return cleaned
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
def _new_entry() -> dict[str, Any]:
    return {
        "derived_metrics": None,
        "concentration": None,
        "year": None,
        "gender": None,
        "image_url": None,
        "core": None,           # gender-label-stripped norm key (or None)
        "implied_gender": None,  # gender read off a stripped leading label
    }


def _register_core(entry: dict[str, Any], brand: str, name: str) -> None:
    """Record the gender-label-stripped core for an entry whose name carries a
    leading gender label, so an abbreviated wardrobe row can resolve to it. Also
    captures the implied gender when the entry has none. Idempotent: the norm key
    is unique per (brand, name), so every fold of a key sees the same name."""
    if entry.get("core"):
        return
    stripped = _strip_leading_gender(name)
    if not stripped:
        return
    rest, gender = stripped
    entry["core"] = _norm_key(brand, rest)
    if entry.get("implied_gender") is None and is_fact_complete(gender, "gender"):
        entry["implied_gender"] = gender


def _fold_record(index: dict[str, dict[str, Any]], record: dict[str, Any]) -> None:
    """Gap-fill an index entry from a fragrance_records-shaped row (engine cache).

    Only fills slots still None, so the *first* source for a given identity wins
    -- callers fold the highest-trust source (the local engine cache) first."""
    brand, name = str(record.get("house") or ""), str(record.get("name") or "")
    keys = _identity_keys(brand, name)
    if not keys:
        return
    key = keys[0]
    entry = index.setdefault(key, _new_entry())
    for alias_key in keys[1:]:
        index.setdefault(alias_key, entry)
    if entry["derived_metrics"] is None and record.get("derived_metrics") is not None:
        entry["derived_metrics"] = record["derived_metrics"]
    if entry["concentration"] is None:
        entry["concentration"] = _record_concentration(record)
    if entry["year"] is None:
        entry["year"] = _record_year(record)
    if entry["gender"] is None:
        entry["gender"] = _record_gender(record)
    if entry["image_url"] is None:
        entry["image_url"] = _record_image_url(record)
    _register_core(entry, brand, name)


def _fold_global_fragrances(index: dict[str, dict[str, Any]]) -> int:
    """Fold the wardrobe's own ``global_fragrances`` catalog into the index as a
    *supplementary* fact source. This is what makes the heal self-sufficient on
    one DB: the canonical 2016 for "Versace Pour Homme Dylan Blue" lives in
    global_fragrances even when fragrance_records (the engine cache copy) lacks
    it, so the abbreviated user_fragrances "Dylan Blue" row can still inherit it
    via the lenient match. Reads from the wardrobe DSN (where the catalog lives).
    Gap-fill only -- never overrides a fragrance_records value already folded."""
    folded = 0
    try:
        with psycopg.connect(_wardrobe_dsn(), row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(brand, profile_data->>'brand') b,"
                    " COALESCE(name, profile_data->>'name') n, profile_data p"
                    " FROM global_fragrances"
                )
                rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        print(f"  [index] global_fragrances fold skipped: {exc}")
        return 0
    for row in rows:
        brand, name = str(row.get("b") or ""), str(row.get("n") or "")
        keys = _identity_keys(brand, name)
        if not keys:
            continue
        key = keys[0]
        pdata = row.get("p") if isinstance(row.get("p"), dict) else {}
        entry = index.setdefault(key, _new_entry())
        for alias_key in keys[1:]:
            index.setdefault(alias_key, entry)
        if entry["year"] is None and is_fact_complete(str(pdata.get("year") or ""), "year"):
            entry["year"] = str(pdata.get("year")).strip()
            folded += 1
        if entry["gender"] is None and is_fact_complete(str(pdata.get("gender") or ""), "gender"):
            entry["gender"] = str(pdata.get("gender")).strip()
            folded += 1
        if entry["concentration"] is None and is_fact_complete(
            str(pdata.get("concentration") or ""), "concentration"
        ):
            entry["concentration"] = str(pdata.get("concentration")).strip()
            folded += 1
        if entry["image_url"] is None:
            entry["image_url"] = _payload_image_url(pdata)
            if entry["image_url"]:
                folded += 1
        _register_core(entry, brand, name)
    return folded


def _fold_engine_database(index: dict[str, dict[str, Any]]) -> int:
    """Fold a dedicated engine DB (ENGINE_DATABASE_URL, e.g. the viaduct cache)
    into the index when configured and distinct from the wardrobe DSN. This is
    the deterministic dual-DSN path: facts that live ONLY in the live engine
    (never copied into the wardrobe DB's fragrance_records) become projectable
    while writes still target the wardrobe DSN. Gap-fill only; no-op when unset."""
    engine_dsn = (os.environ.get("ENGINE_DATABASE_URL") or "").strip()
    if not engine_dsn or engine_dsn == _wardrobe_dsn():
        return 0
    try:
        with psycopg.connect(engine_dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT house, name, year, gender, image_url, derived_metrics_json,"
                    " fg_raw_json FROM fragrance_records"
                )
                rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        print(f"  [index] ENGINE_DATABASE_URL fold skipped: {exc}")
        return 0
    before = sum(1 for e in index.values() if e["year"] or e["gender"] or e["concentration"])
    for row in rows:
        _fold_record(
            index,
            {
                "house": row.get("house"),
                "name": row.get("name"),
                "year": row.get("year"),
                "gender": row.get("gender"),
                "image_url": row.get("image_url"),
                "derived_metrics": row.get("derived_metrics_json"),
                "fg_raw": row.get("fg_raw_json") or {},
            },
        )
    after = sum(1 for e in index.values() if e["year"] or e["gender"] or e["concentration"])
    return after - before


def _build_engine_index(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Build the projection index: normalized brand+name ->
    {derived_metrics, concentration, year, gender, core, implied_gender}.

    Sourced (in trust order, gap-fill) from:
      1. fragrance_records via the db module (the freshest local engine cache -- a
         re-read is intentional since the metrics/concentration steps may have
         just filled values ``records`` predates),
      2. ENGINE_DATABASE_URL fragrance_records when configured (the live engine
         cache, e.g. viaduct, for facts never copied locally), and
      3. the wardrobe's own global_fragrances catalog (so the canonical full-name
         record can heal an abbreviated user_fragrances row on a single DB).
    ``core`` powers the gender-label-stripped match (see _strip_leading_gender)."""
    index: dict[str, dict[str, Any]] = {}
    for record in _all_records():
        _fold_record(index, record)
    eng = _fold_engine_database(index)
    gf = _fold_global_fragrances(index)
    if eng or gf:
        print(f"  [index] supplementary facts folded: engine_db+{eng} global_fragrances+{gf}")
    return index


def _build_core_index(
    index: dict[str, dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    """Group index entries that carry a stripped gender-label core, keyed by that
    core. An abbreviated wardrobe identity ("versacedylanblue") looks itself up
    here to find the canonical record cached as "Pour Homme Dylan Blue"; multiple
    entries under one core signal a real men/women ambiguity (see _core_match)."""
    core_index: dict[str, list[dict[str, Any]]] = {}
    for entry in index.values():
        core = entry.get("core")
        if core:
            core_index.setdefault(core, []).append(entry)
    return core_index


def _wear_profile_complete(value: Any) -> bool:
    if not isinstance(value, dict) or not value:
        return False
    return bool(
        primary_season_from_wear_profile(value)
        or is_fact_complete(value.get("primary_time"), "season")
    )


_DERIVED_METRIC_GROUPS = (
    "performance_score",
    "value_score",
    "community_interest_score",
    "wear_profile",
    "main_accords",
    "notes",
)


def _metric_group_complete(value: Any, group: str) -> bool:
    if group == "wear_profile":
        return _wear_profile_complete(value)
    if group == "main_accords":
        return bool(top_accords_from({"derived_metrics": {"main_accords": value}}))
    if isinstance(value, dict):
        return bool(value)
    return is_fact_complete(value)


def _merge_derived_metrics(
    existing: Any, incoming: Any
) -> tuple[dict[str, Any] | None, bool]:
    """Add missing engine metric-card groups to a wardrobe payload.

    The phone reads score-card data from ``fragrance_data.derived_metrics``.
    Engine recompute fills ``fragrance_records.derived_metrics_json``, but the
    wardrobe projection used to copy only flattened facts, leaving rows with
    blank score cards even when the engine already had the canonical blob.

    Merge fill-only: existing populated groups win, missing groups are copied
    from the engine, and source_coverage is OR-merged so coverage never moves
    backwards. This keeps the projection idempotent and avoids downgrading rows
    that already carry app-visible metric data.
    """
    if not isinstance(incoming, dict) or not incoming:
        return None, False
    if not isinstance(existing, dict) or not existing:
        return dict(incoming), True

    merged = dict(existing)
    changed = False
    for group in _DERIVED_METRIC_GROUPS:
        inc_value = incoming.get(group)
        if not _metric_group_complete(inc_value, group):
            continue
        current_value = merged.get(group)
        if not _metric_group_complete(current_value, group):
            merged[group] = inc_value
            changed = True

    existing_cov = existing.get("source_coverage")
    incoming_cov = incoming.get("source_coverage")
    if isinstance(incoming_cov, dict):
        merged_cov = dict(existing_cov) if isinstance(existing_cov, dict) else {}
        for key, value in incoming_cov.items():
            if value and not merged_cov.get(key):
                merged_cov[key] = value
                changed = True
        if merged_cov:
            merged["source_coverage"] = merged_cov

    return (merged if changed else existing), changed


def _notes_dict_complete(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    for key in ("top", "heart", "base", "flat"):
        items = value.get(key)
        if isinstance(items, list) and any(str(item or "").strip() for item in items):
            return True
    return False


def _payload_notes_complete(value: Any) -> bool:
    if isinstance(value, list):
        return any(str(item or "").strip() for item in value)
    return _notes_dict_complete(value)


def _flat_notes_from(notes: dict[str, Any]) -> list[str]:
    flat = notes.get("flat")
    if isinstance(flat, list) and any(str(item or "").strip() for item in flat):
        return [str(item).strip() for item in flat if str(item or "").strip()]
    out: list[str] = []
    for key in ("top", "heart", "base"):
        items = notes.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            text = str(item or "").strip()
            if text and text not in out:
                out.append(text)
    return out


def _project_notes(payload: dict[str, Any], derived_metrics: dict[str, Any]) -> bool:
    notes = derived_metrics.get("notes")
    if not _notes_dict_complete(notes):
        return False
    assert isinstance(notes, dict)
    changed = False
    if not _payload_notes_complete(payload.get("notes")):
        flat = _flat_notes_from(notes)
        if flat:
            payload["notes"] = flat
            changed = True
    if not _notes_dict_complete(payload.get("pyramid")):
        payload["pyramid"] = {
            "has_pyramid": bool(notes.get("has_pyramid", False)),
            "top": list(notes.get("top", []) or []),
            "heart": list(notes.get("heart", []) or []),
            "base": list(notes.get("base", []) or []),
            "flat": list(notes.get("flat", []) or []),
        }
        changed = True
    return changed


def _project_wear_facts(payload: dict[str, Any], derived_metrics: dict[str, Any]) -> bool:
    """Fill missing wardrobe wear/season context from engine metrics.

    Family projection is intentionally conservative, but wear_profile and season
    are independent derived facts. A row whose family is already set should not
    stay stuck with missing/default wear context forever.
    """
    wear_profile = derive_wear_profile(derived_metrics)
    # Only project a wear_profile that is itself complete. Fragrantica emits a
    # truthy-but-empty profile (primary_seasons/primary_time = None) for cards
    # with zero community votes; projecting that fills nothing useful AND can
    # never satisfy the _wear_profile_complete write guard below, so the row
    # would report changed=True on every drain forever (phantom heal churn).
    if not _wear_profile_complete(wear_profile):
        return False

    changed = False
    if not _wear_profile_complete(payload.get("wear_profile")):
        payload["wear_profile"] = wear_profile
        changed = True

    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    if not _wear_profile_complete(context.get("wear_profile")):
        context["wear_profile"] = wear_profile
        payload["context"] = context
        changed = True

    primary_season = primary_season_from_wear_profile(wear_profile)
    if primary_season and not is_fact_complete(payload.get("season"), "season"):
        payload["season"] = primary_season
        changed = True

    return changed


def project_engine_facts(
    payload: dict[str, Any], entry: dict[str, Any]
) -> tuple[bool, bool, bool, bool, bool, bool, bool, bool, bool]:
    """Project one engine-cache entry's facts onto a wardrobe payload, in place.

    Returns ``(family_filled, concentration_filled, accords_refreshed,
    wear_filled, derived_metrics_filled, notes_filled, year_filled, gender_filled,
    image_filled)``. Pure apart from mutating ``payload`` -- no DB, so it is
    unit-testable. ``entry`` is
    ``{"derived_metrics": <blob|None>, "concentration": <str|None>,
    "year": <str|None>, "gender": <str|None>, "image_url": <str|None>}``.
    """
    fam_filled = conc_filled = acc_refreshed = wear_filled = dm_filled = notes_filled = year_filled = gender_filled = image_filled = False
    dm = entry.get("derived_metrics")
    if dm is not None:
        merged_dm, dm_filled = _merge_derived_metrics(payload.get("derived_metrics"), dm)
        if dm_filled and merged_dm is not None:
            payload["derived_metrics"] = merged_dm
        if isinstance(dm, dict):
            notes_filled = _project_notes(payload, dm)
        # Always re-project the junk-filtered top accords. The scrape leak
        # (vote counts / Hate-Like / seasons) poisons the wardrobe accords copy
        # too, and a row whose family is *already* set never enters the family
        # branch below -- so without this, the SPA card keeps showing the stale
        # junk accords even after the engine DB is repaired.
        clean = top_accords_from(dm)
        if clean and clean != _str_accords(payload.get("accords")):
            payload["accords"] = clean
            acc_refreshed = True
        wear_filled = _project_wear_facts(payload, dm)
        # Family projection only when family is still Unknown (unchanged
        # behavior -- never downgrades an existing family).
        if not is_fact_complete(payload.get("family"), "family"):
            before = payload.get("family")
            _apply_derived_facts(payload, dm)
            fam_filled = payload.get("family") != before and is_fact_complete(
                payload.get("family"), "family"
            )
    if not is_fact_complete(payload.get("concentration"), "concentration") and entry.get("concentration"):
        payload["concentration"] = entry["concentration"]
        payload.setdefault("concentration_meta", {"source": "engine_cache_projection"})
        conc_filled = True
    # Year + gender are stored on the engine record (not derived_metrics), so the
    # family/accords/concentration projection above never carried them -- which is
    # why a wardrobe row could stay yearless no matter how many drains ran. Fill
    # them from the engine only when the wardrobe value is still missing; never
    # downgrade an existing value.
    if not is_fact_complete(payload.get("year"), "year") and is_fact_complete(entry.get("year"), "year"):
        payload["year"] = entry["year"]
        year_filled = True
    if not is_fact_complete(payload.get("gender"), "gender") and is_fact_complete(entry.get("gender"), "gender"):
        payload["gender"] = entry["gender"]
        gender_filled = True
    image_url = _clean_image_url(entry.get("image_url"))
    if not _payload_image_url(payload) and image_url:
        payload["image_url"] = image_url
        image_filled = True
    return fam_filled, conc_filled, acc_refreshed, wear_filled, dm_filled, notes_filled, year_filled, gender_filled, image_filled


def fill_core_scalars(
    payload: dict[str, Any], entry: dict[str, Any]
) -> tuple[bool, bool, bool, bool]:
    """Fill ONLY the scalar identity facts (concentration, year, gender) from a
    gender-label-stripped core match -- the conservative subset that is safe to
    inherit across the "Dylan Blue" <-> "Pour Homme Dylan Blue" name gap. Returns
    ``(conc_filled, year_filled, gender_filled, image_filled)``. Gap-fill only;
    never downgrades an existing value. Derived metrics / accords are
    deliberately NOT projected here (those come only from an exact identity
    match)."""
    conc_filled = year_filled = gender_filled = image_filled = False
    if not is_fact_complete(payload.get("concentration"), "concentration") and is_fact_complete(
        entry.get("concentration"), "concentration"
    ):
        payload["concentration"] = entry["concentration"]
        payload.setdefault("concentration_meta", {"source": "engine_cache_projection"})
        conc_filled = True
    if not is_fact_complete(payload.get("year"), "year") and is_fact_complete(entry.get("year"), "year"):
        payload["year"] = entry["year"]
        year_filled = True
    gender_val = entry.get("gender") if is_fact_complete(entry.get("gender"), "gender") else entry.get("implied_gender")
    if not is_fact_complete(payload.get("gender"), "gender") and is_fact_complete(gender_val, "gender"):
        payload["gender"] = gender_val
        gender_filled = True
    image_url = _clean_image_url(entry.get("image_url"))
    if not _payload_image_url(payload) and image_url:
        payload["image_url"] = image_url
        image_filled = True
    return conc_filled, year_filled, gender_filled, image_filled


def heal_wardrobe(
    index: dict[str, dict[str, Any]],
    *,
    only_keys: set[str] | None,
    dry_run: bool,
) -> dict[str, int]:
    dsn = _wardrobe_dsn()
    core_index = _build_core_index(index)
    stats = {
        "uf_family": 0, "uf_conc": 0, "uf_acc": 0, "uf_wear": 0, "uf_dm": 0, "uf_notes": 0,
        "uf_year": 0, "uf_gender": 0, "uf_image": 0,
        "gf_family": 0, "gf_conc": 0, "gf_acc": 0, "gf_wear": 0, "gf_dm": 0, "gf_notes": 0,
        "gf_year": 0, "gf_gender": 0, "gf_image": 0,
        "no_match": 0, "core_match": 0,
    }

    def _project(
        payload: dict[str, Any], norm: str, brand: str, name: str
    ) -> tuple[bool, bool, bool, bool, bool, bool, bool, bool, bool]:
        """Returns (family, concentration, accords, wear, dm, notes, year, gender, image) flags."""
        entry = index.get(norm)
        # Bridge inconsistent number spellings ("N22" <-> "No 22") to the canonical
        # engine identity (see _NUMBER_TOKEN_RE). This is a *secondary* gap-fill
        # source, not just a miss-fallback: a numbered wardrobe row often already
        # "matches" its own (equally yearless) global_fragrances self-copy under the
        # exact norm, so the real engine record -- indexed under the number key --
        # must still be consulted to fill what that empty entry can't.
        number_key = _number_norm_key(brand, name)
        number_entry = index.get(number_key) if number_key else None
        fam = cc = acc = wear = dm = notes = yr = gen = img = False
        sources: list[dict[str, Any]] = []
        for src in (entry, number_entry):
            if src is not None and not any(src is s for s in sources):
                sources.append(src)
        for src in sources:
            f2, c2, a2, w2, d2, n2, y2, g2, i2 = project_engine_facts(payload, src)
            fam, cc, acc, wear, dm, notes, yr, gen, img = (
                fam or f2, cc or c2, acc or a2, wear or w2, dm or d2, notes or n2,
                yr or y2, gen or g2, img or i2,
            )
        # If scalar facts are still missing, resolve the abbreviated wardrobe name
        # to its canonical record via the gender-label-stripped core (the "Dylan
        # Blue" -> "Pour Homme Dylan Blue" heal). The wardrobe's own norm is the
        # lookup key, since the canonical record indexes under that stripped core.
        # Unambiguous matches only (see _core_match); skip the entry we just used.
        if not (
            is_fact_complete(payload.get("year"), "year")
            and is_fact_complete(payload.get("gender"), "gender")
            and is_fact_complete(payload.get("concentration"), "concentration")
        ):
            stripped = _strip_leading_gender(name)
            lookup = _norm_key(brand, stripped[0]) if stripped else norm
            core_entry = _core_match(core_index, lookup)
            if core_entry is not None and not any(core_entry is s for s in sources):
                c2, y2, g2, i2 = fill_core_scalars(payload, core_entry)
                if c2 or y2 or g2 or i2:
                    stats["core_match"] += 1
                cc, yr, gen, img = cc or c2, yr or y2, gen or g2, img or i2
        if not sources and not (cc or yr or gen or img):
            stats["no_match"] += 1
        return fam, cc, acc, wear, dm, notes, yr, gen, img

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        # user_fragrances
        with conn.cursor() as cur:
            cur.execute("SELECT id, fragrance_data FROM user_fragrances")
            uf_rows = cur.fetchall()
        for row in uf_rows:
            fdata = row["fragrance_data"] or {}
            if not isinstance(fdata, dict):
                continue
            brand_uf = str(fdata.get("brand") or "")
            name_uf = str(fdata.get("name") or "")
            norm = _norm_key(brand_uf, name_uf)
            if only_keys is not None and norm not in only_keys:
                continue
            fam, cc, acc, wear, dm, notes, yr, gen, img = _project(fdata, norm, brand_uf, name_uf)
            if fam:
                stats["uf_family"] += 1
            if cc:
                stats["uf_conc"] += 1
            if acc:
                stats["uf_acc"] += 1
            if wear:
                stats["uf_wear"] += 1
            if dm:
                stats["uf_dm"] += 1
            if notes:
                stats["uf_notes"] += 1
            if yr:
                stats["uf_year"] += 1
            if gen:
                stats["uf_gender"] += 1
            if img:
                stats["uf_image"] += 1
            if (fam or cc or acc or wear or dm or notes or yr or gen or img) and not dry_run:
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
            fam, cc, acc, wear, dm, notes, yr, gen, img = _project(pdata, norm, str(brand), str(name))
            if fam:
                stats["gf_family"] += 1
            if cc:
                stats["gf_conc"] += 1
            if acc:
                stats["gf_acc"] += 1
            if wear:
                stats["gf_wear"] += 1
            if dm:
                stats["gf_dm"] += 1
            if notes:
                stats["gf_notes"] += 1
            if yr:
                stats["gf_year"] += 1
            if gen:
                stats["gf_gender"] += 1
            if img:
                stats["gf_image"] += 1
            if (fam or cc or acc or wear or dm or notes or yr or gen or img) and not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE global_fragrances SET profile_data = %s WHERE id = %s",
                        (Json(pdata), row["id"]),
                    )
        if not dry_run:
            conn.commit()
    print(
        f"wardrobe: user_fragrances[family+{stats['uf_family']} conc+{stats['uf_conc']} "
        f"accords~{stats['uf_acc']} wear+{stats['uf_wear']} dm+{stats['uf_dm']} notes+{stats['uf_notes']} "
        f"year+{stats['uf_year']} gender+{stats['uf_gender']} "
        f"image+{stats['uf_image']}] "
        f"global_fragrances[family+{stats['gf_family']} conc+{stats['gf_conc']} "
        f"accords~{stats['gf_acc']} wear+{stats['gf_wear']} dm+{stats['gf_dm']} notes+{stats['gf_notes']} "
        f"year+{stats['gf_year']} gender+{stats['gf_gender']} "
        f"image+{stats['gf_image']}] "
        f"core_match={stats['core_match']} no_engine_match={stats['no_match']} dry_run={dry_run}"
    )
    return stats


# --------------------------------------------------------------------------
# Step 4 -- seed (queue wardrobe fragrances absent from the engine cache)
# --------------------------------------------------------------------------
# Job statuses the queue will never re-act on. A `completed` job means the
# worker DID scrape the fragrance -- the engine has it, almost always under a
# canonical name the exact-match coverage check can't see ("Dylan Blue" stored
# as "Pour Homme Dylan Blue"; "Initio" as "Initio Parfums Prives"). An `ignored`
# job was deliberately retired. enqueue_job_state never resurrects either, so a
# candidate in one of these states is unscrapeable churn, not a real gap.
_TERMINAL_JOB_STATES = frozenset({"completed", "ignored"})


def _drop_terminal_jobs(
    candidates: dict[str, tuple[str, str]],
    states: dict[str, dict[str, Any]],
) -> int:
    """Remove from ``candidates`` (in place) every job_key whose existing job is
    in a terminal state, returning how many were dropped. Keeps only rows the
    queue can still act on (no job yet, or a failed/pending one)."""
    dropped = 0
    for key, st in states.items():
        if (st.get("status") or "") in _TERMINAL_JOB_STATES and key in candidates:
            del candidates[key]
            dropped += 1
    return dropped


def seed_missing_wardrobe(
    index: dict[str, dict[str, Any]], *, dry_run: bool, include_catalog: bool = True
) -> dict[str, int]:
    """Enqueue an enrichment job for every wardrobe fragrance that has NO engine
    record yet, so the live worker (residential IP) can scrape it.

    Offline heal + projection can only fix wardrobe rows that already exist in
    fragrance_records. The rest (e.g. Creed Royal Oud, Chanel Coco) were never
    scraped, so projection can't help -- they must be resolved + parsed by the
    worker, which on /complete persists family + concentration. This step keys
    each job exactly like api._enqueue_enrichment_job (brand|name identity slug)
    so it dedupes with the queue the SPA's /details path writes.

    Coverage is decided by the *scraped engine cache* (fragrance_records) ONLY --
    NOT the full projection index. ``index`` also folds the global_fragrances
    catalog, but a catalog entry is not a scraped record: a wardrobe row whose
    only match is an incomplete catalog row has no facts to inherit and still
    needs the worker, so it must NOT be treated as covered here.

    ``include_catalog`` controls breadth: when False, only the user's own
    user_fragrances rows are candidates (the global_fragrances catalog -- which
    holds entries no user owns -- is skipped). Default True keeps the original
    cache-population behavior."""
    # Enumerate the APP wardrobe (the phone's DB), not whatever DATABASE_URL was
    # injected -- under `railway run` that is the engine cache's STALE wardrobe
    # copy, so seeding off it would chase the wrong missing-rows set. Scraped
    # coverage is still decided by the engine cache (_all_records reads the engine
    # via DATABASE_URL); only the wardrobe enumeration moves to the app DB.
    dsn = _wardrobe_dsn()
    scraped_keys = {
        key
        for r in _all_records()
        for key in _identity_keys(str(r.get("house") or ""), str(r.get("name") or ""))
    }
    candidates: dict[str, tuple[str, str]] = {}
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT fragrance_data->>'brand' b, fragrance_data->>'name' n FROM user_fragrances")
            rows = cur.fetchall()
            if include_catalog:
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
        if _norm_key(brand, name) in scraped_keys:
            continue  # already in the scraped engine cache -- worker has parsed it
        selected = api.engine.UnifiedFragrance(name=name, brand=brand, year="")
        job_key = api._identity_job_key(selected)
        if job_key:
            candidates.setdefault(job_key, (brand, name))

    # Drop rows whose IDENTITY JOB already reached a terminal state (see
    # _drop_terminal_jobs): a `completed`/`ignored` job is a guaranteed
    # enqueue_job_state no-op, so re-seeding them is the churn that reports them
    # "missing" forever. Keep the status split visible because "completed"
    # usually means "scraped under a canonical name", while "ignored" means the
    # worker deliberately retired it and projection may still have no engine row.
    terminal_counts = {"completed": 0, "ignored": 0}
    if candidates:
        states = db.get_jobs_by_keys(list(candidates))
        for key, st in states.items():
            status = st.get("status") or ""
            if key in candidates and status in terminal_counts:
                terminal_counts[status] += 1
        _drop_terminal_jobs(candidates, states)
    if terminal_counts["completed"]:
        print(
            f"seed: {terminal_counts['completed']} wardrobe row(s) already have "
            "completed identity jobs (likely scraped under a canonical name) -- not re-enqueued"
        )
    if terminal_counts["ignored"]:
        print(
            f"seed: {terminal_counts['ignored']} wardrobe row(s) have ignored identity jobs "
            "(retired/unresolvable; inspect before forcing a retry) -- not re-enqueued"
        )
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
        "--seed-user-only",
        action="store_true",
        help="restrict seed to the user's own user_fragrances rows; skip the "
        "global_fragrances catalog entries no user owns (only affects the seed step)",
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
            seed_missing_wardrobe(
                _build_engine_index(all_records),
                dry_run=args.dry_run,
                include_catalog=not args.seed_user_only,
            )
    print("=== done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
