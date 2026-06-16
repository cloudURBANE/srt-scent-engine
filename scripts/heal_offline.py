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


def _wardrobe_dsn() -> str:
    """DSN of the DB that holds the wardrobe tables (user_fragrances /
    global_fragrances) -- always DATABASE_URL. Engine facts may be read from a
    separate ENGINE_DATABASE_URL, but every wardrobe write targets this one."""
    return os.environ["DATABASE_URL"]


import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Json  # noqa: E402

import db  # noqa: E402
import api  # noqa: E402  (reuse _details_from_fragrance_record -- single source of truth)
from derived_metrics_adapter import build_derived_metrics  # noqa: E402
from enrichment_facts import derive_families, is_fact_complete, top_accords_from  # noqa: E402
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
    key = _norm_key(brand, name)
    if not key:
        return
    entry = index.setdefault(key, _new_entry())
    if entry["derived_metrics"] is None and record.get("derived_metrics") is not None:
        entry["derived_metrics"] = record["derived_metrics"]
    if entry["concentration"] is None:
        entry["concentration"] = _record_concentration(record)
    if entry["year"] is None:
        entry["year"] = _record_year(record)
    if entry["gender"] is None:
        entry["gender"] = _record_gender(record)
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
        key = _norm_key(brand, name)
        if not key:
            continue
        pdata = row.get("p") if isinstance(row.get("p"), dict) else {}
        entry = index.setdefault(key, _new_entry())
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
                    "SELECT house, name, year, gender, derived_metrics_json,"
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


def project_engine_facts(
    payload: dict[str, Any], entry: dict[str, Any]
) -> tuple[bool, bool, bool, bool, bool]:
    """Project one engine-cache entry's facts onto a wardrobe payload, in place.

    Returns ``(family_filled, concentration_filled, accords_refreshed,
    year_filled, gender_filled)``. Pure apart from mutating ``payload`` -- no DB,
    so it is unit-testable. ``entry`` is ``{"derived_metrics": <blob|None>,
    "concentration": <str|None>, "year": <str|None>, "gender": <str|None>}``.
    """
    fam_filled = conc_filled = acc_refreshed = year_filled = gender_filled = False
    dm = entry.get("derived_metrics")
    if dm is not None:
        # Always re-project the junk-filtered top accords. The scrape leak
        # (vote counts / Hate-Like / seasons) poisons the wardrobe accords copy
        # too, and a row whose family is *already* set never enters the family
        # branch below -- so without this, the SPA card keeps showing the stale
        # junk accords even after the engine DB is repaired.
        clean = top_accords_from(dm)
        if clean and clean != _str_accords(payload.get("accords")):
            payload["accords"] = clean
            acc_refreshed = True
        # Family + wear projection only when family is still Unknown
        # (unchanged behavior -- never downgrades an existing family).
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
    return fam_filled, conc_filled, acc_refreshed, year_filled, gender_filled


def fill_core_scalars(
    payload: dict[str, Any], entry: dict[str, Any]
) -> tuple[bool, bool, bool]:
    """Fill ONLY the scalar identity facts (concentration, year, gender) from a
    gender-label-stripped core match -- the conservative subset that is safe to
    inherit across the "Dylan Blue" <-> "Pour Homme Dylan Blue" name gap. Returns
    ``(conc_filled, year_filled, gender_filled)``. Gap-fill only; never downgrades
    an existing value. Derived metrics / accords are deliberately NOT projected
    here (those come only from an exact identity match)."""
    conc_filled = year_filled = gender_filled = False
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
    return conc_filled, year_filled, gender_filled


def heal_wardrobe(
    index: dict[str, dict[str, Any]],
    *,
    only_keys: set[str] | None,
    dry_run: bool,
) -> dict[str, int]:
    dsn = _wardrobe_dsn()
    core_index = _build_core_index(index)
    stats = {
        "uf_family": 0, "uf_conc": 0, "uf_acc": 0, "uf_year": 0, "uf_gender": 0,
        "gf_family": 0, "gf_conc": 0, "gf_acc": 0, "gf_year": 0, "gf_gender": 0,
        "no_match": 0, "core_match": 0,
    }

    def _project(
        payload: dict[str, Any], norm: str, brand: str, name: str
    ) -> tuple[bool, bool, bool, bool, bool]:
        """Returns (family, concentration, accords, year, gender) filled flags."""
        entry = index.get(norm)
        fam = cc = acc = yr = gen = False
        if entry is not None:
            fam, cc, acc, yr, gen = project_engine_facts(payload, entry)
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
            if core_entry is not None and core_entry is not entry:
                c2, y2, g2 = fill_core_scalars(payload, core_entry)
                if c2 or y2 or g2:
                    stats["core_match"] += 1
                cc, yr, gen = cc or c2, yr or y2, gen or g2
        if entry is None and not (cc or yr or gen):
            stats["no_match"] += 1
        return fam, cc, acc, yr, gen

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
            fam, cc, acc, yr, gen = _project(fdata, norm, brand_uf, name_uf)
            if fam:
                stats["uf_family"] += 1
            if cc:
                stats["uf_conc"] += 1
            if acc:
                stats["uf_acc"] += 1
            if yr:
                stats["uf_year"] += 1
            if gen:
                stats["uf_gender"] += 1
            if (fam or cc or acc or yr or gen) and not dry_run:
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
            fam, cc, acc, yr, gen = _project(pdata, norm, str(brand), str(name))
            if fam:
                stats["gf_family"] += 1
            if cc:
                stats["gf_conc"] += 1
            if acc:
                stats["gf_acc"] += 1
            if yr:
                stats["gf_year"] += 1
            if gen:
                stats["gf_gender"] += 1
            if (fam or cc or acc or yr or gen) and not dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE global_fragrances SET profile_data = %s WHERE id = %s",
                        (Json(pdata), row["id"]),
                    )
        if not dry_run:
            conn.commit()
    print(
        f"wardrobe: user_fragrances[family+{stats['uf_family']} conc+{stats['uf_conc']} "
        f"accords~{stats['uf_acc']} year+{stats['uf_year']} gender+{stats['uf_gender']}] "
        f"global_fragrances[family+{stats['gf_family']} conc+{stats['gf_conc']} "
        f"accords~{stats['gf_acc']} year+{stats['gf_year']} gender+{stats['gf_gender']}] "
        f"core_match={stats['core_match']} no_engine_match={stats['no_match']} dry_run={dry_run}"
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
