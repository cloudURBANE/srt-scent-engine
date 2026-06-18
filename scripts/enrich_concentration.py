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
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_local_env() -> None:
    """Populate os.environ from huge_monorepo/.env for CLI runs.

    Invoked only when this file is executed as a script (the __main__ guard
    below), *before* ``import db`` so the CLI's DATABASE_URL is frozen into
    db.ENABLED. Importing this module (e.g. test_concentration_backfill.py
    exercising the pure resolvers, or heal_offline.py reusing them) must NOT
    mutate os.environ: doing this at import scope silently injected the
    production DATABASE_URL and pointed the offline test suite at prod.
    """
    env_path = _REPO_ROOT.parent / "huge_monorepo" / ".env"
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())


# Load .env *before* importing db (whose ENABLED/DATABASE_URL freeze at import),
# but only when run as a script -- never on a bare import -- so the CLI keeps its
# DATABASE_URL while unit-test imports stay hermetic.
if __name__ == "__main__":
    _load_local_env()

import db
from concentration_grabber import SemanticScentEngine, to_scentcast_concentration
from enrichment_facts import (
    CONCENTRATION_VARIANT_AMBIGUOUS_SOURCE,
    concentration_meta_marks_ambiguous,
)
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


# Concentration labels that are pure strength markers and never themselves a
# distinct product name -- safe to read straight off a canonical product title
# ("Ramz Gold Eau de Parfum by Lattafa"). Flanker-prone labels (Elixir, Extrait,
# Intense, ...) are intentionally excluded: "Sauvage Elixir" is a different
# fragrance from "Sauvage", so a title-only read of those must not bind.
_PURE_TITLE_CONC_LABELS = {
    "EDP (Eau de Parfum)",
    "EDT (Eau de Toilette)",
    "EDC (Cologne)",
    "Eau Fraiche",
    "Parfum (Profumo)",
}
_TITLE_CONNECTOR_RE = re.compile(
    r"(?i)\b(eau|de|du|des|spray|ml|oz|the|by|for|and|new|vintage|edp|edt|edc)\b"
)


def _concentration_from_product_title(title: str, brand: str, name: str) -> str | None:
    """Extract a concentration ENGINE LABEL from a canonical product-page title.

    Basenotes/Fragrantica title their product pages "<name> <concentration> by
    <brand>" -- an authoritative statement the SERP-vote consensus can dilute into
    noise (the Lattafa Ramz Gold case: Basenotes says "Eau de Parfum" outright, yet
    the vote split EDT/Parfum/EDP). This reads it back precisely:

      * every distinctive name token must appear in the title (exact product, not a
        sibling), and
      * after stripping the query/brand tokens and the matched concentration phrase
        nothing distinctive may remain -- so a flanker ("...Intense Eau de Parfum",
        "Sauvage Elixir ...") is rejected rather than mis-bound.

    Returns the engine label (e.g. "EDP (Eau de Parfum)") or None. Pure helper --
    no network -- so it is unit-testable in isolation."""
    from concentration_grabber import SemanticScentEngine as SSE

    norm = SSE._normalize(title or "").lower()
    if not norm:
        return None
    name_tokens = [t for t in re.findall(r"[a-z0-9]+", (name or "").lower()) if len(t) > 1]
    if not name_tokens or not all(t in norm for t in name_tokens):
        return None
    head = re.split(r"\bby\b", norm, maxsplit=1)[0]
    modifier = SSE._strip_query_brand(SSE._strip_junk(head), f"{brand} {name}", brand)
    conc = SSE._detect_concentration_in(modifier)
    if conc not in _PURE_TITLE_CONC_LABELS:
        return None
    residual = re.sub(SSE.TAXONOMY[conc], " ", modifier)
    residual = _TITLE_CONNECTOR_RE.sub(" ", residual)
    if [t for t in re.findall(r"[a-z]+", residual) if len(t) >= 3]:
        return None
    return conc


def _basenotes_title_concentration(brand: str, name: str) -> dict | None:
    """Authoritative concentration off the Basenotes product title (Decodo SERP).

    High-precision tier: fetches Basenotes results for the fragrance and reads the
    concentration straight off the canonical product title via
    _concentration_from_product_title. No-ops (returns None) when Decodo is not
    configured, so an offline/local run simply falls through to the SERP vote."""
    try:
        from concentration_grabber import SemanticScentEngine as SSE
    except Exception:
        return None
    try:
        rows = SSE._decodo_serp_rows(f"{brand} {name}".strip(), "site:basenotes.com")
    except Exception:
        return None
    for row in (rows or [])[:6]:
        if "basenotes" not in str(row.get("domain", "")).lower():
            continue
        conc = _concentration_from_product_title(row.get("title", ""), brand, name)
        if not conc:
            continue
        scentcast = to_scentcast_concentration(conc)
        if scentcast and scentcast != "Unknown":
            return {
                "concentration": scentcast,
                "concentration_meta": {
                    "confidence": 92,
                    "source": "basenotes_product_title",
                    "resolved_at": _now_iso(),
                    "engine_label": conc,
                },
            }
    return None


def _tier2_serp(query: str, require_literal_support: bool = False) -> dict | None:
    """Tier-2 SERP semantic engine (~58s, DrissionPage + DuckDuckGo).

    Returns a resolved concentration dict only when the engine clears its
    confidence floor (>=50); None on a low-confidence read or any analyze error.
    Extracted so both the wardrobe resolver (``resolve_concentration``) and the
    engine-gap online resolver (``resolve_concentration_online``) share one
    implementation instead of duplicating the analyze + threshold logic.

    ``require_literal_support`` is the engine-cache ground-truth gate: when set,
    a win that cleared the confidence floor is still rejected unless a real SERP
    row *stated* the winning concentration (``primary_has_literal_support``), so
    a result carried only by the brand/pillar prior never reaches the engine
    cache. Default False keeps the wardrobe path -- which is allowed to lean on
    priors -- byte-for-byte unchanged.
    """
    try:
        profile = SemanticScentEngine.analyze(query)
    except Exception as exc:
        print(f"  [WARN] Tier-2 analyze failed: {exc}")
        return None

    if (
        profile
        and require_literal_support
        and profile.primary_confidence >= 50
        and not getattr(profile, "primary_has_literal_support", True)
    ):
        print(
            f"  [SKIP] Tier-2 win '{profile.primary_concentration}' is prior-carried "
            f"(no source row stated it); not writing to the engine cache."
        )
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


def _engine_tier2(query: str) -> dict | None:
    """Engine-cache Tier-2: resolve, mark variant-ambiguous, or give up.

    Three outcomes off one analyze() pass (so the SERP vote is fetched once):
      * a normal resolved dict when a single concentration clears the floor AND a
        real source row stated it (the ground-truth gate);
      * a *variant-ambiguous marker* (``concentration`` None, ``variant_ambiguous``
        True) when no winner clears the floor but >=2 distinct concentrations were
        each literally stated -- the bare name genuinely spans concentrations, so
        there is no single value to store but the row is page-determined, not a gap;
      * None when the vote is merely weak/prior-carried (a future page-scrape could
        still resolve it, so it stays an ordinary gap -- we do NOT mark it).
    """
    try:
        profile = SemanticScentEngine.analyze(query)
    except Exception as exc:
        print(f"  [WARN] Tier-2 analyze failed: {exc}")
        return None
    if not profile:
        return None

    literal = list(getattr(profile, "literal_concentrations", []) or [])
    if (
        profile.primary_confidence >= 50
        and getattr(profile, "primary_has_literal_support", True)
    ):
        return {
            "concentration": to_scentcast_concentration(profile.primary_concentration),
            "concentration_meta": {
                "confidence": profile.primary_confidence,
                "source": "semantic_engine_v6",
                "resolved_at": _now_iso(),
                "engine_label": profile.primary_concentration,
            },
        }

    if len(literal) >= 2:
        attested = sorted({to_scentcast_concentration(c) for c in literal})
        if len(attested) >= 2:
            print(
                f"  [AMBIGUOUS] '{query}' is stated in {len(attested)} concentrations "
                f"({', '.join(attested)}); marking variant-ambiguous, leaving blank."
            )
            return {
                "concentration": None,
                "variant_ambiguous": True,
                "concentration_meta": {
                    "confidence": profile.primary_confidence,
                    "source": CONCENTRATION_VARIANT_AMBIGUOUS_SOURCE,
                    "resolved_at": _now_iso(),
                    "attested_concentrations": attested,
                    "engine_labels": sorted(set(literal)),
                },
            }
    return None


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

    # Tier 1.5 — authoritative Basenotes product title (high precision, off the
    # canonical "<name> <concentration> by <brand>" page title via Decodo SERP).
    # Placed before the noisy multi-source vote so a fragrance whose source page
    # states the concentration outright (Lattafa Ramz Gold -> "Eau de Parfum") is
    # resolved directly instead of being out-voted. No-ops without Decodo.
    bn_title = _basenotes_title_concentration(brand, name)
    if bn_title:
        return bn_title

    if not use_browser:
        return None

    return _tier2_serp(query)


# Engine labels whose token is short and common enough to also appear inside a
# proper fragrance name ("Parfum des Merveilles", "Le Parfum de Therese"). They
# are only trusted as a *trailing* modifier ("Bleu de Chanel Parfum"), the
# convention for a concentration flanker. Distinctive multi-word labels
# (EDP/EDT/EDC/Extrait/Elixir/Intense/...) are trusted anywhere in the
# house-stripped name -- no perfume is named "Eau de Parfum" as a proper noun.
_AMBIGUOUS_NAME_LABELS = {"Parfum (Profumo)", "Le Parfum"}
_TRAILING_PARFUM_RE = re.compile(r"(?i)\b(?:le\s+)?parfum(?:\s+profumo)?\s*$")


def _strip_house(name: str, house: str) -> str:
    """Drop the house tokens from the name so a house literally containing a
    concentration word ("Initio Parfums Prives") can't poison detection."""
    out = str(name or "").strip()
    house = str(house or "").strip()
    if house:
        out = re.sub(re.escape(house), " ", out, flags=re.I)
    return re.sub(r"\s+", " ", out).strip()


def resolve_concentration_strict(brand: str, name: str) -> dict | None:
    """High-precision, zero-network concentration. Returns a resolved dict or None.

    Only two trusted signals are consulted, both instant and offline:
      * the Parfinity catalog (authoritative product_type), and
      * an explicit concentration token in the *house-stripped* fragrance name.
    Brand priors, pillar priors, and the SERP engine are intentionally excluded:
    those are inferences. This path backfills ground-truth facts, never guesses,
    so a record that currently shows no concentration is only ever filled with a
    value the source data actually states.
    """
    brand = str(brand or "").strip()
    name = str(name or "").strip()

    # Tier 0 -- Parfinity catalog (instant, no browser, authoritative).
    try:
        from parfinity import lookup_concentration as _pf_lookup

        hit = _pf_lookup(brand, name)
    except Exception:
        hit = None
    if hit and hit != "Unknown":
        return {
            "concentration": hit,
            "concentration_meta": {
                "confidence": 95,
                "source": "parfinity_catalog",
                "resolved_at": _now_iso(),
                "engine_label": hit,
            },
        }

    # Tier 1 -- explicit concentration token in the house-stripped name.
    modifier = _strip_house(name, brand)
    label = SemanticScentEngine._detect_concentration_in(modifier)
    if not label:
        return None
    if label in _AMBIGUOUS_NAME_LABELS and not _TRAILING_PARFUM_RE.search(modifier):
        # A bare "Parfum" mid-name is part of the proper noun, not a flanker.
        return None
    scentcast = to_scentcast_concentration(label)
    if scentcast == "Unknown":
        return None
    return {
        "concentration": scentcast,
        "concentration_meta": {
            "confidence": 100,
            "source": "name_explicit",
            "resolved_at": _now_iso(),
            "engine_label": label,
        },
    }


def resolve_concentration_online(
    brand: str, name: str, use_browser: bool = True
) -> dict | None:
    """Engine-gap resolver that adds the Decodo/SERP tiers to the strict path.

    The engine cache must stay ground-truth (``run_engine_gap`` writes straight
    into ``fragrance_records``), so this deliberately layers only *source-stated*
    signals on top of ``resolve_concentration_strict`` and SKIPS the brand- and
    pillar-prior guesses that ``resolve_concentration`` (the wardrobe path) uses:

      1. strict offline (Parfinity catalog + explicit house-stripped name token),
      2. the authoritative Basenotes product-title tier via Decodo (no-ops without
         Decodo creds), and
      3. optionally the Tier-2 SERP semantic engine (browser, ~58s) when
         ``use_browser`` is set, its confidence floor is cleared, AND a real
         source row stated the winning concentration (``require_literal_support``)
         -- a vote carried only by the brand/pillar prior is rejected here so the
         cache never gets an inference even though the wardrobe path would keep it.

    Returns a resolved dict or None. This is what lets ``--engine-gap --online``
    fill the rows that strict-offline cannot, without ever writing an inference
    into the engine cache.
    """
    res = resolve_concentration_strict(brand, name)
    if res:
        return res
    bn_title = _basenotes_title_concentration(brand, name)
    if bn_title:
        return bn_title
    if not use_browser:
        return None
    # Engine cache must stay ground-truth: accept the Tier-2 vote only when a real
    # source row stated the winning concentration; otherwise either mark the row
    # variant-ambiguous (>=2 concentrations stated) or leave it an ordinary gap.
    return _engine_tier2(f"{brand} {name}".strip())


def fetch_engine_gap_rows(cur, limit: int | None) -> list[dict]:
    """fragrance_records identities whose concentration is null/empty/Unknown."""
    cur.execute(
        """
        SELECT record_key, house, name, fg_raw_json
        FROM fragrance_records
        WHERE house IS NOT NULL AND name IS NOT NULL
          AND house <> '' AND name <> ''
          AND (fg_raw_json->>'concentration' IS NULL
               OR LOWER(fg_raw_json->>'concentration') IN ('', 'unknown'))
          AND (fg_raw_json->'raw_identity'->>'concentration' IS NULL
               OR LOWER(fg_raw_json->'raw_identity'->>'concentration') IN ('', 'unknown'))
        ORDER BY record_key
        """
        + (f" LIMIT {int(limit)}" if limit else "")
    )
    # SQL pre-filters cheaply; confirm with the same predicate the audit uses.
    rows = []
    for row in cur.fetchall():
        fg_raw = dict(row["fg_raw_json"] or {})
        if not _is_unknown(_engine_concentration_from_fg_raw(fg_raw)):
            continue
        # A row already marked variant-ambiguous is page-determined, not a gap:
        # the name has no single concentration. Re-resolving it would just re-bill
        # Decodo to reach the same verdict, so drop it from the sweep.
        raw_identity = fg_raw.get("raw_identity") if isinstance(fg_raw.get("raw_identity"), dict) else {}
        if concentration_meta_marks_ambiguous(
            fg_raw.get("concentration_meta")
        ) or concentration_meta_marks_ambiguous(raw_identity.get("concentration_meta")):
            continue
        rows.append(row)
    return rows


def run_engine_gap(args: argparse.Namespace) -> None:
    """Backfill concentration straight off the engine's own completeness gap.

    Sweeps fragrance_records (the table the /api/enrichment/completeness audit
    reads) for rows missing concentration, resolves each row, and merge-writes via
    update_engine_cache_rows -- which preserves notes/reviews/frag_cards and
    refuses to overwrite an existing value. Idempotent and safe to re-run; dry-run
    prints every proposed change without writing.

    By default each row is resolved with the strict OFFLINE resolver (ground-truth
    only, zero network). With ``--online`` it uses ``resolve_concentration_online``,
    which adds the Decodo Basenotes-title tier (and, unless ``--tier1-only``, the
    Tier-2 SERP engine) so rows the source page states but the offline tier can't
    parse get filled -- still without writing a brand/pillar-prior guess into the
    engine cache. ``--online`` no-ops back to strict when no Decodo creds are set.
    """
    db.init_db()
    if not db.ENABLED:
        raise RuntimeError("DATABASE_URL not configured.")
    DATABASE_URL = os.environ.get("DATABASE_URL")
    dry_run = args.dry_run
    online = getattr(args, "online", False)
    use_browser = not getattr(args, "tier1_only", False)
    if online:
        resolver = lambda h, n: resolve_concentration_online(h, n, use_browser=use_browser)
    else:
        resolver = resolve_concentration_strict
    print(
        f"engine-gap resolver: {'online (strict+Decodo' + ('' if use_browser else ', tier1-only') + ')' if online else 'strict-offline'}"
    )

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            rows = fetch_engine_gap_rows(cur, args.limit)

    print(f"engine-gap: {len(rows)} fragrance_records missing concentration")
    resolved = skipped = ambiguous = cache_rows = 0
    seen: set[tuple[str, str]] = set()
    for row in rows:
        house = str(row["house"] or "").strip()
        name = str(row["name"] or "").strip()
        key = (house.lower(), name.lower())
        if key in seen:
            continue
        seen.add(key)

        res = resolver(house, name)
        if not res:
            skipped += 1
            continue
        meta = res["concentration_meta"]

        # Variant-ambiguous: no single value, but stamp the marker so the row
        # leaves the gap and the API can surface it. Counted separately, not as
        # a resolve (nothing was filled).
        if res.get("variant_ambiguous"):
            ambiguous += 1
            if dry_run:
                continue
            with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    d, r = mark_engine_concentration_ambiguous(cur, house, name, meta)
                conn.commit()
            cache_rows += d + r
            continue

        resolved += 1
        conc = res["concentration"]
        print(f"  {house} | {name}  ->  {conc}  (src={meta['source']}, label={meta['engine_label']})")
        if dry_run:
            continue
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                d, r = update_engine_cache_rows(cur, house, name, conc, meta)
            conn.commit()
        cache_rows += d + r

    print(
        f"\nengine-gap done. resolved={resolved} ambiguous={ambiguous} skipped={skipped} "
        f"cache_rows_written={cache_rows} dry_run={dry_run}"
    )


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


def _engine_concentration_from_fg_raw(fg_raw: dict) -> str | None:
    raw_identity = fg_raw.get("raw_identity") if isinstance(fg_raw, dict) else None
    if not isinstance(raw_identity, dict):
        raw_identity = {}
    return fg_raw.get("concentration") or raw_identity.get("concentration")


def update_engine_cache_rows(
    cur,
    brand: str,
    name: str,
    concentration: str,
    concentration_meta: dict,
) -> tuple[int, int]:
    """Fill missing concentration in durable engine cache tables."""
    detail_updates = 0
    record_updates = 0

    cur.execute(
        """
        SELECT canonical_fg_url, raw_identity_json
        FROM fg_detail_cache
        WHERE LOWER(house) = LOWER(%s)
          AND LOWER(name) = LOWER(%s)
        """,
        (brand, name),
    )
    for cache_row in cur.fetchall():
        raw_identity = dict(cache_row["raw_identity_json"] or {})
        if not _is_unknown(raw_identity.get("concentration")):
            continue
        raw_identity["concentration"] = concentration
        raw_identity["concentration_meta"] = concentration_meta
        cur.execute(
            "UPDATE fg_detail_cache SET raw_identity_json = %s, updated_at = now() WHERE canonical_fg_url = %s",
            (Json(raw_identity), cache_row["canonical_fg_url"]),
        )
        detail_updates += 1

    cur.execute(
        """
        SELECT record_key, fg_raw_json
        FROM fragrance_records
        WHERE LOWER(house) = LOWER(%s)
          AND LOWER(name) = LOWER(%s)
        """,
        (brand, name),
    )
    for record_row in cur.fetchall():
        fg_raw = dict(record_row["fg_raw_json"] or {})
        if not _is_unknown(_engine_concentration_from_fg_raw(fg_raw)):
            continue
        raw_identity = fg_raw.get("raw_identity")
        if not isinstance(raw_identity, dict):
            raw_identity = {}
        else:
            raw_identity = dict(raw_identity)
        raw_identity["concentration"] = concentration
        raw_identity["concentration_meta"] = concentration_meta
        fg_raw["raw_identity"] = raw_identity
        fg_raw["concentration"] = concentration
        fg_raw["concentration_meta"] = concentration_meta
        cur.execute(
            "UPDATE fragrance_records SET fg_raw_json = %s, updated_at = now() WHERE record_key = %s",
            (Json(fg_raw), record_row["record_key"]),
        )
        record_updates += 1

    return detail_updates, record_updates


def mark_engine_concentration_ambiguous(
    cur, brand: str, name: str, concentration_meta: dict
) -> tuple[int, int]:
    """Record a variant-ambiguous marker without filling concentration.

    Mirrors the year-unknown marker: the value stays blank (there is no single
    ground-truth concentration), but ``concentration_meta`` is stamped so the
    engine-gap sweep stops re-listing the row, the worker stops requeueing it, and
    the API can surface "varies". Only stamps rows whose concentration is still
    unknown and not already marked, so it is idempotent and never clobbers a value.
    """
    detail_updates = 0
    record_updates = 0

    cur.execute(
        """
        SELECT canonical_fg_url, raw_identity_json
        FROM fg_detail_cache
        WHERE LOWER(house) = LOWER(%s) AND LOWER(name) = LOWER(%s)
        """,
        (brand, name),
    )
    for cache_row in cur.fetchall():
        raw_identity = dict(cache_row["raw_identity_json"] or {})
        if not _is_unknown(raw_identity.get("concentration")):
            continue
        if concentration_meta_marks_ambiguous(raw_identity.get("concentration_meta")):
            continue
        raw_identity["concentration_meta"] = concentration_meta
        cur.execute(
            "UPDATE fg_detail_cache SET raw_identity_json = %s, updated_at = now() WHERE canonical_fg_url = %s",
            (Json(raw_identity), cache_row["canonical_fg_url"]),
        )
        detail_updates += 1

    cur.execute(
        """
        SELECT record_key, fg_raw_json
        FROM fragrance_records
        WHERE LOWER(house) = LOWER(%s) AND LOWER(name) = LOWER(%s)
        """,
        (brand, name),
    )
    for record_row in cur.fetchall():
        fg_raw = dict(record_row["fg_raw_json"] or {})
        if not _is_unknown(_engine_concentration_from_fg_raw(fg_raw)):
            continue
        if concentration_meta_marks_ambiguous(fg_raw.get("concentration_meta")):
            continue
        raw_identity = fg_raw.get("raw_identity")
        raw_identity = dict(raw_identity) if isinstance(raw_identity, dict) else {}
        raw_identity["concentration_meta"] = concentration_meta
        fg_raw["raw_identity"] = raw_identity
        fg_raw["concentration_meta"] = concentration_meta
        cur.execute(
            "UPDATE fragrance_records SET fg_raw_json = %s, updated_at = now() WHERE record_key = %s",
            (Json(fg_raw), record_row["record_key"]),
        )
        record_updates += 1

    return detail_updates, record_updates


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

                detail_updates, record_updates = update_engine_cache_rows(
                    cur, brand, name, new_conc, meta
                )
                if detail_updates or record_updates:
                    print(
                        "  Updated engine cache rows "
                        f"(fg_detail_cache={detail_updates}, fragrance_records={record_updates})"
                    )

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
    parser.add_argument(
        "--engine-gap",
        action="store_true",
        help="Backfill concentration on the engine's own fragrance_records "
        "completeness gap (strict offline resolver, merge-safe, idempotent).",
    )
    parser.add_argument(
        "--online",
        action="store_true",
        help="With --engine-gap, add the Decodo Basenotes-title tier (and Tier-2 "
        "SERP unless --tier1-only) on top of the strict resolver, so rows the "
        "source states but offline can't parse get filled. No-ops without Decodo "
        "creds. Never writes a brand/pillar-prior guess into the engine cache.",
    )
    args = parser.parse_args()

    if args.engine_gap:
        run_engine_gap(args)
        return

    if not args.all_unknown and not args.brand and args.limit is None:
        parser.error("Specify --all-unknown, --limit N, --brand / --name, or --engine-gap to select rows.")

    run(args)


if __name__ == "__main__":
    main()
