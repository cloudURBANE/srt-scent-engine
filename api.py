#!/usr/bin/env python3
"""
api.py

Thin HTTP wrapper around the existing fragrance engine.

It does not rewrite the engine, touch parser selectors, alter resolver/search
or fallback behavior, or modify derived_metrics_adapter.py. It only imports the
engine's existing entry points and exposes them over HTTP:

    engine.get_scraper()                    -> per-request scraper session
    engine.build_parser().parse_args([])    -> the engine's own default args
    engine.search_once(scraper, q, args)    -> list[UnifiedFragrance]
    engine.fetch_selected_details(...)      -> UnifiedDetails (incl. derived_metrics)

Endpoints:
    GET  /health                    -> {"ok": true}
    GET  /api/fragrances/search?q=  -> ranked candidate list
    POST /api/fragrances/details    -> full detail bundle, including derived_metrics

Serialization here is read-only: it reshapes the engine's dataclasses into JSON
and never recomputes any score.

Search-to-detail handoff
------------------------
UnifiedFragrance has no native id, and fetch_selected_details needs *both*
bn_url and frag_url to preserve the engine's dual-source behavior. So /search
emits an opaque, stateless `id` token (base64 of the candidate's identifying
fields + both URLs). The frontend echoes that `id` back to /details and the
engine candidate is reconstructed verbatim -- no server-side state, no resolver
re-run. `source_url` is also returned as the primary human-facing URL; /details
will fall back to it if `id` is absent.
"""
from __future__ import annotations

import base64
import hmac
import json
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import db
import fragrance_parser_full_rewrite_fixed as engine
import mobile
import parfinity

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine handles (built once, reused)
# ---------------------------------------------------------------------------

# The engine's own argparse defaults. Parsing an empty argv yields exactly the
# defaults defined in build_parser(), so search/fallback behavior is untouched.
_ARGS = engine.build_parser().parse_args([])

# Option 3 pre-cache pipeline wiring. Railway's runtime is 403'd by Cloudflare
# on fragrantica.com, so the engine's live FG search resolves nothing and the
# default cache path (~/.cache/...) is an empty file in a fresh container. When
# FG_CACHE_PATH is set, point the engine at a cache file warmed offline by
# scripts/warm_fg_cache.py and shipped with the deploy. This is the exact knob
# the engine's own `--fg-cache` flag already exposes -- no engine/resolver
# change, just choosing which file the existing IdentityCache reads. Unset =
# unchanged behavior, so this line is inert until the env var is provided.
_ARGS.fg_cache = os.environ.get("FG_CACHE_PATH", _ARGS.fg_cache)

# fg_detail_cache_v1.json overlay wiring. FG_CACHE_PATH (above) only restores
# Fragrantica *URL identity* -- it does not bring back the parsed Fragrantica
# detail metrics, so on Railway /details still returns BN-only data with every
# Fragrantica-derived metric group null. FG_DETAIL_CACHE_PATH points at a second
# cache file -- parsed Fragrantica detail output (frag_cards/notes/...) warmed
# offline by scripts/warm_fg_detail_cache.py -- which the API-level overlay in
# _apply_fg_detail_cache() merges onto a BN-only bundle. Unset = path inert.
_FG_DETAIL_CACHE_PATH = os.environ.get("FG_DETAIL_CACHE_PATH", "")
_FG_DETAIL_CACHE: dict[str, Any] | None = None  # lazy-loaded once, see below


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# In production, DATABASE_URL means the durable fg_detail_cache is the source of
# truth. Bundled JSON caches are only trusted there when explicitly opted in, so
# deleting a row from the database does not leave stale filesystem data alive.
_ALLOW_BUNDLED_FG_DETAIL_CACHE = _env_flag(
    "ALLOW_BUNDLED_FG_DETAIL_CACHE", default=not db.ENABLED
)
_ALLOW_BUNDLED_FG_SEARCH_CACHE = _env_flag(
    "ALLOW_BUNDLED_FG_SEARCH_CACHE",
    default=not db.ENABLED or bool(os.environ.get("FG_CACHE_PATH")),
)
_CACHE_SEARCH_MIN_SCORE = engine.QueryRepair.MIN_RESULT_SCORE

# Strict score floor for the /search strong-cache precheck. _CACHE_SEARCH_MIN_SCORE
# (0.72) is the right floor *after* live providers fail -- a loose match beats a
# blank screen. But to skip live resolution entirely, a cached row must be a
# near-exact hit, so the precheck uses a much higher threshold.
_STRONG_CACHE_MIN_SCORE = 0.95

# Warm-cache floor: below the strong threshold, but still a confident identity
# match. A warm hit is served immediately (no live wait) AND triggers a bounded
# background re-resolve, so the next identical query promotes to the strong
# fast path. Bridges the all-or-nothing gap between a 0.95 precheck hit and a
# full ~6s live search. See `_spawn_warm_refresh` and the /search warm tier.
_WARM_CACHE_MIN_SCORE = 0.85


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or "0")
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)) or "0")
    except (TypeError, ValueError):
        return default


# API latency budget. Keep the CLI parser defaults exploratory, but make HTTP
# search suitable for the first screen: live first-pass work is bounded tightly,
# and candidate metrics are left to /details.
_ARGS.initial_timeout = _env_float("API_INITIAL_TIMEOUT", 5.5)
# 1-C: FG/Serper first pass gets a tighter independent cap than the BN-bearing
# initial_timeout; clamped to initial_timeout inside the engine.
_ARGS.fg_timeout = _env_float("API_FG_TIMEOUT", 3.0)
_ARGS.metrics_budget = _env_float("API_METRICS_BUDGET", 0.0)
_ARGS.spell_repair_budget = _env_float("API_SPELL_REPAIR_BUDGET", 0.8)


_FRAGRANCE_RECORD_TTL_HOURS = _env_int("FRAGRANCE_RECORD_TTL_HOURS", 168)
_DERIVED_METRICS_LOCK_GUARD = threading.Lock()
_DERIVED_METRICS_LOCKS: dict[str, threading.Lock] = {}

# Enrichment worker auth. The protected /api/enrichment/jobs/* endpoints require
# `Authorization: Bearer <ENRICHMENT_WORKER_TOKEN>`. This token is for the
# offline worker only -- it is never sent to the frontend and is never logged.
# Unset => the worker endpoints are unconfigured and return 503.
_ENRICHMENT_WORKER_TOKEN = os.environ.get("ENRICHMENT_WORKER_TOKEN", "")


def _require_clearance_token(authorization: str | None = Header(default=None)) -> None:
    if not _ENRICHMENT_WORKER_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="Clearance endpoints are not configured (ENRICHMENT_WORKER_TOKEN unset).",
        )
    expected = f"Bearer {_ENRICHMENT_WORKER_TOKEN}"
    if not hmac.compare_digest(authorization or "", expected):
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token.")


class ClearanceCookieRequest(BaseModel):
    user_agent: str
    cookie_header: str | None = None
    cookies: dict[str, str] | None = None
    validate_session: bool = True


app = FastAPI(title="Fragrance Engine API", version="1.0.0")

_BN_WARMUP_LOCK = threading.Lock()
_BN_WARMUP_STARTED = False


def _warm_basenotes_clearance() -> None:
    try:
        scraper = engine.get_scraper()
        rows = engine.BasenotesEngine.extract_search_data(
            scraper,
            "xerjoff naxos",
            deadline=engine.Deadline(_ARGS.initial_timeout),
        )
        logger.info("Basenotes startup warmup completed with %d rows", len(rows))
    except Exception:
        logger.warning("Basenotes startup warmup failed", exc_info=True)


def _start_basenotes_warmup() -> None:
    global _BN_WARMUP_STARTED
    with _BN_WARMUP_LOCK:
        if _BN_WARMUP_STARTED:
            return
        _BN_WARMUP_STARTED = True
    thread = threading.Thread(
        target=_warm_basenotes_clearance,
        name="basenotes-startup-warmup",
        daemon=True,
    )
    thread.start()


@app.on_event("startup")
def _startup() -> None:
    """Bootstrap durable storage. Inert when DATABASE_URL is unset (local dev)."""
    db.init_db()
    _start_basenotes_warmup()

# ---------------------------------------------------------------------------
# CORS -- restricted to the frontend origin(s).
# Override via FRONTEND_ORIGINS env var (comma-separated) on Railway, e.g.
# FRONTEND_ORIGINS=https://your-frontend.up.railway.app
# Local dev (Vite) is allowed by default.
# ---------------------------------------------------------------------------

_DEFAULT_ORIGINS = "http://localhost:5173,http://127.0.0.1:5173"
_ALLOWED_ORIGINS = [
    o.strip()
    for o in os.environ.get("FRONTEND_ORIGINS", _DEFAULT_ORIGINS).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Mobile router: /m/* HTML, /api/m/* JSON (cookie-authed), plus the
# /api/enrichment/commands/* channel the desktop worker polls.
app.include_router(mobile.router)

# ---------------------------------------------------------------------------
# Opaque id token: stateless encode/decode of an engine candidate.
# ---------------------------------------------------------------------------


def _encode_id(item: engine.UnifiedFragrance) -> str:
    """Pack the fields fetch_selected_details needs into an opaque token."""
    identity = _recover_candidate_identity(item)
    payload = {
        "n": identity["name"] or item.name,
        "b": identity["house"] or item.brand,
        "y": item.year,
        "bn": item.bn_url,
        "fg": item.frag_url,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_id(token: str) -> dict[str, str]:
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("id token did not decode to an object")
    return data


def _coerce_year(year: Any) -> int | None:
    """Engine stores year as a str; emit an int when it cleanly is one."""
    if year is None:
        return None
    text = str(year).strip()
    return int(text) if text.isdigit() else None


def _identity_needs_recovery(value: str) -> bool:
    text = (value or "").strip()
    if not text or text.lower() == "unknown":
        return True
    checker = getattr(engine, "_identity_looks_real", None)
    return bool(callable(checker) and not checker(text))


def _identity_compare_tokens(value: str) -> set[str]:
    return {
        token
        for token in engine.TextSanitizer.normalize_identity(value).split()
        if token and token != "and"
    }


def _stored_name_is_less_specific_than_url(stored: str, url_derived: str) -> bool:
    stored_tokens = engine.TextSanitizer.normalize_identity(stored).split()
    url_tokens = engine.TextSanitizer.normalize_identity(url_derived).split()
    if not stored_tokens or not url_tokens or stored_tokens == url_tokens:
        return False
    return len(url_tokens) > len(stored_tokens)


def _house_is_partial_url_house(house: str, url_house: str) -> bool:
    house_tokens = _identity_compare_tokens(house)
    url_house_tokens = _identity_compare_tokens(url_house)
    return bool(house_tokens and url_house_tokens and house_tokens < url_house_tokens)


def _name_looks_like_url_house_prefixed(name: str, url_name: str, url_house: str) -> bool:
    name_norm = engine.TextSanitizer.normalize_identity(name)
    url_name_norm = engine.TextSanitizer.normalize_identity(url_name)
    if not name_norm or not url_name_norm or name_norm == url_name_norm:
        return False
    if not name_norm.endswith(f" {url_name_norm}"):
        return False
    prefix = name_norm[: -len(url_name_norm)].strip()
    prefix_tokens = _identity_compare_tokens(prefix)
    house_tokens = _identity_compare_tokens(url_house)
    if not prefix_tokens or not house_tokens:
        return False
    return engine.IdentityTools.fuzzy_token_coverage(prefix_tokens, house_tokens) >= 0.82


def _url_name_should_replace(
    name: str,
    house: str,
    url_name: str,
    url_house: str,
) -> bool:
    if _stored_name_is_less_specific_than_url(name, url_name):
        return True
    if (
        house
        and url_house
        and not engine.IdentityTools.compatible_brand(house, url_house)
        and _name_looks_like_url_house_prefixed(name, url_name, url_house)
    ):
        return True
    return False


def _search_identity_is_safe_to_store(
    name: str,
    house: str,
    item: engine.UnifiedFragrance,
) -> bool:
    if not name or not house:
        return False
    if engine.IdentityTools.bad_partial_house_remainder(name, house):
        return False
    if item.frag_url and not parfinity.is_parfinity_url(item.frag_url):
        url_name = engine.FragranticaEngine.name_from_url(item.frag_url)
        url_house = engine.FragranticaEngine.brand_from_url(item.frag_url)
        name_norm = engine.TextSanitizer.normalize_identity(name)
        url_name_norm = engine.TextSanitizer.normalize_identity(url_name)
        if url_name and name_norm != url_name_norm:
            return not _url_name_should_replace(name, house, url_name, url_house)
    return True


def _recover_candidate_identity(item: engine.UnifiedFragrance) -> dict[str, str]:
    """Recover name/house from canonical source URLs when fields are weak."""
    name = (item.name or "").strip()
    house = (item.brand or "").strip()

    if (
        item.frag_url
        and parfinity.is_parfinity_url(item.frag_url)
        and (_identity_needs_recovery(name) or _identity_needs_recovery(house))
    ):
        pf_entry = parfinity.lookup_detail(item.frag_url)
        if pf_entry:
            if _identity_needs_recovery(name):
                name = str(pf_entry.get("name") or "").strip() or name
            if _identity_needs_recovery(house):
                house = str(pf_entry.get("house") or "").strip() or house

    if item.frag_url and not parfinity.is_parfinity_url(item.frag_url):
        url_name = engine.FragranticaEngine.name_from_url(item.frag_url)
        url_house = engine.FragranticaEngine.brand_from_url(item.frag_url)
        if url_name and (
            _identity_needs_recovery(name)
            or _url_name_should_replace(name, house, url_name, url_house)
        ):
            name = url_name
        if url_house and (
            _identity_needs_recovery(house)
            or (
                house
                and not engine.IdentityTools.compatible_brand(house, url_house)
                and _house_is_partial_url_house(house, url_house)
            )
        ):
            house = url_house

    if item.bn_url and (_identity_needs_recovery(name) or _identity_needs_recovery(house)):
        bn_candidate = engine.BasenotesEngine._parse_name_metadata(
            item.bn_url, "", ""
        )
        if _identity_needs_recovery(name) and bn_candidate.name:
            name = bn_candidate.name
        if _identity_needs_recovery(house) and bn_candidate.brand:
            house = bn_candidate.brand

    if name and house:
        name = engine.IdentityTools.strip_house_from_name(name, house)

    return {"name": name, "house": house}


def _candidate_has_display_identity(item: engine.UnifiedFragrance) -> bool:
    identity = _recover_candidate_identity(item)
    return not _identity_needs_recovery(identity["name"]) and not _identity_needs_recovery(
        identity["house"]
    )


# ---------------------------------------------------------------------------
# Serialization helpers (read-only reshaping of engine dataclasses)
# ---------------------------------------------------------------------------


def _search_result_to_dict(item: engine.UnifiedFragrance) -> dict[str, Any]:
    """Serialize a search candidate to the frontend contract shape.

    `gender` is not known at search time -- the engine only resolves it during
    the detail fetch -- so the engine's own default placeholder is surfaced
    here. It is not fabricated; the real value arrives in the detail response.

    Final identity guard: if anything upstream emitted an empty or sentinel
    brand ("Unknown") despite the parser and cache fixes, recover it from
    the source URL slug before serialising. Same for the name. This is the
    last line of defence against `house: ""` or `name: "342"` ever reaching
    the wire; cheap belt-and-braces over the parser-level rejections in
    fragrance_parser_full_rewrite_fixed.py.
    """
    source_url = item.frag_url or item.bn_url
    identity = _recover_candidate_identity(item)
    return {
        "id": _encode_id(item),
        "name": identity["name"],
        "house": identity["house"],
        "year": _coerce_year(item.year),
        "image_url": getattr(item, "image_url", None),
        "gender": "Unisex / Unspecified",  # engine default; resolved at detail
        "source_url": source_url,
        "bn_positive_pct": getattr(item, "bn_positive_pct", -1),
        "bn_vote_count": getattr(item, "bn_vote_count", 0),
    }


def _candidate_from_fragrance_record(
    record: dict[str, Any], source: str = "fragrance_db"
) -> engine.UnifiedFragrance | None:
    fg_url = _canonical_fg_url(str(record.get("canonical_fg_url") or ""))
    bn_url = str(record.get("bn_url") or "").strip()
    name = str(record.get("name") or "").strip()
    house = str(record.get("house") or "").strip()
    year = str(record.get("year") or "").strip()
    if fg_url:
        identity = _recover_candidate_identity(
            engine.UnifiedFragrance(
                name=name,
                brand=house,
                year=year,
                bn_url=bn_url,
                frag_url=fg_url,
            )
        )
        name = identity["name"] or name
        house = identity["house"] or house
    elif name and house:
        name = engine.IdentityTools.strip_house_from_name(name, house)
    if not name or not house:
        return None
    if engine.IdentityTools.bad_partial_house_remainder(name, house):
        return None
    candidate = engine.UnifiedFragrance(
        name=name,
        brand=house,
        year=year,
        bn_url=bn_url,
        frag_url=fg_url,
        resolver_source=source,
        resolver_score=0.99,
    )
    image_url = str(record.get("image_url") or "").strip()
    if image_url:
        setattr(candidate, "image_url", image_url)
    search_blob = record.get("search") or {}
    if isinstance(search_blob, dict):
        try:
            candidate.bn_positive_pct = int(search_blob.get("bn_positive_pct") or -1)
        except (TypeError, ValueError):
            candidate.bn_positive_pct = -1
        try:
            candidate.bn_vote_count = int(search_blob.get("bn_vote_count") or 0)
        except (TypeError, ValueError):
            candidate.bn_vote_count = 0
    return candidate


def _fragrance_record_is_stale(record: dict[str, Any]) -> bool:
    if _FRAGRANCE_RECORD_TTL_HOURS <= 0:
        return False
    captured = str(record.get("source_captured_at") or record.get("updated_at") or "")
    if not captured:
        return False
    try:
        captured_at = datetime.fromisoformat(captured.replace("Z", "+00:00"))
    except ValueError:
        return False
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - captured_at > timedelta(
        hours=_FRAGRANCE_RECORD_TTL_HOURS
    )


def _fragrance_record_search(
    query: str, limit: int, min_score: float = _CACHE_SEARCH_MIN_SCORE
) -> list[engine.UnifiedFragrance]:
    candidates: list[engine.UnifiedFragrance] = []
    seen: set[str] = set()
    for record in db.search_fragrance_records(query, limit=limit * 2):
        if _fragrance_record_is_stale(record):
            continue
        item = _candidate_from_fragrance_record(record)
        if not item:
            continue
        item.query_score = engine.IdentityTools.relevance_score(query, item)
        if item.query_score < min_score:
            continue
        key = item.frag_url or item.bn_url or item.cache_key
        if key in seen:
            continue
        seen.add(key)
        candidates.append(item)
    candidates.sort(
        key=lambda item: (item.query_score, item.resolver_score, item.year),
        reverse=True,
    )
    return candidates[:limit]


def _persist_search_results(query: str, results: list[engine.UnifiedFragrance]) -> None:
    """Best-effort light upsert of live search rows into the aggregate DB."""
    if not db.ENABLED:
        return
    for item in results:
        try:
            identity = _recover_candidate_identity(item)
            name = identity["name"] or item.name
            house = identity["house"] or item.brand
            if not _search_identity_is_safe_to_store(name, house, item):
                continue
            db.upsert_fragrance_search(
                {
                    "canonical_fg_url": _canonical_fg_url(item.frag_url),
                    "bn_url": item.bn_url or None,
                    "name": name,
                    "house": house,
                    "year": _coerce_year(item.year),
                    "image_url": getattr(item, "image_url", None),
                    "search": _json_for_db_blob(
                        {
                            "query": query,
                            "bn_positive_pct": getattr(item, "bn_positive_pct", None),
                            "bn_vote_count": getattr(item, "bn_vote_count", None),
                            "resolver_source": getattr(item, "resolver_source", None),
                            "resolver_score": getattr(item, "resolver_score", None),
                            "query_score": getattr(item, "query_score", None),
                        }
                    ),
                }
            )
        except Exception:
            logger.exception(
                "upsert_fragrance_search failed name=%s house=%s",
                item.name,
                item.brand,
            )


def _candidate_from_cache_entry(
    entry: dict[str, Any], source: str
) -> engine.UnifiedFragrance | None:
    fg_url = _canonical_fg_url(
        str(entry.get("canonical_fg_url") or entry.get("fg_url") or "")
    )
    if not fg_url:
        return None
    identity = _cache_entry_identity(entry, fg_url)
    if not identity["name"] or not identity["house"]:
        return None
    candidate = engine.UnifiedFragrance(
        name=identity["name"],
        brand=identity["house"],
        year=identity["year"],
        frag_url=fg_url,
        resolver_source=source,
        resolver_score=0.95,
    )
    image_url = _cache_entry_image_url(entry)
    if image_url:
        setattr(candidate, "image_url", image_url)
    return candidate


def _json_cache_search(
    query: str, limit: int, min_score: float = _CACHE_SEARCH_MIN_SCORE
) -> list[engine.UnifiedFragrance]:
    if not _ALLOW_BUNDLED_FG_DETAIL_CACHE:
        return []
    candidates: list[engine.UnifiedFragrance] = []
    seen: set[str] = set()
    for url, entry in _load_fg_detail_cache().items():
        item = _candidate_from_cache_entry(
            {"canonical_fg_url": url, **entry}, "fg_detail_json_cache"
        )
        if not item:
            continue
        item.query_score = engine.IdentityTools.relevance_score(query, item)
        if item.query_score < min_score:
            continue
        key = item.frag_url or item.cache_key
        if key in seen:
            continue
        seen.add(key)
        candidates.append(item)
    candidates.sort(
        key=lambda item: (item.query_score, item.resolver_score, item.year),
        reverse=True,
    )
    return candidates[:limit]


def _identity_cache_search(
    query: str, limit: int, min_score: float = _CACHE_SEARCH_MIN_SCORE
) -> list[engine.UnifiedFragrance]:
    """Last-resort search candidates from the engine's existing FG identity cache.

    This is used only when live providers return zero candidates. It does not
    alter resolver scoring or the normal search path; it simply exposes the same
    warmed identity cache that SearchSniper already uses after candidates exist.
    """
    if not _ALLOW_BUNDLED_FG_SEARCH_CACHE:
        return []
    cache = engine.IdentityCache(getattr(_ARGS, "fg_cache", ""))
    candidates: list[engine.UnifiedFragrance] = []
    seen: set[str] = set()
    for row in cache.data.values():
        if not isinstance(row, dict):
            continue
        fg_url = _canonical_fg_url(str(row.get("url") or ""))
        if not fg_url or fg_url in seen:
            continue
        item = engine.UnifiedFragrance(
            name=str(row.get("name") or "").strip(),
            brand=str(row.get("brand") or "").strip(),
            year=str(row.get("year") or "").strip(),
            frag_url=fg_url,
            resolver_source="fg_identity_cache",
            resolver_score=0.98,
        )
        if not item.name or not item.brand:
            continue
        item.query_score = engine.IdentityTools.relevance_score(query, item)
        if item.query_score < min_score:
            continue
        seen.add(fg_url)
        candidates.append(item)
    candidates.sort(
        key=lambda item: (item.query_score, item.resolver_score, item.year),
        reverse=True,
    )
    return candidates[:limit]


def _db_detail_cache_search(
    query: str, limit: int, min_score: float = _CACHE_SEARCH_MIN_SCORE
) -> list[engine.UnifiedFragrance]:
    """Search candidates from the durable Postgres fg_detail_cache."""
    candidates: list[engine.UnifiedFragrance] = []
    seen: set[str] = set()
    for entry in db.search_detail_cache(query, limit=limit * 2):
        item = _candidate_from_cache_entry(entry, "fg_detail_db_cache")
        if not item:
            continue
        item.query_score = engine.IdentityTools.relevance_score(query, item)
        if item.query_score < min_score:
            continue
        key = item.frag_url or item.cache_key
        if key in seen:
            continue
        seen.add(key)
        candidates.append(item)
    candidates.sort(
        key=lambda item: (item.query_score, item.resolver_score, item.year),
        reverse=True,
    )
    return candidates[:limit]


def _cache_search_fallback(query: str, limit: int) -> tuple[list[engine.UnifiedFragrance], str | None]:
    """Fallback search for blocked live providers, with DB as source of truth."""
    candidates = _fragrance_record_search(query, limit)
    source: str | None = None

    if candidates:
        source = "aggregate_db"
    else:
        candidates = _db_detail_cache_search(query, limit)
        if candidates:
            source = "db"
    if not candidates:
        candidates = _json_cache_search(query, limit)
        if candidates:
            source = "json"
    if not candidates:
        candidates = _identity_cache_search(query, limit)
        if candidates:
            source = "identity"

    candidates.sort(
        key=lambda item: (item.query_score, item.resolver_score, item.year),
        reverse=True,
    )
    return candidates[:limit], source


def _strong_cache_candidate_ok(
    query: str, item: engine.UnifiedFragrance, min_score: float
) -> bool:
    if float(getattr(item, "query_score", 0.0) or 0.0) < min_score:
        return False

    # IdentityTools deliberately treats a house-only match as strong enough for
    # fallback ranking. For a live-search bypass, require the query to touch the
    # fragrance name itself, so broad brand searches still get live breadth.
    query_tokens = set(engine.QueryRepair._tokens(query))
    name_tokens = set(engine.QueryRepair._tokens(item.name))
    return bool(query_tokens and name_tokens and query_tokens.intersection(name_tokens))


def _strong_cache_search(
    query: str, limit: int, min_score: float = _STRONG_CACHE_MIN_SCORE
) -> tuple[list[engine.UnifiedFragrance], str | None]:
    """High-confidence cache search used to *bypass* live resolution entirely.

    Same sources as _cache_search_fallback and in the same source-of-truth
    order, but gated by the strict _STRONG_CACHE_MIN_SCORE floor: a cached row
    only short-circuits the live engine when it is a near-exact match for the
    query. The first non-empty source wins. Returns ([], None) when no source
    has a confident-enough hit, in which case the caller should run live search.
    """
    searches = (
        ("aggregate_db", _fragrance_record_search),
        ("db", _db_detail_cache_search),
        ("json", _json_cache_search),
        ("identity", _identity_cache_search),
    )
    for source, search_fn in searches:
        candidates = [
            item
            for item in search_fn(query, limit, min_score)
            if _strong_cache_candidate_ok(query, item, min_score)
        ]
        if candidates:
            return candidates, source
    return [], None


def _can_skip_live_search_with_cache(candidates: list[engine.UnifiedFragrance]) -> bool:
    """Only skip live resolution when cached rows preserve Fragrantica identity.

    Aggregate DB rows can be Basenotes-only. Serving those as a precheck hit
    turns a recoverable query into a degraded `live_search_skipped` response,
    so BN-only cache hits must fall through to the live/Serper resolver.
    """
    return any(bool(item.frag_url) for item in candidates)


# Background warm-cache revalidation. Bounded so a burst of warm hits cannot
# spawn unbounded scraper threads on Railway (see the memory leak note: the
# danger is orphaned processes, so we cap concurrency and dedupe by query).
_warm_refresh_lock = threading.Lock()
_warm_refresh_inflight: set[str] = set()
_WARM_REFRESH_MAX_INFLIGHT = 2


def _spawn_warm_refresh(query: str) -> bool:
    """Kick off a background live re-resolve for `query`, best-effort.

    Runs the same `search_once` the foreground path runs and persists the
    fresh rows, so the next request for `query` hits the strong-cache fast
    path instead of this warm tier. Bounded: at most `_WARM_REFRESH_MAX_INFLIGHT`
    concurrent refreshes, deduped by query. Daemon thread, reuses the shared
    scraper (no Chromium mint on the search path -> no orphaned process).
    Returns True when a refresh was actually started.
    """
    key = query.casefold()
    with _warm_refresh_lock:
        if key in _warm_refresh_inflight:
            return False
        if len(_warm_refresh_inflight) >= _WARM_REFRESH_MAX_INFLIGHT:
            return False
        _warm_refresh_inflight.add(key)

    def _run() -> None:
        try:
            scraper = engine.get_scraper()
            results = engine.search_once(scraper, query, _ARGS)
            results = [
                item for item in results if _candidate_has_display_identity(item)
            ]
            if results:
                _persist_search_results(query, results)
        except Exception:
            logger.warning(
                "warm-cache background refresh failed q=%r", query, exc_info=True
            )
        finally:
            with _warm_refresh_lock:
                _warm_refresh_inflight.discard(key)

    threading.Thread(
        target=_run, name="warm-cache-refresh", daemon=True
    ).start()
    return True


_FG_METRIC_GROUPS = ("performance_score", "value_score",
                     "community_interest_score", "wear_profile")


def _fg_metrics_complete(details: engine.UnifiedDetails) -> bool:
    """True when all 4 Fragrantica status-derived metric groups are present.

    Source of truth for 'the encrypted status payload made it through'. Reads
    the derived_metrics adapter's own source_coverage sub-dict so it is robust
    to whether a card came from status decode or an HTML fallback.
    """
    dm = getattr(details, "derived_metrics", None)
    if not isinstance(dm, dict):
        return False
    cov = dm.get("source_coverage") or {}
    return all(bool(cov.get(g)) for g in _FG_METRIC_GROUPS)


def _source_coverage(
    selected: engine.UnifiedFragrance,
    details: engine.UnifiedDetails,
    fragrantica_cached: bool = False,
    fragrantica_cache_source: str | None = None,
) -> dict[str, Any]:
    """Report which sources actually backed this detail bundle. Read-only.

    Railway's runtime is currently 403'd by Cloudflare on fragrantica.com, so a
    detail fetch routinely succeeds with Basenotes data only. Without this block
    the frontend cannot tell a dual-source result from a BN-only one -- they
    have the same shape. This derives coverage purely from what the engine
    returned; it fabricates nothing and recomputes no score.

    `*_linked` = a source URL was present on the candidate at all.
    `basenotes` / `fragrantica` = that source actually contributed detail data,
    judged by its unambiguous per-source field (`bn_consensus` for BN,
    `frag_cards` for FG). Parfinity is catalog-backed, so it is marked by URL
    presence and its actual payload is judged from notes/description/
    concentration. `fragrantica_cached` = the Fragrantica contribution came
    from fg_detail_cache_v1.json rather than a live fetch -- cached data is real
    parsed parser output, but it is labelled honestly as not-live.
    `derived_metrics` is "full" only when both sources contributed; "partial"
    when one did; "none" when the adapter returned null.
    """
    bn_linked = bool(selected.bn_url)
    parfinity_linked = bool(
        selected.frag_url and parfinity.is_parfinity_url(selected.frag_url)
    )
    fg_linked = bool(selected.frag_url) and not parfinity_linked
    bn_has_data = bn_linked and bool(details.bn_consensus)
    fg_has_data = fg_linked and bool(details.frag_cards)
    pf_has_data = parfinity_linked and bool(
        details.description
        or getattr(details, "concentration", None)
        or details.notes.top
        or details.notes.heart
        or details.notes.base
        or details.notes.flat
    )
    fg_complete = fg_has_data and _fg_metrics_complete(details)
    if details.derived_metrics is None:
        derived = "none"
    elif bn_has_data and (fg_complete or pf_has_data):
        derived = "full"
    else:
        derived = "partial"
    return {
        "basenotes": bn_has_data,
        "fragrantica": fg_has_data,
        "parfinity": parfinity_linked,
        "fragrantica_cached": fragrantica_cached,
        # True only when all 4 Fragrantica status-derived metric groups made it
        # through -- the frontend keys off this to decide whether to keep
        # refreshing a partial wardrobe tile. See `_fg_metrics_complete`.
        "fragrantica_metrics_complete": fg_complete,
        # "db" (durable Postgres cache), "json" (bundled file), or null (live or
        # absent). Additive honesty signal; existing keys keep their meaning.
        "fragrantica_cache_source": fragrantica_cache_source,
        "basenotes_linked": bn_linked,
        "fragrantica_linked": fg_linked,
        "parfinity_linked": parfinity_linked,
        "derived_metrics": derived,
        "complete": bn_has_data and (fg_has_data or pf_has_data),
    }


def _cache_entry_image_url(entry: dict[str, Any]) -> str:
    raw_identity = entry.get("raw_identity") or {}
    if not isinstance(raw_identity, dict):
        raw_identity = {}
    for key in ("image_url", "image", "thumbnail_url", "photo_url"):
        value = str(entry.get(key) or raw_identity.get(key) or "").strip()
        if value.startswith(("http://", "https://")):
            return value
    return ""


def _cache_entry_identity(entry: dict[str, Any], fg_url: str = "") -> dict[str, str]:
    """Best-effort identity from a DB/JSON cache entry or Fragrantica URL.

    Older cache rows were written when the FG search-card parser sometimes
    captured a truncated or wrong name (e.g. stored "Gabrielle" for the URL
    `Gabrielle-Essence-56076.html`, or "Allure Homme" for an Allure EDP URL).
    The URL slug is the canonical Fragrantica identity and is always correct
    by construction. When the stored name disagrees materially with the URL
    slug -- shorter and not a prefix-aligned subset -- we trust the URL and
    overwrite the stored value at read time. Writes are left alone; the
    enrichment pipeline is responsible for healing the row durably.
    """
    raw_identity = entry.get("raw_identity") or {}
    if not isinstance(raw_identity, dict):
        raw_identity = {}
    url = (
        fg_url
        or str(entry.get("canonical_fg_url") or "")
        or str(entry.get("fg_url") or "")
    )
    name = str(entry.get("name") or raw_identity.get("name") or "").strip()
    house = str(entry.get("house") or raw_identity.get("house") or "").strip()
    year = str(entry.get("year") or raw_identity.get("year") or "").strip()
    if url:
        url_name = engine.FragranticaEngine.name_from_url(url)
        url_house = engine.FragranticaEngine.brand_from_url(url)
        if not name and url_name:
            name = url_name
        elif name and url_name and _cache_name_is_suspect(name, url_name):
            name = url_name
        if not house and url_house:
            house = url_house
        elif house and url_house and house.strip().lower() == "unknown":
            house = url_house
    if name and house:
        name = engine.IdentityTools.strip_house_from_name(name, house)
    return {"name": name, "house": house, "year": year}


def _cache_name_is_suspect(stored: str, url_derived: str) -> bool:
    """True when the stored cache name should be overridden by the URL slug.

    The Fragrantica URL slug is the canonical, page-identity name; cache rows
    were written at search time from a card parser that occasionally captured
    a truncated or wrong title. Two failure shapes have to be caught:

      * Truncated qualifier: URL "Gabrielle-Essence" but stored "Gabrielle"
        -- same brand, different product (Chanel ships both). The stored name
        is a strict token-prefix of the URL slug, but the URL is more
        specific, so prefer the URL.
      * Wrong product entirely: URL "Allure-Eau-de-Parfum" but stored
        "Allure Homme" -- the URL is authoritative for what the page is.

    The unifying rule: when the URL slug has more tokens than the stored
    name, the URL is the more specific (and therefore safer) identity. If the
    stored name already has at least as many tokens, leave it alone -- it may
    carry nicer formatting (apostrophes, accents) the slug loses.
    """
    stored_clean = stored.strip().lower()
    url_clean = url_derived.strip().lower()
    if not stored_clean or not url_clean:
        return False
    if stored_clean == url_clean:
        return False
    # Normalize both sides identically so apostrophe/hyphen-only formatting
    # differences (e.g. "L'Air du Temps" vs URL slug "L Air Du Temps") do
    # not trigger an override -- they are the same identity, just different
    # punctuation, and stored carries the prettier formatting we want to keep.
    def _tokenize(value: str) -> list[str]:
        return [tok for tok in re.split(r"[^a-z0-9]+", value) if tok]
    stored_tokens = _tokenize(stored_clean)
    url_tokens = _tokenize(url_clean)
    if stored_tokens == url_tokens:
        return False
    return len(url_tokens) > len(stored_tokens)


def _fill_selected_identity(
    selected: engine.UnifiedFragrance, entry: dict[str, Any]
) -> None:
    """Fill blank request identity from a trusted cache entry."""
    identity = _cache_entry_identity(entry, selected.frag_url)
    if not selected.name and identity["name"]:
        selected.name = identity["name"]
    if not selected.brand and identity["house"]:
        selected.brand = identity["house"]
    if not selected.year and identity["year"]:
        selected.year = identity["year"]
    image_url = _cache_entry_image_url(entry)
    if image_url and not getattr(selected, "image_url", ""):
        setattr(selected, "image_url", image_url)


def _details_to_dict(
    selected: engine.UnifiedFragrance,
    details: engine.UnifiedDetails,
    fragrantica_cached: bool = False,
    fragrantica_cache_source: str | None = None,
    enrichment_status: str | None = None,
    enrichment_requested_count: int | None = None,
) -> dict[str, Any]:
    """Serialize a full detail bundle.

    `derived_metrics` is passed through exactly as derived_metrics_adapter
    produced it -- a dict on success, or `null` if the adapter raised. It is
    never fabricated.

    `source_coverage` is a read-only honesty signal: when Fragrantica is
    unreachable the response is BN-only, and the frontend must not present that
    as complete. `fragrantica_cached` flags when the Fragrantica contribution
    was hydrated from fg_detail_cache_v1.json. See `_source_coverage`.
    """
    engine.heal_missing_gender_and_year(selected, details)
    notes = details.notes
    concentration = getattr(details, "concentration", None)
    payload = {
        # Required top-level fields.
        "name": selected.name,
        "house": selected.brand,
        "year": _coerce_year(selected.year),
        "image_url": getattr(selected, "image_url", None),
        "gender": details.gender,
        "concentration": concentration,
        "derived_metrics": details.derived_metrics,
        "source_coverage": _source_coverage(
            selected, details, fragrantica_cached, fragrantica_cache_source
        ),
        # Raw fields, included for convenience (do not remove derived_metrics).
        "raw": {
            "description": details.description,
            "concentration": concentration,
            "bn_consensus": {
                k: list(v) for k, v in (details.bn_consensus or {}).items()
            },
            "frag_cards": details.frag_cards,
            "pros_cons": details.pros_cons,
            "reviews": [
                {"text": r.text, "source": r.source}
                for r in (details.reviews or [])
            ],
            "notes": {
                "has_pyramid": notes.has_pyramid,
                "top": notes.top,
                "heart": notes.heart,
                "base": notes.base,
                "flat": notes.flat,
            },
            "source_urls": {
                "bn_url": selected.bn_url,
                "frag_url": selected.frag_url,
            },
        },
    }
    if enrichment_status:
        enrichment_block: dict[str, Any] = {
            "status": enrichment_status,
            "requires_worker": enrichment_status == "pending",
            "cache_source": fragrantica_cache_source,
        }
        # Surface the durable retry counter so the frontend's bounded-retry cap
        # (MAX_ENRICHMENT_ATTEMPTS) can actually fire -- without this field it
        # reads `undefined` and a permanently-partial item polls forever.
        if enrichment_requested_count is not None:
            enrichment_block["requested_count"] = enrichment_requested_count
        payload["enrichment"] = enrichment_block
    return payload


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class DetailRequest(BaseModel):
    id: str | None = None
    source_url: str | None = None


class RequeueDetailRequest(DetailRequest):
    priority: int = 10


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> dict[str, bool]:
    """Liveness probe for Railway and the frontend."""
    return {"ok": True}


def _read_proc_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            raw = handle.read()
        return raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _matching_processes(needles: tuple[str, ...]) -> list[dict[str, Any]]:
    if os.name == "nt" or not os.path.isdir("/proc"):
        return []
    my_pid = os.getpid()
    rows: list[dict[str, Any]] = []
    lowered_needles = tuple(needle.lower() for needle in needles)
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        pid = int(name)
        if pid == my_pid:
            continue
        cmdline = _read_proc_cmdline(pid)
        if not cmdline:
            continue
        haystack = cmdline.lower()
        if any(needle in haystack for needle in lowered_needles):
            rows.append({"pid": pid, "cmdline": cmdline[:500]})
    return rows


def _proc_self_memory() -> dict[str, Any]:
    fields: dict[str, Any] = {}
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                if key in {"VmRSS", "VmHWM", "VmSize", "VmData", "Threads"}:
                    cleaned = value.strip()
                    fields[key] = cleaned
                    parts = cleaned.split()
                    if parts and parts[0].isdigit():
                        fields[f"{key}_kb"] = int(parts[0])
    except Exception:
        pass
    rss_kb = fields.get("VmRSS_kb")
    if isinstance(rss_kb, int):
        fields["VmRSS_mb"] = round(rss_kb / 1024, 1)
    return fields


@app.get("/api/diagnostics/memory")
def memory_diagnostics() -> dict[str, Any]:
    """Report API RSS and browser helper processes without shelling to procps."""
    chromium = _matching_processes(("chromium", "chrome"))
    xvfb = _matching_processes(("xvfb", "xvfb-run"))
    return {
        "pid": os.getpid(),
        "memory": _proc_self_memory(),
        "chromium_mint_disabled": bool(engine._chromium_mint_disabled()),
        "process_counts": {
            "chromium_or_chrome": len(chromium),
            "xvfb": len(xvfb),
        },
        "processes": {
            "chromium_or_chrome": chromium[:20],
            "xvfb": xvfb[:20],
        },
    }


# ---------------------------------------------------------------------------
# Runtime parity diagnostics (TEMPORARY -- remove after the Railway 403 cause
# is proven).
#
# Before committing to a cache-only or proxy architecture we must rule out the
# mundane causes of the Fragrantica 403: a dependency/runtime mismatch, the
# wrong start command/entrypoint, a bad working directory or cache path, or
# missing TLS/proxy env vars. This endpoint reports the deployed runtime's
# identity so it can be diffed field-by-field against a working local run.
# It is strictly read-only, reports env vars as presence booleans (never
# values), and touches no engine/parser/resolver/adapter code. Delete this
# block once parity is confirmed.
# ---------------------------------------------------------------------------

# Env vars whose *presence* (not value) is relevant to the 403 hypothesis.
_RUNTIME_ENV_KEYS = (
    "FG_CACHE_PATH",
    "FG_DETAIL_CACHE_PATH",
    "ALLOW_BUNDLED_FG_DETAIL_CACHE",
    "ALLOW_BUNDLED_FG_SEARCH_CACHE",
    "FRONTEND_ORIGINS",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_FILE",
)

# Packages whose version drift could explain a Cloudflare 403 on Railway but
# not locally (cloudscraper/requests/urllib3/certifi above all).
_RUNTIME_PACKAGES = (
    "cloudscraper",
    "requests",
    "urllib3",
    "certifi",
    "beautifulsoup4",
    "bs4",
    "lxml",
    "fastapi",
    "uvicorn",
)


def _git_sha() -> dict[str, Any]:
    """Best-effort git commit SHA of the running code.

    Railway injects RAILWAY_GIT_COMMIT_SHA even when the build image carries no
    .git directory, so prefer that; fall back to `git rev-parse HEAD`.
    """
    for var in ("RAILWAY_GIT_COMMIT_SHA", "SOURCE_VERSION", "GIT_COMMIT"):
        val = os.environ.get(var)
        if val:
            return {"sha": val.strip(), "source": var}
    try:
        import subprocess

        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if out.returncode == 0 and out.stdout.strip():
            return {"sha": out.stdout.strip(), "source": "git rev-parse"}
        return {"sha": None, "source": None, "detail": out.stderr.strip()[:200]}
    except Exception as exc:
        return {"sha": None, "source": None, "detail": f"{type(exc).__name__}: {exc}"}


def _process_start_command() -> list[str]:
    """The argv this process was actually started with.

    On Railway (Linux) /proc/self/cmdline is the real exec line -- it reveals an
    accidental override of the intended `uvicorn api:app ...` start command.
    Everywhere else, fall back to sys.argv.
    """
    try:
        with open("/proc/self/cmdline", "rb") as handle:
            raw = handle.read()
        parts = [p.decode("utf-8", "replace") for p in raw.split(b"\x00") if p]
        if parts:
            return parts
    except Exception:
        pass
    import sys as _sys

    return list(_sys.argv)


def _pkg_version(name: str) -> str | None:
    """Installed version of a package, or None if it is not installed here."""
    try:
        from importlib.metadata import PackageNotFoundError, version

        try:
            return version(name)
        except PackageNotFoundError:
            return None
    except Exception:
        return None


def _cache_file_state(path: str) -> dict[str, Any]:
    """Resolve a configured cache path and report whether it exists on disk."""
    if not path:
        return {"configured": False, "path": None, "exists": False}
    abspath = os.path.abspath(path)
    exists = os.path.isfile(abspath)
    return {
        "configured": True,
        "path": abspath,
        "exists": exists,
        "size_bytes": os.path.getsize(abspath) if exists else None,
    }


@app.get("/api/diagnostics/runtime")
def runtime_diagnostics() -> dict[str, Any]:
    """Report the deployed runtime's identity for parity comparison vs. local.

    Temporary, read-only. Env vars are reported as presence booleans only --
    never their values. Covers git SHA, cwd, Python/platform, the actual start
    command, PORT, cache-file existence, pinned package versions, and a config
    sanity block confirming Railway is running the intended app/entrypoint.
    """
    import platform as _platform
    import sys as _sys

    cwd = os.getcwd()

    env_present = {
        key: (os.environ.get(key) not in (None, "")) for key in _RUNTIME_ENV_KEYS
    }

    # Cache-file existence: report both the env-configured path and the path the
    # engine actually reads (_ARGS.fg_cache), since they can diverge.
    fg_cache_path = os.environ.get("FG_CACHE_PATH", "")
    fg_detail_cache_path = os.environ.get("FG_DETAIL_CACHE_PATH", "")
    cache_files = {
        "FG_CACHE_PATH": _cache_file_state(fg_cache_path),
        "FG_DETAIL_CACHE_PATH": _cache_file_state(fg_detail_cache_path),
        "engine_effective_fg_cache": _cache_file_state(
            getattr(_ARGS, "fg_cache", "") or ""
        ),
    }

    packages = {name: _pkg_version(name) for name in _RUNTIME_PACKAGES}

    # Config sanity -- is Railway running the intended app, entrypoint, and
    # committed code? The deployed SHA here must be diffed against the latest
    # pushed commit; a Railway build/start-command override or stale Procfile
    # would surface as a start_command that is not `uvicorn api:app ...`.
    config_checks = {
        "entrypoint_module": __name__,
        "api_module_file": os.path.abspath(__file__),
        "procfile_present": os.path.isfile(os.path.join(cwd, "Procfile")),
        "railway_toml_present": os.path.isfile(os.path.join(cwd, "railway.toml")),
        "requirements_txt_present": os.path.isfile(
            os.path.join(cwd, "requirements.txt")
        ),
        "railway_service_id": os.environ.get("RAILWAY_SERVICE_ID"),
        "railway_deployment_id": os.environ.get("RAILWAY_DEPLOYMENT_ID"),
        "railway_git_branch": os.environ.get("RAILWAY_GIT_BRANCH"),
        "railway_git_commit_message": os.environ.get("RAILWAY_GIT_COMMIT_MESSAGE"),
        "database_enabled": db.ENABLED,
        "allow_bundled_fg_detail_cache": _ALLOW_BUNDLED_FG_DETAIL_CACHE,
        "allow_bundled_fg_search_cache": _ALLOW_BUNDLED_FG_SEARCH_CACHE,
    }

    scraper = engine.get_scraper()
    return {
        "git": _git_sha(),
        "cwd": cwd,
        "python_version": _sys.version,
        "python_executable": _sys.executable,
        "platform": _platform.platform(),
        "platform_detail": {
            "system": _platform.system(),
            "release": _platform.release(),
            "machine": _platform.machine(),
        },
        "start_command": _process_start_command(),
        "port": os.environ.get("PORT"),
        "env_present": env_present,
        "cache_files": cache_files,
        "package_versions": packages,
        "config_checks": config_checks,
        "scraper": {
            "type": type(scraper).__module__ + "." + type(scraper).__name__,
            "cloudscraper_active": engine.cloudscraper is not None,
        },
    }


# ---------------------------------------------------------------------------
# Upstream reachability diagnostics (TEMPORARY -- remove after proxy scoping).
#
# The deployed runtime resolves zero Fragrantica links, but the engine reaches
# Fragrantica two ways: direct fragrantica.com fetches AND site:fragrantica.com
# lookups via Google/Bing (see QueryRepair._search_url / FragranticaEngine).
# Before scoping an outbound proxy we need to know *which* host group Railway
# is blocked on. This endpoint probes each host directly using the engine's own
# scraper and headers -- it is read-only and changes no engine/parser/resolver
# behavior. Delete this block once the proxy scope decision is made.
# ---------------------------------------------------------------------------

_PROBE_TARGETS: dict[str, str] = {
    "google": (
        "https://www.google.com/search?q=silver+mountain+water+"
        "site:fragrantica.com/perfume"
    ),
    "bing": (
        "https://www.bing.com/search?q=silver+mountain+water+"
        "site:fragrantica.com/perfume"
    ),
    "fragrantica_home": "https://www.fragrantica.com/",
    "fragrantica_perfume": (
        "https://www.fragrantica.com/perfume/Creed/"
        "Silver-Mountain-Water-1517.html"
    ),
}

_CF_CHALLENGE_MARKERS = (
    "just a moment",
    "cf-challenge",
    "attention required",
    "cf-browser-verification",
    "/cdn-cgi/challenge-platform",
)

# A probe whose verdict is none of these is treated as "the host answered us".
_UNREACHABLE_VERDICTS = ("unreachable", "blocked", "cloudflare_challenge")

# The engine's own search-engine call budget (Http.get timeout= at the
# QueryRepair site-search call sites). A host can be fully reachable yet still
# fail every engine fetch if the round-trip exceeds this on Railway.
_ENGINE_SEARCH_BUDGET_MS = 850


# Header names whose values must never be echoed back -- presence only.
_SENSITIVE_HEADER_KEYS = ("cookie", "authorization", "proxy-authorization")


def _sanitize_headers(headers: Any) -> dict[str, str]:
    """Echo request headers for parity comparison, redacting sensitive values."""
    out: dict[str, str] = {}
    try:
        items = dict(headers).items()
    except Exception:
        return out
    for key, value in items:
        if str(key).lower() in _SENSITIVE_HEADER_KEYS:
            out[str(key)] = "<set>" if value else "<empty>"
        else:
            out[str(key)] = str(value)
    return out


def _classify_probe(url: str) -> dict[str, Any]:
    """Probe one upstream host and classify reachability. Read-only.

    Returns enough request/response surface to compare Railway vs. local: the
    scraper type, the exact (sanitized) headers actually sent, the final URL
    after redirects, the status, Cloudflare markers, the names of any cookies
    the host set, and the first 300 chars of the body when the host blocked us.
    """
    import time as _time

    scraper = engine.get_scraper()
    scraper_type = type(scraper).__module__ + "." + type(scraper).__name__
    headers = dict(engine.Http.DEFAULT_HEADERS)
    started = _time.monotonic()
    try:
        res = scraper.get(url, timeout=10, headers=headers)
    except Exception as exc:
        return {
            "url": url,
            "scraper_type": scraper_type,
            "request_headers": _sanitize_headers(headers),
            "verdict": "unreachable",
            "detail": f"{type(exc).__name__}: {exc}",
            "elapsed_ms": int((_time.monotonic() - started) * 1000),
        }
    elapsed_ms = int((_time.monotonic() - started) * 1000)
    body = res.text or ""
    cf_challenge = any(m in body[:4000].lower() for m in _CF_CHALLENGE_MARKERS)
    if res.status_code in (403, 429) or (res.status_code == 503 and cf_challenge):
        verdict = "blocked"
    elif cf_challenge:
        verdict = "cloudflare_challenge"
    elif res.status_code == 200:
        verdict = "ok"
    else:
        verdict = f"http_{res.status_code}"  # e.g. 404 still means reachable
    # A host can answer fine here (10s budget) yet still fail every engine
    # fetch, because the engine's site-search calls only wait ~0.75-0.85s.
    too_slow_for_engine = (
        verdict not in _UNREACHABLE_VERDICTS
        and elapsed_ms > _ENGINE_SEARCH_BUDGET_MS
    )
    blocked = verdict in ("blocked", "cloudflare_challenge")
    # The headers actually put on the wire (cloudscraper injects its own UA /
    # ordering) -- this is what must be diffed against a working local run.
    sent_headers = _sanitize_headers(getattr(res.request, "headers", headers))
    resp_header_keys = {k.lower() for k in res.headers}
    return {
        "url": url,
        "scraper_type": scraper_type,
        "request_headers": sent_headers,
        "final_url": getattr(res, "url", url),
        "verdict": verdict,
        "http_status": res.status_code,
        "body_length": len(body),
        "cloudflare_challenge": cf_challenge,
        "cf_ray_present": "cf-ray" in resp_header_keys,
        "response_server_header": res.headers.get("Server"),
        "cookies_set": sorted(res.cookies.keys()),
        "body_preview_when_blocked": body[:300] if blocked else None,
        "elapsed_ms": elapsed_ms,
        "too_slow_for_engine_budget": too_slow_for_engine,
    }


@app.get("/api/diagnostics/upstream")
def upstream_diagnostics() -> dict[str, Any]:
    """Probe every upstream host the engine depends on. Temporary; read-only.

    Determines whether Railway is blocked on Fragrantica directly, on the
    Google/Bing site-search step, or both -- which decides the proxy scope.
    Runs up to four sequential 10s probes, so it can take ~40s worst case.
    """
    probes = {name: _classify_probe(url) for name, url in _PROBE_TARGETS.items()}

    # Which scraper the engine actually built on this host. If cloudscraper
    # failed to install on Railway, get_scraper() silently returns a plain
    # requests.Session with no Cloudflare bypass -- that alone produces fg=""
    # while Basenotes still works, and it is NOT an IP block.
    scraper = engine.get_scraper()
    scraper_type = type(scraper).__module__ + "." + type(scraper).__name__
    cloudscraper_active = engine.cloudscraper is not None

    def _reachable(name: str) -> bool:
        return probes[name]["verdict"] not in _UNREACHABLE_VERDICTS

    fg_ok = _reachable("fragrantica_home") or _reachable("fragrantica_perfume")
    search_ok = _reachable("google") or _reachable("bing")
    any_too_slow = any(p.get("too_slow_for_engine_budget") for p in probes.values())

    if not cloudscraper_active:
        recommendation = (
            "cloudscraper is NOT installed on this host -- get_scraper() fell "
            "back to a plain requests.Session with no Cloudflare bypass. This "
            "alone explains fg=\"\" and is a BUILD problem, not an IP block. "
            "Fix the Railway build (verify cloudscraper in requirements.txt "
            "actually installed) before considering a proxy."
        )
    elif fg_ok and search_ok and any_too_slow:
        recommendation = (
            "All upstreams answered, but at least one round-trip exceeded the "
            "engine's ~0.85s site-search budget. The engine may be timing out "
            "fetches that are reachable -- this is a latency/timeout issue, not "
            "an IP block. Re-check before adding a proxy."
        )
    elif fg_ok and search_ok:
        recommendation = (
            "All upstreams reachable from Railway and within the engine's "
            "timeout budget -- the zero-FG-link result is not a raw "
            "connectivity block. Re-check the engine run / deadlines."
        )
    elif not fg_ok and not search_ok:
        recommendation = (
            "Both Fragrantica and the search engines are blocked -- the proxy "
            "must cover fragrantica.com AND google.com/bing.com."
        )
    elif not fg_ok:
        recommendation = (
            "Fragrantica is blocked, search engines are reachable -- the proxy "
            "can be scoped to fragrantica.com only."
        )
    else:
        recommendation = (
            "Search engines are blocked, Fragrantica is reachable -- the proxy "
            "must cover google.com/bing.com (the site-search step)."
        )

    return {
        "probes": probes,
        "summary": {
            "scraper_type": scraper_type,
            "cloudscraper_active": cloudscraper_active,
            "fragrantica_reachable": fg_ok,
            "search_engines_reachable": search_ok,
            "any_host_too_slow_for_engine_budget": any_too_slow,
        },
        "recommendation": recommendation,
    }


# ---------------------------------------------------------------------------
# Controlled fetch variants (TEMPORARY -- remove after the Railway 403 cause is
# proven).
#
# /upstream tells us *whether* a host blocks us; this tells us *what about the
# request* the block depends on. The same Fragrantica perfume URL is fetched
# five ways. If all five fail identically, the block is request-independent --
# i.e. a raw IP/reputation block. If one variant succeeds (a modern UA, a warmed
# session, session reuse, a plain requests vs. cloudscraper difference), the 403
# is request-shaped and a proxy is not the only path. Read-only: builds throw-
# away sessions, mutates no engine state. Delete this block once decided.
# ---------------------------------------------------------------------------

_VARIANT_FG_URL = _PROBE_TARGETS["fragrantica_perfume"]
_VARIANT_FG_HOME = _PROBE_TARGETS["fragrantica_home"]

# A current-ish desktop Chrome UA, distinct from engine Http.DEFAULT_HEADERS'
# Chrome/120 string -- isolates "stale User-Agent" as a cause.
_MODERN_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _run_variant(label: str, fetch: Any) -> dict[str, Any]:
    """Run one fetch variant and classify its outcome. Read-only."""
    import time as _time

    started = _time.monotonic()
    try:
        res = fetch()
    except Exception as exc:
        return {
            "variant": label,
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_ms": int((_time.monotonic() - started) * 1000),
        }
    elapsed_ms = int((_time.monotonic() - started) * 1000)
    body = res.text or ""
    cf = any(m in body[:4000].lower() for m in _CF_CHALLENGE_MARKERS)
    blocked = res.status_code in (403, 429) or (res.status_code == 503 and cf)
    return {
        "variant": label,
        "http_status": res.status_code,
        "body_length": len(body),
        "cloudflare_challenge": cf,
        "blocked": blocked,
        "final_url": getattr(res, "url", _VARIANT_FG_URL),
        "cookies_set": sorted(res.cookies.keys()),
        "server_header": res.headers.get("Server"),
        "cf_ray_present": "cf-ray" in {k.lower() for k in res.headers},
        "body_preview": body[:300] if (blocked or cf) else None,
        "elapsed_ms": elapsed_ms,
    }


@app.get("/api/diagnostics/fetch-variants")
def fetch_variant_diagnostics() -> dict[str, Any]:
    """Fetch one Fragrantica perfume page five ways to localize the 403 cause.

    Temporary, read-only. Variants: (1) cloudscraper + engine default headers,
    (2) cloudscraper + explicit modern Chrome UA, (3) plain requests + modern
    UA (no Cloudflare bypass), (4) cloudscraper warmed via the homepage first,
    (5) cloudscraper shared session reused (second hit on the same session).
    """
    import requests as _requests

    base_headers = dict(engine.Http.DEFAULT_HEADERS)
    results: list[dict[str, Any]] = []

    # 1. cloudscraper session, engine's own default headers (the live path).
    def _v1():
        return engine.get_scraper().get(
            _VARIANT_FG_URL, timeout=15, headers=base_headers
        )

    results.append(_run_variant("cloudscraper_default", _v1))

    # 2. cloudscraper session, explicit modern Chrome UA.
    def _v2():
        headers = dict(base_headers)
        headers["User-Agent"] = _MODERN_UA
        return engine.get_scraper().get(
            _VARIANT_FG_URL, timeout=15, headers=headers
        )

    results.append(_run_variant("cloudscraper_modern_ua", _v2))

    # 3. plain requests, same modern UA -- no Cloudflare bypass at all.
    def _v3():
        headers = dict(base_headers)
        headers["User-Agent"] = _MODERN_UA
        return _requests.Session().get(
            _VARIANT_FG_URL, timeout=15, headers=headers
        )

    results.append(_run_variant("plain_requests_modern_ua", _v3))

    # 4. cloudscraper, warmed: hit the homepage first, then the perfume page.
    def _v4():
        scraper = engine.get_scraper()
        scraper.get(_VARIANT_FG_HOME, timeout=15, headers=base_headers)
        return scraper.get(_VARIANT_FG_URL, timeout=15, headers=base_headers)

    results.append(_run_variant("cloudscraper_warm_via_homepage", _v4))

    # 5. shared cloudscraper session, reused: second hit on the same session
    #    (contrast with variant 1's fresh-session-per-request).
    def _v5():
        scraper = engine.get_scraper()
        scraper.get(_VARIANT_FG_URL, timeout=15, headers=base_headers)
        return scraper.get(_VARIANT_FG_URL, timeout=15, headers=base_headers)

    results.append(_run_variant("cloudscraper_shared_session_reuse", _v5))

    ok = [r for r in results if r.get("http_status") == 200 and not r.get("cloudflare_challenge")]
    errored = [r for r in results if "error" in r]
    if errored and not ok:
        interpretation = (
            "Every variant errored or was blocked. Combined with /upstream, this "
            "points at a request-independent block (datacenter IP / reputation) "
            "or a transport failure -- not a header/session/UA issue."
        )
    elif not ok:
        interpretation = (
            "All five variants were blocked (403/429/Cloudflare challenge) but "
            "none errored at the transport layer. The block does not depend on "
            "UA, session warming, or session reuse -- consistent with an IP/"
            "reputation block. A proxy is the likely path; re-confirm with "
            "/runtime that this is not a cloudscraper version regression."
        )
    elif len(ok) == len(results):
        interpretation = (
            "All five variants succeeded. The 403 seen by the engine is NOT a "
            "raw fetch block -- look at engine state/deadlines or intermittent "
            "upstream behavior, not a proxy."
        )
    else:
        succeeded = ", ".join(sorted(r["variant"] for r in ok))
        interpretation = (
            f"The 403 is request-shaped: variant(s) [{succeeded}] succeeded "
            f"while others were blocked. The fix is in the request "
            f"(UA/session/warming), NOT necessarily a proxy."
        )

    return {
        "target_url": _VARIANT_FG_URL,
        "cloudscraper_active": engine.cloudscraper is not None,
        "variants": results,
        "interpretation": interpretation,
    }


def _search_diagnostics(results: list[engine.UnifiedFragrance]) -> dict[str, Any]:
    """Report Fragrantica coverage so a degraded run is never silently 'complete'.

    The serialization path here is correct -- `_encode_id` reads `item.frag_url`,
    the same attribute the engine populates. When the emitted token still has
    `fg = ""` it is because the engine's candidate itself carries no `frag_url`,
    i.e. the Fragrantica search returned zero links for the query.

    Locally every real query resolves Fragrantica links; a deployed run that
    links *none* across *all* candidates almost always means the runtime cannot
    reach Fragrantica (datacenter IP blocked by Cloudflare). That is not a
    successful search, so we flag it explicitly instead of returning a BN-only
    payload that looks complete.
    """
    total = len(results)
    fg_linked = sum(1 for item in results if item.frag_url)
    fragrantica_unreachable = total > 0 and fg_linked == 0
    diag: dict[str, Any] = {
        "result_count": total,
        "fragrantica_linked_count": fg_linked,
        "fragrantica_unreachable": fragrantica_unreachable,
    }
    if fragrantica_unreachable:
        diag["warning"] = (
            "No candidate resolved a Fragrantica URL. The engine's Fragrantica "
            "search returned zero links -- the deployed runtime is most likely "
            "unable to reach Fragrantica (datacenter IP blocked). Detail "
            "responses for these ids will be BN-only; this is a degraded "
            "result, not a complete one."
        )
    return diag


@app.get("/api/fragrances/search")
def search(
    q: str = Query(..., min_length=1, description="Fragrance search query"),
) -> dict[str, Any]:
    """Run the engine's dual-source resolver and return ranked candidates."""
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query 'q' must not be empty.")

    # Strong cache precheck. Before constructing a scraper or running the live
    # resolver, see if any warmed cache holds a near-exact hit for this query.
    # _strong_cache_search uses the strict _STRONG_CACHE_MIN_SCORE floor, so a
    # match here is high-confidence enough to skip live search entirely -- this
    # is the fast path that answers common cached queries in tens of ms instead
    # of waiting behind the engine's first-pass timeout budget.
    strong_results, strong_source = _strong_cache_search(query, _ARGS.max_results)
    strong_results = [
        item for item in strong_results if _candidate_has_display_identity(item)
    ]
    cache_disqualified_reasons: list[str] = []
    if strong_results and _can_skip_live_search_with_cache(strong_results):
        diagnostics = _search_diagnostics(strong_results)
        diagnostics["cache_source"] = strong_source
        diagnostics["cache_mode"] = "precheck"
        diagnostics["live_search_skipped"] = True
        return {
            "query": query,
            "results": [_search_result_to_dict(item) for item in strong_results],
            "diagnostics": diagnostics,
        }
    if strong_results:
        cache_disqualified_reasons.append("precheck_missing_fragrantica_url")

    # Warm-cache tier. No strong hit, but a slightly-below-strong cached row is
    # still a confident identity match. Serve it now -- no live wait -- and
    # revalidate live in the background so the next identical query promotes to
    # the strong fast path above. This is the stale-while-revalidate bridge
    # between a tens-of-ms precheck hit and a full multi-second live search.
    warm_results, warm_source = _strong_cache_search(
        query, _ARGS.max_results, min_score=_WARM_CACHE_MIN_SCORE
    )
    warm_results = [
        item for item in warm_results if _candidate_has_display_identity(item)
    ]
    if warm_results and _can_skip_live_search_with_cache(warm_results):
        refreshing = _spawn_warm_refresh(query)
        diagnostics = _search_diagnostics(warm_results)
        diagnostics["cache_source"] = warm_source
        diagnostics["cache_mode"] = "warm"
        diagnostics["live_search_skipped"] = True
        # True when a live re-resolve is now running; the client may choose to
        # re-query shortly to pick up fresher breadth. Never blocks this response.
        diagnostics["background_refresh"] = refreshing
        return {
            "query": query,
            "results": [_search_result_to_dict(item) for item in warm_results],
            "diagnostics": diagnostics,
        }
    if warm_results:
        cache_disqualified_reasons.append("warm_missing_fragrantica_url")

    scraper = engine.get_scraper()
    # `timing` is filled in place by the engine with per-stage wall-clock marks
    # (initial_fg_bn_fetch, link_attach, ...). Surfaced under
    # diagnostics.timing so latency can be attributed to a stage instead of
    # inferred from stdout. Cheap to collect; always on.
    timing: dict[str, Any] = {}
    try:
        results = engine.search_once(scraper, query, _ARGS, timing=timing)
    except Exception as exc:  # engine degrades cleanly; surface a 502 if not
        raise HTTPException(status_code=502, detail=f"Search failed: {exc}") from exc

    fallback_source: str | None = None
    if not results:
        results, fallback_source = _cache_search_fallback(query, _ARGS.max_results)

    # Final wire-level guard: if a candidate still has no recoverable identity
    # after URL-based backfills, suppress it instead of surfacing a broken
    # "House unavailable" row to the client.
    results = [item for item in results if _candidate_has_display_identity(item)]
    if fallback_source != "aggregate_db":
        _persist_search_results(query, results)

    diagnostics = _search_diagnostics(results)
    if cache_disqualified_reasons:
        diagnostics["cache_fast_path_disqualified"] = cache_disqualified_reasons
    if timing:
        diagnostics["timing"] = timing
    if fallback_source:
        diagnostics["fallback_source"] = fallback_source
        fallback_label = (
            "identity cache" if fallback_source == "identity" else f"{fallback_source} detail cache"
        )
        diagnostics["warning"] = (
            "Live providers returned zero candidates; results came from the "
            f"{fallback_label}."
        )

    return {
        "query": query,
        "results": [_search_result_to_dict(item) for item in results],
        "diagnostics": diagnostics,
    }


# ---------------------------------------------------------------------------
# Engine-search diagnostics (TEMPORARY -- remove after the fg="" cause is found).
#
# /api/diagnostics/upstream proved every upstream host is reachable from Railway
# and within the engine's timeout budget, yet the public /search still emits
# candidates with frag_url="". This endpoint runs the EXACT same code path as
# the public /search endpoint -- same `_ARGS`, same `get_scraper()`, same
# `engine.search_once(...)` -- but instead of serializing to the frontend
# contract it dumps the raw final candidate fields plus whatever the engine
# printed to stdout. It mutates nothing and re-runs no resolver logic. Delete
# this block once the exact point where frag_url disappears is identified.
# ---------------------------------------------------------------------------

# Strips ANSI colour codes the engine wraps its [SYS] log lines in.
_ANSI_RE = __import__("re").compile(r"\x1b\[[0-9;]*m")

# The engine's stdout line emitted by _search_core right after the first pass:
#   [SYS] First-pass resolver: {X} + {Y} candidate links.
# Group(1) is the Basenotes-side count, group(2) is the Fragrantica-side count
# (order preserved from the engine's emission order).
_FIRST_PASS_COUNT_RE = __import__("re").compile(
    r"First-pass resolver:\s*(\d+)\s*\+\s*(\d+)\s+candidate links"
)

# _ARGS fields that govern search timing / deadlines / budgets. Surfaced so a
# Railway-vs-local args drift (esp. shorter deadlines) is immediately visible.
_SEARCH_TIMING_ARG_KEYS = (
    "initial_timeout",
    "fg_timeout",
    "detail_timeout",
    "metrics_budget",
    "metrics_workers",
    "metrics_max",
    "catalog_budget",
    "catalog_workers",
    "catalog_slug_limit",
    "related_budget",
    "related_page_timeout",
    "related_max_pages",
    "external_search",
    "external_budget",
    "external_workers",
    "spell_repair_budget",
    "max_frag_results",
    "max_results",
)


def _engine_candidate_to_diag(item: engine.UnifiedFragrance) -> dict[str, Any]:
    """Dump the raw final candidate fields -- no contract reshaping, no scoring."""
    return {
        "name": item.name,
        "house": item.brand,
        "year": item.year,
        "bn_url": item.bn_url,
        "frag_url": item.frag_url,
        # The public _search_result_to_dict computes source_url the same way.
        "source_url": item.frag_url or item.bn_url,
        "match_score": getattr(item, "query_score", None),
        "resolver_score": getattr(item, "resolver_score", None),
        "resolver_source": getattr(item, "resolver_source", None),
        "bn_positive_pct": getattr(item, "bn_positive_pct", None),
        "bn_vote_count": getattr(item, "bn_vote_count", None),
    }


@app.get("/api/diagnostics/engine-search")
def engine_search_diagnostics(
    q: str = Query("silver mountain water", min_length=1),
) -> dict[str, Any]:
    """Run the public /search code path verbatim and dump raw candidates.

    Temporary, read-only. Same `_ARGS`, same `get_scraper()`, same
    `engine.search_once(...)` as `/api/fragrances/search` -- the only
    difference is this returns the engine's raw candidate fields and captured
    stdout instead of the frontend contract shape.
    """
    import contextlib
    import io

    query = q.strip()

    # Same scraper construction as the public endpoint.
    scraper = engine.get_scraper()
    scraper_type = type(scraper).__module__ + "." + type(scraper).__name__

    # Capture the engine's stdout so the [SYS] log lines (incl. the first-pass
    # candidate counts) are visible without touching resolver internals.
    captured = io.StringIO()
    error: str | None = None
    results: list[engine.UnifiedFragrance] = []
    try:
        with contextlib.redirect_stdout(captured):
            results = engine.search_once(scraper, query, _ARGS)
    except Exception as exc:  # mirror the public endpoint's failure surface
        error = f"{type(exc).__name__}: {exc}"

    sys_log = _ANSI_RE.sub("", captured.getvalue())
    sys_lines = [ln for ln in sys_log.splitlines() if "[SYS]" in ln]

    # Pull the first-pass candidate counts straight out of the captured log
    # line -- this reads no resolver internals, only what the engine printed.
    bn_native_count: int | None = None
    fg_native_count: int | None = None
    count_match = _FIRST_PASS_COUNT_RE.search(sys_log)
    if count_match:
        bn_native_count = int(count_match.group(1))
        fg_native_count = int(count_match.group(2))

    result_dicts = [_engine_candidate_to_diag(item) for item in results]
    any_frag_url = any(item.frag_url for item in results)

    all_args = vars(_ARGS)
    timing_args = {
        k: all_args.get(k) for k in _SEARCH_TIMING_ARG_KEYS if k in all_args
    }

    return {
        "query": query,
        "query_repr": repr(query),  # exposes any hidden normalization/whitespace
        "scraper_type": scraper_type,
        "cloudscraper_active": engine.cloudscraper is not None,
        "args_all": {k: _jsonable(v) for k, v in all_args.items()},
        "args_search_timing": timing_args,
        "result_count": len(results),
        "any_frag_url": any_frag_url,
        # Counts lifted from the captured [SYS] line, not from resolver internals.
        "bn_native_link_count": bn_native_count,
        "fg_native_link_count": fg_native_count,
        # merged candidate count == search_once's returned list length.
        "merged_candidate_count": len(results),
        "results": result_dicts,
        "sys_log_lines": sys_lines,
        "error": error,
    }


def _bn_diag_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _bn_diag_elapsed_ms(started: float) -> int:
    import time as _time

    return int((_time.monotonic() - started) * 1000)


def _bn_diag_response_length(res: Any) -> int | None:
    content = getattr(res, "content", None)
    if content is not None:
        try:
            return len(content)
        except Exception:
            pass
    text = getattr(res, "text", None)
    if text is not None:
        try:
            return len(text)
        except Exception:
            pass
    return None


def _bn_diag_response_text(res: Any) -> str:
    text = getattr(res, "text", None)
    if isinstance(text, str):
        return text
    content = getattr(res, "content", None)
    if isinstance(content, bytes):
        return content.decode("utf-8", errors="replace")
    return ""


def _bn_diag_headers(res: Any) -> Any:
    headers = getattr(res, "headers", {}) or {}
    return headers


def _bn_diag_cf_ray_present(headers: Any) -> bool:
    try:
        return "cf-ray" in {str(key).lower() for key in headers}
    except Exception:
        return False


def _bn_diag_package_version(distribution: str, module_obj: Any) -> str | None:
    try:
        from importlib.metadata import PackageNotFoundError, version

        try:
            return version(distribution)
        except PackageNotFoundError:
            module_version = getattr(module_obj, "__version__", None)
            return str(module_version) if module_version is not None else None
    except Exception:
        module_version = getattr(module_obj, "__version__", None)
        return str(module_version) if module_version is not None else None


def _bn_diag_imports() -> dict[str, dict[str, Any]]:
    cloud_module = getattr(engine, "cloudscraper", None)
    curl_module = getattr(engine, "curl_requests", None)
    drission_available = (
        getattr(engine, "ChromiumOptions", None) is not None
        and getattr(engine, "ChromiumPage", None) is not None
    )
    drission_module = None
    drission_driver_module = None
    if drission_available:
        try:
            drission_module = __import__("DrissionPage")
        except Exception:
            drission_module = None
        try:
            import DrissionPage._base.driver as drission_driver_module  # type: ignore[import-not-found]
        except Exception:
            drission_driver_module = None

    return {
        "cloudscraper": {
            "available": cloud_module is not None,
            "version": _bn_diag_package_version("cloudscraper", cloud_module),
        },
        "curl_cffi": {
            "available": curl_module is not None,
            "version": _bn_diag_package_version("curl_cffi", curl_module),
        },
        "DrissionPage": {
            "available": drission_available,
            "version": _bn_diag_package_version("DrissionPage", drission_module),
            "force_origin_env": os.environ.get("DRISSION_FORCE_ORIGIN") or None,
            "origin_patch_active": bool(
                getattr(engine, "_DRISSION_ORIGIN_PATCH_ACTIVE", False)
            ),
            "driver_create_connection_patched": bool(
                drission_driver_module is not None
                and getattr(
                    getattr(drission_driver_module, "create_connection", None),
                    "_srt_forces_origin",
                    False,
                )
            ),
            "last_ws_connect": getattr(engine, "_DRISSION_LAST_WS_CONNECT", None),
        },
    }


def _bn_diag_default_scraper() -> Any:
    if getattr(engine, "cloudscraper", None) is not None:
        return engine.cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    return engine.requests.Session()


def _bn_diag_chromium_binary() -> dict[str, Any]:
    import shutil
    import subprocess

    names = [
        "chromium",
        "chromium-browser",
        "google-chrome",
        "google-chrome-stable",
        "chrome",
    ]
    found = None
    for name in names:
        found = shutil.which(name)
        if found:
            break

    env_path = os.environ.get("BASENOTES_CHROMIUM_PATH", "").strip() or None
    binary = env_path or found

    version_out: str | None = None
    version_err: str | None = None
    version_returncode: int | None = None
    headless_out: str | None = None
    headless_err: str | None = None
    headless_returncode: int | None = None
    spawn_error: str | None = None

    if binary:
        try:
            proc = subprocess.run(
                [binary, "--version"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            version_out = (proc.stdout or "").strip()[:500]
            version_err = (proc.stderr or "").strip()[:500]
            version_returncode = proc.returncode
        except Exception as exc:
            spawn_error = f"{type(exc).__name__}: {exc}"

        try:
            proc = subprocess.run(
                [
                    binary,
                    "--headless=new",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--user-data-dir=/tmp/chromium-diag",
                    "--dump-dom",
                    "about:blank",
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )
            headless_out = (proc.stdout or "")[:500]
            headless_err = (proc.stderr or "")[:1500]
            headless_returncode = proc.returncode
        except Exception as exc:
            if spawn_error is None:
                spawn_error = f"{type(exc).__name__}: {exc}"

    return {
        "which": found,
        "env_path": env_path,
        "binary_used_for_probe": binary,
        "tried": names,
        "version_returncode": version_returncode,
        "version_stdout": version_out,
        "version_stderr": version_err,
        "headless_dump_returncode": headless_returncode,
        "headless_dump_stdout_first_500": headless_out,
        "headless_dump_stderr_first_1500": headless_err,
        "spawn_error": spawn_error,
    }


def _bn_diag_kill_orphan_chromiums() -> dict[str, Any]:
    """Kill any leftover chromium processes from previous failed mint attempts.

    DrissionPage's mint raises during ChromiumPage(options) construction when
    the WS handshake fails, which leaves the spawned chromium process orphaned
    (the mint's finally only calls page.quit() if `page` was bound, but the
    exception happens before assignment). On Railway these orphans accumulate
    across mint=1 calls and wedge subsequent chromium spawns. Delete this block
    once the clearance gap is resolved.
    """
    info: dict[str, Any] = {"ran": True, "killed_pids": [], "error": None}
    try:
        import signal

        for row in _matching_processes(("chromium", "chrome")):
            pid = int(row["pid"])
            try:
                os.kill(pid, signal.SIGKILL)
                info["killed_pids"].append(pid)
            except Exception as exc:
                info["error"] = f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        info["error"] = f"{type(exc).__name__}: {exc}"
    return info


def _bn_diag_raw_cdp_probe() -> dict[str, Any]:
    """Spawn chromium with --remote-debugging-port=N directly and probe CDP.

    Bypasses DrissionPage entirely so we can tell whether the WS 404 we get from
    /api/diagnostics/basenotes?mint=1 is a DrissionPage URL-construction bug or
    a chromium-side problem (port closed, /json/version returns garbage, WS
    upgrade actually 404s, etc.). Delete this block once the clearance gap is
    resolved.
    """
    import shutil
    import socket
    import subprocess
    import tempfile
    import time

    import requests  # transitive dep via engine

    result: dict[str, Any] = {
        "ran": False,
        "binary": None,
        "port": None,
        "spawn_pid": None,
        "spawn_exit_code": None,
        "port_open_after_wait": None,
        "wait_elapsed_ms": None,
        "json_version_status": None,
        "json_version_body_first_500": None,
        "json_list_status": None,
        "json_list_body_first_500": None,
        "ws_url_attempted": None,
        "ws_upgrade_ok": None,
        "ws_upgrade_error": None,
        "stderr_tail": None,
    }

    binary = (
        os.environ.get("BASENOTES_CHROMIUM_PATH", "").strip()
        or shutil.which("chromium")
        or shutil.which("chromium-browser")
        or shutil.which("google-chrome")
    )
    result["binary"] = binary
    if not binary:
        return result

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    finally:
        sock.close()
    result["port"] = port

    user_data_dir = tempfile.mkdtemp(prefix="bn-rawcdp-")
    proc: subprocess.Popen[bytes] | None = None
    try:
        result["ran"] = True
        popen_kwargs: dict[str, Any] = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
        }
        if os.name != "nt":
            popen_kwargs["start_new_session"] = True
        proc = subprocess.Popen(
            [
                binary,
                "--headless=new",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--mute-audio",
                f"--user-data-dir={user_data_dir}",
                f"--remote-debugging-port={port}",
                "--remote-allow-origins=*",
                "about:blank",
            ],
            **popen_kwargs,
        )
        result["spawn_pid"] = proc.pid

        start = time.time()
        port_open = False
        deadline = start + 10.0
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    port_open = True
                    break
            except OSError:
                time.sleep(0.2)
        result["port_open_after_wait"] = port_open
        result["wait_elapsed_ms"] = int((time.time() - start) * 1000)

        if port_open:
            import json as _json
            try:
                r = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=5)
                result["json_version_status"] = r.status_code
                result["json_version_body_first_500"] = r.text[:500]
                ws_url = None
                try:
                    payload = r.json()
                    ws_url = payload.get("webSocketDebuggerUrl")
                except Exception:
                    ws_url = None
            except Exception as exc:
                result["json_version_status"] = f"ERR: {type(exc).__name__}: {exc}"
                ws_url = None

            try:
                r2 = requests.get(f"http://127.0.0.1:{port}/json", timeout=5)
                result["json_list_status"] = r2.status_code
                result["json_list_body_first_500"] = r2.text[:500]
                if not ws_url:
                    try:
                        lst = r2.json()
                        if isinstance(lst, list) and lst:
                            ws_url = lst[0].get("webSocketDebuggerUrl")
                    except Exception:
                        pass
            except Exception as exc:
                result["json_list_status"] = f"ERR: {type(exc).__name__}: {exc}"

            if ws_url:
                result["ws_url_attempted"] = ws_url
                import websocket  # type: ignore[import-not-found]

                # Variant A: default headers (websocket-client auto-generates Origin)
                try:
                    ws = websocket.create_connection(ws_url, timeout=5)
                    ws.send(_json.dumps({"id": 1, "method": "Browser.getVersion"}))
                    reply = ws.recv()
                    ws.close()
                    result["ws_upgrade_ok"] = True
                    result["ws_upgrade_error"] = None
                    result["ws_first_reply_first_500"] = str(reply)[:500]
                except Exception as exc:
                    result["ws_upgrade_ok"] = False
                    result["ws_upgrade_error"] = f"{type(exc).__name__}: {exc}"

                # Variant B: suppress_origin=True (exactly what DrissionPage does
                # at _base/driver.py:159). This tests the hypothesis that the WS
                # 404 from DrissionPage is because Chromium rejects the upgrade
                # when no Origin header is sent.
                try:
                    ws = websocket.create_connection(
                        ws_url, timeout=5, suppress_origin=True
                    )
                    ws.send(_json.dumps({"id": 1, "method": "Browser.getVersion"}))
                    reply = ws.recv()
                    ws.close()
                    result["ws_suppress_origin_ok"] = True
                    result["ws_suppress_origin_error"] = None
                except Exception as exc:
                    result["ws_suppress_origin_ok"] = False
                    result["ws_suppress_origin_error"] = f"{type(exc).__name__}: {exc}"

                # Variant C: explicit Origin header matching localhost. Tests
                # whether passing a "valid" origin makes Chromium happy even
                # when --remote-allow-origins=* is in effect.
                try:
                    ws = websocket.create_connection(
                        ws_url,
                        timeout=5,
                        origin=f"http://127.0.0.1:{port}",
                    )
                    ws.send(_json.dumps({"id": 1, "method": "Browser.getVersion"}))
                    reply = ws.recv()
                    ws.close()
                    result["ws_explicit_origin_ok"] = True
                    result["ws_explicit_origin_error"] = None
                except Exception as exc:
                    result["ws_explicit_origin_ok"] = False
                    result["ws_explicit_origin_error"] = f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        result["spawn_error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if proc is not None:
            engine._terminate_process_tree(proc)
            result["spawn_exit_code"] = proc.returncode
            try:
                err = proc.stderr.read() if proc.stderr else b""
                result["stderr_tail"] = err.decode("utf-8", errors="replace")[-1500:]
            except Exception:
                pass
        try:
            import shutil as _shutil
            _shutil.rmtree(user_data_dir, ignore_errors=True)
        except Exception:
            pass

    return result


def _bn_diag_clearance_cache() -> dict[str, Any]:
    cache_file = getattr(engine, "_BASENOTES_CACHE_FILE", None)
    configured_path = str(cache_file) if cache_file is not None else ""
    exists = False
    size_bytes: int | None = None
    try:
        exists = bool(cache_file is not None and cache_file.exists())
        size_bytes = int(cache_file.stat().st_size) if exists else None
    except Exception:
        exists = False
        size_bytes = None

    cached = None
    try:
        cached = engine._load_basenotes_cache()
    except Exception:
        cached = None

    user_agent_present = False
    cookie_count: int | None = None
    if cached:
        user_agent, cookies = cached
        user_agent_present = bool(str(user_agent or "").strip())
        cookie_count = len(cookies) if isinstance(cookies, dict) else None

    return {
        "configured_path": configured_path,
        "env_override": os.environ.get("BASENOTES_CLEARANCE_CACHE") or None,
        "env_user_agent_present": bool(os.environ.get("BASENOTES_CLEARANCE_UA")),
        "env_cookie_header_present": bool(os.environ.get("BASENOTES_CLEARANCE_COOKIE_HEADER")),
        "env_cookies_json_present": bool(os.environ.get("BASENOTES_CLEARANCE_COOKIES_JSON")),
        "exists": exists,
        "size_bytes": size_bytes,
        "user_agent_present": user_agent_present,
        "cookie_count": cookie_count,
    }


def _bn_diag_session_state() -> dict[str, Any]:
    import time as _time

    session = getattr(engine, "_BASENOTES_SESSION", None)
    if session is None:
        return {
            "active": False,
            "validated_now": None,
            "validation_elapsed_ms": None,
        }

    started = _time.monotonic()
    try:
        validated = bool(engine._validate_basenotes_session(session))
    except Exception:
        validated = False
    return {
        "active": True,
        "validated_now": validated,
        "validation_elapsed_ms": _bn_diag_elapsed_ms(started),
    }


def _bn_diag_search_url(query: str) -> str:
    from urllib.parse import quote

    return (
        "https://basenotes.com/directory/?sort=pop-desc&search="
        f"{quote(query)}&type=fragrances&page=1"
    )


def _bn_diag_direct_probe() -> dict[str, Any]:
    import time as _time

    url = "https://basenotes.com/"
    result: dict[str, Any] = {
        "url": url,
        "status_code": None,
        "body_length": None,
        "challenge_detected": None,
        "response_server_header": None,
        "cf_ray_present": False,
        "elapsed_ms": None,
        "error": None,
    }
    started = _time.monotonic()
    try:
        res = _bn_diag_default_scraper().get(url, timeout=10)
        headers = _bn_diag_headers(res)
        result.update(
            {
                "status_code": int(getattr(res, "status_code", 0) or 0),
                "body_length": _bn_diag_response_length(res),
                "challenge_detected": bool(engine._response_has_challenge(res)),
                "response_server_header": headers.get("Server"),
                "cf_ray_present": _bn_diag_cf_ray_present(headers),
                "elapsed_ms": _bn_diag_elapsed_ms(started),
            }
        )
    except Exception as exc:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["error"] = _bn_diag_error(exc)
    return result


def _bn_diag_search_probe(query: str) -> dict[str, Any]:
    import time as _time

    url = _bn_diag_search_url(query)
    result: dict[str, Any] = {
        "url": url,
        "status_code": None,
        "body_length": None,
        "challenge_detected": None,
        "elapsed_ms": None,
        "fragrance_anchor_count": None,
        "error": None,
    }
    started = _time.monotonic()
    try:
        scraper = engine.get_scraper()
        deadline = engine.Deadline(10)
        res = engine.BasenotesEngine._get_with_retries(
            scraper,
            url,
            timeout=10,
            attempts=1,
            deadline=deadline,
        )
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        if res is None:
            return result

        body = _bn_diag_response_text(res)
        try:
            soup = engine.BeautifulSoup(body, "html.parser")
            anchor_count = sum(
                1
                for anchor in soup.find_all("a", href=True)
                if "/fragrances/" in str(anchor.get("href", ""))
            )
        except Exception:
            anchor_count = None

        result.update(
            {
                "status_code": int(getattr(res, "status_code", 0) or 0),
                "body_length": _bn_diag_response_length(res),
                "challenge_detected": bool(engine._response_has_challenge(res)),
                "fragrance_anchor_count": anchor_count,
            }
        )
    except Exception as exc:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["error"] = _bn_diag_error(exc)
    return result


def _bn_diag_mint_attempt(mint: bool) -> dict[str, Any]:
    import concurrent.futures
    import time as _time

    result: dict[str, Any] = {
        "ran": bool(mint),
        "success": None,
        "elapsed_ms": None,
        "error": None,
        "session_validated_after_mint": None,
    }
    if not mint:
        return result

    started = _time.monotonic()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(engine._mint_basenotes_clearance)
    try:
        session = future.result(timeout=90)
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["success"] = session is not None
        if session is not None:
            try:
                result["session_validated_after_mint"] = bool(
                    engine._validate_basenotes_session(session)
                )
            except Exception:
                result["session_validated_after_mint"] = False
    except concurrent.futures.TimeoutError:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["success"] = False
        result["error"] = "TimeoutError: mint attempt exceeded 90 seconds"
    except Exception as exc:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["success"] = False
        result["error"] = _bn_diag_error(exc)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return result


def _bn_diag_engine_search(query: str) -> dict[str, Any]:
    import time as _time

    result: dict[str, Any] = {
        "row_count": 0,
        "elapsed_ms": 0,
        "sample_rows": [],
        "error": None,
    }
    started = _time.monotonic()
    try:
        scraper = engine.get_scraper()
        rows = engine.BasenotesEngine.extract_search_data(
            scraper,
            query,
            deadline=engine.Deadline(10),
        )
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["row_count"] = len(rows)
        result["sample_rows"] = [
            {
                "name": getattr(row, "name", ""),
                "brand": getattr(row, "brand", ""),
                "bn_url": getattr(row, "frag_url", None)
                or getattr(row, "bn_url", None),
            }
            for row in rows[:3]
        ]
    except Exception as exc:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["error"] = _bn_diag_error(exc)
    return result


def _coerce_clearance_payload(payload: ClearanceCookieRequest) -> tuple[str, dict[str, str]]:
    user_agent = str(payload.user_agent or "").strip()
    cookies = {
        str(k).strip(): str(v)
        for k, v in (payload.cookies or {}).items()
        if str(k).strip()
    }
    if not cookies and payload.cookie_header:
        cookies = engine._parse_cookie_header(payload.cookie_header)
    if not user_agent:
        raise HTTPException(status_code=400, detail="user_agent is required.")
    if not cookies:
        raise HTTPException(status_code=400, detail="cookies or cookie_header is required.")
    return user_agent, cookies


def _install_manual_clearance(source: str, payload: ClearanceCookieRequest) -> dict[str, Any]:
    user_agent, cookies = _coerce_clearance_payload(payload)
    if source == "basenotes":
        engine.reset_basenotes_scraper(clear_cache=True)
        engine._save_basenotes_cache(user_agent, cookies)
        session = engine._new_basenotes_http_session(user_agent, cookies)
        validated = (
            bool(engine._validate_basenotes_session(session))
            if payload.validate_session and session is not None
            else None
        )
        if validated:
            engine._BASENOTES_SESSION = session
            engine._BASENOTES_LAST_MINT_ERROR = None
    elif source == "fragrantica":
        engine.reset_fragrantica_scraper(clear_cache=True)
        engine._save_fragrantica_cache(user_agent, cookies)
        session = engine._new_fragrantica_http_session(user_agent, cookies)
        validated = (
            bool(engine._validate_fragrantica_session(session))
            if payload.validate_session and session is not None
            else None
        )
        if validated:
            engine._FRAGRANTICA_SESSION = session
            engine._FRAGRANTICA_LAST_MINT_ERROR = None
    else:
        raise HTTPException(status_code=400, detail="source must be basenotes or fragrantica.")

    return {
        "source": source,
        "saved": True,
        "validated": validated,
        "cookie_count": len(cookies),
        "has_cf_clearance": "cf_clearance" in cookies,
        "user_agent_present": bool(user_agent),
    }


# ---------------------------------------------------------------------------
# Basenotes diagnostics (TEMPORARY -- remove after Basenotes/Railway clearance
# gap is resolved).
#
# /api/diagnostics/engine-search showed candidates reaching the resolver but
# being rejected by score, while Basenotes may be returning zero native links on
# Railway. This endpoint isolates import availability, Chromium discovery,
# clearance-cache state, current session validity, raw Basenotes reachability,
# the BN-aware search fetch path, optional clearance minting, and the actual
# BasenotesEngine.extract_search_data result for one query. It is additive and
# diagnostic-only; delete this block once the Railway clearance gap is resolved.
# ---------------------------------------------------------------------------


@app.get("/api/diagnostics/basenotes")
def basenotes_diagnostics(
    q: str = Query("xerjoff", min_length=1),
    mint: bool = Query(False),
    probe: bool = Query(False),
    kill_orphans: bool = Query(False),
) -> dict[str, Any]:
    query = q.strip()

    # Run orphan-kill BEFORE either probe/mint so the container starts each call
    # from a clean process table. Without this, a failed mint leaves chromium
    # zombies that wedge subsequent spawns.
    orphan_info: dict[str, Any] = (
        _bn_diag_kill_orphan_chromiums()
        if (kill_orphans or probe or mint)
        else {"skipped": "set kill_orphans=1, probe=1, or mint=1 to run"}
    )

    raw_info: dict[str, Any] = (
        _bn_diag_raw_cdp_probe()
        if probe
        else {"skipped": "set probe=1 to spawn chromium directly (independent of mint)"}
    )

    mint_info = _bn_diag_mint_attempt(mint)
    return {
        "query": query,
        "imports": _bn_diag_imports(),
        "chromium_binary": _bn_diag_chromium_binary(),
        "orphan_chromium_cleanup": orphan_info,
        "raw_cdp_probe": raw_info,
        "clearance_cache": _bn_diag_clearance_cache(),
        "session_state": _bn_diag_session_state(),
        "direct_probe": _bn_diag_direct_probe(),
        "search_probe": _bn_diag_search_probe(query),
        "mint_attempt": mint_info,
        "raw_cdp_mint_result": getattr(engine, "_CLEARANCE_RAW_CDP_LAST_RESULT", None),
        "engine_search_for_q": _bn_diag_engine_search(query),
        "last_mint_error": getattr(engine, "_BASENOTES_LAST_MINT_ERROR", None),
    }


@app.post(
    "/api/diagnostics/basenotes/clearance",
    dependencies=[Depends(_require_clearance_token)],
)
def install_basenotes_clearance(payload: ClearanceCookieRequest) -> dict[str, Any]:
    return _install_manual_clearance("basenotes", payload)


# ---------------------------------------------------------------------------
# Fragrantica diagnostics. Mirrors /api/diagnostics/basenotes for the FG-side
# clearance pipeline. Cloudflare blocks Railway's datacenter IP on
# fragrantica.com the same way it blocks basenotes.com; the engine now has a
# parallel _mint_fragrantica_clearance / _FRAGRANTICA_SESSION path, and this
# endpoint is the one-shot trigger to mint clearance on the Railway box (cache
# file is written to disk; subsequent normal requests reuse it). Like the BN
# diagnostic, mint=1 is what actually runs Chromium -- without it the endpoint
# is strictly read-only.
# ---------------------------------------------------------------------------


def _fg_diag_clearance_cache() -> dict[str, Any]:
    cache_file = getattr(engine, "_FRAGRANTICA_CACHE_FILE", None)
    configured_path = str(cache_file) if cache_file is not None else ""
    exists = False
    size_bytes: int | None = None
    try:
        exists = bool(cache_file is not None and cache_file.exists())
        size_bytes = int(cache_file.stat().st_size) if exists else None
    except Exception:
        exists = False
        size_bytes = None

    cached = None
    try:
        cached = engine._load_fragrantica_cache()
    except Exception:
        cached = None

    user_agent_present = False
    cookie_count: int | None = None
    if cached:
        user_agent, cookies = cached
        user_agent_present = bool(str(user_agent or "").strip())
        cookie_count = len(cookies) if isinstance(cookies, dict) else None

    return {
        "configured_path": configured_path,
        "env_override": os.environ.get("FRAGRANTICA_CLEARANCE_CACHE") or None,
        "env_user_agent_present": bool(os.environ.get("FRAGRANTICA_CLEARANCE_UA")),
        "env_cookie_header_present": bool(os.environ.get("FRAGRANTICA_CLEARANCE_COOKIE_HEADER")),
        "env_cookies_json_present": bool(os.environ.get("FRAGRANTICA_CLEARANCE_COOKIES_JSON")),
        "exists": exists,
        "size_bytes": size_bytes,
        "user_agent_present": user_agent_present,
        "cookie_count": cookie_count,
    }


def _fg_diag_session_state() -> dict[str, Any]:
    import time as _time

    session = getattr(engine, "_FRAGRANTICA_SESSION", None)
    if session is None:
        return {
            "active": False,
            "validated_now": None,
            "validation_elapsed_ms": None,
        }

    started = _time.monotonic()
    try:
        validated = bool(engine._validate_fragrantica_session(session))
    except Exception:
        validated = False
    return {
        "active": True,
        "validated_now": validated,
        "validation_elapsed_ms": _bn_diag_elapsed_ms(started),
    }


def _fg_diag_direct_probe() -> dict[str, Any]:
    import time as _time

    url = "https://www.fragrantica.com/"
    result: dict[str, Any] = {
        "url": url,
        "status_code": None,
        "body_length": None,
        "challenge_detected": None,
        "response_server_header": None,
        "cf_ray_present": False,
        "elapsed_ms": None,
        "error": None,
    }
    started = _time.monotonic()
    try:
        res = _bn_diag_default_scraper().get(url, timeout=10)
        headers = _bn_diag_headers(res)
        result.update(
            {
                "status_code": int(getattr(res, "status_code", 0) or 0),
                "body_length": _bn_diag_response_length(res),
                "challenge_detected": bool(engine._response_has_challenge(res)),
                "response_server_header": headers.get("Server"),
                "cf_ray_present": _bn_diag_cf_ray_present(headers),
                "elapsed_ms": _bn_diag_elapsed_ms(started),
            }
        )
    except Exception as exc:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["error"] = _bn_diag_error(exc)
    return result


def _fg_diag_search_probe(query: str) -> dict[str, Any]:
    import time as _time
    from urllib.parse import quote as _quote

    url = engine.FragranticaEngine.SEARCH_URL.format(query=_quote(query))
    result: dict[str, Any] = {
        "url": url,
        "status_code": None,
        "body_length": None,
        "challenge_detected": None,
        "elapsed_ms": None,
        "fragrantica_perfume_anchor_count": None,
        "error": None,
    }
    started = _time.monotonic()
    try:
        scraper = engine.get_scraper()
        deadline = engine.Deadline(10)
        res = engine.Http.get(
            scraper,
            url,
            timeout=10,
            referer=engine.FragranticaEngine.BASE_URL,
            deadline=deadline,
            attempts=1,
        )
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        if res is None:
            return result

        body = _bn_diag_response_text(res)
        try:
            soup = engine.BeautifulSoup(body, "html.parser")
            anchor_count = sum(
                1
                for anchor in soup.find_all("a", href=True)
                if "/perfume/" in str(anchor.get("href", ""))
            )
        except Exception:
            anchor_count = None

        result.update(
            {
                "status_code": int(getattr(res, "status_code", 0) or 0),
                "body_length": _bn_diag_response_length(res),
                "challenge_detected": bool(engine._response_has_challenge(res)),
                "fragrantica_perfume_anchor_count": anchor_count,
            }
        )
    except Exception as exc:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["error"] = _bn_diag_error(exc)
    return result


def _fg_diag_mint_attempt(mint: bool) -> dict[str, Any]:
    import concurrent.futures
    import time as _time

    result: dict[str, Any] = {
        "ran": bool(mint),
        "success": None,
        "elapsed_ms": None,
        "error": None,
        "session_validated_after_mint": None,
    }
    if not mint:
        return result

    started = _time.monotonic()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(engine._mint_fragrantica_clearance)
    try:
        session = future.result(timeout=90)
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["success"] = session is not None
        if session is not None:
            try:
                result["session_validated_after_mint"] = bool(
                    engine._validate_fragrantica_session(session)
                )
            except Exception:
                result["session_validated_after_mint"] = False
    except concurrent.futures.TimeoutError:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["success"] = False
        result["error"] = "TimeoutError: mint attempt exceeded 90 seconds"
    except Exception as exc:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["success"] = False
        result["error"] = _bn_diag_error(exc)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return result


def _fg_diag_engine_search(query: str) -> dict[str, Any]:
    import time as _time

    result: dict[str, Any] = {
        "row_count": 0,
        "elapsed_ms": 0,
        "sample_rows": [],
        "error": None,
    }
    started = _time.monotonic()
    try:
        scraper = engine.get_scraper()
        rows = engine.FragranticaEngine.extract_search_data(
            scraper,
            query,
            deadline=engine.Deadline(10),
            native_search_enabled=True,
        )
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["row_count"] = len(rows)
        result["sample_rows"] = [
            {
                "name": getattr(row, "name", ""),
                "brand": getattr(row, "brand", ""),
                "frag_url": getattr(row, "frag_url", None),
            }
            for row in rows[:3]
        ]
    except Exception as exc:
        result["elapsed_ms"] = _bn_diag_elapsed_ms(started)
        result["error"] = _bn_diag_error(exc)
    return result


@app.get("/api/diagnostics/fragrantica")
def fragrantica_diagnostics(
    q: str = Query("dior sauvage", min_length=1),
    mint: bool = Query(False),
) -> dict[str, Any]:
    query = q.strip()

    return {
        "query": query,
        "imports": _bn_diag_imports(),
        "chromium_binary": _bn_diag_chromium_binary(),
        "clearance_cache": _fg_diag_clearance_cache(),
        "session_state": _fg_diag_session_state(),
        "direct_probe": _fg_diag_direct_probe(),
        "search_probe": _fg_diag_search_probe(query),
        "mint_attempt": _fg_diag_mint_attempt(mint),
        "engine_search_for_q": _fg_diag_engine_search(query),
        "last_mint_error": getattr(engine, "_FRAGRANTICA_LAST_MINT_ERROR", None),
    }


@app.post(
    "/api/diagnostics/fragrantica/clearance",
    dependencies=[Depends(_require_clearance_token)],
)
def install_fragrantica_clearance(payload: ClearanceCookieRequest) -> dict[str, Any]:
    return _install_manual_clearance("fragrantica", payload)


def _jsonable(value: Any) -> Any:
    """Best-effort coerce an argparse value into something JSON can carry."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


# ---------------------------------------------------------------------------
# Spell-repair diagnostics (TEMPORARY -- remove after the suggest() empty-return
# cause is proven).
#
# /engine-search showed the pipeline is healthy on a correctly-spelled query but
# a typo'd query ("dior sava") returns []: QueryRepair.suggest() returns "" --
# it never corrects the typo to "dior sauvage" -- even though /upstream reports
# Google/Bing reachable and fast. suggest() repairs typos by SCRAPING Google/
# Bing SERP HTML; the hypothesis is that the markup Railway's datacenter IP gets
# back lacks the result links / "did you mean" text the parsers expect.
#
# This endpoint fetches the exact search URLs suggest() builds, reports what
# markup actually came back (length, key marker presence, anchor count, consent-
# page markers, preview), and runs the engine's OWN extractors + evidence
# harvester over that markup so we can see precisely where the evidence is lost.
# It then runs the real suggest() end-to-end at the live budget AND at a
# generous budget, to rule the 1.6s deadline in or out. Strictly read-only --
# it calls engine functions but mutates no engine state. Delete this block once
# the parser fix lands.
# ---------------------------------------------------------------------------

# Plain fragrantica.com/perfume URLs in raw markup -- the canonical evidence
# _add_markup_evidence harvests. Zero of these in a 200-OK body is the smoking
# gun for "Google served Railway a stripped/JS-only/consent SERP".
_FG_PERFUME_URL_RE = __import__("re").compile(
    r"https?://[^\s\"'<>]*fragrantica\.com/perfume/[^\s\"'<>]+"
)

# Consent / soft-block interstitials Google/Bing serve datacenter IPs instead of
# a real SERP. Their presence explains an empty parse on an HTTP 200.
_CONSENT_MARKERS = (
    "consent.google",
    "consent.youtube",
    "before you continue",
    "/sorry/",
    "unusual traffic",
    "our systems have detected",
)


def _inspect_search_markup(markup: str, kind: str, original: str) -> dict[str, Any]:
    """Run the engine's own SERP extractors over `markup` and report the result.

    Mirrors exactly what QueryRepair.suggest() does internally: it calls the
    same extractor (extract_google_suggestion / extract_bing_suggestion) and the
    same evidence harvester (_add_markup_evidence). If `harvested_records` is
    empty here, suggest() harvested nothing from this page either.
    """
    lowered = markup.lower()
    fg_urls = _FG_PERFUME_URL_RE.findall(markup)

    extractor_kind = "bing" if kind == "bing" else "google"
    try:
        if extractor_kind == "bing":
            extracted = engine.QueryRepair.extract_bing_suggestion(markup, original)
        else:
            extracted = engine.QueryRepair.extract_google_suggestion(markup, original)
    except Exception as exc:
        extracted = f"<error: {type(exc).__name__}: {exc}>"

    # The raw evidence dict suggest() builds up across all its SERP fetches.
    records: dict[str, Any] = {}
    try:
        engine.QueryRepair._add_markup_evidence(
            records, markup, original, kind=extractor_kind
        )
        records_dump = {
            key: {k: _jsonable(v) for k, v in rec.items()}
            for key, rec in records.items()
        }
    except Exception as exc:
        records_dump = {"<error>": f"{type(exc).__name__}: {exc}"}

    return {
        "markup_length": len(markup),
        "fragrantica_perfume_url_count": len(fg_urls),
        "fragrantica_perfume_url_sample": fg_urls[:5],
        "contains_did_you_mean": "did you mean" in lowered,
        "contains_showing_results_for": "showing results for" in lowered,
        "contains_including_results_for": "including results for" in lowered,
        "contains_consent_or_softblock": any(m in lowered for m in _CONSENT_MARKERS),
        "anchor_tag_count": lowered.count("<a "),
        "engine_extractor_returned": extracted,
        "harvested_record_count": len(records),
        "harvested_records": records_dump,
    }


@app.get("/api/diagnostics/spell-repair")
def spell_repair_diagnostics(
    q: str = Query("dior sava", min_length=1),
) -> dict[str, Any]:
    """Localize why QueryRepair.suggest() returns "" for a typo'd query.

    Temporary, read-only. Fetches the exact search URLs suggest() uses, runs the
    engine's own extractors/harvester over the returned markup, then runs the
    real suggest() at the live budget and at a generous budget.
    """
    import time as _time

    query = q.strip()
    scraper = engine.get_scraper()
    headers = dict(engine.Http.DEFAULT_HEADERS)

    # A representative subset of the queries suggest() builds for a short input:
    # the canonical site-search (where the FG perfume links live), a plain
    # "<q> perfume" google pass, the bing equivalent, and the autocomplete JSON.
    probe_queries: list[tuple[str, str]] = [
        ("google", f"{query} site:fragrantica.com/perfume"),
        ("google", f"{query} perfume"),
        ("bing", f"{query} site:fragrantica.com/perfume"),
        ("autocomplete", f"{query} fragrance"),
    ]

    probes: list[dict[str, Any]] = []
    for kind, search_query in probe_queries:
        url = engine.QueryRepair._search_url(kind, search_query)
        entry: dict[str, Any] = {
            "kind": kind,
            "search_query": search_query,
            "url": url,
        }
        started = _time.monotonic()
        try:
            # Generous 10s timeout here on purpose: we want to SEE the markup
            # regardless of the engine's tight ~0.8s budget. The budget itself
            # is tested separately via the suggest() runs below.
            res = scraper.get(url, timeout=10, headers=headers)
            entry["http_status"] = res.status_code
            entry["elapsed_ms"] = int((_time.monotonic() - started) * 1000)
            markup = res.text or ""
            if kind == "autocomplete":
                try:
                    extracted = engine.QueryRepair.extract_google_autocomplete(
                        markup, query
                    )
                except Exception as exc:
                    extracted = f"<error: {type(exc).__name__}: {exc}>"
                entry["markup_length"] = len(markup)
                entry["body_preview"] = markup[:300]
                entry["engine_extractor_returned"] = extracted
            else:
                entry.update(_inspect_search_markup(markup, kind, query))
        except Exception as exc:
            entry["error"] = f"{type(exc).__name__}: {exc}"
            entry["elapsed_ms"] = int((_time.monotonic() - started) * 1000)
        probes.append(entry)

    # Run the real suggest() end-to-end. The first run uses the engine's live
    # budget (_ARGS.spell_repair_budget, default 1.6s) -- this is exactly what
    # the public /search path does. The second uses a generous budget: if the
    # live run returns "" but the generous run returns a correction, the bug is
    # the deadline; if both return "", the bug is the markup/parser.
    def _run_suggest(seconds: float) -> dict[str, Any]:
        started = _time.monotonic()
        try:
            out = engine.QueryRepair.suggest(scraper, query, seconds=seconds)
            return {
                "budget_seconds": seconds,
                "returned": out,
                "elapsed_ms": int((_time.monotonic() - started) * 1000),
            }
        except Exception as exc:
            return {
                "budget_seconds": seconds,
                "error": f"{type(exc).__name__}: {exc}",
                "elapsed_ms": int((_time.monotonic() - started) * 1000),
            }

    suggest_runs = [
        _run_suggest(float(_ARGS.spell_repair_budget)),
        _run_suggest(8.0),
    ]

    # Cheap verdict so the JSON is readable without cross-referencing fields.
    any_fg_links = any(
        p.get("fragrantica_perfume_url_count", 0) > 0 for p in probes
    )
    any_consent = any(p.get("contains_consent_or_softblock") for p in probes)
    any_harvested = any(p.get("harvested_record_count", 0) > 0 for p in probes)
    live_suggest = suggest_runs[0].get("returned")
    generous_suggest = suggest_runs[1].get("returned")
    if not any_fg_links and any_consent:
        verdict = (
            "Google/Bing served a consent / soft-block page, not a real SERP -- "
            "the markup has no fragrantica.com/perfume links to harvest. This is "
            "an IP-shaped block, not a parser bug."
        )
    elif not any_fg_links:
        verdict = (
            "The SERP markup contains ZERO fragrantica.com/perfume links despite "
            "HTTP 200 -- Google/Bing served Railway a stripped or JS-rendered "
            "page. The parser is fine; there is nothing in the HTML to parse."
        )
    elif any_fg_links and not any_harvested:
        verdict = (
            "The markup DOES contain fragrantica.com/perfume links but "
            "_add_markup_evidence harvested none of them -- this is a genuine "
            "parser/selector bug. Patch _add_markup_evidence."
        )
    elif any_harvested and not live_suggest and generous_suggest:
        verdict = (
            "Evidence harvested and a correction is reachable, but the live "
            "1.6s budget returns '' -- the bug is the deadline, not the parser. "
            "Raise spell_repair_budget or speed up the fetch path."
        )
    elif any_harvested and not live_suggest and not generous_suggest:
        verdict = (
            "Evidence WAS harvested but suggest() still returns '' even at a "
            "generous budget -- the bug is downstream of harvesting: scoring "
            "(_best_record / _record_score) or confidence thresholds."
        )
    else:
        verdict = (
            "suggest() returned a correction here. If the public /search still "
            "fails, the divergence is elsewhere (args, scraper, or timing)."
        )

    return {
        "query": query,
        "spell_repair_budget": _ARGS.spell_repair_budget,
        "probes": probes,
        "suggest_runs": suggest_runs,
        "summary": {
            "any_fragrantica_links_in_markup": any_fg_links,
            "any_consent_or_softblock_page": any_consent,
            "any_evidence_harvested": any_harvested,
            "live_budget_suggest_returned": live_suggest,
            "generous_budget_suggest_returned": generous_suggest,
        },
        "verdict": verdict,
    }


# ---------------------------------------------------------------------------
# fg_detail_cache_v1.json -- API-level Fragrantica detail overlay.
#
# FG_CACHE_PATH restores Fragrantica URL identity but not the parsed Fragrantica
# detail metrics, so on Railway a live detail fetch still returns BN-only data:
# details.frag_cards is empty and every Fragrantica-derived metric group is
# null. This overlay hydrates details.frag_cards (plus notes/pros_cons/reviews)
# from a cache file warmed offline by scripts/warm_fg_detail_cache.py, then
# re-runs the existing derived_metrics_adapter over the merged object.
#
# It is strictly additive and changes no engine/parser/resolver/adapter code:
#   * It never runs when live Fragrantica data is present (frag_cards non-empty).
#   * It only fills empty fields -- live data always wins.
#   * It imports and calls build_derived_metrics exactly as the engine does.
# Unset FG_DETAIL_CACHE_PATH => this whole path is inert.
# ---------------------------------------------------------------------------


def _canonical_fg_url(url: str) -> str:
    """Normalize a Fragrantica URL for stable cache keying.

    Lowercases scheme+host and strips trailing slashes/whitespace. The path
    (which carries the perfume id) is preserved verbatim.
    """
    text = (url or "").strip()
    if not text:
        return ""
    if "://" in text:
        scheme, rest = text.split("://", 1)
        if "/" in rest:
            host, path = rest.split("/", 1)
            text = f"{scheme.lower()}://{host.lower()}/{path}"
        else:
            text = f"{scheme.lower()}://{rest.lower()}"
    return text.rstrip("/")


def _load_fg_detail_cache() -> dict[str, Any]:
    """Lazy-load the detail cache once. Missing/corrupt file => empty (inert)."""
    global _FG_DETAIL_CACHE
    if _FG_DETAIL_CACHE is not None:
        return _FG_DETAIL_CACHE
    entries: dict[str, Any] = {}
    if _FG_DETAIL_CACHE_PATH:
        try:
            with open(_FG_DETAIL_CACHE_PATH, encoding="utf-8") as handle:
                payload = json.load(handle)
            raw = payload.get("entries", {}) if isinstance(payload, dict) else {}
            if isinstance(raw, dict):
                entries = {
                    _canonical_fg_url(k): v
                    for k, v in raw.items()
                    if isinstance(v, dict)
                }
        except Exception:
            entries = {}
    _FG_DETAIL_CACHE = entries
    return _FG_DETAIL_CACHE


def _hydrate_details_from_entry(
    details: engine.UnifiedDetails, entry: dict[str, Any]
) -> bool:
    """Overlay a cached Fragrantica detail `entry` onto a BN-only bundle.

    Shared by the DB cache and the bundled JSON cache: both produce an `entry`
    dict with the same shape (`frag_cards` / `notes` / `pros_cons` / `reviews`).
    Returns True only when cached Fragrantica data was actually applied.

    Strictly additive -- live data always wins. `frag_cards` is merged per card
    so a *complete* cache entry can backfill a *partial* live/stored bundle
    (cards present live are never overwritten); notes/pros_cons/reviews stay
    fill-only. The existing derived_metrics adapter is re-run exactly as the
    engine runs it. Returns True only when something was actually added.
    """
    cached_cards = entry.get("frag_cards")
    if not isinstance(cached_cards, dict) or not cached_cards:
        return False

    applied = False
    # Per-card merge: backfill cards missing from the live/stored bundle, never
    # overwrite ones already present (live wins). This lets a complete cache
    # entry heal a partial bundle instead of only hydrating an empty one.
    if not isinstance(details.frag_cards, dict):
        details.frag_cards = {}
    for name, rows in cached_cards.items():
        if rows and name not in details.frag_cards:
            details.frag_cards[name] = rows
            applied = True

    # Notes: fill only when the live bundle carried none (BN supplied nothing).
    notes = details.notes
    if not (notes.top or notes.heart or notes.base or notes.flat):
        cached_notes = entry.get("notes") or {}
        if isinstance(cached_notes, dict):
            notes.has_pyramid = bool(cached_notes.get("has_pyramid", False))
            notes.top = list(cached_notes.get("top", []) or [])
            notes.heart = list(cached_notes.get("heart", []) or [])
            notes.base = list(cached_notes.get("base", []) or [])
            notes.flat = list(cached_notes.get("flat", []) or [])
            if notes.top or notes.heart or notes.base or notes.flat:
                applied = True

    # pros_cons / reviews: additive but de-duplicated -- never replace live
    # content, and never accumulate copies. This helper can run more than once
    # over the same bundle (a stored detail that already carries pros_cons /
    # reviews is then overlaid with a cache entry repeating them, or the same
    # bundle is re-hydrated on a later request), so a plain extend/append would
    # grow duplicate rows each pass. Skip any item already present by value.
    cached_pros_cons = entry.get("pros_cons") or []
    if isinstance(cached_pros_cons, list) and cached_pros_cons:
        seen_pros = set(details.pros_cons)
        for raw in cached_pros_cons:
            text = str(raw)
            if text not in seen_pros:
                details.pros_cons.append(text)
                seen_pros.add(text)
                applied = True
    cached_reviews = entry.get("reviews") or []
    if isinstance(cached_reviews, list):
        seen_reviews = {
            (str(rv.text), str(rv.source)) for rv in details.reviews
        }
        for r in cached_reviews:
            if isinstance(r, dict) and r.get("text"):
                key = (str(r["text"]), str(r.get("source", "")))
                if key not in seen_reviews:
                    details.reviews.append(
                        engine.Review(text=key[0], source=key[1])
                    )
                    seen_reviews.add(key)
                    applied = True

    if not applied:
        return False

    # Re-run the existing adapter over the merged object. Best-effort: a failure
    # here must not break the raw detail response (mirrors the engine contract
    # in fetch_selected_details).
    try:
        from derived_metrics_adapter import build_derived_metrics

        details.derived_metrics = build_derived_metrics(details)
    except Exception:
        details.derived_metrics = None
    return True


def _notes_to_dict(notes: engine.NotesList) -> dict[str, Any]:
    return {
        "has_pyramid": bool(notes.has_pyramid),
        "top": list(notes.top or []),
        "heart": list(notes.heart or []),
        "base": list(notes.base or []),
        "flat": list(notes.flat or []),
    }


def _review_to_dict(review: Any) -> dict[str, str]:
    if isinstance(review, dict):
        text = review.get("text", "")
        source = review.get("source", "")
    else:
        text = getattr(review, "text", "")
        source = getattr(review, "source", "")
    return {
        "text": str(text or ""),
        "source": str(source or ""),
    }


def _review_source_is(review: dict[str, str], needle: str) -> bool:
    return needle.lower() in str(review.get("source") or "").lower()


def _record_has_detail_payload(record: dict[str, Any]) -> bool:
    bn_raw = record.get("bn_raw") or {}
    fg_raw = record.get("fg_raw") or {}
    return bool(
        bn_raw.get("bn_consensus")
        or bn_raw.get("description")
        or bn_raw.get("reviews")
        or fg_raw.get("frag_cards")
        or fg_raw.get("notes")
        or fg_raw.get("reviews")
        or fg_raw.get("raw_identity")
    )


def _details_from_fragrance_record(record: dict[str, Any]) -> engine.UnifiedDetails:
    bn_raw = record.get("bn_raw") or {}
    fg_raw = record.get("fg_raw") or {}
    notes_raw = fg_raw.get("notes") or bn_raw.get("notes") or {}
    if not isinstance(notes_raw, dict):
        notes_raw = {}
    stored_derived_metrics = record.get("derived_metrics")
    details = engine.UnifiedDetails(
        notes=engine.NotesList(
            has_pyramid=bool(notes_raw.get("has_pyramid", False)),
            top=list(notes_raw.get("top", []) or []),
            heart=list(notes_raw.get("heart", []) or []),
            base=list(notes_raw.get("base", []) or []),
            flat=list(notes_raw.get("flat", []) or []),
        ),
        gender=str(record.get("gender") or "Unisex / Unspecified"),
        description=str(bn_raw.get("description") or ""),
        bn_consensus=bn_raw.get("bn_consensus") or {},
        frag_cards=fg_raw.get("frag_cards") or {},
        pros_cons=list(fg_raw.get("pros_cons") or []),
        derived_metrics=stored_derived_metrics,
    )
    source = str(fg_raw.get("source") or "").strip().lower()
    raw_identity = fg_raw.get("raw_identity") or {}
    if not isinstance(raw_identity, dict):
        raw_identity = {}
    if source:
        setattr(details, "source", source)
    parfumo_url = str(raw_identity.get("parfumo_url") or "").strip()
    if parfumo_url:
        setattr(details, "parfumo_url", parfumo_url)
    setattr(details, "_had_stored_derived_metrics", stored_derived_metrics is not None)
    for raw_review in list(bn_raw.get("reviews") or []) + list(fg_raw.get("reviews") or []):
        if isinstance(raw_review, dict) and raw_review.get("text"):
            details.reviews.append(
                engine.Review(
                    text=str(raw_review["text"]),
                    source=str(raw_review.get("source", "")),
                )
            )
    return details


def _derived_metrics_lock_for(record: dict[str, Any]) -> threading.Lock:
    key = (
        str(record.get("record_key") or "").strip()
        or str(record.get("canonical_fg_url") or "").strip()
        or str(record.get("bn_url") or "").strip()
        or f"{record.get('house') or ''}|{record.get('name') or ''}|{record.get('year') or ''}"
    )
    with _DERIVED_METRICS_LOCK_GUARD:
        lock = _DERIVED_METRICS_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _DERIVED_METRICS_LOCKS[key] = lock
        return lock


def _fill_missing_derived_metrics_once(
    record: dict[str, Any],
    selected: engine.UnifiedFragrance,
    details: engine.UnifiedDetails,
) -> None:
    if details.derived_metrics is not None:
        return
    lock = _derived_metrics_lock_for(record)
    with lock:
        latest = db.lookup_fragrance_record(
            canonical_fg_url=record.get("canonical_fg_url") or _canonical_fg_url(selected.frag_url),
            bn_url=record.get("bn_url") or selected.bn_url or None,
        )
        latest_metrics = latest.get("derived_metrics") if latest else None
        if latest_metrics is not None:
            details.derived_metrics = latest_metrics
            setattr(details, "_had_stored_derived_metrics", True)
            return
        try:
            from derived_metrics_adapter import build_derived_metrics

            details.derived_metrics = build_derived_metrics(details)
        except Exception:
            details.derived_metrics = None
        if details.derived_metrics is not None:
            _persist_detail_record(selected, details)
            setattr(details, "_had_stored_derived_metrics", True)


def _is_parfumo_fallback_detail(details: engine.UnifiedDetails) -> bool:
    source = str(getattr(details, "source", "") or "").strip().lower()
    if source == "parfumo":
        return True
    return bool(str(getattr(details, "parfumo_url", "") or "").strip())


def _lookup_stored_detail(
    selected: engine.UnifiedFragrance,
) -> engine.UnifiedDetails | None:
    record = db.lookup_fragrance_record(
        canonical_fg_url=_canonical_fg_url(selected.frag_url),
        bn_url=selected.bn_url or None,
    )
    if (
        not record
        or not _record_has_detail_payload(record)
        or _fragrance_record_is_stale(record)
    ):
        return None
    item = _candidate_from_fragrance_record(record, source="fragrance_db_detail")
    if item:
        if _identity_needs_recovery(selected.name):
            selected.name = item.name
        if _identity_needs_recovery(selected.brand):
            selected.brand = item.brand
        selected.year = selected.year or item.year
        selected.bn_url = selected.bn_url or item.bn_url
        selected.frag_url = selected.frag_url or item.frag_url
        image_url = getattr(item, "image_url", "")
        if image_url and not getattr(selected, "image_url", ""):
            setattr(selected, "image_url", image_url)
    details = _details_from_fragrance_record(record)
    _fill_missing_derived_metrics_once(record, selected, details)
    return details


def _persist_detail_record(
    selected: engine.UnifiedFragrance, details: engine.UnifiedDetails
) -> None:
    if not db.ENABLED:
        return
    try:
        reviews = [_review_to_dict(r) for r in (details.reviews or [])]
        bn_reviews = [r for r in reviews if _review_source_is(r, "basenotes")]
        fg_reviews = [r for r in reviews if _review_source_is(r, "fragrantica")]
        other_reviews = [
            r
            for r in reviews
            if not _review_source_is(r, "basenotes")
            and not _review_source_is(r, "fragrantica")
        ]
        notes_blob = _notes_to_dict(details.notes)
        db.upsert_fragrance_details(
            {
                "canonical_fg_url": _canonical_fg_url(selected.frag_url),
                "bn_url": selected.bn_url or None,
                "name": selected.name or None,
                "house": selected.brand or None,
                "year": _coerce_year(selected.year),
                "gender": details.gender,
                "image_url": getattr(selected, "image_url", None),
                "bn_raw": _json_for_db_blob(
                    {
                        "description": details.description,
                        "bn_consensus": details.bn_consensus or {},
                        "notes": notes_blob,
                        "reviews": bn_reviews + other_reviews,
                    }
                ),
                "fg_raw": _json_for_db_blob(
                    {
                        "frag_cards": details.frag_cards or {},
                        "notes": notes_blob,
                        "pros_cons": details.pros_cons or [],
                        "reviews": fg_reviews,
                    }
                ),
                "derived_metrics": _json_for_db_blob(details.derived_metrics)
                if details.derived_metrics is not None
                else None,
            }
        )
    except Exception:
        logger.exception(
            "upsert_fragrance_details failed name=%s house=%s",
            selected.name,
            selected.brand,
        )


def _apply_fg_detail_cache_db(
    selected: engine.UnifiedFragrance, details: engine.UnifiedDetails
) -> bool:
    """Hydrate from the durable Postgres fg_detail_cache. Inert when DB disabled.

    This is the second tier of the detail lookup order (live FG -> DB cache ->
    bundled JSON cache -> partial). Only `quality_status = 'complete'` entries
    are trusted for hydration; weaker entries are left for a worker re-fetch.
    """
    if _fg_metrics_complete(details) or not selected.frag_url:
        return False
    entry = db.lookup_detail_cache(_canonical_fg_url(selected.frag_url))
    if not entry or entry.get("quality_status") != "complete":
        return False
    _fill_selected_identity(selected, entry)
    return _hydrate_details_from_entry(details, entry)


def _apply_fg_detail_cache(
    selected: engine.UnifiedFragrance, details: engine.UnifiedDetails
) -> bool:
    """Overlay cached Fragrantica detail data from the bundled JSON cache.

    Returns True only when cached Fragrantica data was actually applied. Live
    Fragrantica data always wins: if details.frag_cards is already populated
    this is a no-op and returns False.
    """
    # Complete Fragrantica metrics present -> nothing to backfill.
    if _fg_metrics_complete(details):
        return False
    if not _ALLOW_BUNDLED_FG_DETAIL_CACHE:
        return False
    if not selected.frag_url:
        return False
    entry = _load_fg_detail_cache().get(_canonical_fg_url(selected.frag_url))
    if not entry:
        return False
    _fill_selected_identity(selected, entry)
    return _hydrate_details_from_entry(details, entry)


def _candidate_from_source_url(source_url: str) -> engine.UnifiedFragrance:
    url = (source_url or "").strip()
    if not url:
        raise HTTPException(
            status_code=400,
            detail="Either 'id' or 'source_url' is required.",
        )
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if parsed.scheme not in {"http", "https"} or not host:
        raise HTTPException(
            status_code=400,
            detail="source_url must be an http(s) Basenotes, Fragrantica, or Parfinity URL.",
        )
    is_basenotes = host == "basenotes.com" or host.endswith(".basenotes.com")
    is_fragrantica = host == "fragrantica.com" or host.endswith(".fragrantica.com")
    is_parfinity = parfinity.is_parfinity_url(url)
    if not (is_basenotes or is_fragrantica or is_parfinity):
        raise HTTPException(
            status_code=400,
            detail="source_url must identify a Basenotes, Fragrantica, or Parfinity fragrance page.",
        )
    if is_parfinity:
        frag_url = parfinity.canonical_parfinity_url(url)
        entry = parfinity.lookup_detail(frag_url)
        selected = engine.UnifiedFragrance(
            name=str(entry.get("name") or "") if entry else "",
            brand=str(entry.get("house") or "") if entry else "",
            year="",
            bn_url="",
            frag_url=frag_url,
        )
        if entry and entry.get("image_url"):
            setattr(selected, "image_url", str(entry.get("image_url") or ""))
        return selected
    bn_url = url if is_basenotes else ""
    frag_url = url if is_fragrantica else ""
    selected = engine.UnifiedFragrance(
        name="", brand="", year="", bn_url=bn_url, frag_url=frag_url
    )
    if frag_url:
        entry = db.lookup_detail_cache(_canonical_fg_url(frag_url))
        if entry and entry.get("quality_status") == "complete":
            _fill_selected_identity(selected, entry)
        elif _ALLOW_BUNDLED_FG_DETAIL_CACHE:
            json_entry = _load_fg_detail_cache().get(_canonical_fg_url(frag_url))
            if json_entry:
                _fill_selected_identity(selected, json_entry)
        if not selected.name:
            selected.name = engine.FragranticaEngine.name_from_url(frag_url)
        if not selected.brand:
            selected.brand = engine.FragranticaEngine.brand_from_url(frag_url)
    return selected


def _candidate_from_request(req: DetailRequest) -> engine.UnifiedFragrance:
    """Reconstruct the engine candidate from the detail request.

    Prefers the opaque `id` token (carries both source URLs). Falls back to a
    bare `source_url`, routed to bn_url/frag_url by domain.
    """
    id_text = (req.id or "").strip()
    if id_text.startswith("source:"):
        selected = _candidate_from_source_url(id_text[len("source:") :])
    elif id_text.startswith(("catalog:", "dataset:", "local:")):
        prefix = id_text.split(":", 1)[0]
        raise HTTPException(
            status_code=400,
            detail=(
                f"id '{prefix}:...' routes to the local Express API at "
                "/api/fragrances/details on the api-server, not to this "
                "engine; send a source_url or call the app details endpoint."
            ),
        )
    elif id_text:
        try:
            data = _decode_id(id_text)
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail=f"Invalid 'id' token: {exc}"
            ) from exc
        selected = engine.UnifiedFragrance(
            name=data.get("n", ""),
            brand=data.get("b", ""),
            year=data.get("y", ""),
            bn_url=data.get("bn", ""),
            frag_url=data.get("fg", ""),
        )
    else:
        selected = _candidate_from_source_url(req.source_url or "")

    identity = _recover_candidate_identity(selected)
    if identity["name"] and identity["name"] != selected.name:
        selected.name = identity["name"]
    if identity["house"] and identity["house"] != selected.brand:
        selected.brand = identity["house"]
    return selected


def _identity_job_key(selected: engine.UnifiedFragrance) -> str:
    """Stable fallback job_key when neither source URL is present.

    Lower-cases brand|name so re-searches of the same fragrance dedupe to one
    pending row. Returns "" when there is no name to key on.
    """
    brand = (selected.brand or "").strip().lower()
    name = (selected.name or "").strip().lower()
    if not name:
        return ""
    return f"id:{brand}|{name}"


def _enqueue_enrichment_job(
    selected: engine.UnifiedFragrance, req: DetailRequest
) -> int | None:
    """Enqueue (or upsert) an enrichment job for a partial detail result.

    Job key prefers the canonical Fragrantica URL, falls back to the canonical
    Basenotes URL, then to a brand|name identity slug -- so a BN-only candidate
    still gets a pending row that the worker can resolve to FG at claim time.
    Repeated requests for the same fragrance bump requested_count instead of
    duplicating. Any failure here is swallowed -- enrichment is a background
    nicety and must never break the user-facing /details response.

    Returns the job's resulting `requested_count` (>= 1) on success; `0` when
    the write was accepted but no count is available (storage disabled, or a
    deliberately `ignored` job left untouched); and `None` only when the write
    raised. Callers treat any non-None result as "queued" -- this preserves the
    prior bool contract while letting `/details` surface the retry counter the
    frontend's MAX_ENRICHMENT_ATTEMPTS cap reads.
    """
    if selected.frag_url and parfinity.is_parfinity_url(selected.frag_url):
        return 0
    try:
        job_key = (
            _canonical_fg_url(selected.frag_url)
            or _canonical_fg_url(selected.bn_url)
            or _identity_job_key(selected)
        )
        if not job_key:
            return None
        count = db.enqueue_job(
            job_key=job_key,
            query=(req.source_url or None),
            name=selected.name or None,
            house=selected.brand or None,
            year=_coerce_year(selected.year),
            bn_url=selected.bn_url or None,
            fg_url=selected.frag_url or None,
        )
        return count if isinstance(count, int) else 0
    except Exception:
        # Best-effort: never let queue write failures surface to the client.
        logger.exception(
            "enqueue_job failed job_key=%s name=%s house=%s",
            locals().get("job_key"),
            selected.name,
            selected.brand,
        )
        return None


def _requeue_enrichment_job(
    selected: engine.UnifiedFragrance, req: RequeueDetailRequest
) -> dict[str, Any] | None:
    """Create or force-refresh the durable job for a detail identity."""
    if selected.frag_url and parfinity.is_parfinity_url(selected.frag_url):
        return None
    try:
        job_key = (
            _canonical_fg_url(selected.frag_url)
            or _canonical_fg_url(selected.bn_url)
            or _identity_job_key(selected)
        )
        if not job_key:
            return None
        return db.requeue_or_enqueue_job(
            job_key=job_key,
            query=(req.source_url or None),
            name=selected.name or None,
            house=selected.brand or None,
            year=_coerce_year(selected.year),
            bn_url=selected.bn_url or None,
            fg_url=selected.frag_url or None,
            priority=req.priority,
        )
    except Exception:
        logger.exception(
            "requeue_or_enqueue_job failed name=%s house=%s",
            selected.name,
            selected.brand,
        )
        return None


def _fetch_parfinity_detail_bundle(
    selected: engine.UnifiedFragrance,
    detail_timeout: float,
) -> engine.UnifiedDetails:
    details = engine.UnifiedDetails(notes=engine.NotesList())
    deadline = engine.Deadline(detail_timeout)

    if selected.bn_url:
        try:
            scraper = engine.get_scraper()
            engine.BasenotesEngine.fetch_details(
                scraper, selected.bn_url, details, deadline=deadline
            )
        except Exception:
            pass

    pf_entry = parfinity.lookup_detail(selected.frag_url)
    if pf_entry:
        if _identity_needs_recovery(selected.name):
            selected.name = str(pf_entry.get("name") or "").strip()
        if _identity_needs_recovery(selected.brand):
            selected.brand = str(pf_entry.get("house") or "").strip()
        image_url = str(pf_entry.get("image_url") or "").strip()
        if image_url and not getattr(selected, "image_url", ""):
            setattr(selected, "image_url", image_url)
    parfinity.overlay_onto_details(details, pf_entry or {})
    engine.normalize_notes(details.notes)
    details.reviews = engine.dedupe_reviews(details.reviews)
    try:
        from derived_metrics_adapter import build_derived_metrics

        details.derived_metrics = build_derived_metrics(details)
    except Exception:
        details.derived_metrics = None
    return details


@app.post("/api/fragrances/details")
def details(req: DetailRequest) -> dict[str, Any]:
    """Fetch the full detail bundle for a fragrance returned by /search.

    The response always includes `name`, `house`, `year`, `gender`, and
    `derived_metrics`. If the derived-metrics adapter fails, the raw detail is
    still returned with `derived_metrics: null` (the engine guarantees this).
    """
    selected = _candidate_from_request(req)
    if not selected.bn_url and not selected.frag_url:
        raise HTTPException(
            status_code=400,
            detail="Request resolved to no source URL; provide a valid 'id'.",
        )

    if selected.frag_url and parfinity.is_parfinity_url(selected.frag_url):
        detail_bundle = _fetch_parfinity_detail_bundle(selected, _ARGS.detail_timeout)
        return _details_to_dict(
            selected,
            detail_bundle,
            fragrantica_cached=False,
            fragrantica_cache_source=None,
            enrichment_status="catalog",
            enrichment_requested_count=None,
        )

    stored_detail = _lookup_stored_detail(selected)
    if stored_detail is not None:
        fragrantica_cache_source: str | None = None
        if _apply_fg_detail_cache_db(selected, stored_detail):
            fragrantica_cache_source = "db"
        is_parfumo_fallback = _is_parfumo_fallback_detail(stored_detail)
        enrichment_status: str | None
        enrichment_requested_count: int | None = None
        if (
            fragrantica_cache_source == "db"
            or is_parfumo_fallback
            or bool(stored_detail.frag_cards)
            or _fg_metrics_complete(stored_detail)
        ):
            # Worker already wrote a result; do not re-enqueue even if FG itself
            # never publishes all 4 metric groups. fragrantica_metrics_complete in
            # source_coverage is the truthful signal for "all 4 groups present".
            enrichment_status = "completed"
        else:
            enqueued = _enqueue_enrichment_job(selected, req)
            enrichment_status = "pending" if enqueued is not None else "unavailable"
            if isinstance(enqueued, int) and enqueued > 0:
                enrichment_requested_count = enqueued
        engine.heal_missing_gender_and_year(selected, stored_detail)
        if (
            not is_parfumo_fallback
            and not getattr(stored_detail, "_had_stored_derived_metrics", False)
        ):
            _persist_detail_record(selected, stored_detail)
        return _details_to_dict(
            selected,
            stored_detail,
            fragrantica_cached=bool(stored_detail.frag_cards),
            fragrantica_cache_source=fragrantica_cache_source
            or ("aggregate_db" if stored_detail.frag_cards or is_parfumo_fallback else None),
            enrichment_status=enrichment_status,
            enrichment_requested_count=enrichment_requested_count,
        )

    scraper = engine.get_scraper()
    try:
        detail_bundle = engine.fetch_selected_details(
            scraper, selected, _ARGS.detail_timeout
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Detail fetch failed: {exc}"
        ) from exc

    # Cache lookup order: live Fragrantica -> durable DB cache -> bundled JSON
    # cache -> partial BN-only response. Live data always wins (both overlay
    # helpers no-op when detail_bundle.frag_cards is already populated). The DB
    # cache is the fresh runtime cache so it wins over the shipped JSON file.
    fragrantica_cache_source: str | None = None
    if _apply_fg_detail_cache_db(selected, detail_bundle):
        fragrantica_cache_source = "db"
    elif _apply_fg_detail_cache(selected, detail_bundle):
        fragrantica_cache_source = "json"
    fragrantica_cached = fragrantica_cache_source is not None

    # Partial result: no live or cached FG data. Enqueue so the offline worker
    # can fetch from an environment that is not 403'd. The worker resolves a
    # missing fg_url from name/house at claim time, so BN-only candidates are
    # eligible too. Inert when DATABASE_URL is unset.
    enrichment_status: str | None = None
    enrichment_requested_count: int | None = None
    if fragrantica_cache_source == "db":
        enrichment_status = "completed"
    elif fragrantica_cache_source == "json":
        enrichment_status = "bundled_cache"
    elif not _fg_metrics_complete(detail_bundle):
        enqueued = _enqueue_enrichment_job(selected, req)
        enrichment_status = "pending" if enqueued is not None else "unavailable"
        if isinstance(enqueued, int) and enqueued > 0:
            enrichment_requested_count = enqueued

    engine.heal_missing_gender_and_year(selected, detail_bundle)
    _persist_detail_record(selected, detail_bundle)

    return _details_to_dict(
        selected,
        detail_bundle,
        fragrantica_cached,
        fragrantica_cache_source,
        enrichment_status,
        enrichment_requested_count,
    )


@app.post("/api/fragrances/details/requeue")
def requeue_details(req: RequeueDetailRequest) -> dict[str, Any]:
    """Force-refresh a detail enrichment job for the same payload as /details."""
    selected = _candidate_from_request(req)
    if not selected.bn_url and not selected.frag_url and not _identity_job_key(selected):
        raise HTTPException(
            status_code=400,
            detail="Request resolved to no usable source or identity.",
        )
    if selected.frag_url and parfinity.is_parfinity_url(selected.frag_url):
        return {"queued": False, "reason": "parfinity_catalog"}
    _require_db()
    job = _requeue_enrichment_job(selected, req)
    if not job:
        raise HTTPException(status_code=500, detail="Could not requeue enrichment job.")
    return {"queued": True, "job": job}


# ---------------------------------------------------------------------------
# Enrichment pipeline -- public status + protected worker API.
#
# The worker endpoints are the contract the offline worker (a later pass) will
# consume: it lists pending jobs, claims one, fetches Fragrantica detail in an
# environment that is not 403'd, and uploads the parsed payload back via the
# complete endpoint -- which durably stores it in fg_detail_cache so future
# /details requests hydrate from the DB instead of returning a partial result.
#
# All /api/enrichment/jobs/* endpoints require a bearer token; the public
# status endpoint does not (it leaks only aggregate counts, no job content).
# Every endpoint here is inert with a 503 when DATABASE_URL is unset.
# ---------------------------------------------------------------------------


class CompleteJobRequest(BaseModel):
    fg_url: str | None = None
    # Parfumo fallback (see PARFUMO_FALLBACK_RESOLVER_DESIGN.md §6.D). When the FG
    # resolver finds no page, the worker may complete a job from Parfumo instead.
    # Such a payload carries `source="parfumo"` + `parfumo_url`, no `fg_url`, and
    # no `frag_cards` (Parfumo has no FG status pyramid). Factual fields ride in
    # `notes` / `gender` / `year` plus `raw_identity` (accords/rating/status).
    parfumo_url: str | None = None
    name: str | None = None
    house: str | None = None
    year: int | str | None = None
    gender: str | None = None
    image_url: str | None = None
    schema_version: int = 1
    captured_at: str | None = None
    source: str | None = None
    frag_cards: dict[str, Any] = {}
    notes: dict[str, Any] = {}
    pros_cons: list[Any] = []
    reviews: list[Any] = []
    raw_identity: dict[str, Any] = {}
    quality_status: str | None = None


class FailJobRequest(BaseModel):
    error: str
    retryable: bool = False


class IgnoreJobRequest(BaseModel):
    note: str | None = None
    reason: str | None = None


class RequeueJobRequest(BaseModel):
    priority: int = 10


class PatchJobRequest(BaseModel):
    fg_url: str | None = None
    query: str | None = None
    name: str | None = None
    house: str | None = None


def _json_for_db_blob(obj: Any) -> Any:
    """Return strict JSON-native data for psycopg ``Json()`` payloads.

    The worker/parser boundary is intentionally permissive, so normalize common
    Python objects (dataclasses, Decimal, datetime) before the final round-trip
    strips anything the stdlib encoder still cannot represent.
    """
    return json.loads(json.dumps(jsonable_encoder(obj), default=str))


def _require_db() -> None:
    """Reject enrichment requests when durable storage is not configured."""
    if not db.ENABLED:
        raise HTTPException(
            status_code=503,
            detail="Enrichment storage is not configured (DATABASE_URL unset).",
        )


def _validate_job_id(job_id: str) -> str:
    try:
        return str(uuid.UUID(str(job_id or "")))
    except (TypeError, ValueError, AttributeError) as exc:
        raise HTTPException(status_code=400, detail="job_id must be a UUID.") from exc


def _require_worker_token(authorization: str | None = Header(default=None)) -> None:
    """FastAPI dependency: enforce the worker bearer token. Never logs the token.

    503 when the token env var is unset (endpoint unconfigured), 401 when the
    Authorization header is missing or does not carry the exact bearer token.
    """
    if not _ENRICHMENT_WORKER_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="Worker endpoints are not configured (ENRICHMENT_WORKER_TOKEN unset).",
        )
    expected = f"Bearer {_ENRICHMENT_WORKER_TOKEN}"
    presented = authorization or ""
    # Constant-time comparison; the token value never appears in logs or errors.
    if not hmac.compare_digest(presented, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing worker token.")


def _payload_identity(
    payload: CompleteJobRequest, job: dict[str, Any], canonical_fg_url: str
) -> dict[str, Any]:
    """Prefer the freshly resolved worker identity over the stale job row."""
    raw_identity = payload.raw_identity if isinstance(payload.raw_identity, dict) else {}
    name = str(
        payload.name
        or raw_identity.get("name")
        or job.get("name")
        or ""
    ).strip()
    house = str(
        payload.house
        or raw_identity.get("house")
        or raw_identity.get("brand")
        or job.get("house")
        or ""
    ).strip()
    year = _coerce_year(
        payload.year
        if payload.year is not None
        else raw_identity.get("year", job.get("year"))
    )
    image_url = str(
        payload.image_url
        or raw_identity.get("image_url")
        or raw_identity.get("image")
        or raw_identity.get("thumbnail_url")
        or raw_identity.get("photo_url")
        or ""
    ).strip()
    if image_url and not image_url.startswith(("http://", "https://")):
        image_url = ""

    if canonical_fg_url:
        url_name = engine.FragranticaEngine.name_from_url(canonical_fg_url)
        url_house = engine.FragranticaEngine.brand_from_url(canonical_fg_url)
        if not name and url_name:
            name = url_name
        elif name and url_name and _cache_name_is_suspect(name, url_name):
            name = url_name
        if not house and url_house:
            house = url_house
        elif house and url_house and house.lower() == "unknown":
            house = url_house

    if name and house:
        name = engine.IdentityTools.strip_house_from_name(name, house)

    return {"name": name or None, "house": house or None, "year": year, "image_url": image_url or None}


# --------------------------------------------------------------------------
# Parfumo fallback completion (PARFUMO_FALLBACK_RESOLVER_DESIGN.md §6.D).
# Local, minimal canonicalizer; promote to engine.ParfumoEngine when §6.A lands.
# --------------------------------------------------------------------------
_PARFUMO_HOST = "www.parfumo.com"
_PARFUMO_PERFUME_PATH_RE = re.compile(r"^/(?:Perfumes|Parfums)/[^/]+/[^/?#]+$")


def _canonical_parfumo_url(url: str) -> str:
    """Normalize a public Parfumo perfume URL, or "" if it is not one.

    Forces https + www host, drops query/fragment, strips the trailing slash.
    The /Parfums alias is accepted (Parfumo's own og:url collapses it to
    /Perfumes; the worker is expected to send that canonical form)."""
    text = (url or "").strip()
    if not text:
        return ""
    if "://" not in text and text.lower().startswith(("parfumo.com", "www.parfumo.com")):
        text = "https://" + text
    parsed = urlparse(text)
    host = (parsed.netloc or "").lower()
    if host.endswith("parfumo.com"):
        host = _PARFUMO_HOST
    path = (parsed.path or "").rstrip("/")
    if host != _PARFUMO_HOST or not _PARFUMO_PERFUME_PATH_RE.match(path):
        return ""
    return f"https://{host}{path}"


def _parfumo_payload_has_facts(payload: CompleteJobRequest) -> bool:
    """A Parfumo partial must carry at least one usable factual field, so we
    never store an empty shell that looks enriched but shows nothing."""
    notes = payload.notes if isinstance(payload.notes, dict) else {}
    if any(notes.get(layer) for layer in ("top", "heart", "base", "flat")):
        return True
    raw = payload.raw_identity if isinstance(payload.raw_identity, dict) else {}
    return bool(raw.get("accords") or payload.gender or payload.year)


def _build_parfumo_cache_row(
    payload: CompleteJobRequest, job: dict[str, Any], canonical_parfumo_url: str
) -> dict[str, Any]:
    """Cache row for a Parfumo-sourced partial.

    Keyed by the canonical Parfumo URL in the `canonical_fg_url` slot (that
    column is the cache's generic identity key; using the Parfumo URL keeps it
    unique and never collides with a real FG URL). `frag_cards` is empty: the FG
    hydration path requires quality_status='complete', so this `partial` is never
    mistaken for FG status metrics -- it surfaces via the identity-keyed
    fragrance_records aggregate, exactly like the Parfinity seed pipeline."""
    raw_identity = dict(payload.raw_identity) if isinstance(payload.raw_identity, dict) else {}
    raw_identity.setdefault("parfumo_url", canonical_parfumo_url)
    name = str(payload.name or raw_identity.get("name") or job.get("name") or "").strip()
    house = str(payload.house or raw_identity.get("house") or job.get("house") or "").strip()
    if name and house:
        name = engine.IdentityTools.strip_house_from_name(name, house)
    image_url = str(payload.image_url or "").strip()
    if image_url and not image_url.startswith(("http://", "https://")):
        image_url = ""
    notes = payload.notes if isinstance(payload.notes, dict) else {}
    temp_details = engine.UnifiedDetails(
        notes=engine.NotesList(
            has_pyramid=bool(notes.get("has_pyramid")),
            top=list(notes.get("top") or []),
            heart=list(notes.get("heart") or []),
            base=list(notes.get("base") or []),
            flat=list(notes.get("flat") or []),
        ),
        gender=payload.gender or "Unisex / Unspecified",
        description="",
        frag_cards={},
        pros_cons=[],
    )
    try:
        from derived_metrics_adapter import build_derived_metrics

        derived_metrics = build_derived_metrics(temp_details)
    except Exception:
        derived_metrics = None
    return {
        "canonical_fg_url": canonical_parfumo_url,
        "name": name or None,
        "house": house or None,
        "year": _coerce_year(payload.year),
        "gender": payload.gender or "Unisex / Unspecified",
        "image_url": image_url or None,
        "schema_version": payload.schema_version,
        "source": "parfumo",
        "captured_at": payload.captured_at,
        # Empty on purpose -- Parfumo has no FG status pyramid.
        "frag_cards": _json_for_db_blob({}),
        "notes": _json_for_db_blob(payload.notes or {}),
        "pros_cons": _json_for_db_blob([]),  # never ingest Parfumo review text
        "reviews": _json_for_db_blob([]),
        "raw_identity": _json_for_db_blob(raw_identity),
        "derived_metrics": _json_for_db_blob(derived_metrics)
        if derived_metrics is not None
        else None,
        # Parfumo is never FG-complete; persist as a terminal partial.
        "quality_status": "partial",
    }


@app.get("/api/enrichment/status")
def enrichment_status() -> dict[str, Any]:
    """Public enrichment health signal: job counts by status. No job content."""
    if not db.ENABLED:
        return {"enabled": False, "counts": {}}
    return {"enabled": True, "counts": db.get_status_counts()}


@app.get("/api/enrichment/jobs", dependencies=[Depends(_require_worker_token)])
def list_enrichment_jobs(
    status: str = Query(default="pending"),
    limit: int = Query(default=db.DEFAULT_JOB_LIMIT, ge=1, le=db.MAX_JOB_LIMIT),
) -> dict[str, Any]:
    """Protected: list jobs for the worker, priority-first. Defaults to pending."""
    _require_db()
    if status not in db.VALID_JOB_STATUSES:
        raise HTTPException(status_code=400, detail=f"Unknown status '{status}'.")
    return {"jobs": db.list_jobs(status=status, limit=limit)}


@app.post(
    "/api/enrichment/jobs/{job_id}/claim",
    dependencies=[Depends(_require_worker_token)],
)
def claim_enrichment_job(job_id: str) -> dict[str, Any]:
    """Protected: claim a pending (or stale-processing) job with a lease window."""
    _require_db()
    job_id = _validate_job_id(job_id)
    try:
        result = db.claim_job(job_id)
    except Exception as exc:
        logger.exception("claim_job failed job_id=%s", job_id)
        raise HTTPException(
            status_code=500,
            detail=f"claim_job failed ({type(exc).__name__}).",
        ) from exc
    if result.get("reason") == "not_found":
        raise HTTPException(status_code=404, detail="Job not found.")
    return result


@app.post(
    "/api/enrichment/jobs/{job_id}/requeue",
    dependencies=[Depends(_require_worker_token)],
)
def requeue_enrichment_job(
    job_id: str, payload: RequeueJobRequest | None = None
) -> dict[str, Any]:
    """Protected: force a lost/stale job back to pending."""
    _require_db()
    job_id = _validate_job_id(job_id)
    priority = payload.priority if payload else 10
    updated = db.requeue_job(job_id, priority=priority)
    if not updated:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"queued": True, "job": updated}


@app.post(
    "/api/enrichment/jobs/{job_id}/complete",
    dependencies=[Depends(_require_worker_token)],
)
def complete_enrichment_job(
    job_id: str, payload: CompleteJobRequest
) -> dict[str, Any]:
    """Protected: store parsed Fragrantica detail output and mark job completed.

    Rejects an empty `frag_cards` -- a weak/empty parse must be reported via the
    fail endpoint, never recorded as a successful cache entry.
    """
    _require_db()
    job_id = _validate_job_id(job_id)
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")

    # Parfumo fallback path (§6.D): no fg_url, no frag_cards. Triggered only by an
    # explicit source="parfumo" (NOT by the mere presence of parfumo_url), so a
    # payload that happens to carry a parfumo_url cross-reference alongside real
    # fg_url/frag_cards still takes the FG contract below and is never silently
    # downgraded to a partial. The FG contract is untouched for every caller that
    # does not opt in via source.
    if (payload.source or "").strip().lower() == "parfumo":
        canonical_parfumo = _canonical_parfumo_url(payload.parfumo_url or "")
        if not canonical_parfumo:
            raise HTTPException(
                status_code=400,
                detail="source=parfumo requires a public parfumo_url (/Perfumes or /Parfums).",
            )
        if not _parfumo_payload_has_facts(payload):
            raise HTTPException(
                status_code=400,
                detail="Parfumo payload has no usable facts (notes/accords/gender/year); use /fail instead.",
            )
        cache_row = _build_parfumo_cache_row(payload, job, canonical_parfumo)
        try:
            updated = db.complete_job(job_id, cache_row)
        except Exception as exc:
            logger.exception("complete_job (parfumo) failed job_id=%s url=%s", job_id, canonical_parfumo)
            raise HTTPException(
                status_code=500,
                detail=f"complete_job failed ({type(exc).__name__}): {exc}",
            ) from exc
        if not updated:
            raise HTTPException(status_code=404, detail="Job not found.")
        return {"completed": True, "source": "parfumo", "parfumo_url": canonical_parfumo, "job": updated}

    fg_url = (payload.fg_url or job.get("fg_url") or "").strip()
    if not fg_url:
        raise HTTPException(
            status_code=400,
            detail="No fg_url on the payload or the job; cannot key the cache.",
        )
    canonical = _canonical_fg_url(fg_url)
    if not canonical:
        raise HTTPException(status_code=400, detail="fg_url did not canonicalize.")

    if not isinstance(payload.frag_cards, dict) or not payload.frag_cards:
        raise HTTPException(
            status_code=400,
            detail="frag_cards is empty; report a weak parse via /fail instead.",
        )

    quality = payload.quality_status or "complete"
    if quality not in db.VALID_QUALITY_STATUSES:
        raise HTTPException(
            status_code=400, detail=f"Unknown quality_status '{quality}'."
        )

    identity = _payload_identity(payload, job, canonical)
    temp_selected = engine.UnifiedFragrance(
        name=identity["name"] or "",
        brand=identity["house"] or "",
        year=str(identity["year"] or ""),
        frag_url=canonical,
    )
    p_notes = payload.notes or {}
    temp_details = engine.UnifiedDetails(
        notes=engine.NotesList(
            has_pyramid=bool(p_notes.get("has_pyramid")),
            top=list(p_notes.get("top") or []),
            heart=list(p_notes.get("heart") or []),
            base=list(p_notes.get("base") or []),
            flat=list(p_notes.get("flat") or []),
        ),
        gender=payload.gender or "Unisex / Unspecified",
        description="",
        frag_cards=payload.frag_cards or {},
        pros_cons=payload.pros_cons or [],
    )
    engine.heal_missing_gender_and_year(temp_selected, temp_details)
    try:
        from derived_metrics_adapter import build_derived_metrics

        temp_details.derived_metrics = build_derived_metrics(temp_details)
    except Exception:
        temp_details.derived_metrics = None
    gender = temp_details.gender
    year = _coerce_year(temp_selected.year)

    cache_row = {
        "canonical_fg_url": canonical,
        "name": identity["name"],
        "house": identity["house"],
        "year": year,
        "gender": gender,
        "image_url": identity["image_url"],
        "schema_version": payload.schema_version,
        "source": payload.source or "worker",
        "captured_at": payload.captured_at,
        "frag_cards": _json_for_db_blob(payload.frag_cards),
        "notes": _json_for_db_blob(payload.notes or {}),
        "pros_cons": _json_for_db_blob(payload.pros_cons or []),
        "reviews": _json_for_db_blob(payload.reviews or []),
        "raw_identity": _json_for_db_blob(payload.raw_identity or {}),
        "derived_metrics": _json_for_db_blob(temp_details.derived_metrics)
        if temp_details.derived_metrics is not None
        else None,
        "quality_status": quality,
    }
    try:
        updated = db.complete_job(job_id, cache_row)
    except Exception as exc:
        logger.exception("complete_job failed job_id=%s canonical=%s", job_id, canonical)
        raise HTTPException(
            status_code=500,
            detail=f"complete_job failed ({type(exc).__name__}): {exc}",
        ) from exc
    if not updated:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"completed": True, "canonical_fg_url": canonical, "job": updated}


@app.post(
    "/api/enrichment/jobs/{job_id}/fail",
    dependencies=[Depends(_require_worker_token)],
)
def fail_enrichment_job(job_id: str, payload: FailJobRequest) -> dict[str, Any]:
    """Protected: record a failure. Retryable returns the job to 'pending'."""
    _require_db()
    job_id = _validate_job_id(job_id)
    updated = db.fail_job(job_id, payload.error, payload.retryable)
    if not updated:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"failed": True, "job": updated}


@app.post(
    "/api/enrichment/jobs/{job_id}/ignore",
    dependencies=[Depends(_require_worker_token)],
)
def ignore_enrichment_job(
    job_id: str, payload: IgnoreJobRequest
) -> dict[str, Any]:
    """Protected: permanently retire a job (non-fragrance, bad identity, dead URL)."""
    _require_db()
    job_id = _validate_job_id(job_id)
    note = payload.note or payload.reason
    updated = db.ignore_job(job_id, note)
    if not updated:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"ignored": True, "job": updated}


@app.patch(
    "/api/enrichment/jobs/{job_id}",
    dependencies=[Depends(_require_worker_token)],
)
def patch_enrichment_job(job_id: str, payload: PatchJobRequest) -> dict[str, Any]:
    """Protected: set fg_url or identity hints on a pending/failed job."""
    _require_db()
    job_id = _validate_job_id(job_id)
    fg_url = (payload.fg_url or "").strip() if payload.fg_url is not None else None
    if fg_url is not None:
        canonical = _canonical_fg_url(fg_url)
        if not engine.FragranticaEngine.is_perfume_url(canonical):
            raise HTTPException(
                status_code=400,
                detail="fg_url must be a Fragrantica perfume URL.",
            )
        fg_url = canonical
    updated = db.patch_job(
        job_id,
        fg_url=fg_url,
        query=(payload.query or "").strip() if payload.query is not None else None,
        name=(payload.name or "").strip() if payload.name is not None else None,
        house=(payload.house or "").strip() if payload.house is not None else None,
    )
    if not updated:
        existing = db.get_job(job_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Job not found.")
        raise HTTPException(
            status_code=409,
            detail=f"Job status '{existing.get('status')}' cannot be patched.",
        )
    return {"patched": True, "job": updated}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
