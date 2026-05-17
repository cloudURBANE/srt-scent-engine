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
import os
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import db
import fragrance_parser_full_rewrite_fixed as engine

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
    "ALLOW_BUNDLED_FG_SEARCH_CACHE", default=not db.ENABLED
)
_CACHE_SEARCH_MIN_SCORE = engine.QueryRepair.MIN_RESULT_SCORE

# Enrichment worker auth. The protected /api/enrichment/jobs/* endpoints require
# `Authorization: Bearer <ENRICHMENT_WORKER_TOKEN>`. This token is for the
# offline worker only -- it is never sent to the frontend and is never logged.
# Unset => the worker endpoints are unconfigured and return 503.
_ENRICHMENT_WORKER_TOKEN = os.environ.get("ENRICHMENT_WORKER_TOKEN", "")

app = FastAPI(title="Fragrance Engine API", version="1.0.0")


@app.on_event("startup")
def _startup() -> None:
    """Bootstrap durable storage. Inert when DATABASE_URL is unset (local dev)."""
    db.init_db()

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

# ---------------------------------------------------------------------------
# Opaque id token: stateless encode/decode of an engine candidate.
# ---------------------------------------------------------------------------


def _encode_id(item: engine.UnifiedFragrance) -> str:
    """Pack the fields fetch_selected_details needs into an opaque token."""
    payload = {
        "n": item.name,
        "b": item.brand,
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
    name = (item.name or "").strip()
    house = (item.brand or "").strip()
    if (not house or house.lower() == "unknown") and item.frag_url:
        url_brand = engine.FragranticaEngine.brand_from_url(item.frag_url)
        if url_brand:
            house = url_brand
    if not name and item.frag_url:
        url_name = engine.FragranticaEngine.name_from_url(item.frag_url)
        if url_name:
            name = url_name
    return {
        "id": _encode_id(item),
        "name": name,
        "house": house,
        "year": _coerce_year(item.year),
        "gender": "Unisex / Unspecified",  # engine default; resolved at detail
        "source_url": source_url,
    }


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
    return candidate


def _json_cache_search(query: str, limit: int) -> list[engine.UnifiedFragrance]:
    if not _ALLOW_BUNDLED_FG_SEARCH_CACHE:
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
        if item.query_score < _CACHE_SEARCH_MIN_SCORE:
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


def _identity_cache_search(query: str, limit: int) -> list[engine.UnifiedFragrance]:
    """Last-resort search candidates from the engine's existing FG identity cache.

    This is used only when live providers return zero candidates. It does not
    alter resolver scoring or the normal search path; it simply exposes the same
    warmed identity cache that SearchSniper already uses after candidates exist.
    """
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
        if item.query_score < _CACHE_SEARCH_MIN_SCORE:
            continue
        seen.add(fg_url)
        candidates.append(item)
    candidates.sort(
        key=lambda item: (item.query_score, item.resolver_score, item.year),
        reverse=True,
    )
    return candidates[:limit]


def _cache_search_fallback(query: str, limit: int) -> tuple[list[engine.UnifiedFragrance], str | None]:
    """Fallback search for blocked live providers, with DB as source of truth."""
    seen: set[str] = set()
    candidates: list[engine.UnifiedFragrance] = []
    source: str | None = None

    for entry in db.search_detail_cache(query, limit=limit * 2):
        item = _candidate_from_cache_entry(entry, "fg_detail_db_cache")
        if not item:
            continue
        item.query_score = engine.IdentityTools.relevance_score(query, item)
        if item.query_score < _CACHE_SEARCH_MIN_SCORE:
            continue
        key = item.frag_url or item.cache_key
        if key in seen:
            continue
        seen.add(key)
        candidates.append(item)
    if candidates:
        source = "db"
    else:
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
    `frag_cards` for FG). `fragrantica_cached` = the Fragrantica contribution
    came from fg_detail_cache_v1.json rather than a live fetch -- cached data is
    real parsed parser output, but it is labelled honestly as not-live.
    `derived_metrics` is "full" only when both sources contributed; "partial"
    when one did; "none" when the adapter returned null.
    """
    bn_linked = bool(selected.bn_url)
    fg_linked = bool(selected.frag_url)
    bn_has_data = bn_linked and bool(details.bn_consensus)
    fg_has_data = fg_linked and bool(details.frag_cards)
    if details.derived_metrics is None:
        derived = "none"
    elif bn_has_data and fg_has_data:
        derived = "full"
    else:
        derived = "partial"
    return {
        "basenotes": bn_has_data,
        "fragrantica": fg_has_data,
        "fragrantica_cached": fragrantica_cached,
        # "db" (durable Postgres cache), "json" (bundled file), or null (live or
        # absent). Additive honesty signal; existing keys keep their meaning.
        "fragrantica_cache_source": fragrantica_cache_source,
        "basenotes_linked": bn_linked,
        "fragrantica_linked": fg_linked,
        "derived_metrics": derived,
        "complete": bn_has_data and fg_has_data,
    }


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


def _details_to_dict(
    selected: engine.UnifiedFragrance,
    details: engine.UnifiedDetails,
    fragrantica_cached: bool = False,
    fragrantica_cache_source: str | None = None,
    enrichment_status: str | None = None,
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
    notes = details.notes
    payload = {
        # Required top-level fields.
        "name": selected.name,
        "house": selected.brand,
        "year": _coerce_year(selected.year),
        "gender": details.gender,
        "derived_metrics": details.derived_metrics,
        "source_coverage": _source_coverage(
            selected, details, fragrantica_cached, fragrantica_cache_source
        ),
        # Raw fields, included for convenience (do not remove derived_metrics).
        "raw": {
            "description": details.description,
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
        payload["enrichment"] = {
            "status": enrichment_status,
            "requires_worker": enrichment_status == "pending",
            "cache_source": fragrantica_cache_source,
        }
    return payload


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class DetailRequest(BaseModel):
    id: str | None = None
    source_url: str | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> dict[str, bool]:
    """Liveness probe for Railway and the frontend."""
    return {"ok": True}


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

    scraper = engine.get_scraper()
    try:
        results = engine.search_once(scraper, query, _ARGS)
    except Exception as exc:  # engine degrades cleanly; surface a 502 if not
        raise HTTPException(status_code=502, detail=f"Search failed: {exc}") from exc

    fallback_source: str | None = None
    if not results:
        results, fallback_source = _cache_search_fallback(query, _ARGS.max_results)

    diagnostics = _search_diagnostics(results)
    if fallback_source:
        diagnostics["fallback_source"] = fallback_source
        diagnostics["warning"] = (
            "Live providers returned zero candidates; results came from the "
            f"{fallback_source} detail cache."
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

    Strictly additive -- live data always wins (callers only invoke this when
    `details.frag_cards` is empty), fields are filled only when empty, and the
    existing derived_metrics adapter is re-run exactly as the engine runs it.
    """
    cached_cards = entry.get("frag_cards")
    if not isinstance(cached_cards, dict) or not cached_cards:
        return False
    details.frag_cards = cached_cards

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

    # pros_cons / reviews: additive only -- never replace live content.
    cached_pros_cons = entry.get("pros_cons") or []
    if isinstance(cached_pros_cons, list):
        details.pros_cons.extend(str(p) for p in cached_pros_cons)
    cached_reviews = entry.get("reviews") or []
    if isinstance(cached_reviews, list):
        for r in cached_reviews:
            if isinstance(r, dict) and r.get("text"):
                details.reviews.append(
                    engine.Review(
                        text=str(r["text"]), source=str(r.get("source", ""))
                    )
                )

    # Re-run the existing adapter over the merged object. Best-effort: a failure
    # here must not break the raw detail response (mirrors the engine contract
    # in fetch_selected_details).
    try:
        from derived_metrics_adapter import build_derived_metrics

        details.derived_metrics = build_derived_metrics(details)
    except Exception:
        details.derived_metrics = None
    return True


def _apply_fg_detail_cache_db(
    selected: engine.UnifiedFragrance, details: engine.UnifiedDetails
) -> bool:
    """Hydrate from the durable Postgres fg_detail_cache. Inert when DB disabled.

    This is the second tier of the detail lookup order (live FG -> DB cache ->
    bundled JSON cache -> partial). Only `quality_status = 'complete'` entries
    are trusted for hydration; weaker entries are left for a worker re-fetch.
    """
    if details.frag_cards or not selected.frag_url:
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
    # Live Fragrantica data present -> never touch it.
    if details.frag_cards:
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


def _candidate_from_request(req: DetailRequest) -> engine.UnifiedFragrance:
    """Reconstruct the engine candidate from the detail request.

    Prefers the opaque `id` token (carries both source URLs). Falls back to a
    bare `source_url`, routed to bn_url/frag_url by domain.
    """
    if req.id:
        try:
            data = _decode_id(req.id)
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail=f"Invalid 'id' token: {exc}"
            ) from exc
        return engine.UnifiedFragrance(
            name=data.get("n", ""),
            brand=data.get("b", ""),
            year=data.get("y", ""),
            bn_url=data.get("bn", ""),
            frag_url=data.get("fg", ""),
        )

    url = (req.source_url or "").strip()
    if not url:
        raise HTTPException(
            status_code=400,
            detail="Either 'id' or 'source_url' is required.",
        )
    lowered = url.lower()
    bn_url = url if "basenotes" in lowered else ""
    frag_url = url if "fragrantica" in lowered or not bn_url else ""
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
) -> bool:
    """Enqueue (or upsert) an enrichment job for a partial detail result.

    Job key prefers the canonical Fragrantica URL, falls back to the canonical
    Basenotes URL, then to a brand|name identity slug -- so a BN-only candidate
    still gets a pending row that the worker can resolve to FG at claim time.
    Repeated requests for the same fragrance bump requested_count instead of
    duplicating. Any failure here is swallowed -- enrichment is a background
    nicety and must never break the user-facing /details response.
    """
    try:
        job_key = (
            _canonical_fg_url(selected.frag_url)
            or _canonical_fg_url(selected.bn_url)
            or _identity_job_key(selected)
        )
        if not job_key:
            return False
        db.enqueue_job(
            job_key=job_key,
            query=(req.source_url or None),
            name=selected.name or None,
            house=selected.brand or None,
            year=_coerce_year(selected.year),
            bn_url=selected.bn_url or None,
            fg_url=selected.frag_url or None,
        )
        return True
    except Exception:
        # Best-effort: never let queue write failures surface to the client.
        return False


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
    if fragrantica_cache_source == "db":
        enrichment_status = "complete"
    elif fragrantica_cache_source == "json":
        enrichment_status = "bundled_cache"
    elif not detail_bundle.frag_cards:
        enrichment_status = "pending" if _enqueue_enrichment_job(selected, req) else "unavailable"

    return _details_to_dict(
        selected,
        detail_bundle,
        fragrantica_cached,
        fragrantica_cache_source,
        enrichment_status,
    )


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


def _require_db() -> None:
    """Reject enrichment requests when durable storage is not configured."""
    if not db.ENABLED:
        raise HTTPException(
            status_code=503,
            detail="Enrichment storage is not configured (DATABASE_URL unset).",
        )


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
    result = db.claim_job(job_id)
    if result.get("reason") == "not_found":
        raise HTTPException(status_code=404, detail="Job not found.")
    return result


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
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")

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

    cache_row = {
        "canonical_fg_url": canonical,
        "name": job.get("name"),
        "house": job.get("house"),
        "year": job.get("year"),
        "schema_version": payload.schema_version,
        "source": payload.source or "worker",
        "captured_at": payload.captured_at,
        "frag_cards": payload.frag_cards,
        "notes": payload.notes,
        "pros_cons": payload.pros_cons,
        "reviews": payload.reviews,
        "raw_identity": payload.raw_identity,
        "quality_status": quality,
    }
    updated = db.complete_job(job_id, cache_row)
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
    note = payload.note or payload.reason
    updated = db.ignore_job(job_id, note)
    if not updated:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"ignored": True, "job": updated}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
