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
import json
import os
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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

app = FastAPI(title="Fragrance Engine API", version="1.0.0")

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
    """
    source_url = item.frag_url or item.bn_url
    return {
        "id": _encode_id(item),
        "name": item.name,
        "house": item.brand,
        "year": _coerce_year(item.year),
        "gender": "Unisex / Unspecified",  # engine default; resolved at detail
        "source_url": source_url,
    }


def _source_coverage(
    selected: engine.UnifiedFragrance,
    details: engine.UnifiedDetails,
    fragrantica_cached: bool = False,
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
        "basenotes_linked": bn_linked,
        "fragrantica_linked": fg_linked,
        "derived_metrics": derived,
        "complete": bn_has_data and fg_has_data,
    }


def _details_to_dict(
    selected: engine.UnifiedFragrance,
    details: engine.UnifiedDetails,
    fragrantica_cached: bool = False,
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
    return {
        # Required top-level fields.
        "name": selected.name,
        "house": selected.brand,
        "year": _coerce_year(selected.year),
        "gender": details.gender,
        "derived_metrics": details.derived_metrics,
        "source_coverage": _source_coverage(selected, details, fragrantica_cached),
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


def _classify_probe(url: str) -> dict[str, Any]:
    """Probe one upstream host and classify reachability. Read-only."""
    import time as _time

    scraper = engine.get_scraper()
    headers = dict(engine.Http.DEFAULT_HEADERS)
    started = _time.monotonic()
    try:
        res = scraper.get(url, timeout=10, headers=headers)
    except Exception as exc:
        return {
            "url": url,
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
    return {
        "url": url,
        "verdict": verdict,
        "http_status": res.status_code,
        "body_length": len(body),
        "cloudflare_challenge": cf_challenge,
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

    return {
        "query": query,
        "results": [_search_result_to_dict(item) for item in results],
        "diagnostics": _search_diagnostics(results),
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
#   [SYS] Found {X} BN links and {Y} FG native links.
_BN_FG_COUNT_RE = __import__("re").compile(
    r"Found\s+(\d+)\s+BN links and\s+(\d+)\s+FG native links"
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

    # Capture the engine's stdout so the [SYS] log lines (incl. the BN/FG link
    # counts) are visible without touching resolver internals.
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

    # Pull the BN / FG native link counts straight out of the captured log
    # line -- this reads no resolver internals, only what the engine printed.
    bn_native_count: int | None = None
    fg_native_count: int | None = None
    count_match = _BN_FG_COUNT_RE.search(sys_log)
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


def _apply_fg_detail_cache(
    selected: engine.UnifiedFragrance, details: engine.UnifiedDetails
) -> bool:
    """Overlay cached Fragrantica detail data onto a BN-only detail bundle.

    Returns True only when cached Fragrantica data was actually applied. Live
    Fragrantica data always wins: if details.frag_cards is already populated
    this is a no-op and returns False.
    """
    # Live Fragrantica data present -> never touch it.
    if details.frag_cards:
        return False
    if not selected.frag_url:
        return False
    entry = _load_fg_detail_cache().get(_canonical_fg_url(selected.frag_url))
    if not entry:
        return False

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
    return engine.UnifiedFragrance(
        name="", brand="", year="", bn_url=bn_url, frag_url=frag_url
    )


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

    # If the live fetch produced no Fragrantica contribution (Railway is 403'd
    # on fragrantica.com), hydrate it from fg_detail_cache_v1.json. Inert when
    # live FG data exists or FG_DETAIL_CACHE_PATH is unset.
    fragrantica_cached = _apply_fg_detail_cache(selected, detail_bundle)

    return _details_to_dict(selected, detail_bundle, fragrantica_cached)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
