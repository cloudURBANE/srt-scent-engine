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


def _details_to_dict(
    selected: engine.UnifiedFragrance, details: engine.UnifiedDetails
) -> dict[str, Any]:
    """Serialize a full detail bundle.

    `derived_metrics` is passed through exactly as derived_metrics_adapter
    produced it -- a dict on success, or `null` if the adapter raised. It is
    never fabricated.
    """
    notes = details.notes
    return {
        # Required top-level fields.
        "name": selected.name,
        "house": selected.brand,
        "year": _coerce_year(selected.year),
        "gender": details.gender,
        "derived_metrics": details.derived_metrics,
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
    }


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

    return _details_to_dict(selected, detail_bundle)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
    )
