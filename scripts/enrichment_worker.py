#!/usr/bin/env python3
"""
enrichment_worker.py -- local Fragrantica enrichment worker.

Runs from an environment that can reach Fragrantica, consumes the protected
Pass 2 worker API, and uploads the engine's already-parsed detail payload into
the durable fg_detail_cache. This file is an orchestrator only: it does not
change parser selectors, resolver scoring, fallback order, or derived-metric
formulas.

Required environment:
    SCENT_API_BASE_URL=https://srt-scent-engine-production.up.railway.app
    ENRICHMENT_WORKER_TOKEN=<secret>

Examples:
    python scripts/enrichment_worker.py --management
    python scripts/enrichment_worker.py --dashboard --debug
    python scripts/enrichment_worker.py --dashboard --auto-approve
    python scripts/enrichment_worker.py --process-pending --limit 20 --delay 45 --jitter 15
    python scripts/enrichment_worker.py --process-pending --limit 10 --delay 90
    python scripts/enrichment_worker.py --warm-list top_queries.txt --delay 60

Environment:
    ENRICHMENT_WORKER_DEBUG=1  Verbose worker/engine output (same as --debug).
    ENRICHMENT_DASHBOARD_AUTO_APPROVE=1  Start the dashboard with auto_approve engaged (same as --auto-approve).
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import random
import re
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlencode

import requests
from fastapi.encoders import jsonable_encoder

# The engine module lives one directory up from scripts/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import fragrance_parser_full_rewrite_fixed as engine  # noqa: E402

DEFAULT_API_BASE_URL = "https://srt-scent-engine-production.up.railway.app"
DEFAULT_DELAY = 60.0
DEFAULT_JITTER = 15.0
DEFAULT_LIMIT = 10
# Per-page fetch deadline (FG+BN). Previously 8s, which left FG without
# headroom on slow responses and surfaced as parser_empty_frag_cards.
DEFAULT_DETAIL_TIMEOUT = 15.0
SCHEMA_VERSION = 1
MAX_LIST_LIMIT = 100
HTTP_TIMEOUT = 30.0
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


class WorkerError(Exception):
    def __init__(self, code: str, message: str | None = None, *, retryable: bool = True):
        self.code = code
        self.retryable = retryable
        super().__init__(message or code)


class StopController:
    def __init__(self) -> None:
        self.stop_requested = False
        self._interrupts = 0

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._handle_sigint)

    def _handle_sigint(self, _signum, _frame) -> None:
        self._interrupts += 1
        self.stop_requested = True
        if self._interrupts >= 2:
            raise KeyboardInterrupt
        print("\nStop requested. Finishing the current safe step, then exiting.")


@dataclass
class WorkerConfig:
    api_base_url: str
    token: str
    delay: float
    jitter: float
    limit: int
    detail_timeout: float
    debug: bool
    dry_run: bool

    @property
    def auth_configured(self) -> bool:
        return bool(self.token)


class ApiClient:
    def __init__(self, base_url: str, token: str, *, debug: bool = False) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.debug = debug
        self.session = requests.Session()

    def _url(self, path: str, params: dict[str, Any] | None = None) -> str:
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"
        return url

    def _headers(self, auth: bool) -> dict[str, str]:
        if not auth:
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        auth: bool = True,
    ) -> dict[str, Any]:
        wire_json = _json_for_request(json)
        try:
            response = self.session.request(
                method,
                self._url(path, params),
                json=wire_json,
                headers=self._headers(auth),
                timeout=HTTP_TIMEOUT,
            )
        except requests.Timeout as exc:
            raise WorkerError("network_timeout", retryable=True) from exc
        except requests.RequestException as exc:
            raise WorkerError("network_error", str(exc), retryable=True) from exc

        if response.status_code == 401:
            raise WorkerError("api_auth_failed", retryable=False)
        if response.status_code == 404:
            raise WorkerError("api_not_found", retryable=False)
        if response.status_code >= 500:
            raise WorkerError(
                "api_unavailable", f"{response.status_code}: {_safe_response_text(response)}", retryable=True
            )
        if response.status_code >= 400:
            raise WorkerError(
                "api_request_failed",
                f"{response.status_code}: {_safe_response_text(response)}",
                retryable=False,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise WorkerError("api_invalid_json", retryable=True) from exc
        if not isinstance(payload, dict):
            raise WorkerError("api_invalid_json", "expected a JSON object", retryable=True)
        return payload

    def status(self) -> dict[str, Any]:
        return self._request("GET", "/api/enrichment/status", auth=False)

    def list_jobs(self, status: str = "pending", limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            "/api/enrichment/jobs",
            params={"status": status, "limit": max(1, min(limit, MAX_LIST_LIMIT))},
        )
        jobs = payload.get("jobs") or []
        if not isinstance(jobs, list):
            raise WorkerError("api_invalid_json", "jobs was not a list", retryable=True)
        return [j for j in jobs if isinstance(j, dict)]

    def claim_job(self, job_id: str) -> dict[str, Any]:
        return self._request("POST", f"/api/enrichment/jobs/{job_id}/claim")

    def complete_job(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/api/enrichment/jobs/{job_id}/complete", json=payload)

    def fail_job(self, job_id: str, error: str, retryable: bool) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/enrichment/jobs/{job_id}/fail",
            json={"error": error, "retryable": bool(retryable)},
        )

    def requeue_job(self, job_id: str, *, priority: int = 10) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/enrichment/jobs/{job_id}/requeue",
            json={"priority": int(priority)},
        )

    def ignore_job(self, job_id: str, note: str | None = None) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/enrichment/jobs/{job_id}/ignore",
            json={"note": note or "ignored from local worker"},
        )

    def claim_command(self) -> dict[str, Any] | None:
        payload = self._request("POST", "/api/enrichment/commands/claim")
        cmd = payload.get("command")
        return cmd if isinstance(cmd, dict) else None

    def finish_command(self, command_id: str, *, ok: bool, result_text: str | None) -> None:
        self._request(
            "POST",
            f"/api/enrichment/commands/{command_id}/finish",
            json={"ok": bool(ok), "result_text": result_text},
        )

    def heartbeat(self) -> None:
        self._request("POST", "/api/enrichment/commands/heartbeat")


def _safe_response_text(response: requests.Response) -> str:
    text = (response.text or "").strip().replace("\n", " ")
    if not text:
        return response.reason or ""
    detail = None
    ctype = (response.headers.get("Content-Type") or "").lower()
    looks_json = "json" in ctype or text.startswith("{")
    if looks_json:
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                raw_detail = data.get("detail")
                if isinstance(raw_detail, str):
                    detail = raw_detail
                elif raw_detail is not None:
                    detail = json.dumps(raw_detail)
        except ValueError:
            detail = None
    out = detail if detail else text
    return out[:500] if len(out) > 500 else out


def _json_for_request(obj: Any) -> Any:
    """Return strict JSON-native data before handing payloads to requests."""
    if obj is None:
        return None
    return json.loads(json.dumps(jsonable_encoder(obj), default=str))


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise SystemExit(f"{name} must be a number, got {raw!r}.")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer, got {raw!r}.")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _canonical_fg_url(url: str) -> str:
    text = (url or "").strip()
    if not text:
        return ""
    canonical = engine.FragranticaEngine.canonical_url(text)
    if engine.FragranticaEngine.is_perfume_url(canonical):
        return canonical.rstrip("/")
    if "://" in text:
        scheme, rest = text.split("://", 1)
        if "/" in rest:
            host, path = rest.split("/", 1)
            text = f"{scheme.lower()}://{host.lower()}/{path}"
        else:
            text = f"{scheme.lower()}://{rest.lower()}"
    return text.rstrip("/")


def _job_label(job: dict[str, Any]) -> str:
    bits = [str(job.get("name") or "").strip(), str(job.get("house") or "").strip()]
    label = " ".join(b for b in bits if b)
    return label or str(job.get("query") or job.get("fg_url") or job.get("job_key") or job.get("id") or "job")


def _build_query(job: dict[str, Any]) -> str:
    query = str(job.get("query") or "").strip()
    if query and not query.lower().startswith("http"):
        return query
    name = str(job.get("name") or "").strip()
    house = str(job.get("house") or "").strip()
    return " ".join(part for part in [house, name] if part).strip()


def _coerce_year(year: Any) -> str:
    if year is None:
        return ""
    return str(year)


def _notes_payload(notes: Any) -> dict[str, Any]:
    return {
        "has_pyramid": bool(getattr(notes, "has_pyramid", False)),
        "top": list(getattr(notes, "top", []) or []),
        "heart": list(getattr(notes, "heart", []) or []),
        "base": list(getattr(notes, "base", []) or []),
        "flat": list(getattr(notes, "flat", []) or []),
    }


def _reviews_payload(reviews: list[Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for review in reviews or []:
        text = str(getattr(review, "text", "") or "").strip()
        source = str(getattr(review, "source", "") or "").strip()
        if text:
            rows.append({"text": text, "source": source})
    return rows


def _raw_identity(candidate: engine.UnifiedFragrance, job: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = {
        "name": candidate.name,
        "house": candidate.brand,
        "year": candidate.year,
        "fg_url": candidate.frag_url,
        "resolver_source": candidate.resolver_source,
        "resolver_score": candidate.resolver_score,
        "query_score": candidate.query_score,
    }
    if job:
        raw["job_key"] = job.get("job_key")
        raw["query"] = job.get("query")
    return {k: v for k, v in raw.items() if v not in ("", None)}


def _specific_identity(candidate: engine.UnifiedFragrance) -> dict[str, Any]:
    """Prefer the URL-derived Fragrantica identity when it is more specific."""
    name = str(candidate.name or "").strip()
    house = str(candidate.brand or "").strip()
    year = candidate.year
    if candidate.frag_url:
        url_name = engine.FragranticaEngine.name_from_url(candidate.frag_url)
        url_house = engine.FragranticaEngine.brand_from_url(candidate.frag_url)
        if url_name and (not name or _url_name_is_more_specific(name, url_name)):
            name = url_name
        if url_house and (not house or house.lower() == "unknown"):
            house = url_house
    return {"name": name or None, "house": house or None, "year": _coerce_year(year) or None}


def _url_name_is_more_specific(stored: str, url_derived: str) -> bool:
    def _tokens(value: str) -> list[str]:
        return [tok for tok in re.split(r"[^a-z0-9]+", value.lower()) if tok]

    stored_tokens = _tokens(stored)
    url_tokens = _tokens(url_derived)
    return bool(url_tokens) and len(url_tokens) > len(stored_tokens)


def _extract_image_url(scraper, fg_url: str, *, debug: bool = False) -> str:
    """Best-effort Fragrantica bottle image extraction for cache refreshes."""
    if not fg_url:
        return ""
    try:
        res = _run_engine_call(
            lambda: scraper.get(
                fg_url, timeout=12, headers=dict(engine.Http.DEFAULT_HEADERS)
            ),
            debug=debug,
        )
    except Exception:
        return ""
    body = getattr(res, "text", "") or ""
    if not body:
        return ""
    try:
        soup = engine.BeautifulSoup(body, "html.parser")
    except Exception:
        return ""

    selectors = [
        ('meta[property="og:image"]', "content"),
        ('meta[name="twitter:image"]', "content"),
        ('img[itemprop="image"]', "src"),
        ('img[src*="/mdimg/perfume/"]', "src"),
        ('img[data-src*="/mdimg/perfume/"]', "data-src"),
    ]
    for selector, attr in selectors:
        node = soup.select_one(selector)
        value = str(node.get(attr) if node else "").strip()
        if value:
            return urljoin(fg_url, value)
    return ""


def _build_engine_args() -> argparse.Namespace:
    args = engine.build_parser().parse_args([])
    # Keep resolution conservative and close to API defaults; no open-web sniper.
    if hasattr(args, "external_search"):
        args.external_search = False
    return args


def _run_engine_call(fn, *, debug: bool):
    if debug:
        return fn()
    captured = io.StringIO()
    try:
        with contextlib.redirect_stdout(captured):
            return fn()
    except Exception:
        _print_engine_lines(captured.getvalue())
        raise


def _print_engine_lines(captured_text: str) -> None:
    for line in _ANSI_RE.sub("", captured_text).splitlines():
        if "[SYS]" in line or "[TIME]" in line:
            print(f"    {line.strip()}")


def resolve_candidate(
    scraper,
    args: argparse.Namespace,
    job: dict[str, Any],
    *,
    debug: bool = False,
) -> engine.UnifiedFragrance:
    fg_url = engine.FragranticaEngine.canonical_url(str(job.get("fg_url") or "").strip())
    if fg_url:
        return engine.UnifiedFragrance(
            name=str(job.get("name") or ""),
            brand=str(job.get("house") or ""),
            year=_coerce_year(job.get("year")),
            bn_url="",
            frag_url=fg_url,
        )

    query = _build_query(job)
    if not query:
        raise WorkerError("fg_url_missing_after_resolution", "job has no fg_url or usable query", retryable=False)

    try:
        results = _run_engine_call(lambda: engine.search_once(scraper, query, args), debug=debug)
    except Exception as exc:
        raise WorkerError("engine_exception", f"resolver failed: {exc}", retryable=True) from exc

    for item in results:
        if item.frag_url:
            return engine.UnifiedFragrance(
                name=item.name,
                brand=item.brand,
                year=item.year,
                bn_url="",
                frag_url=item.frag_url,
                resolver_source=item.resolver_source,
                resolver_score=item.resolver_score,
                query_score=item.query_score,
            )
    raise WorkerError("fg_url_missing_after_resolution", "resolver returned no Fragrantica URL", retryable=False)


_FG_METRIC_GROUPS = (
    "performance_score",
    "value_score",
    "community_interest_score",
    "wear_profile",
)


def _worker_metrics_complete(details: Any) -> bool:
    """True when all 4 Fragrantica status-derived metric groups parsed.

    Mirrors api._fg_metrics_complete: a partial fetch (encrypted status payload
    did not decode) must not be stored as a trustworthy 'complete' cache entry,
    otherwise api._apply_fg_detail_cache_db would hydrate from it and the job
    would never be re-enqueued for a retry.
    """
    try:
        from derived_metrics_adapter import build_derived_metrics

        dm = build_derived_metrics(details)
    except Exception:
        return False
    if not isinstance(dm, dict):
        return False
    cov = dm.get("source_coverage") or {}
    return all(bool(cov.get(g)) for g in _FG_METRIC_GROUPS)


def fetch_payload(
    scraper,
    candidate: engine.UnifiedFragrance,
    job: dict[str, Any],
    *,
    detail_timeout: float,
    debug: bool = False,
) -> dict[str, Any]:
    if not candidate.frag_url:
        raise WorkerError("fg_url_missing_after_resolution", retryable=False)
    candidate.frag_url = engine.FragranticaEngine.canonical_url(candidate.frag_url)
    try:
        details = _run_engine_call(
            lambda: engine.fetch_selected_details(scraper, candidate, detail_timeout),
            debug=debug,
        )
    except Exception as exc:
        raise WorkerError("engine_exception", f"detail fetch failed: {exc}", retryable=True) from exc

    frag_cards = details.frag_cards or {}
    if not isinstance(frag_cards, dict) or not frag_cards:
        retryable = bool(engine.FragranticaEngine.is_perfume_url(candidate.frag_url))
        fetch_errors = getattr(details, "fetch_errors", {}) or {}
        parse_diagnostics = getattr(details, "parse_diagnostics", {}) or {}
        fg_diag = parse_diagnostics.get("fg") if isinstance(parse_diagnostics, dict) else {}
        if not isinstance(fg_diag, dict):
            fg_diag = {}
        fg_err = fetch_errors.get("fg")
        url_shape_ok = engine.FragranticaEngine.is_perfume_url(candidate.frag_url)
        parts = [f"url_shape_ok={url_shape_ok}"]
        if fg_err:
            parts.append(f"fg_fetch_error={fg_err}")
        else:
            parts.append("fg_fetch_error=none (parser produced no cards from fetched HTML)")
        for key in (
            "html_bytes",
            "has_status_payload",
            "status_decode_ok",
            "challenge_detected",
        ):
            if key in fg_diag:
                parts.append(f"{key}={fg_diag.get(key)}")
        notes = getattr(details, "notes", None)
        notes_count = fg_diag.get("notes_count")
        if notes_count is None and notes is not None:
            notes_count = (
                len(getattr(notes, "top", []) or [])
                + len(getattr(notes, "heart", []) or [])
                + len(getattr(notes, "base", []) or [])
                + len(getattr(notes, "flat", []) or [])
            )
        if notes_count is not None:
            parts.append(f"notes_count={notes_count}")
        if fg_diag.get("status_decode_error"):
            parts.append(f"status_decode_error={fg_diag.get('status_decode_error')}")
        print(f"  [diag] parser_empty_frag_cards fg_url={candidate.frag_url!r} {' '.join(parts)}")
        raise WorkerError("parser_empty_frag_cards", "; ".join(parts), retryable=retryable)

    identity = _specific_identity(candidate)
    image_url = _extract_image_url(scraper, candidate.frag_url, debug=debug)
    raw_identity = _raw_identity(candidate, job)
    if image_url:
        raw_identity["image_url"] = image_url

    return {
        "fg_url": candidate.frag_url,
        "name": identity["name"],
        "house": identity["house"],
        "year": identity["year"],
        "image_url": image_url or None,
        "schema_version": SCHEMA_VERSION,
        "captured_at": _now_iso(),
        "source": "local_enrichment_worker",
        "frag_cards": frag_cards,
        "notes": _notes_payload(details.notes),
        "pros_cons": list(details.pros_cons or []),
        "reviews": _reviews_payload(details.reviews or []),
        "raw_identity": raw_identity,
        # "complete" only when all 4 status-derived metric groups parsed. A
        # "partial" entry is ignored by api._apply_fg_detail_cache_db, so the
        # job is naturally re-enqueued and retried on the next /details call.
        "quality_status": "complete" if _worker_metrics_complete(details) else "partial",
    }


def process_job(
    client: ApiClient,
    scraper,
    args: argparse.Namespace,
    job: dict[str, Any],
    *,
    index_label: str,
    config: WorkerConfig,
    already_claimed: bool = False,
) -> bool:
    job_id = str(job.get("id") or "")
    label = _job_label(job)
    if not job_id:
        print(f"{index_label} Skipping malformed job without id: {label}")
        return False

    try:
        if config.dry_run:
            print(f"{index_label} Dry run: would claim {label}")
        elif not already_claimed:
            claim = client.claim_job(job_id)
            if not claim.get("claimed"):
                print(f"{index_label} Claim skipped for {label}: {claim.get('reason', 'not_claimed')}")
                return False
            print(f"{index_label} Claimed {label}")
        else:
            print(f"{index_label} Processing claimed job {label}")

        candidate = resolve_candidate(scraper, args, job, debug=config.debug)
        payload = fetch_payload(
            scraper,
            candidate,
            job,
            detail_timeout=config.detail_timeout,
            debug=config.debug,
        )
        notes = payload.get("notes") or {}
        note_count = sum(len(notes.get(k) or []) for k in ("top", "heart", "base", "flat"))
        print(
            f"{index_label} Parsed frag_cards={len(payload['frag_cards'])} "
            f"notes={note_count} reviews={len(payload['reviews'])}"
        )

        if config.dry_run:
            print(f"{index_label} Dry run: would upload cache for {payload['fg_url']}")
            return True

        try:
            client.complete_job(job_id, payload)
        except WorkerError as exc:
            if exc.code == "api_request_failed":
                raise WorkerError(
                    "complete_endpoint_rejected_payload",
                    str(exc),
                    retryable=False,
                ) from exc
            raise
        print(f"{index_label} Completed and uploaded cache")
        return True

    except KeyboardInterrupt:
        print(f"{index_label} Interrupted while processing {label}; claim will expire naturally.")
        return False
    except WorkerError as exc:
        print(f"{index_label} Failed {label}: {exc.code}")
        if exc.code == "api_unavailable" and str(exc) != exc.code:
            detail = str(exc).split(":", 1)[-1].strip()
            if detail:
                print(f"{index_label} Server: {detail[:200]}")
        elif config.debug and str(exc) != exc.code:
            print(f"{index_label} {exc}")
        if config.dry_run:
            print(f"{index_label} Dry run: would mark retryable={exc.retryable}")
            return False
        try:
            client.fail_job(job_id, str(exc), exc.retryable)
            print(f"{index_label} Marked {'retryable' if exc.retryable else 'non-retryable'} failure")
        except WorkerError as fail_exc:
            print(f"{index_label} Could not report failure: {fail_exc.code}")
        return False
    except Exception as exc:
        print(f"{index_label} Failed {label}: engine_exception")
        if config.debug:
            raise
        if not config.dry_run:
            with contextlib.suppress(Exception):
                client.fail_job(job_id, "engine_exception", True)
            print(f"{index_label} Marked retryable failure")
        return False


def requeue_terminal_failed_jobs(
    client: ApiClient,
    config: WorkerConfig,
    *,
    limit: int,
) -> int:
    """Move terminal `failed` rows back to `pending` so the worker can process them."""
    failed_jobs = client.list_jobs("failed", limit=max(limit, MAX_LIST_LIMIT))
    requeued = 0
    for job in failed_jobs[:limit]:
        job_id = str(job.get("id") or "").strip()
        if not job_id:
            continue
        if config.dry_run:
            print(f"Dry run: would requeue failed job {job_id}")
            requeued += 1
            continue
        client.requeue_job(job_id, priority=max(10, int(job.get("priority") or 0) + 1))
        requeued += 1
    if requeued:
        print(f"Requeued {requeued} terminal failed job(s) to pending.")
    return requeued


def process_pending(
    client: ApiClient,
    config: WorkerConfig,
    *,
    only_retries: bool = False,
    stop: StopController | None = None,
) -> int:
    if not config.auth_configured:
        raise SystemExit("ENRICHMENT_WORKER_TOKEN is required for worker operations.")

    if only_retries:
        requeue_terminal_failed_jobs(client, config, limit=config.limit)

    list_limit = max(config.limit, MAX_LIST_LIMIT if only_retries else config.limit)
    pending_all = client.list_jobs("pending", limit=list_limit)
    jobs = pending_all
    if only_retries:
        jobs = [j for j in jobs if int(j.get("failure_count") or 0) > 0]
    jobs = jobs[: config.limit]
    if not jobs:
        print("No matching pending jobs.")
        return 0

    scraper = engine.get_scraper()
    fg_scraper = engine.get_fragrantica_scraper(scraper.default_scraper, mint_clearance=False)
    if not engine._validate_fragrantica_session(fg_scraper):
        raise SystemExit("Clearance preflight failed: fragrantica_clearance is missing or invalid. Trigger mint or provide env vars.")
    engine_args = _build_engine_args()
    completed = 0
    total = len(jobs)
    for idx, job in enumerate(jobs, 1):
        if stop and stop.stop_requested:
            break
        ok = process_job(
            client,
            scraper,
            engine_args,
            job,
            index_label=f"[{idx}/{total}]",
            config=config,
        )
        completed += 1 if ok else 0
        if idx < total and not (stop and stop.stop_requested):
            sleep_conservatively(config.delay, config.jitter, stop)
    print(f"Processed {len(jobs)} job(s); completed {completed}.")
    return completed


def sleep_conservatively(delay: float, jitter: float, stop: StopController | None = None) -> None:
    seconds = max(0.0, float(delay or 0.0))
    if jitter > 0:
        seconds += random.uniform(-float(jitter), float(jitter))
        seconds = max(0.0, seconds)
    if seconds <= 0:
        return
    print(f"Sleeping {int(round(seconds))}s...")
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if stop and stop.stop_requested:
            return
        time.sleep(min(1.0, deadline - time.monotonic()))


def load_queries(path: str) -> list[str]:
    rows = Path(path).read_text(encoding="utf-8").splitlines()
    queries = []
    for line in rows:
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        queries.append(text)
    return queries


def _find_matching_job(
    jobs: list[dict[str, Any]],
    query: str,
    candidate: engine.UnifiedFragrance,
) -> dict[str, Any] | None:
    canonical = _canonical_fg_url(candidate.frag_url)
    query_lower = query.lower().strip()
    for job in jobs:
        job_urls = [
            _canonical_fg_url(str(job.get("fg_url") or "")),
            _canonical_fg_url(str(job.get("job_key") or "")),
        ]
        if canonical and canonical in job_urls:
            return job
        if query_lower and query_lower == str(job.get("query") or "").lower().strip():
            return job
    return None


def warm_list(client: ApiClient, config: WorkerConfig, path: str, stop: StopController | None = None) -> int:
    if not config.auth_configured:
        raise SystemExit("ENRICHMENT_WORKER_TOKEN is required for warm-list uploads.")
    queries = load_queries(path)
    if not queries:
        raise SystemExit(f"No queries found in {path!r}.")

    scraper = engine.get_scraper()
    fg_scraper = engine.get_fragrantica_scraper(scraper.default_scraper, mint_clearance=False)
    if not engine._validate_fragrantica_session(fg_scraper):
        raise SystemExit("Clearance preflight failed: fragrantica_clearance is missing or invalid. Trigger mint or provide env vars.")
    engine_args = _build_engine_args()
    completed = 0
    print(f"Warm list: {len(queries)} queries from {path}")
    for idx, query in enumerate(queries, 1):
        if stop and stop.stop_requested:
            break
        print(f"[{idx}/{len(queries)}] Resolving {query!r}")
        synthetic = {"id": f"warm:{idx}", "query": query}
        try:
            candidate = resolve_candidate(scraper, engine_args, synthetic, debug=config.debug)
            pending = client.list_jobs("pending", limit=MAX_LIST_LIMIT)
            match = _find_matching_job(pending, query, candidate)
            if not match:
                print(
                    f"[{idx}/{len(queries)}] No matching pending job for {candidate.frag_url}; "
                    "manual enqueue endpoint is unavailable."
                )
                continue
            ok = process_job(
                client,
                scraper,
                engine_args,
                match,
                index_label=f"[{idx}/{len(queries)}]",
                config=config,
            )
            completed += 1 if ok else 0
        except WorkerError as exc:
            print(f"[{idx}/{len(queries)}] Failed {query!r}: {exc.code}")
            if config.debug and str(exc) != exc.code:
                print(f"[{idx}/{len(queries)}] {exc}")
        if idx < len(queries) and not (stop and stop.stop_requested):
            sleep_conservatively(config.delay, config.jitter, stop)
    print(f"Warm-list completed {completed} matching job(s).")
    return completed


def _status_counts(client: ApiClient) -> dict[str, Any]:
    try:
        status = client.status()
    except WorkerError as exc:
        return {"enabled": "unavailable", "counts": {}, "error": exc.code}
    counts = status.get("counts") if isinstance(status.get("counts"), dict) else {}
    return {"enabled": status.get("enabled"), "counts": counts}


def print_manager_header(client: ApiClient, config: WorkerConfig) -> None:
    status = _status_counts(client)
    counts = status.get("counts") or {}
    print()
    print("SRT SET ENGINE  ·  enrichment control")
    print()
    print(f"API: {config.api_base_url}")
    print(f"Worker auth: {'configured' if config.auth_configured else 'missing'}")
    if status.get("error"):
        print(f"Status endpoint: unavailable ({status['error']})")
    print(f"Pending jobs: {counts.get('pending', 'unavailable')}")
    print(f"Processing stale: unavailable")
    print(f"Failed retryable: unavailable")
    print(f"Completed today: unavailable")
    print()
    print("1. List pending jobs")
    print("2. Process next job")
    print("3. Process next N jobs")
    print("4. Process jobs slowly with delay")
    print("5. Retry failed jobs")
    print("6. Ignore job")
    print("7. Show cache stats")
    print("8. Warm from manual query list")
    print("9. Exit")


def _print_jobs(jobs: list[dict[str, Any]]) -> None:
    if not jobs:
        print("No jobs.")
        return
    for idx, job in enumerate(jobs, 1):
        print(
            f"{idx:02d}. {job.get('id')} | {_job_label(job)} | "
            f"status={job.get('status')} failures={job.get('failure_count')} "
            f"fg_url={job.get('fg_url') or 'missing'}"
        )


def dispatch_management_action(
    choice: str, client: ApiClient, config: WorkerConfig, stop: StopController
) -> str | None:
    """Run a single management action by menu number.

    Shared by the static menu (run_management) and the live dashboard hotkeys.
    Returns "exit" when the user chose to leave; None otherwise. Callers are
    responsible for catching WorkerError / KeyboardInterrupt around this.
    """
    if choice == "1":
        ensure_token(config)
        _print_jobs(client.list_jobs("pending", limit=MAX_LIST_LIMIT))
    elif choice == "2":
        ensure_token(config)
        one = WorkerConfig(**{**config.__dict__, "limit": 1})
        process_pending(client, one, stop=stop)
    elif choice == "3":
        ensure_token(config)
        limit = _prompt_int("How many jobs? ", DEFAULT_LIMIT)
        batch = WorkerConfig(**{**config.__dict__, "limit": limit})
        process_pending(client, batch, stop=stop)
    elif choice == "4":
        ensure_token(config)
        limit = _prompt_int("How many jobs? ", DEFAULT_LIMIT)
        delay = _prompt_float("Delay seconds? ", config.delay)
        jitter = _prompt_float("Jitter seconds? ", config.jitter)
        batch = WorkerConfig(**{**config.__dict__, "limit": limit, "delay": delay, "jitter": jitter})
        process_pending(client, batch, stop=stop)
    elif choice == "5":
        ensure_token(config)
        retry_cfg = WorkerConfig(**{**config.__dict__, "limit": config.limit})
        process_pending(client, retry_cfg, only_retries=True, stop=stop)
    elif choice == "6":
        ensure_token(config)
        job_id = input("Job id to ignore: ").strip()
        note = input("Note/reason: ").strip()
        if job_id:
            if config.dry_run:
                print(f"Dry run: would ignore {job_id}")
            else:
                client.ignore_job(job_id, note)
                print("Job ignored.")
    elif choice == "7":
        print("Cache stats endpoint is unavailable in the current API contract.")
    elif choice == "8":
        ensure_token(config)
        path = input("Query list path: ").strip()
        if path:
            warm_list(client, config, path, stop=stop)
    elif choice == "9":
        return "exit"
    else:
        print("Unknown option.")
    return None


def run_management(client: ApiClient, config: WorkerConfig, stop: StopController) -> int:
    while not stop.stop_requested:
        print_manager_header(client, config)
        choice = input("Choose an option: ").strip()
        try:
            if dispatch_management_action(choice, client, config, stop) == "exit":
                return 0
        except WorkerError as exc:
            print(f"Operation failed: {exc.code}")
            if config.debug and str(exc) != exc.code:
                print(str(exc))
        except KeyboardInterrupt:
            stop.stop_requested = True
            print("\nStop requested.")
    return 0


# --------------------------------------------------------------------------
# Live management dashboard
# --------------------------------------------------------------------------

DASHBOARD_MIN_INTERVAL = 5
DASHBOARD_MAX_INTERVAL = 600
DASHBOARD_STEP = 15
# Auto-approve claims + completes one pending enrichment on this fixed cadence.
AUTO_APPROVE_INTERVAL = 5
_DASH_STATUSES = ("pending", "processing", "completed", "failed", "ignored")

# The number keys the live dashboard accepts, with plain-language labels so the
# operator never has to guess what a hotkey does. Keys 1-6 here must stay in
# sync with dispatch_management_action().
DASHBOARD_ACTIONS: dict[str, str] = {
    "1": "List pending jobs",
    "2": "Process next job",
    "3": "Process next N jobs",
    "4": "Process slowly (with delay)",
    "5": "Retry failed jobs",
    "6": "Ignore a job",
}


def _read_key_nonblocking() -> str:
    """Return a pending keypress without blocking, or "" if none.

    Interactive hotkeys rely on Windows' msvcrt; on other platforms this always
    returns "" so the dashboard still auto-refreshes, just without live keys.
    """
    try:
        import msvcrt  # type: ignore
    except ImportError:
        return ""
    if not msvcrt.kbhit():
        return ""
    ch = msvcrt.getwch()
    # Arrow / function keys arrive as a two-part sequence; swallow the second
    # part so it is not mistaken for a hotkey.
    if ch in ("\x00", "\xe0"):
        if msvcrt.kbhit():
            msvcrt.getwch()
        return ""
    return ch


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _colors() -> dict[str, str]:
    """Reuse the engine's ANSI palette; degrade to plain text if unavailable."""
    return {name: str(getattr(engine, name, "") or "") for name in ("B", "Y", "C", "D", "R", "G", "Z")}


# Rolling buffer of the most recent operator-visible events. Rendered in the
# LIVE_STREAM block on every refresh so messages emitted between frames are not
# wiped by the screen clear. Keep the cap small — the block has a fixed height
# in the layout and longer history just pushes old lines off-screen anyway.
_LIVE_LOG: deque[tuple[str, str, str]] = deque(maxlen=4)


def _log_event(message: str, kind: str = "info") -> None:
    """Push a one-line event onto the LIVE_STREAM buffer.

    kind ∈ {info, ok, warn, err} controls color in the rendered stream.
    """
    stamp = datetime.now().strftime("%H:%M:%S")
    _LIVE_LOG.append((stamp, kind, message))


def _gather_dashboard_snapshot(client: ApiClient) -> dict[str, Any]:
    """One status call (always) plus two targeted job-list calls for the
    actionable "needs attention" metrics. The job-list calls require a worker
    token; without one they fail quietly and those metrics render as unknown."""
    status = _status_counts(client)
    snapshot: dict[str, Any] = {
        "enabled": status.get("enabled"),
        "counts": dict(status.get("counts") or {}),
        "error": status.get("error"),
        "retryable_failures": None,
        "stale_processing": None,
    }
    if snapshot["error"]:
        return snapshot
    try:
        # Pending with prior failures plus terminal failed rows (requeued on retry).
        pending = client.list_jobs("pending", limit=MAX_LIST_LIMIT)
        pending_retries = sum(1 for j in pending if int(j.get("failure_count") or 0) > 0)
        terminal_failed = int((snapshot.get("counts") or {}).get("failed") or 0)
        snapshot["retryable_failures"] = pending_retries + terminal_failed
    except WorkerError:
        pass
    try:
        processing = client.list_jobs("processing", limit=MAX_LIST_LIMIT)
        now = datetime.now(timezone.utc)
        snapshot["stale_processing"] = sum(
            1
            for j in processing
            if (_parse_iso(j.get("claim_expires_at")) or now) < now
        )
    except WorkerError:
        pass
    return snapshot


def _fmt_delta(delta: int, c: dict[str, str]) -> str:
    if delta > 0:
        return f"{c['G']}[+{delta}]{c['Z']}"
    if delta < 0:
        return f"{c['R']}[{delta}]{c['Z']}"
    return f"{c['D']}[ 0]{c['Z']}"


def _section_header(label: str, c: dict[str, str], width: int) -> str:
    """Hacker-style block header: ├─[ LABEL ]──────── …┤

    Spans width+2 columns so the corners align with the ╔/╗ on the top frame.
    """
    inside = f" {label} "
    fill = max(0, width - len(inside) - 2)
    return f"  {c['G']}├─[{c['B']}{inside}{c['Z']}{c['G']}]{'─' * fill}┤{c['Z']}"


def _render_dashboard(
    snapshot: dict[str, Any],
    baseline: dict[str, int],
    config: WorkerConfig,
    interval: int,
    auto_approve: bool = False,
) -> None:
    c = _colors()
    os.system("cls" if os.name == "nt" else "clear")
    width = 64
    now = datetime.now().strftime("%H:%M:%S")
    top = f"  {c['G']}╔{'═' * width}╗{c['Z']}"

    # ── Banner ────────────────────────────────────────────────────────────
    print()
    print(top)
    title = "SRT//SET-ENGINE ▒▒ enrichment.control"
    leftover = max(0, width - len(title))
    left_pad = leftover // 2
    right_pad = leftover - left_pad
    print(
        f"  {c['G']}║{c['Z']}{' ' * left_pad}{c['B']}{c['G']}{title}{c['Z']}"
        f"{' ' * right_pad}{c['G']}║{c['Z']}"
    )
    auth = (
        f"{c['G']}[LINK ▲ OK]{c['Z']}"
        if config.auth_configured
        else f"{c['R']}[LINK ▼ NO-TOKEN]{c['Z']}"
    )
    print(
        f"   {c['G']}>{c['Z']} {c['D']}t={c['Z']}{c['G']}{now}{c['Z']}   "
        f"{c['D']}refresh={c['Z']}{c['G']}{interval}s{c['Z']}   {auth}"
    )
    print(f"   {c['G']}>{c['Z']} {c['D']}api={c['Z']}{c['G']}{config.api_base_url}{c['Z']}")

    # ── Auto-approve state ────────────────────────────────────────────────
    if auto_approve:
        print(
            f"   {c['B']}{c['G']}▓▓ AUTO_APPROVE: ENGAGED ▓▓{c['Z']}   "
            f"{c['D']}tick={AUTO_APPROVE_INTERVAL}s · claim+complete oldest pending{c['Z']}"
        )
    else:
        print(
            f"   {c['D']}░░ auto_approve: idle ░░   "
            f"press [a] to engage hands-free approval{c['Z']}"
        )

    # ── QUEUE_STATUS ──────────────────────────────────────────────────────
    print(_section_header("QUEUE_STATUS", c, width))
    if snapshot.get("error"):
        print(f"  {c['G']}│{c['Z']}  {c['R']}!! status endpoint unavailable: {snapshot['error']}{c['Z']}")
        print(f"  {c['G']}│{c['Z']}  {c['D']}retrying on next refresh…{c['Z']}")
    else:
        counts = snapshot.get("counts") or {}
        for name in _DASH_STATUSES:
            val = int(counts.get(name) or 0)
            delta = val - int(baseline.get(name) or 0)
            val_col = c["R"] if name == "failed" and val > 0 else c["G"]
            print(
                f"  {c['G']}│{c['Z']}  {c['G']}>{c['Z']} {name:<12}"
                f"{c['B']}{val_col}{val:>6}{c['Z']}   {_fmt_delta(delta, c)}"
            )

    # ── NEEDS_ATTENTION ───────────────────────────────────────────────────
    print(_section_header("NEEDS_ATTENTION", c, width))
    rf = snapshot.get("retryable_failures")
    sp = snapshot.get("stale_processing")
    def _alert(val: Any) -> str:
        if val is None:
            return f"{c['D']}-- (needs token){c['Z']}"
        n = int(val)
        col = c["R"] if n > 0 else c["G"]
        return f"{c['B']}{col}{n}{c['Z']}"
    print(f"  {c['G']}│{c['Z']}  {c['G']}>{c['Z']} retryable_failures   {_alert(rf)}")
    print(f"  {c['G']}│{c['Z']}  {c['G']}>{c['Z']} stale_processing     {_alert(sp)}")

    # ── ACTIONS ───────────────────────────────────────────────────────────
    print(_section_header("ACTIONS", c, width))
    left = [("1", DASHBOARD_ACTIONS["1"]), ("2", DASHBOARD_ACTIONS["2"]), ("3", DASHBOARD_ACTIONS["3"])]
    right = [("4", DASHBOARD_ACTIONS["4"]), ("5", DASHBOARD_ACTIONS["5"]), ("6", DASHBOARD_ACTIONS["6"])]
    for (lk, ll), (rk, rl) in zip(left, right):
        plain_left = f"[{lk}] {ll}"
        pad = max(3, 30 - len(plain_left))
        print(
            f"  {c['G']}│{c['Z']}  {c['G']}[{lk}]{c['Z']} {ll}{' ' * pad}"
            f"{c['G']}[{rk}]{c['Z']} {rl}"
        )

    # ── LIVE_STREAM ───────────────────────────────────────────────────────
    print(_section_header("LIVE_STREAM", c, width))
    if not _LIVE_LOG:
        print(f"  {c['G']}│{c['Z']}  {c['D']}(no events yet — engage auto_approve or pick an action){c['Z']}")
    else:
        kind_col = {"ok": c["G"], "info": c["G"], "warn": c["Y"], "err": c["R"]}
        for stamp, kind, msg in _LIVE_LOG:
            col = kind_col.get(kind, c["G"])
            marker = {"ok": "✓", "info": ">", "warn": "!", "err": "x"}.get(kind, ">")
            print(f"  {c['G']}│{c['Z']}  {c['D']}{stamp}{c['Z']} {col}{marker}{c['Z']} {msg}")

    # ── Footer / hotkeys ─────────────────────────────────────────────────
    print(f"  {c['G']}└{'─' * width}┘{c['Z']}")
    auto_key = (
        f"{c['B']}{c['G']}[a] auto_approve=ON{c['Z']}"
        if auto_approve
        else f"{c['D']}[a] auto_approve=off{c['Z']}"
    )
    print(f"   {auto_key}")
    print(
        f"   {c['D']}[r] refresh   [m] paste token   [+/-] interval   [q] quit{c['Z']}"
    )


def _run_auto_approve(
    client: ApiClient,
    config: WorkerConfig,
    stop: StopController,
    auto_state: dict[str, Any],
) -> None:
    """Claim and complete the single oldest pending enrichment.

    This is the per-tick body of auto-approve mode: it grabs one pending job,
    runs it through the same process_job() path the manual actions use, and
    returns. The engine scraper/args are built once and cached on auto_state so
    repeated ticks stay cheap.
    """
    try:
        jobs = client.list_jobs("pending", limit=1)
    except WorkerError as exc:
        _log_event(f"auto: cannot read queue ({exc.code})", "err")
        return
    if not jobs:
        _log_event("auto: queue empty — nothing to approve", "info")
        return
    if auto_state.get("scraper") is None:
        auto_state["scraper"] = engine.get_scraper()
        auto_state["args"] = _build_engine_args()
    job = jobs[0]
    target = job.get("name") or job.get("brand") or job.get("id") or "next job"
    _log_event(f"auto: processing {target}", "info")
    # process_job() prints progress to stdout; swallow it so the dashboard
    # frame stays intact and only our LIVE_STREAM lines surface to the user.
    # It also catches its own scraping errors and returns False rather than
    # re-raising, so we MUST inspect the return value — otherwise a row that
    # silently fails-and-requeues looks identical to a success here.
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            ok = process_job(
                client,
                auto_state["scraper"],
                auto_state["args"],
                job,
                index_label="[auto]",
                config=config,
            )
    except KeyboardInterrupt:
        stop.stop_requested = True
        _log_event("auto: interrupted by operator", "warn")
        return
    except WorkerError as exc:
        _log_event(f"auto: failed ({exc.code})", "err")
        return
    if ok:
        _log_event(f"auto: completed {target}", "ok")
    else:
        _log_event(f"auto: failed {target}", "err")


def _execute_phone_command(
    cmd: dict[str, Any],
    client: ApiClient,
    config: WorkerConfig,
    stop: StopController,
    auto_state: dict[str, Any],
    runtime: dict[str, Any],
) -> tuple[bool, str]:
    """Execute one phone-issued command. Returns (ok, result_text).

    runtime carries the live dashboard's mutable settings — currently the
    auto_approve flag — so phone toggles match what the local UI does.
    Scraping commands run inline; the dashboard tick is short and the operator
    initiated the action remotely.
    """
    kind = cmd.get("kind") or ""
    payload = cmd.get("payload") or {}

    if kind == "toggle_auto_approve":
        runtime["auto_approve"] = not bool(runtime.get("auto_approve"))
        return True, f"auto_approve={'on' if runtime['auto_approve'] else 'off'}"

    if kind == "set_auto_approve":
        state = str(payload.get("state") or "").lower()
        if state not in ("on", "off"):
            return False, "bad state"
        runtime["auto_approve"] = state == "on"
        return True, f"auto_approve={state}"

    if kind == "process_pending":
        try:
            limit = int(payload.get("limit") or 1)
        except (TypeError, ValueError):
            limit = 1
        limit = max(1, min(limit, 50))
        # Drive process_pending() with a clone config restricted to this batch.
        batch_cfg = WorkerConfig(**{**config.__dict__, "limit": limit})
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                completed = process_pending(client, batch_cfg, stop=stop)
            return True, f"processed {completed}/{limit}"
        except SystemExit as exc:
            return False, str(exc)
        except WorkerError as exc:
            return False, exc.code

    if kind == "retry_failed":
        retry_cfg = WorkerConfig(**{**config.__dict__})
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                completed = process_pending(client, retry_cfg, only_retries=True, stop=stop)
            return True, f"retried {completed}"
        except SystemExit as exc:
            return False, str(exc)
        except WorkerError as exc:
            return False, exc.code

    return False, f"unknown_kind:{kind}"


def _poll_phone_commands(
    client: ApiClient,
    config: WorkerConfig,
    stop: StopController,
    auto_state: dict[str, Any],
    runtime: dict[str, Any],
) -> None:
    """Heartbeat + drain queued phone commands. Best-effort: errors are logged
    via LIVE_STREAM but never break the dashboard tick."""
    if not config.auth_configured:
        return
    try:
        client.heartbeat()
    except WorkerError as exc:
        _log_event(f"cmd: heartbeat failed ({exc.code})", "err")
        return
    # Drain up to a few commands per tick to keep latency low without
    # starving the auto-approve loop.
    for _ in range(3):
        if stop.stop_requested:
            return
        try:
            cmd = client.claim_command()
        except WorkerError as exc:
            _log_event(f"cmd: claim failed ({exc.code})", "err")
            return
        if not cmd:
            return
        kind = cmd.get("kind") or "?"
        _log_event(f"cmd: executing {kind} (from phone)", "info")
        try:
            ok, msg = _execute_phone_command(cmd, client, config, stop, auto_state, runtime)
        except Exception as exc:  # noqa: BLE001 - we report any failure back
            ok, msg = False, f"exception:{type(exc).__name__}"
        try:
            client.finish_command(cmd["id"], ok=ok, result_text=msg[:240] if msg else None)
        except WorkerError as exc:
            _log_event(f"cmd: finish failed ({exc.code})", "err")
        _log_event(f"cmd: {kind} -> {'ok' if ok else 'fail'} ({msg})", "ok" if ok else "err")


def run_live_dashboard(
    client: ApiClient,
    config: WorkerConfig,
    stop: StopController,
    *,
    initial_auto_approve: bool = False,
) -> int:
    """Auto-refreshing visual of the enrichment queue cume.

    The refresh interval seeds from the worker delay and is adjustable live with
    +/-. Menu numbers 1-6 dispatch the same actions as the static manager menu.
    Pressing [a] toggles auto-approve, which claims + completes one pending job
    every AUTO_APPROVE_INTERVAL seconds with no further input.

    Pass ``initial_auto_approve=True`` (or set ENRICHMENT_DASHBOARD_AUTO_APPROVE / ``--auto-approve``)
    to start with that mode already engaged.
    """
    interval = int(max(DASHBOARD_MIN_INTERVAL, min(DASHBOARD_MAX_INTERVAL, round(config.delay or DEFAULT_DELAY))))
    baseline: dict[str, int] | None = None
    refresh_now = True
    # auto_approve is held inside `runtime` so phone-issued commands can flip
    # it from _execute_phone_command() without a second source of truth.
    runtime: dict[str, Any] = {"auto_approve": False}
    if initial_auto_approve:
        if config.auth_configured:
            runtime["auto_approve"] = True
            _log_event("auto_approve ENGAGED (from terminal flag/env)", "ok")
        else:
            _log_event("auto_approve not started: set ENRICHMENT_WORKER_TOKEN first", "err")
    auto_state: dict[str, Any] = {"scraper": None, "args": None}
    c = _colors()
    while not stop.stop_requested:
        auto_approve = bool(runtime["auto_approve"])
        # Drain phone commands before drawing the frame so any auto_approve
        # toggle issued from the phone shows up immediately on this refresh.
        _poll_phone_commands(client, config, stop, auto_state, runtime)
        auto_approve = bool(runtime["auto_approve"])
        if refresh_now:
            snapshot = _gather_dashboard_snapshot(client)
            if baseline is None and not snapshot.get("error"):
                baseline = dict(snapshot.get("counts") or {})
            _render_dashboard(snapshot, baseline or {}, config, interval, auto_approve)
            refresh_now = False

        # Wait one cadence, polling for a keypress each second. When auto-approve
        # is on the cadence is the fixed 5s tick; otherwise it is the refresh
        # interval. Either way a keypress breaks out immediately.
        action_key = ""
        wait_total = AUTO_APPROVE_INTERVAL if auto_approve else interval
        waited = 0
        while waited < wait_total and not stop.stop_requested:
            left = wait_total - waited
            if auto_approve:
                line = (
                    f"  {c['Y']}● AUTO{c['Z']} {c['D']}approving next job in "
                    f"{left:>2}s — press [a] to stop…{c['Z']}   "
                )
            else:
                line = f"  {c['D']}next refresh in {left:>3}s…{c['Z']}   "
            print(line, end="\r", flush=True)
            slept = 0.0
            while slept < 1.0:
                key = _read_key_nonblocking()
                if key:
                    action_key = key
                    break
                time.sleep(0.1)
                slept += 0.1
            if action_key:
                break
            waited += 1
        print(" " * 72, end="\r")  # wipe the countdown line

        if stop.stop_requested:
            break

        key = action_key.lower()
        if not action_key:
            # The cadence elapsed with no key. In auto-approve mode that means
            # it is time to process one job; otherwise just refresh the view.
            if auto_approve:
                _run_auto_approve(client, config, stop, auto_state)
            refresh_now = True
            continue
        if key == "q":
            break
        if key == "a":
            if not runtime["auto_approve"] and not config.auth_configured:
                _log_event("auto_approve needs a worker token — press [m] to paste token", "err")
            else:
                runtime["auto_approve"] = not runtime["auto_approve"]
                _log_event(
                    "auto_approve ENGAGED" if runtime["auto_approve"] else "auto_approve disengaged",
                    "ok" if runtime["auto_approve"] else "info",
                )
            refresh_now = True
        elif key in ("+", "="):
            interval = min(DASHBOARD_MAX_INTERVAL, interval + DASHBOARD_STEP)
            refresh_now = True
        elif key in ("-", "_"):
            interval = max(DASHBOARD_MIN_INTERVAL, interval - DASHBOARD_STEP)
            refresh_now = True
        elif key == "m":
            had_token = config.auth_configured
            _dashboard_token_prompt(client, config)
            if config.auth_configured and not had_token:
                _log_event("worker token attached", "ok")
            elif not config.auth_configured:
                _log_event("worker token missing — paste with [m] or set ENRICHMENT_WORKER_TOKEN", "warn")
            refresh_now = True
        elif key in ("1", "2", "3", "4", "5", "6"):
            print()
            try:
                dispatch_management_action(key, client, config, stop)
            except WorkerError as exc:
                print(f"Operation failed: {exc.code}")
                if config.debug and str(exc) != exc.code:
                    print(str(exc))
            except KeyboardInterrupt:
                print("\nAction interrupted.")
            input("\nPress Enter to return to the dashboard… ")
            refresh_now = True
        else:
            # 'r' or any other key — just refresh.
            refresh_now = True
    print("\nLeaving SRT Set Engine.")
    return 0


def _read_secret(label: str) -> str:
    """Prompt for a secret value, hiding the typed characters where possible.

    Falls back to a plain visible prompt on terminals that cannot mask input
    (so pasting a token still works no matter where this runs)."""
    try:
        import getpass

        return getpass.getpass(label)
    except Exception:
        return input(label)


def _run_automatic_clearance_mint() -> None:
    """Trigger the Fragrantica clearance minting process locally."""
    c = _colors()
    print(f"   {c['D']}>> Minting Fragrantica clearance session...{c['Z']}")
    if os.name == "nt" and not os.environ.get("BASENOTES_CHROMIUM_PATH"):
        default_chrome = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        if os.path.exists(default_chrome):
            os.environ["BASENOTES_CHROMIUM_PATH"] = default_chrome
    
    try:
        os.environ["FRAGRANTICA_CHROMIUM_HEADLESS"] = "0"
        session = engine._mint_fragrantica_clearance()
        if session is not None:
            print(f"   {c['G']}✓ Fragrantica clearance minted and cached successfully.{c['Z']}")
        else:
            err = getattr(engine, "_FRAGRANTICA_LAST_MINT_ERROR", None) or "Unknown error"
            print(f"   {c['R']}x Failed to mint Fragrantica clearance: {err}{c['Z']}")
    except Exception as exc:
        print(f"   {c['R']}x Error during minting: {exc}{c['Z']}")


def _dashboard_token_prompt(client: ApiClient, config: WorkerConfig) -> bool:
    """Paste worker token while the live dashboard is running ([m]).

    Updates ``config`` and ``client``. Returns True if a token is now configured."""
    print()
    c = _colors()
    print(f"   {c['D']}Worker token — paste to attach (empty = cancel){c['Z']}")
    try:
        entered = _read_secret(f"   {c['B']}token ›{c['Z']} ").strip()
    except (EOFError, KeyboardInterrupt):
        print(f"   {c['Y']}cancelled{c['Z']}")
        return config.auth_configured
    if not entered:
        print(f"   {c['Y']}cancelled{c['Z']}")
        return config.auth_configured

    config.token = entered
    client.token = entered
    print(f"   {c['D']}verifying…{c['Z']}")
    try:
        client.list_jobs("pending", limit=1)
    except WorkerError as exc:
        if exc.code in ("api_auth_failed", "api_request_failed"):
            print(f"   {c['R']}x token rejected ({exc.code}).{c['Z']}")
            config.token = ""
            client.token = ""
        else:
            print(
                f"   {c['Y']}! could not verify right now ({exc.code}); keeping token — retry on refresh.{c['Z']}"
            )
            time.sleep(1.0)
    else:
        print(f"   {c['G']}✓ access granted.{c['Z']}")
        _run_automatic_clearance_mint()

    try:
        input(f"\n   {c['D']}Press Enter to return to the dashboard…{c['Z']} ")
    except (EOFError, KeyboardInterrupt):
        pass
    return config.auth_configured


def _login_gate(client: ApiClient, config: WorkerConfig, stop: StopController) -> bool:
    """Branded sign-in screen shown before the live dashboard opens.

    Asks for the enrichment worker token, verifies it against the protected
    job-list endpoint, and stores the accepted token on both the config and the
    client. Returns True once access is granted, False if the operator backs
    out. A token found in the environment is offered as the default.
    """
    c = _colors()
    width = 64
    env_token = os.environ.get("ENRICHMENT_WORKER_TOKEN", "")
    title = "SRT//SET-ENGINE ▒▒ enrichment.control"
    while not stop.stop_requested:
        os.system("cls" if os.name == "nt" else "clear")
        print()
        print(f"  {c['G']}╔{'═' * width}╗{c['Z']}")
        leftover = max(0, width - len(title))
        lp = leftover // 2
        rp = leftover - lp
        print(
            f"  {c['G']}║{c['Z']}{' ' * lp}{c['B']}{c['G']}{title}{c['Z']}"
            f"{' ' * rp}{c['G']}║{c['Z']}"
        )
        print(f"  {c['G']}╚{'═' * width}╝{c['Z']}")
        print()
        print(f"   {c['G']}>{c['Z']} {c['D']}api={c['Z']}{c['G']}{config.api_base_url}{c['Z']}")
        print()
        if env_token:
            print(f"   {c['G']}>{c['Z']} {c['G']}token detected in environment.{c['Z']}")
            print(f"   {c['G']}>{c['Z']} press {c['B']}{c['G']}Enter{c['Z']} to use it, or paste a different token.")
        else:
            print(f"   {c['G']}>{c['Z']} enter your {c['B']}{c['G']}enrichment worker token{c['Z']} to unlock controls.")
        print(f"   {c['D']}>> typing is hidden. submit an empty token to cancel.{c['Z']}")
        print()
        try:
            entered = _read_secret(f"   {c['B']}{c['G']}token ›{c['Z']} ").strip()
        except (EOFError, KeyboardInterrupt):
            return False
        token = entered or env_token
        if not token:
            print(f"\n   {c['Y']}!! no token provided — disconnecting.{c['Z']}")
            return False
        config.token = token
        client.token = token
        print(f"\n   {c['D']}>> verifying token…{c['Z']}")
        try:
            client.list_jobs("pending", limit=1)
        except WorkerError as exc:
            if exc.code in ("api_auth_failed", "api_request_failed"):
                print(f"   {c['R']}x token rejected ({exc.code}).{c['Z']}")
                env_token = ""  # a rejected env token should not be re-offered
                try:
                    input(f"   {c['D']}press Enter to retry, or Ctrl+C to exit…{c['Z']} ")
                except (EOFError, KeyboardInterrupt):
                    return False
                continue
            # Network/server hiccup rather than a bad token — let the operator
            # in and let the dashboard keep retrying its own calls on refresh.
            print(f"   {c['Y']}! could not verify right now ({exc.code}); continuing anyway.{c['Z']}")
            time.sleep(1.4)
            return True
        print(f"   {c['G']}✓ access granted.{c['Z']}")
        _run_automatic_clearance_mint()
        time.sleep(0.8)
        return True
    return False


def launch_dashboard(
    api_base_url: str | None = None,
    *,
    initial_auto_approve: bool | None = None,
    debug: bool | None = None,
) -> int:
    """Build a config from the environment and open the live dashboard.

    This is the single entry point the main parser script imports for its
    startup "Management" mode, and what the worker's --dashboard flag calls.
    The operator must clear the token sign-in gate before the dashboard opens.

    If ``initial_auto_approve`` is None, reads ENRICHMENT_DASHBOARD_AUTO_APPROVE (1/true/on).
    If ``debug`` is None, reads ENRICHMENT_WORKER_DEBUG (1/true/on).
    """
    base = (api_base_url or os.environ.get("SCENT_API_BASE_URL", DEFAULT_API_BASE_URL)).rstrip("/")
    dbg = bool(debug) if debug is not None else _env_bool("ENRICHMENT_WORKER_DEBUG", False)
    config = WorkerConfig(
        api_base_url=base,
        token=os.environ.get("ENRICHMENT_WORKER_TOKEN", ""),
        delay=_env_float("ENRICHMENT_DEFAULT_DELAY", DEFAULT_DELAY),
        jitter=_env_float("ENRICHMENT_DEFAULT_JITTER", DEFAULT_JITTER),
        limit=_env_int("ENRICHMENT_DEFAULT_LIMIT", DEFAULT_LIMIT),
        detail_timeout=DEFAULT_DETAIL_TIMEOUT,
        debug=dbg,
        dry_run=False,
    )
    client = ApiClient(config.api_base_url, config.token, debug=config.debug)
    stop = StopController()
    stop.install()
    try:
        if not _login_gate(client, config, stop):
            print("\nLeaving SRT Set Engine.")
            return 0
        auto0 = (
            bool(initial_auto_approve)
            if initial_auto_approve is not None
            else _env_bool("ENRICHMENT_DASHBOARD_AUTO_APPROVE", False)
        )
        return run_live_dashboard(client, config, stop, initial_auto_approve=auto0)
    except KeyboardInterrupt:
        print("\nLeaving SRT Set Engine.")
        return 0


def ensure_token(config: WorkerConfig) -> None:
    if not config.auth_configured:
        raise SystemExit(
            "missing_worker_token: set ENRICHMENT_WORKER_TOKEN, run `python scripts/enrichment_worker.py --dashboard` "
            "interactively to use the sign-in screen, or from the live dashboard press [m] to paste a token."
        )


def _prompt_int(label: str, default: int) -> int:
    raw = input(label).strip()
    if not raw:
        return default
    return int(raw)


def _prompt_float(label: str, default: float) -> float:
    raw = input(label).strip()
    if not raw:
        return default
    return float(raw)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--management", action="store_true", help="Show the interactive management menu.")
    parser.add_argument("--dashboard", action="store_true", help="Open the live auto-refreshing management dashboard.")
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="With --dashboard: start with hands-free auto_approve engaged (needs ENRICHMENT_WORKER_TOKEN). "
        "Or set ENRICHMENT_DASHBOARD_AUTO_APPROVE=1.",
    )
    parser.add_argument("--process-pending", action="store_true", help="Process pending enrichment jobs.")
    parser.add_argument("--warm-list", help="Path to one-query-per-line warm list.")
    parser.add_argument("--api-base-url", default=os.environ.get("SCENT_API_BASE_URL", DEFAULT_API_BASE_URL))
    parser.add_argument("--limit", type=int, default=_env_int("ENRICHMENT_DEFAULT_LIMIT", DEFAULT_LIMIT))
    parser.add_argument("--delay", type=float, default=_env_float("ENRICHMENT_DEFAULT_DELAY", DEFAULT_DELAY))
    parser.add_argument("--jitter", type=float, default=_env_float("ENRICHMENT_DEFAULT_JITTER", DEFAULT_JITTER))
    parser.add_argument("--once", action="store_true", help="Alias for --process-pending --limit 1.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve and parse without mutating worker job state.")
    parser.add_argument(
        "--detail-timeout",
        type=float,
        default=DEFAULT_DETAIL_TIMEOUT,
        help="Per-page detail fetch deadline (seconds).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Verbose worker/engine output and full API error text. Or set ENRICHMENT_WORKER_DEBUG=1.",
    )
    return parser.parse_args()


def build_config(opts: argparse.Namespace) -> WorkerConfig:
    limit = 1 if opts.once else max(1, int(opts.limit or DEFAULT_LIMIT))
    debug_on = bool(opts.debug) or _env_bool("ENRICHMENT_WORKER_DEBUG", False)
    return WorkerConfig(
        api_base_url=str(opts.api_base_url or DEFAULT_API_BASE_URL).rstrip("/"),
        token=os.environ.get("ENRICHMENT_WORKER_TOKEN", ""),
        delay=max(0.0, float(opts.delay)),
        jitter=max(0.0, float(opts.jitter)),
        limit=limit,
        detail_timeout=max(0.1, float(opts.detail_timeout)),
        debug=debug_on,
        dry_run=bool(opts.dry_run),
    )


def main() -> int:
    opts = parse_args()
    if opts.auto_approve and not opts.dashboard:
        raise SystemExit("--auto-approve only applies with --dashboard.")
    config = build_config(opts)
    client = ApiClient(config.api_base_url, config.token, debug=config.debug)
    stop = StopController()
    stop.install()

    try:
        if opts.dashboard:
            # Unlike `launch_dashboard()` (parser [M] entry), `--dashboard` used to skip the
            # sign-in gate — operators pasted a token only in the parser flow, then hit this
            # path without ENRICHMENT_WORKER_TOKEN and failed at ensure_token().
            if not config.auth_configured:
                if sys.stdin.isatty():
                    if not _login_gate(client, config, stop):
                        print("\nLeaving SRT Set Engine.")
                        return 0
                else:
                    raise SystemExit(
                        "missing_worker_token: set ENRICHMENT_WORKER_TOKEN (stdin is not interactive; cannot prompt)."
                    )
            initial_auto = opts.auto_approve or _env_bool("ENRICHMENT_DASHBOARD_AUTO_APPROVE", False)
            return run_live_dashboard(client, config, stop, initial_auto_approve=initial_auto)
        if opts.process_pending or opts.once:
            ensure_token(config)
            process_pending(client, config, stop=stop)
            return 0
        if opts.warm_list:
            ensure_token(config)
            warm_list(client, config, opts.warm_list, stop=stop)
            return 0
        return run_management(client, config, stop)
    except KeyboardInterrupt:
        print("\nInterrupted. Claimed in-flight work was not marked complete; its lease may expire naturally.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
