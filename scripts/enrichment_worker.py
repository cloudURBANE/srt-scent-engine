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
import csv
import contextlib
import io
import json
import os
import random
import re
import signal
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from enrichment_facts import FACT_FIELDS, expand_raw_accords, record_fact_status  # noqa: E402

DEFAULT_API_BASE_URL = "https://srt-scent-engine-production.up.railway.app"
DEFAULT_DELAY = 60.0
DEFAULT_JITTER = 15.0
DEFAULT_LIMIT = 10
# Per-page fetch deadline (FG+BN). Previously 8s, which left FG without
# headroom on slow responses and surfaced as parser_empty_frag_cards.
DEFAULT_DETAIL_TIMEOUT = 15.0
SCHEMA_VERSION = 1
MAX_LIST_LIMIT = 100
# Dashboard/list reads fail fast; complete/fail uploads get more headroom.
HTTP_CONNECT_TIMEOUT = 3.0
HTTP_READ_TIMEOUT = 12.0
HTTP_WRITE_TIMEOUT = 45.0
# Cap job-list scans used only for attention counters (not full queue dumps).
DASHBOARD_ATTENTION_LIMIT = 25
# Phone remote control heartbeat — not every 5s dashboard tick.
PHONE_HEARTBEAT_INTERVAL = 30.0
# Self-healing sweep: when auto_approve finds the queue empty, it asks the API
# to audit stored fragrances for missing facts and requeue the incomplete ones
# (POST /api/enrichment/heal). Throttled to once per interval; 0 disables.
HEAL_SWEEP_INTERVAL_MINUTES = float(os.environ.get("ENRICHMENT_HEAL_SWEEP_MINUTES", "30"))
HEAL_SWEEP_PAGE_LIMIT = 200
# Churn guard: stop re-opening a job after this many total requests -- a row
# whose upstream sources genuinely lack a fact should not loop forever.
HEAL_SWEEP_MAX_REQUESTED = int(os.environ.get("ENRICHMENT_HEAL_SWEEP_MAX_REQUESTED", "8"))
# Resolver misses get this many retryable failures before going terminal.
# Queue analysis showed every resolver miss was marked non-retryable on its
# first attempt, so transient SERP/budget hiccups became permanent failures.
# db.fail_job still hard-caps total retryable failures at MAX_RETRYABLE_FAILURES.
RESOLVER_RETRY_GRACE = int(os.environ.get("ENRICHMENT_RESOLVER_RETRY_GRACE", "2"))
# Reuse the concentration SERP browser inside a worker run, but periodically
# refresh it so a long queue cannot hold a stale DuckDuckGo page forever.
CONCENTRATION_SERP_SESSION_JOBS = max(
    1,
    int(os.environ.get("ENRICHMENT_CONCENTRATION_SERP_SESSION_JOBS", "25") or "25"),
)
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Basenotes-normalized house names that no longer match their Fragrantica
# designer. Keys are alias-normalized (lowercase, alphanumerics + spaces);
# values are the Fragrantica brand. Applied to args.brand, identity probes,
# and the normalized query variant.
HOUSE_ALIASES: dict[str, str] = {
    "initio": "Initio Parfums Privés",
    "initio parfums": "Initio Parfums Privés",
    "penhaligons": "Penhaligon's",
    "lataffa": "Lattafa",
    "lattafa perfumes": "Lattafa",
    "pull bear": "Pull&Bear",
    "pull and bear": "Pull&Bear",
    "d s and durga": "D.S. & Durga",
    "d s durga": "D.S. & Durga",
    "ds durga": "D.S. & Durga",
    "maitre parfumeur et gantier": "Maitre Parfumeur et Gantier",
    "le jardin retrouve": "Le Jardin Retrouvé",
    "victorias secret": "Victoria's Secret",
    "bath and body works": "Bath & Body Works",
    "body shop": "The Body Shop",
    "the body shop": "The Body Shop",
}

# Product lines with essentially no Fragrantica coverage (mass-market body
# mists). When one of these exhausts the resolver (terminal
# fg_url_missing_after_resolution), the worker retires the job to `ignored`
# instead of `failed`, so it stops polluting the failure counts and is never
# requeued by "Retry failed jobs". Mode: "any" retires the whole house,
# "prefix"/"contains" match against the alias-normalized name.
NO_FRAGRANTICA_LINES: tuple[tuple[str, str, str], ...] = (
    ("bath and body works", "any", ""),
    ("bath body works", "any", ""),
    ("victorias secret", "prefix", "pink"),
    ("calvin klein", "contains", "body mist"),
)


def _matches_no_fragrantica_line(job: dict[str, Any]) -> bool:
    house = _alias_key(str(job.get("house") or ""))
    name = _alias_key(str(job.get("name") or ""))
    if not house:
        return False
    for house_key, mode, marker in NO_FRAGRANTICA_LINES:
        if house != house_key:
            continue
        if mode == "any":
            return True
        if mode == "prefix" and name.startswith(marker):
            return True
        if mode == "contains" and marker in name:
            return True
    return False


def _error_bucket(job: dict[str, Any]) -> str:
    """Stable, non-secret bucket for queue diagnostics and dashboard summaries."""
    raw = str(job.get("last_error") or job.get("error") or "").strip()
    if not raw:
        return "none"
    text = _ANSI_RE.sub("", raw).lower()
    if "fg_resolver_blocked" in text or "rate-limited" in text or "rate limited" in text:
        return "fg_resolver_blocked"
    if "resolver returned no fragrantica url" in text or "fg_url_missing_after_resolution" in text:
        return "fg_url_missing_after_resolution"
    if "parser_empty_frag_cards" in text or "empty frag" in text:
        return "parser_empty_frag_cards"
    if "api_unavailable" in text:
        return "api_unavailable"
    if "engine_exception" in text:
        return "engine_exception"
    return text[:80]


def _job_identity(job: dict[str, Any]) -> str:
    house = str(job.get("house") or "").strip()
    name = str(job.get("name") or "").strip()
    query = str(job.get("query") or "").strip()
    if house or name:
        return f"{house} - {name}".strip(" -")
    return query or str(job.get("id") or "unknown")


def _job_attention_line(job: dict[str, Any]) -> str:
    label = _job_identity(job)
    failures = int(job.get("failure_count") or 0)
    bucket = _error_bucket(job)
    if len(label) > 32:
        label = label[:29] + "..."
    return f"{label} ({bucket}, failures={failures})"


# Garbled Basenotes names that cannot be repaired generically (accents and
# punctuation were stripped at import). Keyed by (alias-normalized house,
# alias-normalized name) -> the Fragrantica display name.
NAME_ALIASES: dict[tuple[str, str], str] = {
    ("le jardin retrouve", "verveine dete"): "Verveine d'Été",
    ("xerjoff", "17 17 irisss"): "Irisss",
}

# Concentration suffixes baked into Basenotes-normalized names ("24 Faubourg
# Eau De Parfum") that defeat Fragrantica search. Longest forms first.
_CONCENTRATION_RE = re.compile(
    r"\b("
    r"eau\s+de\s+parfum(\s+intense)?"
    r"|eau\s+de\s+toilette(\s+intense)?"
    r"|eau\s+de\s+cologne"
    r"|eau\s+fraiche"
    r"|extrait\s+de\s+parfum"
    r"|esprit\s+de\s+parfum"
    r"|parfum\s+de\s+toilette"
    r"|elixir\s+de\s+parfum"
    r"|edp|edt|edc"
    r")\b",
    re.IGNORECASE,
)


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


def _concentration_uses_decodo() -> bool:
    """True when the concentration SERP tier resolves through Decodo rather than
    a local Chromium browser. Delegates to concentration_grabber so the policy
    lives in one place; degrades to False if that module is unavailable."""
    try:
        from concentration_grabber import _decodo_concentration_enabled
    except Exception:
        return False
    try:
        return bool(_decodo_concentration_enabled())
    except Exception:
        return False


class ConcentrationSerpState:
    """Lazy reusable browser page for concentration SERP fallback.

    Fragrantica detail parsing already reuses the engine-level clearance
    session. This state handles the separate DuckDuckGo concentration tier so a
    large worker batch does not boot and quit Chromium for every unresolved
    concentration.
    """

    def __init__(self, *, enabled: bool, debug: bool = False) -> None:
        # When the Decodo structured SERP powers the concentration tier there is
        # no Chromium page to reuse: analyze(page=None) fetches over Decodo's
        # egress instead of booting a browser. Disabling the page pool here is
        # what keeps the worker's concentration tier thread-safe (a shared
        # DrissionPage is not), and is a prerequisite for parallel processing.
        if enabled and _concentration_uses_decodo():
            enabled = False
        self.enabled = enabled
        self.debug = debug
        self._page: Any = None
        self._uses = 0

    def page(self) -> Any | None:
        if not self.enabled:
            return None
        if self._page is None or self._uses >= CONCENTRATION_SERP_SESSION_JOBS:
            self.close()
            from concentration_grabber import SemanticScentEngine

            deadline = time.monotonic() + SemanticScentEngine.SERP_TOTAL_BUDGET_SEC
            self._page = SemanticScentEngine._new_serp_page(deadline)
            self._uses = 0
            if self.debug:
                print("  [concentration] opened reusable SERP browser")
        self._uses += 1
        return self._page

    def close(self) -> None:
        page = self._page
        self._page = None
        self._uses = 0
        quit_page = getattr(page, "quit", None)
        if callable(quit_page):
            with contextlib.suppress(Exception):
                quit_page()


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
    # Requested job-processing concurrency. 0 = auto (Decodo-aware default);
    # >1 only takes effect when the Decodo egress path is active (see
    # _effective_concurrency), since the single-IP clearance path must stay
    # serial to avoid rate-limit blocks.
    concurrency: int = 0

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
        timeout: float | None = None,
    ) -> dict[str, Any]:
        wire_json = _json_for_request(json)
        read_timeout = HTTP_READ_TIMEOUT if timeout is None else max(1.0, float(timeout))
        try:
            response = self.session.request(
                method,
                self._url(path, params),
                json=wire_json,
                headers=self._headers(auth),
                timeout=(HTTP_CONNECT_TIMEOUT, read_timeout),
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

    def list_jobs(
        self, status: str = "pending", limit: int = DEFAULT_LIMIT, offset: int = 0
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"status": status, "limit": max(1, min(limit, MAX_LIST_LIMIT))}
        if offset:
            params["offset"] = max(0, int(offset))
        payload = self._request("GET", "/api/enrichment/jobs", params=params)
        jobs = payload.get("jobs") or []
        if not isinstance(jobs, list):
            raise WorkerError("api_invalid_json", "jobs was not a list", retryable=True)
        return [j for j in jobs if isinstance(j, dict)]

    def search_fragrances(self, query: str) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            "/api/fragrances/search",
            params={"q": query},
            auth=False,
            timeout=HTTP_READ_TIMEOUT + 8.0,
        )
        results = payload.get("results") or []
        if not isinstance(results, list):
            raise WorkerError("api_invalid_json", "search results was not a list", retryable=True)
        return [item for item in results if isinstance(item, dict)]

    def heal_sweep(
        self,
        *,
        limit: int = HEAL_SWEEP_PAGE_LIMIT,
        offset: int = 0,
        priority: int = 5,
        max_requested_count: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"limit": limit, "offset": offset, "priority": priority}
        if max_requested_count is not None:
            body["max_requested_count"] = max_requested_count
        # The API audits a whole page of records in one call; give it headroom.
        return self._request(
            "POST", "/api/enrichment/heal", json=body, timeout=HTTP_WRITE_TIMEOUT
        )

    def claim_job(self, job_id: str) -> dict[str, Any]:
        return self._request("POST", f"/api/enrichment/jobs/{job_id}/claim")

    def complete_job(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/enrichment/jobs/{job_id}/complete",
            json=payload,
            timeout=HTTP_WRITE_TIMEOUT,
        )

    def fail_job(self, job_id: str, error: str, retryable: bool) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/enrichment/jobs/{job_id}/fail",
            json={"error": error, "retryable": bool(retryable)},
            timeout=HTTP_WRITE_TIMEOUT,
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

    def patch_job(
        self,
        job_id: str,
        *,
        fg_url: str | None = None,
        query: str | None = None,
        name: str | None = None,
        house: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if fg_url is not None:
            body["fg_url"] = fg_url
        if query is not None:
            body["query"] = query
        if name is not None:
            body["name"] = name
        if house is not None:
            body["house"] = house
        return self._request("PATCH", f"/api/enrichment/jobs/{job_id}", json=body)

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
    """Canonical perfume-page URL, or "" when the input is not one.

    Non-perfume Fragrantica URLs (designer pages, search pages) must never be
    treated as a usable fg_url: feeding one to fetch_payload yields an instant
    parser_empty_frag_cards that repeats identically on every retry. Returning
    "" here makes every caller fall through to real query resolution instead.
    """
    text = (url or "").strip()
    if not text:
        return ""
    canonical = engine.FragranticaEngine.canonical_url(text)
    if engine.FragranticaEngine.is_perfume_url(canonical):
        return canonical.rstrip("/")
    return ""


def _job_label(job: dict[str, Any]) -> str:
    bits = [str(job.get("name") or "").strip(), str(job.get("house") or "").strip()]
    label = " ".join(b for b in bits if b)
    return label or str(job.get("query") or job.get("fg_url") or job.get("job_key") or job.get("id") or "job")


def _alias_key(text: str) -> str:
    """Lowercase alphanumeric-and-space form used to match HOUSE_ALIASES keys."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (text or "").lower())).strip()


def _canonical_house(house: str) -> str:
    """Map a Basenotes-normalized house to its Fragrantica brand, if known."""
    text = (house or "").strip()
    if not text:
        return ""
    return HOUSE_ALIASES.get(_alias_key(text), text)


def _canonical_name(house: str, name: str) -> str:
    """Repair a known-garbled fragrance name for a given house."""
    text = (name or "").strip()
    if not text:
        return ""
    return NAME_ALIASES.get((_alias_key(house), _alias_key(text)), text)


def _apply_house_aliases(query: str) -> str:
    """Rewrite known-broken house spellings inside a free-text query.

    Longest keys apply first so "initio parfums" wins over "initio", and a
    query that already carries the canonical brand is left alone — otherwise a
    shorter key matching inside an already-rewritten canonical value would
    duplicate its tokens ("Initio Parfums Privés Privés …").
    """
    out = query
    for key, canonical in sorted(HOUSE_ALIASES.items(), key=lambda kv: -len(kv[0])):
        if canonical.lower() in out.lower():
            continue
        pattern = re.compile(r"\b" + r"\s+".join(re.escape(t) for t in key.split()) + r"\b", re.IGNORECASE)
        out = pattern.sub(canonical, out)
    return out


def _strip_concentration_terms(text: str) -> str:
    stripped = _CONCENTRATION_RE.sub(" ", text)
    stripped = re.sub(r"\s+", " ", stripped).strip(" -–—|:")
    # Never strip the query down to nothing (e.g. a bare "Eau de Cologne").
    return stripped or text


def _dedupe_query_tokens(text: str) -> str:
    """Drop repeated tokens, keeping first occurrence — fixes Basenotes rows
    like "Classique Eau De Toilette Jean Paul Gaultier Classique"."""
    seen: set[str] = set()
    out: list[str] = []
    for tok in text.split():
        key = tok.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(tok)
    return " ".join(out)


def _normalize_query_text(query: str) -> str:
    """The cleaned-up search variant: alias houses, drop concentration
    suffixes, collapse duplicated tokens. The raw query stays available as a
    fallback variant, so normalization only has to help the common case."""
    text = re.sub(r"\s+", " ", (query or "")).strip()
    if not text:
        return ""
    text = _apply_house_aliases(text)
    text = _strip_concentration_terms(text)
    return _dedupe_query_tokens(text)


_APOSTROPHE_GARBLE_RE = re.compile(r"^([dl])([aeiouh][a-z]+)$", re.IGNORECASE)


def _apostrophe_repaired_name(name: str) -> str:
    """Best-effort repair of import-garbled French elisions ("Parfum Dhabit" ->
    "Parfum d'Habit", "Ombre Rose Loriginal" -> "Ombre Rose l'Original",
    "Terre Dhermes" -> "Terre d'Hermes"). Returns "" when no token looks
    garbled. Only ever used as an ADDITIONAL variant/probe: a false repair
    ("Dune" -> "d'Une") just scores zero while the original form keeps running.
    """
    out: list[str] = []
    changed = False
    for word in (name or "").split():
        m = _APOSTROPHE_GARBLE_RE.match(word)
        if m and len(word) >= 4:
            rest = m.group(2)
            out.append(f"{m.group(1).lower()}'{rest[0].upper()}{rest[1:]}")
            changed = True
        else:
            out.append(word)
    return " ".join(out) if changed else ""


def _recombined_identities(job: dict[str, Any]) -> list[tuple[str, str]]:
    """Alternate (house, name) pairs for rows whose import swapped the product
    line into the house: "The Fireplace By Martin Margiela" / "Replica" ->
    ("Martin Margiela", "Replica The Fireplace"). Probes are additive; a
    recombination that is wrong simply matches nothing."""
    house = str(job.get("house") or "").strip()
    name = str(job.get("name") or "").strip()
    out: list[tuple[str, str]] = []
    m = re.match(r"^(?P<line>.+?)\s+by\s+(?P<brand>.+)$", house, flags=re.IGNORECASE)
    if m and name:
        brand = m.group("brand").strip()
        line = m.group("line").strip(" -–—|:")
        if brand and line:
            out.append((brand, f"{name} {line}".strip()))
    return out


def _query_variants(job: dict[str, Any]) -> list[str]:
    """Search-query variants in priority order: repaired identity (when a
    house/name alias applies), then the normalized query, then the raw query,
    then the garbled-apostrophe and swapped house/line repairs."""
    raw = _build_query(job)
    variants: list[str] = []
    house = str(job.get("house") or "").strip()
    name = str(job.get("name") or "").strip()
    if house and name:
        repaired_house = _canonical_house(house)
        repaired_name = _canonical_name(house, name)
        if repaired_house != house or repaired_name != name:
            repaired = _normalize_query_text(f"{repaired_house} {repaired_name}")
            if repaired:
                variants.append(repaired)
    for q in (_normalize_query_text(raw), raw):
        if q and q not in variants:
            variants.append(q)
    if house and name:
        apostrophe = _apostrophe_repaired_name(_canonical_name(house, name))
        if apostrophe:
            q = _normalize_query_text(f"{_canonical_house(house)} {apostrophe}")
            if q and q not in variants:
                variants.append(q)
    for alt_house, alt_name in _recombined_identities(job):
        q = _normalize_query_text(f"{_canonical_house(alt_house)} {alt_name}")
        if q and q not in variants:
            variants.append(q)
    return variants


def _fg_url_from_job_query(job: dict[str, Any]) -> str:
    """Promote a query that is itself a Fragrantica perfume URL to fg_url.

    Some Basenotes-imported rows arrive with the literal FG URL in `query` and
    no fg_url; _build_query used to discard those, so the job could never
    resolve despite carrying its own answer.
    """
    query = str(job.get("query") or "").strip()
    if not query.lower().startswith("http"):
        return ""
    canonical = engine.FragranticaEngine.canonical_url(query)
    if engine.FragranticaEngine.is_perfume_url(canonical):
        return canonical.rstrip("/")
    return ""


def _resolver_miss_retryable(job: dict[str, Any]) -> bool:
    """First RESOLVER_RETRY_GRACE misses stay retryable; after that, terminal."""
    return int(job.get("failure_count") or 0) < RESOLVER_RETRY_GRACE


def _build_query(job: dict[str, Any]) -> str:
    query = str(job.get("query") or "").strip()
    if query and not query.lower().startswith("http"):
        return query
    name = str(job.get("name") or "").strip()
    house = str(job.get("house") or "").strip()
    if house and name:
        house_lower = house.lower()
        name_lower = name.lower()
        if name_lower.startswith(house_lower):
            remainder = name[len(house) :].strip(" -–—|:")
            return f"{house} {remainder}".strip() if remainder else name
        if name_lower.endswith(house_lower):
            trimmed = name[: -len(house)].strip(" -–—|:")
            return f"{house} {trimmed}".strip() if trimmed else f"{house} {name}".strip()
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
    if name and house:
        name = engine.IdentityTools.strip_house_from_name(name, house)
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


def _build_engine_args(*, enhanced: bool = False) -> argparse.Namespace:
    args = engine.build_parser().parse_args([])
    # Default resolution stays conservative; enhanced mode enables open-web sniper.
    if hasattr(args, "external_search"):
        args.external_search = bool(enhanced)
    if enhanced and hasattr(args, "external_search_budget"):
        args.external_search_budget = max(float(getattr(args, "external_search_budget", 0) or 0), 8.0)
    return args


def _first_linked_result(results: list[engine.UnifiedFragrance]) -> engine.UnifiedFragrance | None:
    for item in results:
        if item.frag_url and _canonical_fg_url(item.frag_url):
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
    return None


def _api_result_to_candidate(item: dict[str, Any]) -> engine.UnifiedFragrance | None:
    fg_url = _canonical_fg_url(
        str(
            item.get("fg_url")
            or item.get("fragrantica_url")
            or item.get("source_url")
            or ""
        )
    )
    if not fg_url:
        return None
    return engine.UnifiedFragrance(
        name=str(item.get("name") or ""),
        brand=str(item.get("house") or item.get("brand") or ""),
        year=_coerce_year(item.get("year")),
        bn_url=str(item.get("bn_url") or ""),
        frag_url=fg_url,
        resolver_source="api_search",
        resolver_score=0.95,
    )


def _candidate_matches_job(candidate: engine.UnifiedFragrance, job: dict[str, Any]) -> bool:
    job_name = _canonical_name(str(job.get("house") or ""), str(job.get("name") or ""))
    job_house = _canonical_house(str(job.get("house") or ""))
    if not job_name and not job_house:
        return True
    probe = engine.UnifiedFragrance(
        name=job_name or candidate.name,
        brand=job_house or candidate.brand,
        year=_coerce_year(job.get("year")),
    )
    catalog_item = engine.CatalogItem(
        name=candidate.name,
        brand=candidate.brand,
        year=candidate.year,
        url=candidate.frag_url,
    )
    return engine.Orchestrator.identity_score(probe, catalog_item) >= engine.Orchestrator.CATALOG_ACCEPT


CATALOG_TIER_BUDGET = float(os.environ.get("ENRICHMENT_CATALOG_TIER_BUDGET", "12") or 12)
CATALOG_TIER_SIBLING_MARGIN = 0.06
# Floor for the year-confirmed subset acceptance below. Covers the engine's
# 0.68 subset-length-mismatch band (and its +0.04 exact-year bonus) without
# reaching down to genuinely weak partial matches.
CATALOG_TIER_SUBSET_FLOOR = 0.66
# Tokens that may differ between a Basenotes name and the Fragrantica catalog
# name without changing identity ("Lys" vs "The Lys").
_SUBSET_STOPWORD_TOKENS = frozenset(
    {"the", "le", "la", "les", "l", "de", "du", "des", "d", "a", "an", "of", "and", "et"}
)


def _subset_identity_match(probe: engine.UnifiedFragrance, item: engine.CatalogItem) -> bool:
    """True when probe and catalog names differ only in ways that cannot flip
    identity: extra stopword tokens on either side ("Lys" vs "The Lys"), or
    extra line/collection tokens on the PROBE side confirmed by an exact year
    match ("1872 Twist Vetiver" 2016 vs catalog "1872 Vetiver" 2016 — Basenotes
    keeps the marketing line name, Fragrantica drops it). The catalog side
    having extra non-stopword tokens is never accepted: that is the flanker
    shape ("Sauvage" must not take "Sauvage Elixir")."""
    probe_tokens = engine.IdentityTools.name_tokens_keep_stopwords(probe.name, probe.brand)
    item_tokens = engine.IdentityTools.name_tokens_keep_stopwords(item.name, item.brand)
    if not probe_tokens or not item_tokens or not (probe_tokens & item_tokens):
        return False
    extra_probe = probe_tokens - item_tokens
    extra_item = item_tokens - probe_tokens
    if extra_probe <= _SUBSET_STOPWORD_TOKENS and extra_item <= _SUBSET_STOPWORD_TOKENS:
        return True
    if not (extra_item - _SUBSET_STOPWORD_TOKENS) and extra_probe:
        probe_year = str(probe.year or "").strip()
        item_year = str(item.year or "").strip()
        return bool(probe_year and item_year and probe_year == item_year)
    return False


def _catalog_probes(job: dict[str, Any]) -> list[engine.UnifiedFragrance]:
    """Identity probes for the designer-catalog tier: the canonical identity
    plus the apostrophe-repaired and recombined-house variants. Probes are
    scored max-wise, so a wrong extra probe can only add zero."""
    house = _canonical_house(str(job.get("house") or ""))
    name = _canonical_name(str(job.get("house") or ""), str(job.get("name") or ""))
    year = _coerce_year(job.get("year"))
    probes: list[engine.UnifiedFragrance] = []
    seen: set[tuple[str, str]] = set()

    def add(h: str, n: str) -> None:
        key = (_alias_key(h), _alias_key(n))
        if h and n and key not in seen:
            seen.add(key)
            probes.append(engine.UnifiedFragrance(name=n, brand=h, year=year))

    add(house, name)
    apostrophe = _apostrophe_repaired_name(name)
    if apostrophe:
        add(house, apostrophe)
    for alt_house, alt_name in _recombined_identities(job):
        add(_canonical_house(alt_house), alt_name)
    return probes


def _designer_catalog_candidate(
    scraper,
    job: dict[str, Any],
    *,
    debug: bool = False,
    health: dict[str, Any] | None = None,
) -> engine.UnifiedFragrance | None:
    """Deterministic Fragrantica tier: crawl the house's designer page(s).

    The engine's own designer-catalog fallback only runs behind the
    QueryRepair.needs_repair gate under a small shared deadline, which is how
    sibling flankers flake ("1872 Twist Basil" resolved while "1872 Twist
    Geranium" exhausted its retries). This tier always runs for house+name
    jobs once search came up empty: a designer page enumerates the whole
    line, so if the perfume exists on Fragrantica it is in this list. Scored
    with the engine's own identity acceptor plus a sibling margin so
    near-ties (EDT vs EDP rows) are never guessed.

    When every designer-page fetch fails outright (rate-limit/block windows),
    health["blocked"] is set so the caller marks the miss retryable instead of
    concluding the perfume does not exist on Fragrantica.
    """
    probes = _catalog_probes(job)
    if not probes:
        return None
    scored: list[tuple[float, engine.CatalogItem]] = []
    labels_failed = 0
    labels_tried = 0
    try:
        label_keys: list[str] = []
        seen_labels: set[str] = set()
        for probe in probes:
            for label in engine.IdentityTools.catalog_brand_keys(probe.brand, probe.name):
                norm = _alias_key(label)
                if norm and norm not in seen_labels:
                    seen_labels.add(norm)
                    label_keys.append(label)
        for label in label_keys:
            deadline = engine.Deadline(CATALOG_TIER_BUDGET)
            labels_tried += 1
            items = _run_engine_call(
                lambda: engine.SearchSniper.catalog_candidates_for_brand(
                    scraper, label, deadline, slug_limit=8, page_timeout=6.0
                ),
                debug=debug,
            )
            if not items and engine.SearchSniper.last_catalog_fetch_failed():
                labels_failed += 1
            for item in items:
                score = max(engine.Orchestrator.identity_score(p, item) for p in probes)
                scored.append((score, item))
            if scored and max(score for score, _ in scored) >= engine.Orchestrator.CATALOG_ACCEPT:
                break
    except Exception as exc:
        print(f"  [catalog] designer-catalog tier failed: {exc}")
        if health is not None:
            health["blocked"] = True
        return None
    if not scored:
        if health is not None and labels_tried and labels_failed == labels_tried:
            health["blocked"] = True
            print("  [catalog] all designer-page fetches failed; treating as blocked, not missing")
        return None
    # Collapse duplicates before ranking: the same perfume frequently appears
    # on more than one designer label page (parent house + line alias), and a
    # duplicate of the best row must never masquerade as a sibling near-tie.
    best_by_url: dict[str, tuple[float, engine.CatalogItem]] = {}
    for score, item in scored:
        key = engine.FragranticaEngine.dedupe_key(item.url)
        if key not in best_by_url or score > best_by_url[key][0]:
            best_by_url[key] = (score, item)
    ranked = sorted(best_by_url.values(), key=lambda pair: pair[0], reverse=True)
    best_score, best = ranked[0]
    runner_up = ranked[1][0] if len(ranked) > 1 else 0.0
    if best_score < engine.Orchestrator.CATALOG_ACCEPT:
        # Year-confirmed subset acceptance: the designer page enumerates the
        # whole house, so when the only plausible row is a unique subset-name
        # match the full name simply does not exist on Fragrantica.
        if (
            best_score >= CATALOG_TIER_SUBSET_FLOOR
            and runner_up < CATALOG_TIER_SUBSET_FLOOR
            and any(_subset_identity_match(p, best) for p in probes)
        ):
            print(f"  [catalog] accepted unique subset match {best.url} score={best_score:.2f}")
            return engine.UnifiedFragrance(
                name=best.name,
                brand=best.brand,
                year=best.year,
                frag_url=best.url,
                resolver_source="worker_designer_catalog_subset",
                resolver_score=best_score,
            )
        print(f"  [catalog] best {best.url} score={best_score:.2f} below accept; skipping")
        return None
    if runner_up >= engine.Orchestrator.CATALOG_ACCEPT and best_score - runner_up < CATALOG_TIER_SIBLING_MARGIN:
        print(f"  [catalog] sibling collision ({best_score:.2f} vs {runner_up:.2f}); skipping")
        return None
    print(f"  [catalog] accepted {best.url} score={best_score:.2f}")
    return engine.UnifiedFragrance(
        name=best.name,
        brand=best.brand,
        year=best.year,
        frag_url=best.url,
        resolver_source="worker_designer_catalog",
        resolver_score=best_score,
    )


def _api_linked_candidates(
    client: ApiClient,
    query: str,
    job: dict[str, Any],
    *,
    debug: bool = False,
) -> list[engine.UnifiedFragrance]:
    try:
        rows = client.search_fragrances(query)
    except WorkerError as exc:
        if debug:
            print(f"  [resolve] API search skipped: {exc.code}")
        return []
    linked: list[engine.UnifiedFragrance] = []
    seen: set[str] = set()
    for row in rows:
        candidate = _api_result_to_candidate(row)
        if not candidate or not _candidate_matches_job(candidate, job):
            continue
        dedupe = engine.FragranticaEngine.dedupe_key(candidate.frag_url)
        if dedupe in seen:
            continue
        seen.add(dedupe)
        linked.append(candidate)
    return linked


def _normalize_fg_url_input(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    canonical = engine.FragranticaEngine.canonical_url(text)
    if engine.FragranticaEngine.is_perfume_url(canonical):
        return canonical.rstrip("/")
    raise WorkerError("invalid_fg_url", "URL must be a Fragrantica perfume page", retryable=False)


def _job_missing_fg_url(job: dict[str, Any]) -> bool:
    return not _canonical_fg_url(str(job.get("fg_url") or ""))


def _search_candidates(
    scraper,
    query: str,
    args: argparse.Namespace,
    *,
    debug: bool = False,
) -> list[engine.UnifiedFragrance]:
    try:
        return _run_engine_call(lambda: engine.search_once(scraper, query, args), debug=debug)
    except Exception as exc:
        raise WorkerError("engine_exception", f"search failed: {exc}", retryable=True) from exc


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


def _accept_candidate(candidate: engine.UnifiedFragrance | None, job: dict[str, Any]) -> bool:
    if not candidate:
        return False
    if candidate.resolver_source == "designer_catalog_brand_query":
        job_name = _canonical_name(str(job.get("house") or ""), str(job.get("name") or ""))
        job_house = _canonical_house(str(job.get("house") or ""))
        probe = engine.UnifiedFragrance(name=job_name, brand=job_house, year="")
        catalog_item = engine.CatalogItem(name=candidate.name, brand=candidate.brand, year="", url=candidate.frag_url)
        score = engine.Orchestrator.identity_score(probe, catalog_item)
        if score < engine.Orchestrator.CATALOG_ACCEPT:
            print(f"  [resolve] rejected weak catalog match {candidate.frag_url} score={score:.2f}")
            return False
    return True


def resolve_candidate(
    scraper,
    args: argparse.Namespace,
    job: dict[str, Any],
    *,
    debug: bool = False,
) -> engine.UnifiedFragrance:
    raw_fg_url = str(job.get("fg_url") or "").strip()
    fg_url = _canonical_fg_url(raw_fg_url)
    if raw_fg_url and not fg_url:
        # A stored non-perfume URL (designer/search page) would fail parsing
        # instantly on every attempt; ignore it and resolve by query instead.
        print(f"  [resolve] ignoring non-perfume fg_url on job: {raw_fg_url!r}")
    if not fg_url:
        fg_url = _fg_url_from_job_query(job)
    if fg_url:
        return engine.UnifiedFragrance(
            name=str(job.get("name") or ""),
            brand=str(job.get("house") or ""),
            year=_coerce_year(job.get("year")),
            bn_url="",
            frag_url=fg_url,
        )

    queries = _query_variants(job)
    if not queries:
        raise WorkerError("fg_url_missing_after_resolution", "job has no fg_url or usable query", retryable=False)

    house = _canonical_house(str(job.get("house") or ""))
    # Work on a per-job copy: mutating the shared engine args would leak this
    # job's brand into every later job's search (the catalog fallback keys off
    # args.brand, so a stale brand sends house-less jobs to the wrong catalog).
    args = argparse.Namespace(**vars(args))
    args.brand = house

    for query in queries:
        results = _search_candidates(scraper, query, args, debug=debug)
        candidate = _first_linked_result(results)
        if _accept_candidate(candidate, job):
            return candidate

    if not getattr(args, "external_search", False):
        enhanced_args = _build_engine_args(enhanced=True)
        enhanced_args.brand = house
        for query in queries:
            print(f"  [resolve] retrying with enhanced search for {query!r}")
            results = _search_candidates(scraper, query, enhanced_args, debug=debug)
            candidate = _first_linked_result(results)
            if _accept_candidate(candidate, job):
                return candidate

    # Deterministic designer-catalog tier: search missed, so enumerate the
    # house's Fragrantica catalog directly and identity-match against it.
    health: dict[str, Any] = {}
    catalog_candidate = _designer_catalog_candidate(scraper, job, debug=debug, health=health)
    if catalog_candidate is not None:
        return catalog_candidate

    # Parfumo (§6.B): Fragrantica resolution found no URL. A job-pinned Parfumo
    # perfume URL (Basenotes imports store these for FG-absent perfumes) is
    # tried first — it skips discovery entirely. Otherwise run the Parfumo
    # factual-field resolver. On accept these return a candidate with no
    # fg_url, resolver_source="parfumo_fallback", and the parsed ParfumoRecord
    # stashed for fetch_payload to build a partial.
    parfumo_candidate = _parfumo_candidate_from_job_url(scraper, job, debug=debug)
    if parfumo_candidate is None:
        parfumo_candidate = _try_parfumo_fallback(scraper, job, debug=debug)
    if parfumo_candidate is not None:
        return parfumo_candidate

    if health.get("blocked"):
        # The designer catalog never loaded (rate-limit/block window): we did
        # not actually observe that the perfume is absent from Fragrantica, so
        # this attempt must never burn the job's terminal-failure budget.
        raise WorkerError(
            "fg_resolver_blocked",
            "designer catalog unreachable (likely rate-limited); retry later",
            retryable=True,
        )
    raise WorkerError(
        "fg_url_missing_after_resolution",
        "resolver returned no Fragrantica URL",
        retryable=_resolver_miss_retryable(job),
    )


def resolve_candidate_for_job(
    client: ApiClient,
    scraper,
    args: argparse.Namespace,
    job: dict[str, Any],
    *,
    debug: bool = False,
) -> engine.UnifiedFragrance:
    if not _job_missing_fg_url(job):
        return resolve_candidate(scraper, args, job, debug=debug)
    for query in _query_variants(job):
        linked = _api_linked_candidates(client, query, job, debug=debug)
        if linked:
            return linked[0]
    return resolve_candidate(scraper, args, job, debug=debug)


def _parfumo_candidate_from_job_url(
    scraper,
    job: dict[str, Any],
    *,
    debug: bool = False,
) -> engine.UnifiedFragrance | None:
    """Resolve directly from a Parfumo perfume URL stored on the job.

    Basenotes imports put Parfumo links in fg_url (or query) for perfumes that
    Fragrantica does not carry (e.g. Clive Christian Miami Poolside). The old
    path discarded them as "not a Fragrantica URL" and the job could never
    resolve despite carrying its own answer. Identity is still verified against
    the fetched page so a mislinked URL cannot poison the cache. Never raises.
    """
    if not engine.ParfumoEngine.enabled():
        return None
    pinned = ""
    for field in ("fg_url", "query"):
        text = str(job.get(field) or "").strip()
        if text and engine.ParfumoEngine.is_perfume_url(text):
            pinned = engine.ParfumoEngine.canonical_url(text)
            break
    if not pinned:
        return None
    try:
        rec = _run_engine_call(
            lambda: engine.ParfumoEngine.fetch_record(scraper, pinned), debug=debug
        )
    except Exception as exc:
        print(f"  [parfumo] pinned URL fetch failed: {exc}")
        return None
    if rec is None:
        print(f"  [parfumo] pinned URL is a soft-404 or non-perfume page: {pinned}")
        return None
    house = _canonical_house(str(job.get("house") or "")) or rec.house
    name = _canonical_name(str(job.get("house") or ""), str(job.get("name") or "")) or rec.name
    score = engine.ParfumoEngine.score_record(house, name, _coerce_year(job.get("year")), rec)
    if score < engine.ParfumoEngine.MANUAL_REVIEW:
        print(f"  [parfumo] pinned URL identity too weak (score={score:.2f}): {pinned}")
        return None
    print(f"  [parfumo] resolved from job-pinned URL (score={score:.2f}): {pinned}")
    candidate = engine.UnifiedFragrance(
        name=rec.name or name,
        brand=rec.house or house,
        year=rec.year or _coerce_year(job.get("year")),
        frag_url="",
        resolver_source="parfumo_fallback",
        resolver_score=score,
    )
    candidate.parfumo_record = rec
    return candidate


def _try_parfumo_fallback(
    scraper,
    job: dict[str, Any],
    *,
    debug: bool = False,
) -> engine.UnifiedFragrance | None:
    """Run the Parfumo fallback resolver for a job with no Fragrantica URL.

    Returns a Parfumo-sourced candidate on auto-accept, else None (manual_review,
    reject, not_found, disabled, or any failure -- the FG path then raises
    fg_url_missing_after_resolution as before). Never raises: a Parfumo failure
    must not crash the job, only forgo the fallback.
    """
    if not engine.ParfumoEngine.enabled():
        return None
    house = _canonical_house(str(job.get("house") or ""))
    name = _canonical_name(str(job.get("house") or ""), str(job.get("name") or ""))
    if not house or not name:
        return None  # Parfumo discovery needs both a house and a name.
    year = _coerce_year(job.get("year"))
    try:
        verdict = _run_engine_call(
            lambda: engine.ParfumoEngine.resolve(scraper, house, name, year),
            debug=debug,
        )
    except Exception as exc:
        print(f"  [parfumo] resolve failed: {exc}")
        return None
    print(
        f"  [parfumo] {house} / {name!r} -> {verdict.decision} "
        f"score={verdict.score:.3f} runner_up={verdict.runner_up:.3f} "
        f"via={verdict.discovery} scored={verdict.candidates_scored}"
    )
    if verdict.decision != "accept" or verdict.record is None:
        return None
    rec = verdict.record
    candidate = engine.UnifiedFragrance(
        name=rec.name or name,
        brand=rec.house or house,
        year=rec.year or year,
        frag_url="",
        resolver_source="parfumo_fallback",
        resolver_score=verdict.score,
    )
    # Side channel: the parsed record rides on the candidate so fetch_payload
    # can build the partial Parfumo payload without re-fetching the page.
    candidate.parfumo_record = rec
    return candidate


_FG_METRIC_GROUPS = (
    "performance_score",
    "value_score",
    "community_interest_score",
    "wear_profile",
)


def _resolve_concentration(
    house: str,
    name: str,
    *,
    allow_serp: bool = True,
    debug: bool = False,
    serp_state: ConcentrationSerpState | None = None,
) -> dict[str, Any] | None:
    """Resolve a ScentCast concentration label for "<house> <name>".

    Tier order mirrors scripts/enrich_concentration.py: Parfinity catalog ->
    explicit token in the query -> brand prior -> pillar prior -> (optionally)
    the SemanticScentEngine DuckDuckGo SERP consensus. The SERP tier boots a
    local Chromium instance, which is exactly why this runs in the worker and
    never in the cloud api.py process. Never raises: concentration is a
    best-effort enrichment and must not fail the job.
    """
    query = " ".join(
        part for part in [str(house or "").strip(), str(name or "").strip()] if part
    )
    if not query:
        return None

    def _resolved(label: str, confidence: int, source: str, engine_label: str) -> dict[str, Any]:
        return {
            "concentration": label,
            "concentration_meta": {
                "confidence": confidence,
                "source": source,
                "engine_label": engine_label,
                "resolved_at": _now_iso(),
            },
        }

    try:
        from concentration_grabber import SemanticScentEngine, to_scentcast_concentration

        # Tier 0 -- Parfinity catalog (instant, no browser).
        try:
            from parfinity import lookup_concentration as _pf_lookup

            hit = _pf_lookup(house or "", name or "")
        except Exception:
            hit = None
        if hit:
            return _resolved(hit, 95, "parfinity_catalog", hit)

        # Tier 1a -- explicit concentration token in the name/query.
        explicit = SemanticScentEngine._explicit_intent(query)
        if explicit:
            label = to_scentcast_concentration(explicit)
            if label != "Unknown":
                return _resolved(label, 100, "tier1_explicit", explicit)

        # Tier 1b -- brand prior.
        brand_hit, brand_conc = SemanticScentEngine._brand_prior(query)
        if brand_hit and brand_conc:
            label = to_scentcast_concentration(brand_conc)
            if label != "Unknown":
                return _resolved(label, 70, "tier1_brand_prior", brand_conc)

        # Tier 1c -- pillar prior.
        pillar = SemanticScentEngine._query_pillar_prior(query)
        if pillar:
            label = to_scentcast_concentration(pillar)
            if label != "Unknown":
                return _resolved(label, 80, "tier1_pillar", pillar)

        if not allow_serp:
            return None

        # Tier 2 -- SERP engine (slow; spins a local headless browser unless
        # the worker supplies a reusable page for the current batch).
        serp_page = serp_state.page() if serp_state is not None else None
        profile = _run_engine_call(
            lambda: SemanticScentEngine.analyze(query, page=serp_page), debug=debug
        )
        if profile and profile.primary_confidence >= 50:
            label = to_scentcast_concentration(profile.primary_concentration)
            if label != "Unknown":
                return _resolved(
                    label,
                    profile.primary_confidence,
                    "semantic_engine_v6",
                    profile.primary_concentration,
                )
    except Exception as exc:
        print(f"  [concentration] resolution failed for {query!r}: {exc}")
    return None


def _apply_concentration(
    payload: dict[str, Any],
    house: str,
    name: str,
    *,
    debug: bool = False,
    serp_state: ConcentrationSerpState | None = None,
) -> None:
    """Attach the resolved concentration to a /complete payload in place.

    Rides both top-level (`concentration`, consumed by api.py's payload model)
    and inside `raw_identity` (the durable JSONB home -- fg_detail_cache has no
    concentration column, so the value survives there without a migration).
    Set ENRICHMENT_CONCENTRATION_SERP=0 to skip the slow browser tier.
    """
    resolved = _resolve_concentration(
        house,
        name,
        allow_serp=_env_bool("ENRICHMENT_CONCENTRATION_SERP", True),
        debug=debug,
        serp_state=serp_state,
    )
    if not resolved:
        return
    payload["concentration"] = resolved["concentration"]
    raw_identity = payload.get("raw_identity")
    if isinstance(raw_identity, dict):
        raw_identity.setdefault("concentration", resolved["concentration"])
        raw_identity.setdefault("concentration_meta", resolved["concentration_meta"])
    print(
        f"  [concentration] {resolved['concentration']} "
        f"(conf={resolved['concentration_meta']['confidence']}, "
        f"src={resolved['concentration_meta']['source']})"
    )


def _build_worker_derived_metrics(details: Any) -> dict[str, Any] | None:
    """The derived metrics the API will recompute from this payload, or None."""
    try:
        from derived_metrics_adapter import build_derived_metrics

        dm = build_derived_metrics(details)
    except Exception:
        return None
    return dm if isinstance(dm, dict) else None


def _dm_metrics_complete(dm: dict[str, Any] | None) -> bool:
    if not isinstance(dm, dict):
        return False
    cov = dm.get("source_coverage") or {}
    return all(bool(cov.get(g)) for g in _FG_METRIC_GROUPS)


def _worker_metrics_complete(details: Any) -> bool:
    """True when all 4 Fragrantica status-derived metric groups parsed.

    Mirrors api._fg_metrics_complete: a partial fetch (encrypted status payload
    did not decode) must not be stored as a trustworthy 'complete' cache entry,
    otherwise api._apply_fg_detail_cache_db would hydrate from it and the job
    would never be re-enqueued for a retry.
    """
    return _dm_metrics_complete(_build_worker_derived_metrics(details))


def _payload_fact_summary(
    payload: dict[str, Any], dm: dict[str, Any] | None
) -> tuple[list[str], list[str]]:
    """(filled, missing) FACT_FIELDS for a /complete payload.

    Judged with the exact contract the heal sweep audits stored records with
    (enrichment_facts.record_fact_status), so the worker can report per
    fragrance which facts this run actually delivered and which the sources
    still lack -- the operator-facing answer to "what did this update?".
    """
    raw_identity = payload.get("raw_identity") or {}
    if payload.get("source") == "parfumo":
        # Mirror api._build_parfumo_cache_row: Parfumo accords ride in
        # raw_identity and are projected into derived_metrics.main_accords at
        # completion, so judge the payload the way the API will store it.
        accords = expand_raw_accords(
            raw_identity.get("accords") if isinstance(raw_identity, dict) else None
        )
        if accords:
            dm = dict(dm) if isinstance(dm, dict) else {}
            main = dm.get("main_accords")
            if not (isinstance(main, dict) and main.get("top_accords")):
                dm["main_accords"] = {
                    "top_accords": accords,
                    "source": "parfumo_accords",
                }
    record = {
        "name": payload.get("name"),
        "house": payload.get("house"),
        "year": payload.get("year"),
        "gender": payload.get("gender"),
        "fg_raw": {
            "frag_cards": payload.get("frag_cards") or {},
            "notes": payload.get("notes") or {},
            "reviews": payload.get("reviews") or [],
            "raw_identity": raw_identity,
            "concentration": payload.get("concentration"),
        },
        "bn_raw": {},
        "derived_metrics": dm if isinstance(dm, dict) else None,
    }
    status = record_fact_status(record)
    filled = [field for field in FACT_FIELDS if status.get(field)]
    missing = [field for field in FACT_FIELDS if not status.get(field)]
    return filled, missing


def fetch_payload(
    scraper,
    candidate: engine.UnifiedFragrance,
    job: dict[str, Any],
    *,
    detail_timeout: float,
    debug: bool = False,
    concentration_serp: ConcentrationSerpState | None = None,
) -> dict[str, Any]:
    # Parfumo fallback (§6.C): the candidate carries a parsed Parfumo record and
    # no fg_url. Build a partial payload from it instead of fetching FG details.
    if candidate.resolver_source == "parfumo_fallback":
        return _build_parfumo_payload(candidate, job, concentration_serp=concentration_serp)
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
        # A perfume-shaped URL is always worth retrying; a non-perfume landing
        # (designer/search page) still gets the resolver grace window, because
        # the next attempt re-resolves and may pick a different URL.
        retryable = bool(
            engine.FragranticaEngine.is_perfume_url(candidate.frag_url)
        ) or _resolver_miss_retryable(job)
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

    engine.heal_missing_gender_and_year(candidate, details)
    identity = _specific_identity(candidate)
    image_url = _extract_image_url(scraper, candidate.frag_url, debug=debug)
    raw_identity = _raw_identity(candidate, job)
    if image_url:
        raw_identity["image_url"] = image_url

    dm = _build_worker_derived_metrics(details)
    payload = {
        "fg_url": candidate.frag_url,
        "name": identity["name"],
        "house": identity["house"],
        "year": identity["year"],
        "gender": details.gender,
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
        "quality_status": "complete" if _dm_metrics_complete(dm) else "partial",
        # Worker-side only: popped by process_job before upload so the fact
        # summary does not have to recompute the metrics adapter.
        "_derived_metrics": dm,
    }
    _apply_concentration(
        payload,
        identity["house"] or candidate.brand,
        identity["name"] or candidate.name,
        debug=debug,
        serp_state=concentration_serp,
    )
    return payload


def _build_parfumo_payload(
    candidate: engine.UnifiedFragrance,
    job: dict[str, Any],
    *,
    concentration_serp: ConcentrationSerpState | None = None,
) -> dict[str, Any]:
    """Build the partial /complete payload for a Parfumo-sourced candidate (§6.C).

    No frag_cards and no FG detail fetch -- Parfumo has no FG status pyramid.
    Factual fields ride in `notes` / `gender` / `year`; accords, rating, status,
    and perfumers ride in `raw_identity`. quality_status is always "partial", and
    the API completion path (§6.D) keys it by the canonical parfumo_url and never
    lets it masquerade as FG-complete.
    """
    rec = getattr(candidate, "parfumo_record", None)
    if rec is None:
        raise WorkerError("parfumo_record_missing", retryable=False)

    notes = {
        "has_pyramid": bool(rec.notes_top or rec.notes_heart or rec.notes_base),
        "top": list(rec.notes_top or []),
        "heart": list(rec.notes_heart or []),
        "base": list(rec.notes_base or []),
        "flat": list(rec.notes_flat or []),
    }
    raw_identity = {
        "name": candidate.name,
        "house": candidate.brand,
        "year": candidate.year,
        "parfumo_url": rec.url,
        "resolver_source": candidate.resolver_source,
        "resolver_score": candidate.resolver_score,
        "accords": list(rec.accords or []),
        "rating": rec.rating,
        "rating_count": rec.rating_count,
        "status": rec.status,
        "perfumers": list(rec.perfumers or []),
        "job_key": job.get("job_key") if job else None,
        "query": job.get("query") if job else None,
    }
    raw_identity = {k: v for k, v in raw_identity.items() if v not in ("", None, [])}

    payload = {
        "source": "parfumo",
        "parfumo_url": rec.url,
        "fg_url": None,
        "name": candidate.name or None,
        "house": candidate.brand or None,
        "year": candidate.year or None,
        "gender": rec.gender or None,
        "image_url": None,
        "schema_version": SCHEMA_VERSION,
        "captured_at": _now_iso(),
        "notes": notes,
        "raw_identity": raw_identity,
        # Parfumo is never FG-complete; persist as a terminal partial.
        "quality_status": "partial",
    }
    _apply_concentration(payload, candidate.brand, candidate.name, serp_state=concentration_serp)
    return payload


def process_job(
    client: ApiClient,
    scraper,
    args: argparse.Namespace,
    job: dict[str, Any],
    *,
    index_label: str,
    config: WorkerConfig,
    already_claimed: bool = False,
    result_sink: dict[str, Any] | None = None,
    concentration_serp: ConcentrationSerpState | None = None,
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

        candidate = resolve_candidate_for_job(client, scraper, args, job, debug=config.debug)
        payload = fetch_payload(
            scraper,
            candidate,
            job,
            detail_timeout=config.detail_timeout,
            debug=config.debug,
            concentration_serp=concentration_serp,
        )
        dm = payload.pop("_derived_metrics", None)
        facts_filled, facts_missing = _payload_fact_summary(payload, dm)
        if result_sink is not None:
            result_sink["facts_filled"] = facts_filled
            result_sink["facts_missing"] = facts_missing
            result_sink["quality_status"] = payload.get("quality_status")
        notes = payload.get("notes") or {}
        note_count = sum(len(notes.get(k) or []) for k in ("top", "heart", "base", "flat"))
        target_url = payload.get("fg_url") or payload.get("parfumo_url") or "?"
        if payload.get("source") == "parfumo":
            print(f"{index_label} Parsed (parfumo) notes={note_count} url={target_url}")
        else:
            print(
                f"{index_label} Parsed frag_cards={len(payload.get('frag_cards') or {})} "
                f"notes={note_count} reviews={len(payload.get('reviews') or [])}"
            )
        if facts_missing:
            print(
                f"{index_label} Facts {len(facts_filled)}/{len(FACT_FIELDS)} — "
                f"missing: {', '.join(facts_missing)}"
            )
        else:
            print(f"{index_label} Facts {len(FACT_FIELDS)}/{len(FACT_FIELDS)} — all present")

        if config.dry_run:
            print(f"{index_label} Dry run: would upload cache for {target_url}")
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
        if result_sink is not None:
            result_sink["error"] = exc.code
        print(f"{index_label} Failed {label}: {exc.code}")
        if exc.code == "api_unavailable" and str(exc) != exc.code:
            detail = str(exc).split(":", 1)[-1].strip()
            if detail:
                print(f"{index_label} Server: {detail[:200]}")
        elif config.debug and str(exc) != exc.code:
            print(f"{index_label} {exc}")
        retire = (
            exc.code == "fg_url_missing_after_resolution"
            and not exc.retryable
            and _matches_no_fragrantica_line(job)
        )
        if config.dry_run:
            if exc.code == "fg_resolver_blocked":
                print(f"{index_label} Dry run: would requeue without consuming failure budget")
                return False
            if retire:
                print(f"{index_label} Dry run: would retire to ignored (no Fragrantica coverage)")
            else:
                print(f"{index_label} Dry run: would mark retryable={exc.retryable}")
            return False
        if exc.code == "fg_resolver_blocked":
            try:
                client.requeue_job(job_id, priority=max(10, int(job.get("priority") or 0)))
                print(f"{index_label} Requeued without consuming failure budget (Fragrantica blocked)")
                return False
            except WorkerError as requeue_exc:
                print(f"{index_label} Could not requeue blocked resolver miss: {requeue_exc.code}")
        if retire:
            try:
                client.ignore_job(
                    job_id,
                    "no Fragrantica coverage for this product line; retired after resolver exhausted retries",
                )
                print(f"{index_label} Retired {label} to ignored (no Fragrantica coverage)")
                return False
            except WorkerError as ignore_exc:
                print(f"{index_label} Could not retire to ignored: {ignore_exc.code}")
        try:
            client.fail_job(job_id, str(exc), exc.retryable)
            print(f"{index_label} Marked {'retryable' if exc.retryable else 'non-retryable'} failure")
        except WorkerError as fail_exc:
            print(f"{index_label} Could not report failure: {fail_exc.code}")
        return False
    except Exception:
        if result_sink is not None:
            result_sink["error"] = "engine_exception"
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
) -> list[str]:
    """Move terminal `failed` rows back to `pending`; returns the requeued ids.

    The ids matter to the caller: requeueing resets failure_count to 0, so the
    only-retries filter in process_pending cannot recognize these rows by their
    failure history and must match them by id instead.
    """
    failed_jobs = client.list_jobs("failed", limit=max(limit, MAX_LIST_LIMIT))
    requeued: list[str] = []
    for job in failed_jobs[:limit]:
        job_id = str(job.get("id") or "").strip()
        if not job_id:
            continue
        if config.dry_run:
            print(f"Dry run: would requeue failed job {job_id}")
            requeued.append(job_id)
            continue
        client.requeue_job(job_id, priority=max(10, int(job.get("priority") or 0) + 1))
        requeued.append(job_id)
    if requeued:
        print(f"Requeued {len(requeued)} terminal failed job(s) to pending.")
    return requeued


def _retry_batch(jobs: list[dict[str, Any]], requeued_ids: set[str]) -> list[dict[str, Any]]:
    """Pending rows that belong in a "Retry failed jobs" batch.

    Two populations qualify: rows with a live failure history (retryable
    failures keep their failure_count on the way back to pending), and rows
    just requeued from terminal `failed` — requeueing resets failure_count to
    0, so those are only recognizable by id.
    """
    return [
        j
        for j in jobs
        if int(j.get("failure_count") or 0) > 0
        or str(j.get("id") or "") in requeued_ids
    ]


class BlockGate:
    """Cross-thread Fragrantica rate-limit backoff for parallel processing.

    When any worker thread reports ``fg_resolver_blocked`` the gate trips and
    new job starts pause until the backoff window elapses, so a block storm
    does not get hammered in parallel by every other in-flight worker. This is
    the concurrent equivalent of the serial path's 180s inter-job backoff.
    """

    def __init__(self, backoff_seconds: float = 180.0) -> None:
        self._lock = threading.Lock()
        self._blocked_until = 0.0
        self._backoff = max(0.0, float(backoff_seconds))

    def trip(self) -> None:
        with self._lock:
            self._blocked_until = max(
                self._blocked_until, time.monotonic() + self._backoff
            )

    def wait(self, stop: StopController | None = None) -> None:
        while True:
            with self._lock:
                remaining = self._blocked_until - time.monotonic()
            if remaining <= 0 or (stop and stop.stop_requested):
                return
            time.sleep(min(1.0, remaining))


def _effective_concurrency(config: WorkerConfig) -> int:
    """Resolve the requested worker count into a safe effective value.

    Parallelism is only safe when the Decodo egress path is active: detail
    fetches then go over rotating premium-proxy IPs (not the single, easily
    rate-limited clearance IP) and the concentration tier is browserless
    (no shared, non-thread-safe Chromium page). When Decodo is off the worker
    stays serial regardless of the request.
    """
    raw = max(0, int(getattr(config, "concurrency", 0) or 0))
    decodo = _concentration_uses_decodo()
    if raw <= 0:  # auto
        return 4 if decodo else 1
    if raw > 1 and not decodo:
        print(
            "Parallel processing needs the Decodo egress path (rotating proxies "
            "+ browserless concentration); falling back to 1 worker."
        )
        return 1
    return raw


def _process_jobs_parallel(
    client: ApiClient,
    scraper,
    engine_args: argparse.Namespace,
    jobs: list[dict[str, Any]],
    config: WorkerConfig,
    concentration_serp: ConcentrationSerpState | None,
    workers: int,
    stop: StopController | None,
) -> int:
    """Process jobs across a bounded thread pool. Each job is claimed
    server-side inside process_job, so the shared pending snapshot cannot be
    double-processed; the concurrency cap (not per-job sleeps) paces throughput,
    and a shared BlockGate enforces the rate-limit backoff across threads."""
    total = len(jobs)
    gate = BlockGate(180.0)
    print(f"Processing {total} job(s) with {workers} parallel workers.")

    def run_one(idx: int, job: dict[str, Any]) -> bool:
        if stop and stop.stop_requested:
            return False
        gate.wait(stop)
        if stop and stop.stop_requested:
            return False
        sink: dict[str, Any] = {}
        ok = process_job(
            client,
            scraper,
            engine_args,
            job,
            index_label=f"[{idx}/{total}]",
            config=config,
            result_sink=sink,
            concentration_serp=concentration_serp,
        )
        if sink.get("error") == "fg_resolver_blocked":
            print("Fragrantica looks rate-limited; pausing new job starts for 180s.")
            gate.trip()
        return ok

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(run_one, idx, job) for idx, job in enumerate(jobs, 1)
        ]
        for fut in as_completed(futures):
            try:
                if fut.result():
                    completed += 1
            except Exception as exc:  # a thread crash must not sink the batch
                print(f"Worker thread error: {exc}")
    return completed


def process_pending(
    client: ApiClient,
    config: WorkerConfig,
    *,
    only_retries: bool = False,
    stop: StopController | None = None,
) -> int:
    if not config.auth_configured:
        raise SystemExit("ENRICHMENT_WORKER_TOKEN is required for worker operations.")

    requeued_ids: set[str] = set()
    if only_retries:
        requeued_ids = set(requeue_terminal_failed_jobs(client, config, limit=config.limit))

    list_limit = max(config.limit, MAX_LIST_LIMIT if only_retries else config.limit)
    pending_all = client.list_jobs("pending", limit=list_limit)
    jobs = pending_all
    if only_retries:
        jobs = _retry_batch(jobs, requeued_ids)
    jobs = jobs[: config.limit]
    if not jobs:
        print("No matching pending jobs.")
        return 0

    scraper = engine.get_scraper()
    fg_scraper = engine.get_fragrantica_scraper(
        scraper.default_scraper,
        mint_clearance=not engine._chromium_mint_disabled(),
    )
    if not engine._validate_fragrantica_session(fg_scraper):
        err = getattr(engine, "_FRAGRANTICA_LAST_MINT_ERROR", None)
        detail = f" Last mint error: {err}" if err else ""
        raise SystemExit(
            "Clearance preflight failed: fragrantica_clearance is missing or invalid."
            f"{detail}"
        )
    engine_args = _build_engine_args()
    concentration_serp = ConcentrationSerpState(
        enabled=_env_bool("ENRICHMENT_CONCENTRATION_SERP", True),
        debug=config.debug,
    )
    completed = 0
    total = len(jobs)
    workers = _effective_concurrency(config)
    try:
        if workers > 1:
            completed = _process_jobs_parallel(
                client, scraper, engine_args, jobs, config, concentration_serp, workers, stop
            )
        else:
            for idx, job in enumerate(jobs, 1):
                if stop and stop.stop_requested:
                    break
                sink: dict[str, Any] = {}
                ok = process_job(
                    client,
                    scraper,
                    engine_args,
                    job,
                    index_label=f"[{idx}/{total}]",
                    config=config,
                    result_sink=sink,
                    concentration_serp=concentration_serp,
                )
                completed += 1 if ok else 0
                if idx < total and not (stop and stop.stop_requested):
                    if sink.get("error") == "fg_resolver_blocked":
                        print("Fragrantica looks rate-limited; backing off 180s before the next job.")
                        sleep_conservatively(180.0, 0.0, stop)
                    sleep_conservatively(config.delay, config.jitter, stop)
    finally:
        concentration_serp.close()
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
    fg_scraper = engine.get_fragrantica_scraper(
        scraper.default_scraper,
        mint_clearance=not engine._chromium_mint_disabled(),
    )
    if not engine._validate_fragrantica_session(fg_scraper):
        err = getattr(engine, "_FRAGRANTICA_LAST_MINT_ERROR", None)
        detail = f" Last mint error: {err}" if err else ""
        raise SystemExit(
            "Clearance preflight failed: fragrantica_clearance is missing or invalid."
            f"{detail}"
        )
    engine_args = _build_engine_args()
    completed = 0
    concentration_serp = ConcentrationSerpState(
        enabled=_env_bool("ENRICHMENT_CONCENTRATION_SERP", True),
        debug=config.debug,
    )
    print(f"Warm list: {len(queries)} queries from {path}")
    try:
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
                    concentration_serp=concentration_serp,
                )
                completed += 1 if ok else 0
            except WorkerError as exc:
                print(f"[{idx}/{len(queries)}] Failed {query!r}: {exc.code}")
                if config.debug and str(exc) != exc.code:
                    print(f"[{idx}/{len(queries)}] {exc}")
            if idx < len(queries) and not (stop and stop.stop_requested):
                sleep_conservatively(config.delay, config.jitter, stop)
    finally:
        concentration_serp.close()
    print(f"Warm-list completed {completed} matching job(s).")
    return completed


def _queue_diagnostic_rows(client: ApiClient) -> dict[str, Any]:
    """Read enough queue state to distinguish terminal failures from retry history."""
    failed = client.list_jobs("failed", limit=MAX_LIST_LIMIT)
    pending = client.list_jobs("pending", limit=MAX_LIST_LIMIT)
    ignored = client.list_jobs("ignored", limit=MAX_LIST_LIMIT)
    pending_with_failures = [
        job
        for job in pending
        if int(job.get("failure_count") or 0) > 0 or _error_bucket(job) != "none"
    ]
    failed_or_retry = failed + pending_with_failures
    ignored_candidates = [
        job
        for job in failed_or_retry
        if _error_bucket(job) == "fg_url_missing_after_resolution"
        and not str(job.get("fg_url") or "").strip()
        and _matches_no_fragrantica_line(job)
    ]

    buckets: dict[str, dict[str, int]] = {}
    for status, jobs in (
        ("failed", failed),
        ("pending_with_failures", pending_with_failures),
        ("ignored", ignored),
    ):
        status_buckets: dict[str, int] = {}
        for job in jobs:
            bucket = _error_bucket(job)
            status_buckets[bucket] = status_buckets.get(bucket, 0) + 1
        buckets[status] = status_buckets

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "failed": len(failed),
            "pending_with_failures": len(pending_with_failures),
            "ignored": len(ignored),
            "ignored_candidates": len(ignored_candidates),
        },
        "buckets": buckets,
        "failed": failed,
        "pending_with_failures": pending_with_failures,
        "ignored": ignored,
        "ignored_candidates": ignored_candidates,
    }


def _queue_diag_flat_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group in ("failed", "pending_with_failures", "ignored", "ignored_candidates"):
        for job in snapshot.get(group) or []:
            if not isinstance(job, dict):
                continue
            rows.append(
                {
                    "group": group,
                    "id": job.get("id") or "",
                    "status": job.get("status") or "",
                    "house": job.get("house") or "",
                    "name": job.get("name") or "",
                    "query": job.get("query") or "",
                    "failure_count": job.get("failure_count") or 0,
                    "error_bucket": _error_bucket(job),
                    "last_error": job.get("last_error") or "",
                    "fg_url": job.get("fg_url") or "",
                    "bn_url": job.get("bn_url") or "",
                    "priority": job.get("priority") or "",
                    "last_requested_at": job.get("last_requested_at") or "",
                }
            )
    return rows


def export_queue_diagnostics(client: ApiClient, output_dir: str | Path = "diagnostics") -> tuple[Path, Path]:
    """Export failed/retry-history/ignored queue state to timestamped files."""
    snapshot = _queue_diagnostic_rows(client)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = out_dir / f"enrichment_queue_{stamp}.json"
    csv_path = out_dir / f"enrichment_queue_{stamp}.csv"

    json_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    rows = _queue_diag_flat_rows(snapshot)
    fieldnames = [
        "group",
        "id",
        "status",
        "house",
        "name",
        "query",
        "failure_count",
        "error_bucket",
        "last_error",
        "fg_url",
        "bn_url",
        "priority",
        "last_requested_at",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return json_path, csv_path


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
    print("Processing stale: unavailable")
    print("Failed retryable: unavailable")
    print("Completed today: unavailable")
    print()
    print("1. List pending jobs")
    print("2. Process next job")
    print("3. Process next N jobs")
    print("4. Process jobs slowly with delay")
    print("5. Retry failed jobs")
    print("6. Resolve missing Fragrantica URL")
    print("7. Export queue diagnostics")
    print("8. Show cache stats")
    print("9. Warm from manual query list")
    print("0. Exit")


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
        resolve_missing_urls(client, config, stop=stop)
    elif choice == "7":
        ensure_token(config)
        raw = input("Output directory [diagnostics]: ").strip()
        json_path, csv_path = export_queue_diagnostics(client, raw or "diagnostics")
        print(f"Wrote {json_path}")
        print(f"Wrote {csv_path}")
    elif choice == "8":
        print("Cache stats endpoint is unavailable in the current API contract.")
    elif choice == "9":
        ensure_token(config)
        path = input("Query list path: ").strip()
        if path:
            warm_list(client, config, path, stop=stop)
    elif choice == "0":
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
    "6": "Resolve missing URL",
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
    """Status plus lightweight attention metrics.

    Uses aggregate counts to skip unnecessary list calls, caps list scans, and
    runs the remaining list fetches in parallel so a slow Railway cold start does
    not block the dashboard for 90+ seconds."""
    status = _status_counts(client)
    snapshot: dict[str, Any] = {
        "enabled": status.get("enabled"),
        "counts": dict(status.get("counts") or {}),
        "error": status.get("error"),
        "retryable_failures": None,
        "stale_processing": None,
        "pending_retry_reasons": [],
        "failed_reasons": [],
    }
    if snapshot["error"]:
        return snapshot

    counts = snapshot.get("counts") or {}
    pending_n = int(counts.get("pending") or 0)
    processing_n = int(counts.get("processing") or 0)
    terminal_failed = int(counts.get("failed") or 0)

    if pending_n <= 0:
        snapshot["retryable_failures"] = terminal_failed
    if processing_n <= 0:
        snapshot["stale_processing"] = 0

    def _pending_retries() -> dict[str, Any]:
        limit = min(DASHBOARD_ATTENTION_LIMIT, max(5, pending_n))
        pending = client.list_jobs("pending", limit=limit)
        retry_rows = [
            j
            for j in pending
            if int(j.get("failure_count") or 0) > 0 or _error_bucket(j) != "none"
        ]
        return {
            "count": len(retry_rows),
            "recent": [_job_attention_line(j) for j in retry_rows[:3]],
        }

    def _failed_reasons() -> list[str]:
        limit = min(DASHBOARD_ATTENTION_LIMIT, max(5, terminal_failed))
        failed = client.list_jobs("failed", limit=limit)
        return [_job_attention_line(j) for j in failed[:3]]

    def _stale_count() -> int:
        limit = min(DASHBOARD_ATTENTION_LIMIT, max(5, processing_n))
        processing = client.list_jobs("processing", limit=limit)
        now = datetime.now(timezone.utc)
        return sum(
            1
            for j in processing
            if (_parse_iso(j.get("claim_expires_at")) or now) < now
        )

    futures: dict[Any, str] = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        if pending_n > 0:
            futures[pool.submit(_pending_retries)] = "retryable"
        if processing_n > 0:
            futures[pool.submit(_stale_count)] = "stale"
        if terminal_failed > 0:
            futures[pool.submit(_failed_reasons)] = "failed_reasons"
        for fut in as_completed(futures):
            kind = futures[fut]
            try:
                result = fut.result()
            except WorkerError:
                continue
            if kind == "retryable":
                retry_count = int((result or {}).get("count") or 0)
                snapshot["retryable_failures"] = retry_count + terminal_failed
                snapshot["pending_retry_reasons"] = list((result or {}).get("recent") or [])
            elif kind == "stale":
                snapshot["stale_processing"] = int(result)
            else:
                snapshot["failed_reasons"] = list(result or [])

    if snapshot["retryable_failures"] is None:
        snapshot["retryable_failures"] = terminal_failed if pending_n <= 0 else None
    if snapshot["stale_processing"] is None:
        snapshot["stale_processing"] = 0 if processing_n <= 0 else None
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
    failed_reasons = list(snapshot.get("failed_reasons") or [])
    pending_reasons = list(snapshot.get("pending_retry_reasons") or [])
    if failed_reasons:
        print(f"  {c['G']}│{c['Z']}  {c['Y']}!{c['Z']} failed_latest        {failed_reasons[0]}")
    if pending_reasons:
        print(f"  {c['G']}│{c['Z']}  {c['Y']}!{c['Z']} pending_retry       {pending_reasons[0]}")

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


def _pick_stale_processing_job(client: ApiClient) -> dict[str, Any] | None:
    """Return the oldest stale processing row, if any."""
    try:
        jobs = client.list_jobs("processing", limit=DASHBOARD_ATTENTION_LIMIT)
    except WorkerError:
        return None
    now = datetime.now(timezone.utc)
    stale = [j for j in jobs if (_parse_iso(j.get("claim_expires_at")) or now) < now]
    return stale[0] if stale else None


def _maybe_run_heal_sweep(client: ApiClient, auto_state: dict[str, Any]) -> None:
    """Idle-queue self-healing: requeue stored fragrances with missing facts.

    Runs only when auto_approve finds nothing pending, at most once per
    HEAL_SWEEP_INTERVAL_MINUTES. Pages through fragrance_records across
    successive idle ticks (offset kept on auto_state) so big databases are
    audited in slices, then wraps back to the start.
    """
    if HEAL_SWEEP_INTERVAL_MINUTES <= 0:
        return
    now = time.time()
    last = float(auto_state.get("heal_last_at") or 0.0)
    if now - last < HEAL_SWEEP_INTERVAL_MINUTES * 60.0:
        return
    auto_state["heal_last_at"] = now
    offset = int(auto_state.get("heal_offset") or 0)
    try:
        result = client.heal_sweep(
            limit=HEAL_SWEEP_PAGE_LIMIT,
            offset=offset,
            priority=5,
            max_requested_count=HEAL_SWEEP_MAX_REQUESTED,
        )
    except WorkerError as exc:
        _log_event(f"auto: heal sweep failed ({exc.code})", "err")
        return
    audited = int(result.get("audited") or 0)
    queued = int(result.get("queued") or 0)
    incomplete = int(result.get("incomplete") or 0)
    auto_state["heal_offset"] = 0 if audited < HEAL_SWEEP_PAGE_LIMIT else offset + audited
    _log_event(
        f"auto: heal sweep audited {audited} (offset {offset}) — "
        f"{incomplete} incomplete, {queued} requeued",
        "ok" if queued else "info",
    )


def _run_auto_approve(
    client: ApiClient,
    config: WorkerConfig,
    stop: StopController,
    auto_state: dict[str, Any],
    *,
    render_ctx: dict[str, Any] | None = None,
) -> None:
    """Claim and complete the single oldest pending enrichment.

    This is the per-tick body of auto-approve mode: it grabs one pending job,
    runs it through the same process_job() path the manual actions use, and
    returns. The engine scraper/args are built once and cached on auto_state so
    repeated ticks stay cheap.
    """
    cooldown_until = float(auto_state.get("fg_cooldown_until") or 0.0)
    if time.time() < cooldown_until:
        if not auto_state.get("fg_cooldown_logged"):
            remaining = int(cooldown_until - time.time())
            _log_event(f"auto: Fragrantica rate-limited — pausing ~{remaining}s", "warn")
            auto_state["fg_cooldown_logged"] = True
        return
    auto_state.pop("fg_cooldown_logged", None)

    reclaiming = False
    job = _pick_stale_processing_job(client)
    if job:
        reclaiming = True
    else:
        try:
            jobs = client.list_jobs("pending", limit=1)
        except WorkerError as exc:
            _log_event(f"auto: cannot read queue ({exc.code})", "err")
            return
        if not jobs:
            _log_event("auto: queue empty — nothing to approve", "info")
            _maybe_run_heal_sweep(client, auto_state)
            return
        job = jobs[0]

    if auto_state.get("scraper") is None:
        auto_state["scraper"] = engine.get_scraper()
        auto_state["args"] = _build_engine_args()
        auto_state["concentration_serp"] = ConcentrationSerpState(
            enabled=_env_bool("ENRICHMENT_CONCENTRATION_SERP", True),
            debug=config.debug,
        )
        # Same preflight process_pending hard-fails on. Auto mode keeps going
        # (jobs may still resolve via the API or Parfumo), but the operator
        # must see why Fragrantica detail fetches are unavailable.
        try:
            fg_scraper = engine.get_fragrantica_scraper(
                auto_state["scraper"].default_scraper,
                mint_clearance=not engine._chromium_mint_disabled(),
            )
            if not engine._validate_fragrantica_session(fg_scraper):
                err = getattr(engine, "_FRAGRANTICA_LAST_MINT_ERROR", None)
                detail = f" ({err})" if err else ""
                _log_event(f"auto: FG clearance unavailable{detail}", "err")
            else:
                _log_event("auto: FG clearance ready", "ok")
        except Exception:
            _log_event("auto: FG clearance preflight errored (continuing)", "warn")
    target = job.get("name") or job.get("house") or job.get("id") or "next job"
    if reclaiming:
        _log_event(f"auto: reclaiming stale {target}", "warn")
    else:
        _log_event(f"auto: processing {target}", "info")
    if render_ctx:
        _render_dashboard(
            render_ctx["snapshot"],
            render_ctx["baseline"],
            config,
            int(render_ctx["interval"]),
            bool(render_ctx["auto_approve"]),
        )
    # process_job() prints progress to stdout; swallow it so the dashboard
    # frame stays intact and only our LIVE_STREAM lines surface to the user.
    # It also catches its own scraping errors and returns False rather than
    # re-raising, so we MUST inspect the return value — otherwise a row that
    # silently fails-and-requeues looks identical to a success here.
    sink: dict[str, Any] = {}
    started = time.monotonic()
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            ok = process_job(
                client,
                auto_state["scraper"],
                auto_state["args"],
                job,
                index_label="[auto]",
                config=config,
                result_sink=sink,
                concentration_serp=auto_state.get("concentration_serp"),
            )
    except KeyboardInterrupt:
        stop.stop_requested = True
        _log_event("auto: interrupted by operator", "warn")
        return
    except WorkerError as exc:
        _log_event(f"auto: failed ({exc.code})", "err")
        return
    elapsed = int(time.monotonic() - started)
    if ok:
        filled = sink.get("facts_filled") or []
        missing = sink.get("facts_missing") or []
        _log_event(f"auto: completed {target} in {elapsed}s", "ok")
        if missing:
            shown = ", ".join(missing[:4]) + ("…" if len(missing) > 4 else "")
            _log_event(
                f"auto: {target}: {len(filled)}/{len(FACT_FIELDS)} facts — missing {shown}",
                "warn",
            )
        else:
            _log_event(f"auto: {target}: all {len(FACT_FIELDS)} facts captured", "ok")
    else:
        reason = sink.get("error") or "see worker log"
        _log_event(f"auto: failed {target} after {elapsed}s ({reason})", "err")
        if reason == "fg_resolver_blocked":
            # Hammering Fragrantica during a block window only extends it and
            # burns every queued job's retry budget; back off instead.
            auto_state["fg_cooldown_until"] = time.time() + 180.0


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
    now = time.monotonic()
    last = float(runtime.get("last_phone_poll") or 0.0)
    if now - last < PHONE_HEARTBEAT_INTERVAL:
        return
    runtime["last_phone_poll"] = now
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
    last_snapshot: dict[str, Any] = {"counts": {}}
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
            last_snapshot = snapshot
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
                _run_auto_approve(
                    client,
                    config,
                    stop,
                    auto_state,
                    render_ctx={
                        "snapshot": last_snapshot,
                        "baseline": baseline or {},
                        "interval": interval,
                        "auto_approve": True,
                    },
                )
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
    concentration_serp = auto_state.get("concentration_serp")
    close_concentration_serp = getattr(concentration_serp, "close", None)
    if callable(close_concentration_serp):
        close_concentration_serp()
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
            with engine._FRAGRANTICA_SESSION_LOCK:
                old_session = engine._FRAGRANTICA_SESSION
                engine._FRAGRANTICA_SESSION = session
            if old_session is not None and old_session is not session and hasattr(old_session, "close"):
                try:
                    old_session.close()
                except Exception:
                    pass
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
    engine.load_local_env()
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
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _prompt_float(label: str, default: float) -> float:
    raw = input(label).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _pick_job_interactive(jobs: list[dict[str, Any]], *, title: str) -> dict[str, Any] | None:
    if not jobs:
        print("No matching jobs.")
        return None
    print(title)
    _print_jobs(jobs)
    choice = input("Pick job [#], paste job id, or Enter to cancel: ").strip()
    if not choice:
        return None
    if choice.isdigit():
        index = int(choice)
        if 1 <= index <= len(jobs):
            return jobs[index - 1]
        print("Invalid selection.")
        return None
    for job in jobs:
        if str(job.get("id") or "") == choice:
            return job
    print("Job id not found in the list.")
    return None


def _patch_job_local(
    job_id: str,
    *,
    fg_url: str | None = None,
    query: str | None = None,
) -> dict[str, Any] | None:
    """Direct DB patch when the remote API has not yet deployed the PATCH route."""
    import db

    db_url = (
        os.environ.get("ENRICHMENT_DATABASE_URL", "").strip()
        or os.environ.get("DATABASE_URL", "").strip()
    )
    if not db_url:
        return None
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = db_url
    try:
        import importlib

        importlib.reload(db)
        if not db.ENABLED:
            return None
        db.init_db()
        return db.patch_job(job_id, fg_url=fg_url, query=query)
    finally:
        if prev is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = prev
        import importlib

        importlib.reload(db)


def _assign_fg_url_to_job(
    client: ApiClient,
    config: WorkerConfig,
    job: dict[str, Any],
    fg_url: str,
    *,
    query: str | None = None,
) -> dict[str, Any]:
    job_id = str(job.get("id") or "")
    if not job_id:
        raise WorkerError("invalid_job", "job has no id", retryable=False)
    canonical = _normalize_fg_url_input(fg_url)
    if config.dry_run:
        print(f"Dry run: would set fg_url={canonical} on {job_id}")
        return {**job, "fg_url": canonical}
    try:
        payload = client.patch_job(job_id, fg_url=canonical, query=query)
    except WorkerError as exc:
        if exc.code == "api_not_found":
            updated = _patch_job_local(job_id, fg_url=canonical, query=query)
            if updated:
                print("(API PATCH unavailable — applied via DATABASE_URL direct patch)")
                print(f"Set fg_url on {_job_label(updated)}:")
                print(f"  {updated.get('fg_url')}")
                return updated
            raise WorkerError(
                "patch_endpoint_unavailable",
                "API patch endpoint not deployed — redeploy api.py with PATCH /api/enrichment/jobs/{id}, "
                "or set DATABASE_URL so the worker can patch jobs directly.",
                retryable=False,
            ) from exc
        raise
    updated = payload.get("job") if isinstance(payload.get("job"), dict) else None
    if not updated:
        raise WorkerError("patch_failed", "server did not return updated job", retryable=False)
    print(f"Set fg_url on {_job_label(updated)}:")
    print(f"  {updated.get('fg_url')}")
    return updated


def resolve_missing_url_for_job(
    client: ApiClient,
    config: WorkerConfig,
    job: dict[str, Any],
    *,
    stop: StopController | None = None,
) -> bool:
    """Interactive search-or-paste flow to attach a Fragrantica URL to one job."""
    label = _job_label(job)
    default_query = _build_query(job)
    print()
    print(f"Resolve URL for: {label}")
    print(f"  id={job.get('id')}")
    print(f"  fg_url={job.get('fg_url') or 'missing'}")
    if default_query:
        print(f"  search query={default_query!r}")
    print()
    print("[S] Enhanced search   [P] Paste URL   [Q] Custom query + search   [Enter] Cancel")
    mode = input("Choice: ").strip().lower()

    if not mode:
        return False
    if stop and stop.stop_requested:
        return False

    if mode in {"p", "paste", "url"}:
        raw = input("Fragrantica perfume URL: ").strip()
        if not raw:
            return False
        _assign_fg_url_to_job(client, config, job, raw)
        return True

    query = default_query
    custom_query: str | None = None
    if mode in {"q", "query"}:
        custom_query = input(f"Search query [{default_query}]: ").strip() or default_query
        query = custom_query
    elif mode not in {"s", "search"}:
        print("Unknown choice.")
        return False

    if not query:
        print("No searchable query on this job — paste a URL with [P] instead.")
        return False

    scraper = engine.get_scraper()
    engine_args = _build_engine_args(enhanced=True)
    print(f"\nSearching (enhanced) for {query!r}…")
    linked = _api_linked_candidates(client, query, job, debug=config.debug)
    if not linked:
        results = _search_candidates(scraper, query, engine_args, debug=config.debug)
        linked = [item for item in results if item.frag_url]
    if not linked:
        print("No Fragrantica URLs found.")
        paste = input("Paste a URL manually, or Enter to cancel: ").strip()
        if paste:
            _assign_fg_url_to_job(client, config, job, paste, query=custom_query)
            return True
        return False

    engine.print_results_table(linked[: min(15, len(linked))], 0.0, debug=config.debug)
    pick = input(
        f"Pick [1-{min(15, len(linked))}], paste a URL, or Enter to cancel: "
    ).strip()
    if not pick:
        return False
    if pick.lower().startswith("http"):
        _assign_fg_url_to_job(client, config, job, pick, query=custom_query)
        return True
    if pick.isdigit():
        index = int(pick)
        if 1 <= index <= min(15, len(linked)):
            chosen = linked[index - 1]
            _assign_fg_url_to_job(client, config, job, chosen.frag_url, query=custom_query)
            return True
    print("Invalid selection.")
    return False


def resolve_missing_urls(
    client: ApiClient,
    config: WorkerConfig,
    *,
    stop: StopController | None = None,
) -> int:
    """List pending jobs missing fg_url and resolve one interactively."""
    ensure_token(config)
    pending = client.list_jobs("pending", limit=MAX_LIST_LIMIT)
    missing = [j for j in pending if _job_missing_fg_url(j)]
    if not missing:
        print("No pending jobs with missing fg_url.")
        job = _pick_job_interactive(pending, title="All pending jobs:")
    else:
        job = _pick_job_interactive(
            missing,
            title=f"Pending jobs missing fg_url ({len(missing)}):",
        )
    if not job:
        return 0
    return 1 if resolve_missing_url_for_job(client, config, job, stop=stop) else 0


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
    parser.add_argument(
        "--queue-diagnostics",
        nargs="?",
        const="diagnostics",
        metavar="DIR",
        help="Export failed, retry-history, and ignored queue rows to timestamped JSON/CSV files.",
    )
    parser.add_argument("--api-base-url", default=os.environ.get("SCENT_API_BASE_URL", DEFAULT_API_BASE_URL))
    parser.add_argument("--limit", type=int, default=_env_int("ENRICHMENT_DEFAULT_LIMIT", DEFAULT_LIMIT))
    parser.add_argument("--delay", type=float, default=_env_float("ENRICHMENT_DEFAULT_DELAY", DEFAULT_DELAY))
    parser.add_argument("--jitter", type=float, default=_env_float("ENRICHMENT_DEFAULT_JITTER", DEFAULT_JITTER))
    parser.add_argument(
        "--workers",
        type=int,
        default=_env_int("ENRICHMENT_WORKER_CONCURRENCY", 0),
        help=(
            "Parallel job-processing workers (0 = auto: 4 when the Decodo egress "
            "path is active, else 1). Values >1 are honored only with Decodo; the "
            "single-IP clearance path is forced serial to avoid rate-limit blocks."
        ),
    )
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
        concurrency=max(0, int(getattr(opts, "workers", 0) or 0)),
    )


def main() -> int:
    engine.load_local_env()
    # The Parfumo factual-field fallback is the only resolution path for
    # perfumes Fragrantica does not carry; without it those jobs fail
    # terminally with fg_url_missing_after_resolution. Default it on for the
    # worker (set PARFUMO_FALLBACK_ENABLED=0 in the env to disable).
    os.environ.setdefault("PARFUMO_FALLBACK_ENABLED", "1")
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
        if opts.queue_diagnostics:
            ensure_token(config)
            json_path, csv_path = export_queue_diagnostics(client, opts.queue_diagnostics)
            print(f"Wrote {json_path}")
            print(f"Wrote {csv_path}")
            return 0
        return run_management(client, config, stop)
    except KeyboardInterrupt:
        print("\nInterrupted. Claimed in-flight work was not marked complete; its lease may expire naturally.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
