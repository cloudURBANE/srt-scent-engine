#!/usr/bin/env python3
"""
probe_enrichment_job.py -- single-URL diagnostic for parser_empty_frag_cards.

Bypasses the worker queue. Given a Fragrantica URL (or an enrichment_jobs row
id, when SCENT_API_BASE_URL + ENRICHMENT_WORKER_TOKEN are set), runs the same
fetch path the worker uses and reports:

    - is_perfume_url (URL shape check)
    - HTTP status / error from Http.get
    - len(frag_cards), card names, note counts
    - bucket classification (URL / HTTP / parser / timeout)

Exit code is 0 when frag_cards is non-empty, 1 otherwise (CI-friendly).

Examples:
    python scripts/probe_enrichment_job.py --fg-url https://www.fragrantica.com/perfume/Chanel/N-19-528.html
    python scripts/probe_enrichment_job.py --fg-url <url> --detail-timeout 20 --save-html out.html
    python scripts/probe_enrichment_job.py --job-id <uuid>
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

import requests

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import fragrance_parser_full_rewrite_fixed as engine  # noqa: E402


def _fetch_job_row(api_base: str, token: str, job_id: str) -> dict[str, Any] | None:
    headers = {"Authorization": f"Bearer {token}"}
    for status in ("failed", "pending"):
        url = f"{api_base.rstrip('/')}/api/enrichment/jobs?status={status}&limit=100"
        try:
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()
            payload = res.json()
        except Exception as exc:
            print(f"[warn] could not list status={status}: {exc}")
            continue
        jobs = payload.get("jobs") if isinstance(payload, dict) else payload
        for job in jobs or []:
            if str(job.get("id") or "") == job_id:
                return job
    return None


def _classify(
    *,
    url: str,
    url_shape_ok: bool,
    res_ok: bool,
    fetch_error: str | None,
    frag_card_count: int,
    deadline_hit: bool,
) -> str:
    if not url:
        return "A — bad URL (empty)"
    if not url_shape_ok:
        return "A — bad URL (does not match perfume URL pattern)"
    if not res_ok:
        if deadline_hit:
            return "D — timeout (deadline expired before HTTP completed)"
        if fetch_error and fetch_error.startswith("HTTP"):
            return f"B — HTTP failure ({fetch_error})"
        return f"B — HTTP failure ({fetch_error or 'unknown'})"
    if frag_card_count == 0:
        return "C — parser produced no cards from fetched HTML"
    return "OK — frag_cards populated"


def probe(
    *,
    fg_url: str,
    detail_timeout: float,
    save_html: str | None,
    debug: bool,
) -> int:
    scraper = engine.get_scraper()
    url_shape_ok = bool(engine.FragranticaEngine.is_perfume_url(fg_url))

    print(f"[probe] fg_url={fg_url}")
    print(f"[probe] is_perfume_url={url_shape_ok}")
    if not fg_url:
        print(_classify(url=fg_url, url_shape_ok=False, res_ok=False, fetch_error=None, frag_card_count=0, deadline_hit=False))
        return 1

    deadline = engine.Deadline(detail_timeout)
    res = engine.Http.get(
        scraper,
        fg_url,
        timeout=detail_timeout,
        referer=engine.FragranticaEngine.BASE_URL,
        deadline=deadline,
        attempts=2,
    )
    fetch_error = engine.Http.last_error() if res is None else None
    deadline_hit = deadline.expired() and res is None

    if res is not None:
        print(f"[probe] HTTP {getattr(res, 'status_code', '?')} bytes={len(getattr(res, 'content', b'') or b'')}")
    else:
        print(f"[probe] HTTP failed: {fetch_error or 'unknown'} deadline_expired={deadline_hit}")

    if save_html and res is not None:
        try:
            Path(save_html).write_bytes(res.content)
            print(f"[probe] saved HTML to {save_html}")
        except Exception as exc:
            print(f"[probe] save_html failed: {exc}")

    selected = engine.UnifiedFragrance(name="", brand="", year="", bn_url="", frag_url=fg_url)
    details = engine.fetch_selected_details(scraper, selected, detail_timeout)
    cards = details.frag_cards or {}
    notes = details.notes
    note_total = len(notes.top) + len(notes.heart) + len(notes.base) + len(notes.flat)

    print(f"[probe] frag_cards={len(cards)} names={sorted(cards.keys())}")
    print(f"[probe] notes top={len(notes.top)} heart={len(notes.heart)} base={len(notes.base)} flat={len(notes.flat)} (total={note_total})")
    fetch_errors = getattr(details, "fetch_errors", {}) or {}
    if fetch_errors:
        print(f"[probe] fetch_errors={fetch_errors}")

    if save_html and res is None:
        print("[probe] --save-html skipped (no response)")

    bucket = _classify(
        url=fg_url,
        url_shape_ok=url_shape_ok,
        res_ok=res is not None,
        fetch_error=fetch_errors.get("fg") or fetch_error,
        frag_card_count=len(cards),
        deadline_hit=deadline_hit,
    )
    print(f"[probe] bucket: {bucket}")
    return 0 if cards else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--fg-url", help="Fragrantica perfume URL to probe.")
    src.add_argument("--job-id", help="enrichment_jobs row id (requires SCENT_API_BASE_URL + ENRICHMENT_WORKER_TOKEN).")
    p.add_argument("--detail-timeout", type=float, default=15.0, help="Per-page fetch deadline (seconds).")
    p.add_argument("--save-html", help="When the FG fetch succeeds, write the response body to this path.")
    p.add_argument("--debug", action="store_true")
    return p.parse_args()


def main() -> int:
    opts = parse_args()

    if opts.job_id:
        api_base = os.environ.get("SCENT_API_BASE_URL", "").strip()
        token = os.environ.get("ENRICHMENT_WORKER_TOKEN", "").strip()
        if not api_base or not token:
            print("--job-id requires SCENT_API_BASE_URL and ENRICHMENT_WORKER_TOKEN env vars.")
            return 2
        job = _fetch_job_row(api_base, token, opts.job_id)
        if not job:
            print(f"job_id {opts.job_id} not found in failed/pending lists")
            return 2
        fg_url = str(job.get("fg_url") or "").strip()
        print(f"[probe] job_id={opts.job_id} failure_count={job.get('failure_count')} last_error={job.get('last_error')!r}")
        if not fg_url:
            print("[probe] job has no fg_url; bucket A — bad URL (empty)")
            return 1
    else:
        fg_url = opts.fg_url.strip()

    return probe(
        fg_url=fg_url,
        detail_timeout=max(0.1, float(opts.detail_timeout)),
        save_html=opts.save_html,
        debug=bool(opts.debug),
    )


if __name__ == "__main__":
    sys.exit(main())
