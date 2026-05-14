#!/usr/bin/env python3
"""
warm_fg_detail_cache.py  --  offline Fragrantica *detail* cache warmer.

Companion to warm_fg_cache.py. That script warms fg_identity_cache_v2.json,
which restores Fragrantica *URL identity* on Railway. But URL identity alone
does not bring back the parsed Fragrantica detail metrics: Railway's runtime is
403'd by Cloudflare on fragrantica.com, so a live /details fetch still returns
BN-only data and every Fragrantica-derived group in derived_metrics is null.

This script runs the engine's *existing* detail-fetch path from an environment
that CAN reach Fragrantica, and captures the already-parsed detail output --
frag_cards / notes / pros_cons / reviews -- into fg_detail_cache_v1.json. That
file is shipped with the deploy; the API-level overlay in api.py
(_apply_fg_detail_cache) merges it onto BN-only bundles and re-runs the existing
derived_metrics_adapter.

It changes no engine code. It only:
  * reads the identity cache to learn which Fragrantica URLs to capture,
  * builds an FG-only UnifiedFragrance candidate (bn_url=""),
  * calls engine.fetch_selected_details(...) -- the exact deployed code path,
  * serializes the parser output verbatim (no HTML, no fabricated rows, no
    derived-scores-only entries).

Compliance: run this ONLY from an environment where access to fragrantica.com
is permitted. No CAPTCHA solving, browser automation, stealth behavior, or
proxy-credential logic -- it reuses the engine's own scraper and a conservative
inter-request delay.

Usage
-----
    # default: read fg_cache/fg_identity_cache_v2.json,
    #          write  fg_cache/fg_detail_cache_v1.json
    python scripts/warm_fg_detail_cache.py

    # cap the run, slow it down, show the engine's [SYS] log lines
    python scripts/warm_fg_detail_cache.py --limit 20 --delay 2.0 --verbose

Exit code is non-zero if *no* URL produced parsed Fragrantica detail data (so a
scheduled refresh can detect this environment is now also blocked).
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

# The engine module lives one directory up from scripts/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import fragrance_parser_full_rewrite_fixed as engine  # noqa: E402

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

_DEFAULT_IDENTITY_CACHE = _REPO_ROOT / "fg_cache" / "fg_identity_cache_v2.json"
_DEFAULT_OUT = _REPO_ROOT / "fg_cache" / "fg_detail_cache_v1.json"

_SCHEMA_VERSION = 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _canonical_fg_url(url: str) -> str:
    """Match api.py._canonical_fg_url exactly so cache keys align at runtime."""
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


def _coerce_year(year) -> int | None:
    """Identity cache stores year as a str; emit an int when it cleanly is one."""
    if year is None:
        return None
    text = str(year).strip()
    return int(text) if text.isdigit() else None


def _load_targets(identity_cache_path: Path) -> list[dict[str, str]]:
    """Read the identity cache and return one record per unique Fragrantica URL.

    Keys are derived FROM the identity cache so they align byte-for-byte with
    the frag_url that SearchSniper.apply_cache stamps onto candidates at
    runtime -- the overlay's lookup then cannot miss on a key mismatch.
    """
    raw = json.loads(identity_cache_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"Identity cache {identity_cache_path} is not a JSON object.")
    by_url: dict[str, dict[str, str]] = {}
    for row in raw.values():
        if not isinstance(row, dict):
            continue
        url = str(row.get("url") or "")
        if not url or not engine.FragranticaEngine.is_perfume_url(url):
            continue
        canonical = _canonical_fg_url(url)
        # First occurrence wins; identity cache rows for the same URL agree on
        # name/brand/year anyway.
        by_url.setdefault(
            canonical,
            {
                "fg_url": url,
                "name": str(row.get("name") or ""),
                "house": str(row.get("brand") or ""),
                "year": str(row.get("year") or ""),
            },
        )
    return list(by_url.values())


def _capture_entry(scraper, target: dict[str, str], detail_timeout: float) -> dict | None:
    """Fetch + parse one Fragrantica detail page via the engine's own path.

    Returns a cache entry dict, or None if no Fragrantica detail data parsed
    (blocked / empty page) -- we never fabricate missing rows.
    """
    candidate = engine.UnifiedFragrance(
        name=target["name"],
        brand=target["house"],
        year=target["year"],
        bn_url="",  # FG-only: BasenotesEngine.fetch_details no-ops on empty url
        frag_url=target["fg_url"],
    )
    details = engine.fetch_selected_details(scraper, candidate, detail_timeout)
    if not details.frag_cards:
        return None
    notes = details.notes
    return {
        "fg_url": target["fg_url"],
        "name": target["name"],
        "house": target["house"],
        "year": _coerce_year(target["year"]),
        "captured_at": _now_iso(),
        "source": "cached_fragrantica_detail",
        "frag_cards": details.frag_cards,
        "notes": {
            "has_pyramid": bool(notes.has_pyramid),
            "top": list(notes.top),
            "heart": list(notes.heart),
            "base": list(notes.base),
            "flat": list(notes.flat),
        },
        "pros_cons": list(details.pros_cons),
        "reviews": [
            {"text": r.text, "source": r.source} for r in (details.reviews or [])
        ],
    }


def _write_atomic(out_path: Path, payload: dict) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        prefix=out_path.name, suffix=".tmp", dir=str(out_path.parent)
    )
    tmp_path = Path(tmp_str)
    try:
        with __import__("os").fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        __import__("os").replace(tmp_path, out_path)
    except Exception:
        if tmp_path.exists():
            with contextlib.suppress(Exception):
                tmp_path.unlink()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--identity-cache",
        default=str(_DEFAULT_IDENTITY_CACHE),
        help=f"Identity cache to read URLs from. Default: {_DEFAULT_IDENTITY_CACHE}",
    )
    parser.add_argument(
        "--out",
        default=str(_DEFAULT_OUT),
        help=f"Detail cache file to write. Default: {_DEFAULT_OUT}",
    )
    parser.add_argument(
        "--detail-timeout",
        type=float,
        default=8.0,
        help="Per-page detail fetch deadline (engine default is 6.0).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Conservative delay between requests, in seconds. Default: 1.5",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Cap the number of URLs processed (0 = no cap).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print the engine's [SYS] log lines for each page.",
    )
    opts = parser.parse_args()

    identity_cache_path = Path(opts.identity_cache).resolve()
    out_path = Path(opts.out).resolve()
    if not identity_cache_path.exists():
        raise SystemExit(f"Identity cache not found: {identity_cache_path}")

    targets = _load_targets(identity_cache_path)
    if opts.limit > 0:
        targets = targets[: opts.limit]
    if not targets:
        raise SystemExit(
            f"No Fragrantica perfume URLs found in {identity_cache_path}."
        )

    scraper = engine.get_scraper()
    scraper_type = type(scraper).__module__ + "." + type(scraper).__name__
    print(
        f"[warm-detail] scraper={scraper_type} "
        f"cloudscraper={engine.cloudscraper is not None}"
    )
    print(f"[warm-detail] {len(targets)} URLs -> {out_path}")

    entries: dict[str, dict] = {}
    started = time.time()
    for index, target in enumerate(targets, 1):
        canonical = _canonical_fg_url(target["fg_url"])
        captured = io.StringIO()
        try:
            with contextlib.redirect_stdout(captured):
                entry = _capture_entry(scraper, target, opts.detail_timeout)
        except Exception as exc:  # one bad page must not abort the batch
            print(
                f"  {index:03d}. {target['name']!r:38} "
                f"ERROR {type(exc).__name__}: {exc}"
            )
            continue
        if entry is None:
            print(f"  {index:03d}. {target['name']!r:38} MISS  (no FG detail parsed)")
        else:
            entries[canonical] = entry
            card_count = len(entry["frag_cards"])
            print(f"  {index:03d}. {target['name']!r:38} ok    {card_count} cards")
        if opts.verbose:
            for line in _ANSI_RE.sub("", captured.getvalue()).splitlines():
                if "[SYS]" in line:
                    print(f"          {line.strip()}")
        if opts.delay > 0 and index < len(targets):
            time.sleep(opts.delay)

    elapsed = time.time() - started
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "entries": entries,
    }
    _write_atomic(out_path, payload)

    print(
        f"[warm-detail] done in {elapsed:.1f}s -- {len(entries)}/{len(targets)} "
        f"URLs produced parsed FG detail data, written to {out_path}"
    )

    if not entries:
        print(
            "[warm-detail] WARNING: zero URLs produced Fragrantica detail data -- "
            "this environment may also be blocked. Cache was not usefully warmed.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
