#!/usr/bin/env python3
"""Warm the bundled Parfinity catalog detail cache.

Usage:
    python scripts/warm_parfinity_cache.py --limit 10
    python scripts/warm_parfinity_cache.py
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import parfinity  # noqa: E402

_DEFAULT_OUT = _REPO_ROOT / "fg_cache" / "parfinity_cache.json"
_SCHEMA_VERSION = 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _write_atomic(out_path: Path, payload: dict) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        prefix=out_path.name, suffix=".tmp", dir=str(out_path.parent)
    )
    tmp_path = Path(tmp_str)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_path, out_path)
    except Exception:
        if tmp_path.exists():
            with contextlib.suppress(Exception):
                tmp_path.unlink()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--locale",
        default="en",
        help="Parfinity locale to fetch. Default: en",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Cap products processed for a sanity run (0 = full catalog).",
    )
    parser.add_argument(
        "--out",
        default=str(_DEFAULT_OUT),
        help=f"Cache file to write. Default: {_DEFAULT_OUT}",
    )
    opts = parser.parse_args()

    products = parfinity.fetch_bulk(locale=opts.locale)
    if opts.limit > 0:
        products = products[: opts.limit]
    if not products:
        raise SystemExit("Parfinity bulk API returned no products.")

    entries: dict[str, dict] = {}
    skipped = 0
    for product in products:
        entry = parfinity.product_to_cache_entry(product)
        canonical = parfinity.canonical_parfinity_url(
            entry.get("canonical_parfinity_url") or entry.get("canonical_fg_url") or ""
        )
        if not canonical or not entry.get("name") or not entry.get("house"):
            skipped += 1
            continue
        entries[canonical] = entry

    payload = {
        "schema_version": _SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "count": len(entries),
        "entries": entries,
    }
    out_path = Path(opts.out).resolve()
    _write_atomic(out_path, payload)

    print(
        f"[warm-parfinity] locale={opts.locale} products={len(products)} "
        f"entries={len(entries)} skipped={skipped} -> {out_path}"
    )
    if not entries:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
