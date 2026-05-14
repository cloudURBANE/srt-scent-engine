#!/usr/bin/env python3
"""
warm_fg_cache.py  --  Option 3 pre-cache pipeline (offline enrichment).

Railway's runtime is 403'd by Cloudflare on fragrantica.com, so the deployed
engine resolves zero Fragrantica links and `_ARGS.fg_cache` on Railway is an
empty file in a fresh container. This script runs the engine's *existing*
search path from an environment that CAN reach Fragrantica (a residential
machine), and lets the engine's own `SearchSniper.cache_successes` populate the
FG identity cache as a side effect. The resulting JSON file is then shipped to
Railway so cached queries resolve Fragrantica URLs without the runtime ever
touching fragrantica.com.

It changes no engine code. It only:
  * builds the engine's own default args (`build_parser().parse_args([])`),
  * overrides `args.fg_cache` to a chosen output path,
  * calls `engine.search_once(...)` per query -- the exact deployed code path,
  * relies on the engine to write the cache (it already does, every run).

Nothing here is a parser, resolver, adapter, or fallback-order change.

Usage
-----
    # default seed list, default output ./fg_cache/fg_identity_cache_v2.json
    python scripts/warm_fg_cache.py

    # custom query list (one query per line, '#' comments allowed) + output
    python scripts/warm_fg_cache.py --queries my_queries.txt --out build/fg.json

    # show the engine's [SYS] log lines per query
    python scripts/warm_fg_cache.py --verbose

Exit code is non-zero if *no* query resolved a Fragrantica URL (so CI / a
scheduled refresh can detect that this environment is now also blocked).
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import sys
import time
from pathlib import Path

# The engine module lives one directory up from scripts/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import fragrance_parser_full_rewrite_fixed as engine  # noqa: E402

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Minimal seed list -- replace/extend with your real top-query export. Kept
# short on purpose: this file is a starting point, not the canonical list.
_DEFAULT_SEED_QUERIES = [
    "silver mountain water",
    "creed aventus",
    "dior sauvage",
    "chanel bleu de chanel",
    "tom ford tobacco vanille",
    "ysl la nuit de l'homme",
    "jean paul gaultier le male",
    "paco rabanne 1 million",
    "versace eros",
    "armani acqua di gio",
]

_DEFAULT_OUT = _REPO_ROOT / "fg_cache" / "fg_identity_cache_v2.json"


def _load_queries(path: str | None) -> list[str]:
    if not path:
        return list(_DEFAULT_SEED_QUERIES)
    raw = Path(path).read_text(encoding="utf-8").splitlines()
    queries = []
    for line in raw:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        queries.append(line)
    if not queries:
        raise SystemExit(f"No queries found in {path!r}.")
    return queries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queries",
        help="Path to a newline-delimited query list. Defaults to a built-in seed list.",
    )
    parser.add_argument(
        "--out",
        default=str(_DEFAULT_OUT),
        help=f"Cache file to write. Default: {_DEFAULT_OUT}",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print the engine's [SYS] log lines for each query.",
    )
    opts = parser.parse_args()

    queries = _load_queries(opts.queries)
    out_path = Path(opts.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # The engine's own default args -- identical to what api.py uses -- with the
    # cache path redirected to our chosen output. This is the ONLY mutation, and
    # it is exactly the knob `--fg-cache` already exposes.
    args = engine.build_parser().parse_args([])
    args.fg_cache = str(out_path)

    scraper = engine.get_scraper()
    scraper_type = type(scraper).__module__ + "." + type(scraper).__name__
    print(f"[warm] scraper={scraper_type} cloudscraper={engine.cloudscraper is not None}")
    print(f"[warm] {len(queries)} queries -> {out_path}")

    fg_resolved = 0
    started = time.time()
    for index, query in enumerate(queries, 1):
        captured = io.StringIO()
        try:
            with contextlib.redirect_stdout(captured):
                results = engine.search_once(scraper, query, args)
        except Exception as exc:  # one bad query must not abort the batch
            print(f"  {index:02d}. {query!r:40} ERROR {type(exc).__name__}: {exc}")
            continue
        linked = sum(1 for item in results if item.frag_url)
        fg_resolved += 1 if linked else 0
        status = "ok " if linked else "MISS"
        print(f"  {index:02d}. {query!r:40} {status} {linked}/{len(results)} FG-linked")
        if opts.verbose:
            for line in _ANSI_RE.sub("", captured.getvalue()).splitlines():
                if "[SYS]" in line:
                    print(f"        {line.strip()}")

    elapsed = time.time() - started

    # The engine wrote the cache via SearchSniper.cache_successes during each
    # run. Read it back only to report what landed -- we never hand-edit it.
    entry_count = 0
    if out_path.exists():
        try:
            entry_count = len(json.loads(out_path.read_text(encoding="utf-8")))
        except Exception:
            entry_count = -1

    print(
        f"[warm] done in {elapsed:.1f}s -- {fg_resolved}/{len(queries)} queries "
        f"FG-linked, cache now holds {entry_count} entries at {out_path}"
    )

    if fg_resolved == 0:
        print(
            "[warm] WARNING: zero queries resolved a Fragrantica URL -- this "
            "environment may also be blocked. Cache was not usefully warmed.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
