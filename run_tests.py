#!/usr/bin/env python3
"""Run the plain-script test suite in parallel subprocesses.

Each test file is a standalone script that manages its own env scrubbing and
monkeypatch restoration, and all disk writes happen inside per-test temp
directories -- so the files are process-isolated and safe to run concurrently.
The wall-clock win comes from overlapping the ~3-4s api/engine import that
every file pays; sequentially that import tax dominates the suite.

Usage:
    python run_tests.py            # run everything in parallel
    python run_tests.py --serial   # legacy one-at-a-time behavior
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

TESTS = (
    "test_decodo_scraper_api.py",
    "test_cold_path_decodo_recovery.py",
    "test_decodo_serp_proxy.py",
    "test_parfinity.py",
    "test_search_relevance.py",
    "test_enrichment.py",
)


def main(argv: list[str]) -> int:
    serial = "--serial" in argv
    root = Path(__file__).resolve().parent
    start = time.perf_counter()

    def spawn(name: str) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, str(root / name)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(root),
        )

    failures: list[str] = []

    def collect(name: str, proc: subprocess.Popen) -> None:
        out, _ = proc.communicate()
        ok = proc.returncode == 0
        print(f"=== {name}: {'PASS' if ok else f'FAIL (rc={proc.returncode})'} ===")
        if not ok:
            failures.append(name)
            print(out)

    if serial:
        for name in TESTS:
            collect(name, spawn(name))
    else:
        for name, proc in [(name, spawn(name)) for name in TESTS]:
            collect(name, proc)

    elapsed = time.perf_counter() - start
    print(f"\n{len(TESTS) - len(failures)}/{len(TESTS)} test files passed in {elapsed:.1f}s")
    if failures:
        print("Failed: " + ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
