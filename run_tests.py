#!/usr/bin/env python3
"""Run the project test suite in parallel subprocesses.

Test files are **auto-discovered** (every ``test_*.py`` beside this script) so a
new test file is picked up automatically -- there is no hand-maintained list to
forget to update, which is how files like ``test_auto_requeue_incomplete.py``
previously sat unrun for weeks.

Two test styles coexist in this repo and each is invoked the way it expects:

* **script-style** -- a standalone script with an ``if __name__ == "__main__"``
  block that runs its own checks. Invoked as ``python test_x.py``.
* **pytest-style** -- bare ``test_*`` functions and no ``__main__`` block.
  Invoked as ``python -m pytest test_x.py``. Running such a file as a plain
  script defines the functions and exits 0 *without running anything*, so it
  would silently "pass" while testing nothing -- routing it through pytest
  closes that vacuous-pass trap.

Each file is process-isolated (its own env scrubbing, monkeypatch restoration,
and per-test temp dirs), so the files are safe to run concurrently. The
wall-clock win comes from overlapping the ~3-4s api/engine import that every
file pays; sequentially that import tax dominates the suite.

Usage:
    python run_tests.py            # run everything in parallel
    python run_tests.py --serial   # legacy one-at-a-time behavior
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


def discover(root: Path) -> list[Path]:
    """Every ``test_*.py`` beside this script, in a stable order."""
    return sorted(p for p in root.glob("test_*.py") if p.is_file())


def is_script_style(path: Path) -> bool:
    """True if the file runs its own checks under ``if __name__ == "__main__"``.

    pytest-style files have no such guard; they must be driven by pytest or
    they no-op into a false pass.
    """
    try:
        return "__main__" in path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        # Unreadable: fall back to direct invocation so the failure is visible.
        return True


def command_for(path: Path) -> list[str]:
    """The subprocess argv that actually exercises ``path``'s assertions."""
    if is_script_style(path):
        return [sys.executable, str(path)]
    return [sys.executable, "-m", "pytest", str(path), "-q"]


def main(argv: list[str]) -> int:
    serial = "--serial" in argv
    root = Path(__file__).resolve().parent
    tests = discover(root)
    start = time.perf_counter()

    def spawn(cmd: list[str]) -> subprocess.Popen:
        return subprocess.Popen(
            cmd,
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
        for path in tests:
            collect(path.name, spawn(command_for(path)))
    else:
        running = [(path.name, spawn(command_for(path))) for path in tests]
        for name, proc in running:
            collect(name, proc)

    elapsed = time.perf_counter() - start
    print(f"\n{len(tests) - len(failures)}/{len(tests)} test files passed in {elapsed:.1f}s")
    if failures:
        print("Failed: " + ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
