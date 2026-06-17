#!/usr/bin/env python3
"""Guards for the test runner itself (run_tests.py).

Two regressions this locks down:
  1. Drift -- the runner must auto-discover every ``test_*.py`` so a new test
     file can't sit unrun because nobody appended it to a hardcoded list.
  2. The vacuous-pass trap -- a pytest-style file (no ``__main__`` guard) run as
     a bare script exits 0 without running anything. The runner must route such
     files through pytest, not invoke them directly.

Script-style so run_tests.py discovers and runs it like the rest of the suite.
"""
from __future__ import annotations

import sys
from pathlib import Path

import run_tests

ROOT = Path(__file__).resolve().parent
PASS, FAIL = "[PASS]", "[FAIL]"


def _check(cond: bool, label: str) -> bool:
    print(f"  {PASS if cond else FAIL} {label}")
    return cond


def main() -> int:
    ok = True

    discovered = {p.name for p in run_tests.discover(ROOT)}
    all_test_files = {p.name for p in ROOT.glob("test_*.py")}

    ok &= _check(
        discovered == all_test_files,
        f"discover() finds every test_*.py ({len(all_test_files)} files)",
    )
    ok &= _check(
        "test_run_tests.py" in discovered,
        "discovery includes this self-hosting file",
    )

    # A pytest-style file (no __main__ guard) must be routed through pytest, or
    # it would no-op into a false pass when invoked as a plain script.
    pytest_style = next(
        (p for p in run_tests.discover(ROOT) if not run_tests.is_script_style(p)),
        None,
    )
    ok &= _check(pytest_style is not None, "at least one pytest-style file exists to guard")
    if pytest_style is not None:
        cmd = run_tests.command_for(pytest_style)
        ok &= _check(
            "-m" in cmd and "pytest" in cmd,
            f"pytest-style file routed through pytest ({pytest_style.name})",
        )

    # A script-style file (this one has a __main__ guard) is invoked directly.
    this_file = ROOT / "test_run_tests.py"
    ok &= _check(run_tests.is_script_style(this_file), "this file detected as script-style")
    direct_cmd = run_tests.command_for(this_file)
    ok &= _check(
        "pytest" not in direct_cmd and direct_cmd[-1].endswith("test_run_tests.py"),
        "script-style file invoked directly (no pytest)",
    )

    print("ALL CHECKS PASSED" if ok else "FAILURES PRESENT")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
