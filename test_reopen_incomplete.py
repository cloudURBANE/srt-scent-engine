#!/usr/bin/env python3
"""
test_reopen_incomplete.py

Guards the one-shot `--reopen-incomplete` worker command: it pages the whole
engine DB through POST /api/enrichment/heal, reopening every fact-incomplete row
as a pending job so an ordinary drain re-verifies it. The paging must advance by
the audited count, stop on the final (short) page, forward dry_run/fields, and
survive a mid-sweep API error with partial progress kept.

Runs as a plain script (no pytest, no DB, no network):

    python test_reopen_incomplete.py

Exit code is non-zero if any check fails.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_worker as w

LIMIT = w.HEAL_SWEEP_PAGE_LIMIT


class _FakeClient:
    def __init__(self, pages: list[dict]) -> None:
        self.pages = pages
        self.calls: list[dict] = []

    def heal_sweep(self, **kw) -> dict:
        self.calls.append(kw)
        return self.pages[len(self.calls) - 1]


class _FailAfterFirst:
    def __init__(self) -> None:
        self.n = 0

    def heal_sweep(self, **kw) -> dict:
        self.n += 1
        if self.n == 1:
            return {"total_records": 400, "audited": LIMIT, "incomplete": 50, "queued": 50, "skipped": 0}
        raise w.WorkerError("api_unavailable")


def _check(name: str, cond: bool) -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    return cond


def main() -> int:
    ok = True

    # Full page then a short page -> two calls, offset advances by audited, stop.
    c = _FakeClient(
        [
            {"total_records": 230, "audited": LIMIT, "incomplete": 60, "queued": 50, "skipped": 10},
            {"total_records": 230, "audited": 30, "incomplete": 12, "queued": 10, "skipped": 2},
        ]
    )
    totals = w.reopen_incomplete(c, dry_run=False)
    ok &= _check("pages until the final short page", len(c.calls) == 2)
    ok &= _check("offset advances by audited count", [x["offset"] for x in c.calls] == [0, LIMIT])
    ok &= _check(
        "totals accumulate across pages",
        totals == {"audited": LIMIT + 30, "incomplete": 72, "queued": 60, "skipped": 12},
    )

    # dry_run + field filter are forwarded to the endpoint.
    c2 = _FakeClient([{"total_records": 5, "audited": 5, "incomplete": 3, "queued": 0, "skipped": 0}])
    w.reopen_incomplete(c2, dry_run=True, fields=["year", "concentration"])
    ok &= _check(
        "dry_run + fields forwarded",
        c2.calls[0].get("dry_run") is True and c2.calls[0].get("fields") == ["year", "concentration"],
    )

    # A mid-sweep API error stops cleanly and keeps the first page's progress.
    fc = _FailAfterFirst()
    totals3 = w.reopen_incomplete(fc)
    ok &= _check("mid-sweep error keeps partial progress", totals3["queued"] == 50)

    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
