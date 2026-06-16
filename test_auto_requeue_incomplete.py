#!/usr/bin/env python3
"""
test_auto_requeue_incomplete.py

Guards the inline self-heal added to the enrichment worker: a row that comes
back metrics-complete (all four derived-metric groups parsed) but still missing
heal-worthy facts (year / concentration / gender / ...) is re-enqueued right
after completion, so a later worker run fills the gap without a manual heal
sweep. The requeue is bounded by HEAL_SWEEP_MAX_REQUESTED so an un-fillable fact
cannot loop forever.

Runs as a plain script (no pytest, no DB, no network):

    python test_auto_requeue_incomplete.py

Exit code is non-zero if any check fails.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrichment_worker as w


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def requeue_job(self, job_id: str, *, priority: int = 10) -> dict:
        self.calls.append((job_id, priority))
        return {}


def _check(name: str, cond: bool) -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    return cond


def main() -> int:
    ok = True

    # Source-unsuppliable facts (e.g. Parfumo has no FG wear/score/review data)
    # are never heal-worthy and must not drive a requeue.
    healable = w._heal_worthy_missing_facts(
        {"source": "parfumo"}, ["year", "reviews", "wear_profile"]
    )
    ok &= _check("parfumo unsuppliable facts filtered out", healable == ["year"])

    # Metrics-complete + heal-worthy missing + under the cap -> requeue.
    c = _FakeClient()
    w._auto_requeue_incomplete(
        c,
        {"id": "j1", "requested_count": 2, "priority": 5},
        {"source": "local_enrichment_worker", "quality_status": "complete"},
        ["year", "concentration"],
        index_label="[t]",
    )
    ok &= _check("complete+incomplete under cap requeues once", c.calls == [("j1", 5)])

    # Partial-metrics rows are handled by the normal retry path -> no requeue.
    c = _FakeClient()
    w._auto_requeue_incomplete(
        c, {"id": "j2", "requested_count": 0}, {"quality_status": "partial"}, ["year"], index_label="[t]"
    )
    ok &= _check("partial-metrics row is left alone", c.calls == [])

    # At the churn cap -> stop retrying, leave the row complete.
    c = _FakeClient()
    w._auto_requeue_incomplete(
        c,
        {"id": "j3", "requested_count": w.HEAL_SWEEP_MAX_REQUESTED},
        {"quality_status": "complete", "source": "local"},
        ["year"],
        index_label="[t]",
    )
    ok &= _check("requeue cap is respected", c.calls == [])

    # Complete with nothing missing -> nothing to do.
    c = _FakeClient()
    w._auto_requeue_incomplete(
        c, {"id": "j4", "requested_count": 0}, {"quality_status": "complete", "source": "local"}, [], index_label="[t]"
    )
    ok &= _check("fully complete row is not requeued", c.calls == [])

    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
