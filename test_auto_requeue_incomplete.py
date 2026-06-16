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

    # Page-determined facts (reviews + community vote scores) are excluded for
    # EVERY source -- a metrics-complete page that lacks them stays lacking on a
    # re-fetch, so they must never drive a requeue (the old drain-churn cause).
    healable = w._heal_worthy_missing_facts(
        {"source": "local_enrichment_worker"},
        ["year", "reviews", "performance_score", "value_score", "community_interest_score", "wear_profile"],
    )
    ok &= _check("page-determined facts filtered for FG source", healable == ["year"])

    # ...but before a row is metrics-complete, the hard facts a retry CAN still
    # improve stay heal-worthy (no quality_status => the completion guard below
    # does not fire).
    healable = w._heal_worthy_missing_facts(
        {"source": "local_enrichment_worker"}, ["year", "gender", "concentration", "family", "main_accords", "notes"]
    )
    ok &= _check(
        "hard facts remain heal-worthy pre-completion",
        healable == ["year", "gender", "concentration", "family", "main_accords", "notes"],
    )

    # Completion guard: once an FG worker page is metrics-complete it has yielded
    # everything its identical URL can. year/concentration/gender are then
    # page-determined too -- a re-fetch reruns the same deterministic extraction
    # and off-page concentration resolver, so NOTHING is heal-worthy. This is the
    # fix for the year/concentration drain churn (8 attempts then stall).
    healable = w._heal_worthy_missing_facts(
        {"source": "local_enrichment_worker", "quality_status": "complete"},
        ["year", "gender", "concentration", "family", "main_accords", "notes"],
    )
    ok &= _check("complete FG page has no heal-worthy facts", healable == [])

    # A row whose ONLY gaps are page-determined must not requeue at all.
    c = _FakeClient()
    w._auto_requeue_incomplete(
        c,
        {"id": "j0", "requested_count": 0, "priority": 5},
        {"source": "local_enrichment_worker", "quality_status": "complete"},
        ["reviews", "performance_score"],
        index_label="[t]",
    )
    ok &= _check("page-determined-only gaps do not requeue", c.calls == [])

    # A metrics-complete FG page whose only gaps are its own (now page-determined)
    # facts must NOT requeue -- this is the churn that made every drain stall.
    c = _FakeClient()
    w._auto_requeue_incomplete(
        c,
        {"id": "j1", "requested_count": 2, "priority": 5},
        {"source": "local_enrichment_worker", "quality_status": "complete"},
        ["year", "concentration"],
        index_label="[t]",
    )
    ok &= _check("complete FG year/concentration gaps do not requeue", c.calls == [])

    # The requeue mechanism itself is intact: a completing source NOT covered by
    # the FG completion guard, with a heal-worthy gap under the cap, still requeues
    # once at the bumped priority.
    c = _FakeClient()
    w._auto_requeue_incomplete(
        c,
        {"id": "j1b", "requested_count": 2, "priority": 5},
        {"source": "other_source", "quality_status": "complete"},
        ["year", "concentration"],
        index_label="[t]",
    )
    ok &= _check("non-FG completing source still requeues once", c.calls == [("j1b", 5)])

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
