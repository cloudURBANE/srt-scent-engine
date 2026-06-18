#!/usr/bin/env python3
"""Regression checks for offline wardrobe projection.

These stay DB-free and network-free: the bug was in the pure projection helper
that copies engine-cache facts into app wardrobe payloads.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, ROOT)

import heal_offline as heal


def _check(name: str, cond: bool) -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
    return cond


def _complete_dm() -> dict:
    return {
        "performance_score": {"score_raw": 82, "label": "Strong"},
        "value_score": {"score_raw": 71, "label": "Fair"},
        "community_interest_score": {"score_raw": 64, "label": "Moderate"},
        "wear_profile": {
            "primary_seasons": ["Fall"],
            "primary_time": "Night",
        },
        "main_accords": {
            "top_accords": ["amber", "woody", "vanilla"],
            "source": "test",
        },
        "notes": {"top": ["bergamot"], "base": ["vanilla"]},
        "source_coverage": {
            "performance_score": True,
            "value_score": True,
            "community_interest_score": True,
            "wear_profile": True,
            "main_accords": True,
            "notes": True,
        },
    }


def main() -> int:
    ok = True

    dm = _complete_dm()
    payload = {"brand": "Christian Dior", "name": "Sauvage Elixir", "family": "Woody"}
    flags = heal.project_engine_facts(
        payload,
        {
            "derived_metrics": dm,
            "concentration": None,
            "year": None,
            "gender": None,
            "image_url": None,
        },
    )
    ok &= _check("projection reports derived_metrics fill", flags[4] is True)
    ok &= _check("blank wardrobe row receives derived_metrics", payload.get("derived_metrics") == dm)
    ok &= _check("blank wardrobe row receives clean accords copy", payload.get("accords") == ["amber", "woody", "vanilla"])
    ok &= _check("blank wardrobe row receives complete wear profile", payload.get("wear_profile", {}).get("primary_time") == "Night")
    ok &= _check("blank wardrobe row receives top-level notes", flags[5] is True and payload.get("notes") == ["bergamot", "vanilla"])
    ok &= _check("blank wardrobe row receives note pyramid", payload.get("pyramid", {}).get("top") == ["bergamot"])

    partial_payload = {
        "derived_metrics": {
            "performance_score": {"score_raw": 99, "label": "Keep me"},
            "source_coverage": {"performance_score": True},
        },
        "wear_profile": {"primary_seasons": ["Spring"], "primary_time": "Day"},
        "accords": ["amber", "woody", "vanilla"],
        "family": "Amber",
    }
    flags = heal.project_engine_facts(
        partial_payload,
        {
            "derived_metrics": dm,
            "concentration": None,
            "year": None,
            "gender": None,
            "image_url": None,
        },
    )
    merged = partial_payload["derived_metrics"]
    ok &= _check("partial derived_metrics are topped up", flags[4] is True)
    ok &= _check("existing populated score group is not overwritten", merged["performance_score"]["score_raw"] == 99)
    ok &= _check("missing score group is filled", merged["value_score"]["score_raw"] == 71)
    ok &= _check("source coverage is OR-merged", merged["source_coverage"]["value_score"] is True)
    ok &= _check("complete top-level wear profile is not downgraded", partial_payload["wear_profile"]["primary_time"] == "Day")
    ok &= _check("missing top-level notes are filled from existing derived metrics", partial_payload.get("notes") == ["bergamot", "vanilla"])

    complete_payload = {"derived_metrics": _complete_dm(), "accords": ["amber", "woody", "vanilla"], "family": "Amber"}
    before_dm = repr(complete_payload["derived_metrics"])
    flags = heal.project_engine_facts(
        complete_payload,
        {
            "derived_metrics": dm,
            "concentration": None,
            "year": None,
            "gender": None,
            "image_url": None,
        },
    )
    ok &= _check(
        "complete derived_metrics are left as a no-op",
        flags[4] is False and repr(complete_payload["derived_metrics"]) == before_dm,
    )

    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
