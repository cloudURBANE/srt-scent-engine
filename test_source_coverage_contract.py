#!/usr/bin/env python3
"""Cross-service source_coverage contract test (readiness gap X2).

The SPA's self-heal loop is driven by ``isSourceCoverageComplete`` in the sCAST
repo (artifacts/scent-cast/src/lib/fragranceApi.ts), which reads the dict this
engine's ``_source_coverage`` emits. The two repos deploy independently, so a
shape change on either side would previously only surface in production.

``contract/source_coverage_fixtures.json`` is committed BYTE-IDENTICAL in both
repos (sCAST copy: artifacts/scent-cast/src/lib/sourceCoverageContractFixtures
.json). This side asserts the engine PRODUCES exactly the ``engine_producible``
fixture shapes; the sCAST side asserts its predicate returns ``spa_complete``
for every fixture. Change the contract deliberately -> update both copies in
lockstep. Fully offline (no DB, no network); pytest-style, run via
``python -m pytest``.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import api
import fragrance_parser_full_rewrite_fixed as engine

_FIXTURES_PATH = _REPO_ROOT / "contract" / "source_coverage_fixtures.json"
_FIXTURES = json.loads(_FIXTURES_PATH.read_text(encoding="utf-8"))["fixtures"]
_BY_NAME = {f["name"]: f for f in _FIXTURES}

_ALL_METRIC_GROUPS_PRESENT = {g: True for g in api._FG_METRIC_GROUPS}


def _selected(with_fg: bool = True) -> engine.UnifiedFragrance:
    return engine.UnifiedFragrance(
        name="Contract Fixture",
        brand="Test Brand",
        year="",
        bn_url="https://basenotes.com/fragrances/contract-by-brand.0",
        frag_url=(
            "https://www.fragrantica.com/perfume/Test-Brand/Contract-Fixture-9999.html"
            if with_fg
            else ""
        ),
    )


def _details(
    bn: bool,
    fg_cards: bool,
    metric_groups: dict[str, bool] | None,
) -> engine.UnifiedDetails:
    details = engine.UnifiedDetails(notes=engine.NotesList())
    if bn:
        details.bn_consensus = {"Longevity": [{"label": "Long", "count": "3"}]}
    if fg_cards:
        details.frag_cards = {"Community Interest": [{"label": "Have", "count": "9"}]}
    if metric_groups is not None:
        details.derived_metrics = {"source_coverage": metric_groups}
    return details


# Builders keyed by fixture name: (selected, details) inputs that must yield the
# fixture's coverage dict. Every engine_producible fixture must appear here.
_INPUT_BUILDERS = {
    "dual_source_complete": lambda: (
        _selected(with_fg=True),
        _details(bn=True, fg_cards=True, metric_groups=dict(_ALL_METRIC_GROUPS_PRESENT)),
    ),
    "bn_only_partial": lambda: (
        _selected(with_fg=False),
        _details(bn=True, fg_cards=False, metric_groups={}),
    ),
    "fg_data_metrics_incomplete": lambda: (
        _selected(with_fg=True),
        _details(
            bn=True,
            fg_cards=True,
            metric_groups={**_ALL_METRIC_GROUPS_PRESENT, "wear_profile": False},
        ),
    ),
}


def test_every_engine_producible_fixture_has_an_input_builder():
    producible = {f["name"] for f in _FIXTURES if f.get("engine_producible")}
    assert producible == set(_INPUT_BUILDERS), (
        "contract fixtures and input builders out of sync; update "
        "_INPUT_BUILDERS when adding/removing engine_producible fixtures"
    )


def test_fixture_file_sanity():
    assert len(_FIXTURES) >= 5
    assert any(f["spa_complete"] for f in _FIXTURES)
    assert any(not f["spa_complete"] for f in _FIXTURES)


def _assert_produces(name: str) -> None:
    fixture = _BY_NAME[name]
    selected, details = _INPUT_BUILDERS[name]()
    produced = api._source_coverage(selected, details)
    assert produced == fixture["coverage"], (
        f"_source_coverage drifted from contract fixture {name!r}: "
        f"{produced} != {fixture['coverage']} -- if intentional, update the "
        "fixture file here AND the byte-identical sCAST copy"
    )


def test_dual_source_complete_shape():
    _assert_produces("dual_source_complete")


def test_bn_only_partial_shape():
    _assert_produces("bn_only_partial")


def test_fg_data_metrics_incomplete_shape():
    _assert_produces("fg_data_metrics_incomplete")
