"""Evaluate Decodo scraper API as a Serper replacement.

No network call is made unless ``--live`` is passed. Credentials are read only
from environment variables consumed by ``DecodoScraperClient``:

    DECODO_API_BASIC_TOKEN=<base64 username:password>

or:

    DECODO_API_USERNAME=<username>
    DECODO_API_PASSWORD=<password>
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fragrance_parser_full_rewrite_fixed as engine  # noqa: E402


DEFAULT_TIMEOUT = 8.0


@dataclass(frozen=True)
class SearchCase:
    name: str
    query: str
    target: str
    expected_substring: str
    expected_url: str | None = None


@dataclass
class SearchCaseResult:
    name: str
    query: str
    target: str
    elapsed_ms: int
    accepted_count: int
    first_accepted_url: str | None
    accepted_sample: list[str]
    expected_matched: bool
    error: str | None = None


@dataclass
class ImageCaseResult:
    query: str
    elapsed_ms: int
    candidate_count: int
    first_candidate: dict | None
    error: str | None = None


DEFAULT_CASES: tuple[SearchCase, ...] = (
    SearchCase(
        name="fragrantica_naxos",
        query="xerjoff naxos site:fragrantica.com/perfume",
        target="fragrantica",
        expected_substring="/Xerjoff/",
    ),
    SearchCase(
        name="fragrantica_aventus",
        query="creed aventus site:fragrantica.com/perfume",
        target="fragrantica",
        expected_substring="/Creed/Aventus-9828.html",
    ),
    SearchCase(
        name="fragrantica_tempio",
        query="casamorati 1888 tempio d acqua site:fragrantica.com/perfume",
        target="fragrantica",
        expected_substring="/Casamorati-1888/Tempio-d-Acqua-128356.html",
    ),
    SearchCase(
        name="parfumo_khamrah",
        query='site:parfumo.com/Perfumes "Lattafa" "Khamrah"',
        target="parfumo",
        expected_substring="/Lattafa/khamrah",
        expected_url="https://www.parfumo.com/Perfumes/Lattafa/khamrah",
    ),
)


def _accepted_urls(raw_urls: Iterable[str], target: str) -> list[str]:
    accepted: list[str] = []
    seen: set[str] = set()
    for raw_url in raw_urls:
        if target == "fragrantica":
            canonical = engine.FragranticaEngine.canonical_url(raw_url)
            if not canonical or not engine.FragranticaEngine.is_perfume_url(canonical):
                continue
            dedupe = engine.FragranticaEngine.dedupe_key(canonical)
        elif target == "parfumo":
            canonical = engine.ParfumoEngine.canonical_url(raw_url)
            if not canonical or not engine.ParfumoEngine.is_perfume_url(canonical):
                continue
            dedupe = canonical
        else:
            continue
        if dedupe in seen:
            continue
        seen.add(dedupe)
        accepted.append(canonical)
    return accepted


def run_search_case(case: SearchCase, *, timeout: float) -> SearchCaseResult:
    started = time.perf_counter()
    try:
        payload = engine.DecodoScraperClient._post_google(case.query, timeout)
        raw_urls = [
            engine.DecodoScraperClient._entry_link(entry)
            for entry in engine.DecodoScraperClient._search_entries(payload)
        ]
        accepted = _accepted_urls(raw_urls, case.target)
        error = None
    except Exception as exc:
        accepted = []
        error = f"{type(exc).__name__}: {exc}"
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return SearchCaseResult(
        name=case.name,
        query=case.query,
        target=case.target,
        elapsed_ms=elapsed_ms,
        accepted_count=len(accepted),
        first_accepted_url=accepted[0] if accepted else None,
        accepted_sample=accepted[:5],
        expected_matched=(
            case.expected_url in accepted
            if case.expected_url
            else any(case.expected_substring.lower() in url.lower() for url in accepted)
        ),
        error=error,
    )


def run_image_case(query: str, *, timeout: float, max_results: int) -> ImageCaseResult:
    started = time.perf_counter()
    try:
        payload = engine.DecodoScraperClient._post_google(query, timeout, image_search=True)
        candidates = engine.DecodoScraperClient.image_candidates_from_payload(payload, max_results=max_results)
        error = None
    except Exception as exc:
        candidates = []
        error = f"{type(exc).__name__}: {exc}"
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return ImageCaseResult(
        query=query,
        elapsed_ms=elapsed_ms,
        candidate_count=len(candidates),
        first_candidate=candidates[0] if candidates else None,
        error=error,
    )


def _self_test() -> None:
    search_payload = {
        "results": [
            {
                "content": {
                    "results": [
                        {"link": "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html"},
                        {"link": "https://example.com/nope"},
                    ]
                }
            }
        ]
    }
    entries = engine.DecodoScraperClient._search_entries(search_payload)
    accepted = _accepted_urls([engine.DecodoScraperClient._entry_link(entry) for entry in entries], "fragrantica")
    assert accepted == ["https://www.fragrantica.com/perfume/Creed/Aventus-9828.html"]

    image_payload = {
        "results": [
            {
                "content": {
                    "results": [
                        {
                            "image_url": "https://images.example.test/bottle.jpg",
                            "source_url": "https://source.example.test/page",
                            "width": "1000",
                            "height": "900",
                        }
                    ]
                }
            }
        ]
    }
    images = engine.DecodoScraperClient.image_candidates_from_payload(image_payload)
    assert images and images[0]["imageWidth"] == 1000 and images[0]["imageHeight"] == 900


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="Run Decodo-backed network checks.")
    parser.add_argument("--case", action="append", help="Search case name to run. Repeat for multiple cases.")
    parser.add_argument("--all-cases", action="store_true", help="Run the full default search matrix.")
    parser.add_argument("--image-query", default="xerjoff naxos fragrance bottle")
    parser.add_argument("--skip-images", action="store_true", help="Skip the image-search smoke in live mode.")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--max-image-results", type=int, default=12)
    parser.add_argument("--json-output", type=Path, help="Optional path to write JSON results.")
    parser.add_argument("--allow-misses", action="store_true", help="Exit 0 even when a live case misses.")
    return parser.parse_args(argv)


def choose_cases(args: argparse.Namespace) -> list[SearchCase]:
    cases_by_name = {case.name: case for case in DEFAULT_CASES}
    if args.case:
        selected = []
        for name in args.case:
            if name not in cases_by_name:
                raise ValueError(f"Unknown case {name!r}; choose from {', '.join(cases_by_name)}")
            selected.append(cases_by_name[name])
        return selected
    if args.all_cases:
        return list(DEFAULT_CASES)
    return [DEFAULT_CASES[0]]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    _self_test()
    if not args.live:
        print("Local Decodo scraper API parser self-test passed. Add --live for network checks.")
        return 0

    previous_provider = os.environ.get("SERP_API_PROVIDER")
    os.environ["SERP_API_PROVIDER"] = "decodo"
    try:
        if not engine.DecodoScraperClient.enabled():
            raise RuntimeError(
                "Set DECODO_API_BASIC_TOKEN or DECODO_API_USERNAME/DECODO_API_PASSWORD for live checks."
            )
        output: dict[str, object] = {"search_cases": [], "image_case": None}
        for case in choose_cases(args):
            result = run_search_case(case, timeout=args.timeout)
            output["search_cases"].append(asdict(result))
            status = "PASS" if result.expected_matched and not result.error else "FAIL"
            print(
                f"{status} {case.name}: latency={result.elapsed_ms}ms "
                f"accepted={result.accepted_count} first={result.first_accepted_url} "
                f"error={result.error}"
            )

        if not args.skip_images:
            image_result = run_image_case(
                args.image_query,
                timeout=args.timeout,
                max_results=args.max_image_results,
            )
            output["image_case"] = asdict(image_result)
            status = "PASS" if image_result.candidate_count > 0 and not image_result.error else "FAIL"
            print(
                f"{status} images: latency={image_result.elapsed_ms}ms "
                f"candidates={image_result.candidate_count} first={image_result.first_candidate} "
                f"error={image_result.error}"
            )

        if args.json_output:
            args.json_output.write_text(json.dumps(output, indent=2), encoding="utf-8")
            print(f"Wrote {args.json_output}")

        failed_search = [
            item
            for item in output["search_cases"]
            if item.get("error") or not item.get("expected_matched")
        ]
        failed_images = output["image_case"] and (
            output["image_case"].get("error") or output["image_case"].get("candidate_count", 0) <= 0
        )
        if (failed_search or failed_images) and not args.allow_misses:
            return 1
        return 0
    finally:
        if previous_provider is None:
            os.environ.pop("SERP_API_PROVIDER", None)
        else:
            os.environ["SERP_API_PROVIDER"] = previous_provider


if __name__ == "__main__":
    raise SystemExit(main())
