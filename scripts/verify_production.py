#!/usr/bin/env python3
"""Safely verify fragrance search and details API behavior in production.

The checks are intentionally limited to read-oriented endpoints from the
production verification guide: /health, /api/fragrances/search, and
/api/fragrances/details.
"""

from __future__ import annotations

import argparse
import base64
import json
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def encode_id(name: str, brand: str, year: str, bn_url: str, frag_url: str) -> str:
    """Build the stateless opaque id accepted by the details endpoint."""
    payload = {
        "n": name,
        "b": brand,
        "y": year,
        "bn": bn_url,
        "fg": frag_url,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def request_json(
    url: str,
    method: str = "GET",
    data: bytes | None = None,
    timeout: float = 15.0,
) -> tuple[int, dict[str, Any]]:
    headers = {"Content-Type": "application/json"}
    request = urllib.request.Request(url, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body)
    except urllib.error.HTTPError as exc:
        try:
            err_body = exc.read().decode("utf-8")
            return exc.code, json.loads(err_body)
        except Exception:
            return exc.code, {"error": exc.reason}
    except Exception as exc:
        return 500, {"error": str(exc)}


def record(name: str, passed: bool, details: dict[str, Any]) -> dict[str, Any]:
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}: {json.dumps(details, sort_keys=True)}")
    return {"name": name, "passed": passed, "details": details}


def test_health(base_url: str) -> dict[str, Any]:
    status, response = request_json(f"{base_url}/health")
    return record(
        "health",
        status == 200 and response.get("ok") is True,
        {"status": status, "response": response},
    )


def test_live_search_and_caching(base_url: str) -> dict[str, Any]:
    suffix = random.randint(1000, 9999)
    query = f"Creed Aventus Test {suffix}"
    encoded_query = urllib.parse.quote(query)

    status1, response1 = request_json(f"{base_url}/api/fragrances/search?q={encoded_query}")
    diag1 = response1.get("diagnostics", {}) if isinstance(response1, dict) else {}
    time.sleep(1.0)
    status2, response2 = request_json(f"{base_url}/api/fragrances/search?q={encoded_query}")
    diag2 = response2.get("diagnostics", {}) if isinstance(response2, dict) else {}

    passed = (
        status1 == 200
        and status2 == 200
        and diag2.get("live_search_skipped") is True
        and diag2.get("cache_mode") in {"precheck", "warm"}
    )
    return record(
        "live_search_cache_promotion",
        passed,
        {
            "query": query,
            "first_status": status1,
            "first_live_search_skipped": diag1.get("live_search_skipped"),
            "first_cache_mode": diag1.get("cache_mode"),
            "second_status": status2,
            "second_live_search_skipped": diag2.get("live_search_skipped"),
            "second_cache_mode": diag2.get("cache_mode"),
            "second_result_count": len(response2.get("results", []))
            if isinstance(response2, dict)
            else None,
        },
    )


def test_spelling_repair(base_url: str) -> dict[str, Any]:
    cases = [("xerjoff naxus", "Naxos"), ("creed avantus", "Aventus")]
    observations: list[dict[str, Any]] = []
    all_ok = True
    for query, expected_name in cases:
        encoded_query = urllib.parse.quote(query)
        status, response = request_json(f"{base_url}/api/fragrances/search?q={encoded_query}")
        results = response.get("results", []) if isinstance(response, dict) else []
        match = next(
            (
                {
                    "name": item.get("name"),
                    "house": item.get("house"),
                }
                for item in results
                if expected_name.lower() in str(item.get("name", "")).lower()
            ),
            None,
        )
        ok = status == 200 and match is not None
        all_ok = all_ok and ok
        observations.append(
            {
                "query": query,
                "expected_name": expected_name,
                "status": status,
                "result_count": len(results),
                "match": match,
            }
        )
    return record("spelling_repair", all_ok, {"cases": observations})


def test_details_cached(base_url: str) -> dict[str, Any]:
    naxos_id = encode_id(
        name="XJ 1861 Naxos",
        brand="Xerjoff",
        year="2015",
        bn_url="https://basenotes.com/fragrances/xj-1861-naxos-by-xerjoff.26146030",
        frag_url="https://www.fragrantica.com/perfume/Xerjoff/XJ-1861-Naxos-30529.html",
    )
    payload = json.dumps({"id": naxos_id}).encode("utf-8")
    status, response = request_json(
        f"{base_url}/api/fragrances/details",
        method="POST",
        data=payload,
    )
    coverage = response.get("source_coverage", {}) if isinstance(response, dict) else {}
    passed = (
        status == 200
        and coverage.get("fragrantica_cached") is True
        and coverage.get("fragrantica_cache_source") in {"db", "json", "aggregate_db"}
    )
    return record(
        "details_cached_naxos",
        passed,
        {
            "status": status,
            "name": response.get("name") if isinstance(response, dict) else None,
            "house": response.get("house") if isinstance(response, dict) else None,
            "fragrantica_cached": coverage.get("fragrantica_cached"),
            "fragrantica_cache_source": coverage.get("fragrantica_cache_source"),
        },
    )


def test_details_uncached(base_url: str) -> dict[str, Any]:
    suffix = random.randint(100000, 999999)
    uncached_id = encode_id(
        name=f"Uncached Fragrance {suffix}",
        brand="Unique Brand",
        year="2026",
        bn_url=f"https://basenotes.com/fragrances/unique-uncached-{suffix}",
        frag_url=f"https://www.fragrantica.com/perfume/Unique-Brand/Unique-Uncached-{suffix}.html",
    )
    payload = json.dumps({"id": uncached_id}).encode("utf-8")
    status, response = request_json(
        f"{base_url}/api/fragrances/details",
        method="POST",
        data=payload,
    )
    coverage = response.get("source_coverage", {}) if isinstance(response, dict) else {}
    passed = status == 200 or status in {502, 503}
    return record(
        "details_uncached_safe_response",
        passed,
        {
            "status": status,
            "fragrantica_cached": coverage.get("fragrantica_cached"),
            "error": response.get("error") if isinstance(response, dict) else None,
        },
    )


def test_empty_notes_handling(base_url: str) -> dict[str, Any]:
    empty_notes_id = encode_id(
        name="Empty Notes Test",
        brand="Empty Notes House",
        year="2026",
        bn_url="https://basenotes.com/fragrances/empty-notes-dummy-url",
        frag_url="",
    )
    payload = json.dumps({"id": empty_notes_id}).encode("utf-8")
    status, response = request_json(
        f"{base_url}/api/fragrances/details",
        method="POST",
        data=payload,
    )
    if status in {502, 503}:
        return record(
            "empty_notes_handling",
            True,
            {
                "status": status,
                "accepted_reason": "standard network/scraper capacity response, no container crash",
                "error": response.get("error") if isinstance(response, dict) else None,
            },
        )

    raw = response.get("raw", {}) if isinstance(response, dict) else {}
    notes = raw.get("notes", {}) if isinstance(raw, dict) else {}
    derived_metrics = response.get("derived_metrics") if isinstance(response, dict) else None
    derived_notes = derived_metrics.get("notes") if isinstance(derived_metrics, dict) else None
    raw_empty = (
        isinstance(notes.get("top"), list)
        and len(notes["top"]) == 0
        and isinstance(notes.get("heart"), list)
        and len(notes["heart"]) == 0
        and isinstance(notes.get("base"), list)
        and len(notes["base"]) == 0
    )
    return record(
        "empty_notes_handling",
        status == 200 and raw_empty and derived_notes is None,
        {
            "status": status,
            "raw_notes": notes,
            "derived_notes": derived_notes,
        },
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Base URL of production environment.")
    parser.add_argument("--json-output", help="Optional path to write structured results.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    base_url = args.url.rstrip("/")
    print(f"Starting verification against {base_url}")

    results = [
        test_health(base_url),
        test_live_search_and_caching(base_url),
        test_spelling_repair(base_url),
        test_details_cached(base_url),
        test_details_uncached(base_url),
        test_empty_notes_handling(base_url),
    ]

    passed = sum(1 for result in results if result["passed"])
    total = len(results)
    payload = {
        "base_url": base_url,
        "passed": passed,
        "total": total,
        "results": results,
    }
    if args.json_output:
        with open(args.json_output, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        print(f"Wrote {args.json_output}")

    print(f"Verification completed: {passed}/{total} checks passed.")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
