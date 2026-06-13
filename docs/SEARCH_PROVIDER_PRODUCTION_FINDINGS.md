# Search Provider Production Findings

Date: 2026-06-12 20:22:44 -05:00

Production target: `https://srt-scent-engine-production.up.railway.app`

Source materials:

- `C:\Users\urban\.gemini\antigravity\brain\f1c182bf-035d-4e2e-97c8-160157aca950\search_provider_test_guide.md`
- `C:\Users\urban\.gemini\antigravity\brain\f1c182bf-035d-4e2e-97c8-160157aca950\walkthrough.md`

Browser scenario tests and queue-mutating endpoints were skipped. Testing was limited to `GET /health`, `GET /api/fragrances/search`, and `POST /api/fragrances/details`.

## Commands Run

```powershell
python scripts\verify_production.py --url https://srt-scent-engine-production.up.railway.app --json-output production_verification_results.json
python scripts\verify_production.py --url https://srt-scent-engine-production.up.railway.app --timeout 45 --json-output docs\search_provider_production_verification_results_timeout45.json
```

Additional targeted follow-up calls were made to repeat the failed cache-promotion query and inspect the `creed avantus` / `xerjoff naxus` search result ordering without invoking browser scenarios.

## Result Summary

| Check | 15s guide timeout | 45s control timeout | Finding |
| --- | --- | --- | --- |
| Health | Pass | Pass | No issue verified. |
| Live search cache promotion | Fail | Fail | Repeated exact unique query did not demonstrate cache promotion. |
| Spelling repair | Fail | Pass | Production search can exceed the guide's 15s timeout, but eventually returned canonical matches with a longer timeout. |
| Cached details for Xerjoff Naxos | Pass | Pass | No issue verified. |
| Uncached details safe response | Pass | Pass | No crash verified. |
| Empty notes handling | Pass | Pass | Empty-notes 200 response verified; this is relevant to the save-rejection bug path. |

## Verified Issues

### 1. Production search exceeds the guide's 15-second request timeout

Severity: High

Evidence:

- `production_verification_results.json` recorded `TimeoutError` for both passes of `live_search_cache_promotion` using `Creed Aventus Test 9678`.
- The same 15s run also timed out for both spelling-repair queries: `xerjoff naxus` and `creed avantus`.
- A 45s control run passed spelling repair, which indicates the search path was slow rather than completely unavailable.

Impact:

The guide's verifier cannot reliably complete the production search checks within its own 15-second request budget. Users may experience search latency long enough to look like a failed or hung search.

### 2. Repeated unique searches did not prove cache promotion

Severity: High

Evidence:

- In the 45s control run, `Creed Aventus Test 1036` returned `200` on both passes, but `diagnostics.live_search_skipped` and `diagnostics.cache_mode` were both absent on the second pass.
- The second pass also returned `second_result_count: 0`.
- Targeted follow-up calls for `Creed Aventus Test 6105` and repeated `Creed Aventus Test 9977` returned `200` with empty `results` and diagnostics limited to `result_count`, `fragrantica_linked_count`, `fragrantica_unreachable`, and `timing`.
- One repeated `Creed Aventus Test 9977` follow-up still took 16.106s and did not include cache fast-path diagnostics.

Impact:

The required "second query becomes a fast-path cache hit in under 1 second" behavior was not verified. For these unique queries, production appears to return empty search results instead of a promoted cached result, so the exact test case in the guide does not validate cache promotion.

### 3. Details can return `200 OK` with no usable notes

Severity: High

Evidence:

- The empty-notes details check returned `200`.
- `raw.notes` contained empty `top`, `heart`, and `base` arrays.
- `derived_metrics.notes` was `null`.
- The uncached-details check also returned `200` with `fragrantica_cached: false`.

Impact:

This verifies the search-engine side of the reported save-rejection path: a details response can be successful at the HTTP layer while still carrying no usable notes. The downstream Express `/api/scent-profile` rejection was not called because the verification guide prohibits mutating endpoints, so the final save failure remains inferred from the documented validation path rather than directly reproduced in this run.

## Non-Issues Verified

- `/health` returned `200` with `{"ok": true}`.
- Cached Xerjoff Naxos details returned `200` with `source_coverage.fragrantica_cached: true` and `fragrantica_cache_source: aggregate_db`.
- Uncached details requests returned safe `200` responses and did not crash the production container.
- With a longer 45-second timeout, spelling repair returned Xerjoff `Xj 1861 Naxos` for `xerjoff naxus` and Creed `Aventus` for `creed avantus`.

## Recommended Follow-Up

1. Add an explicit "details pending enrichment" state, or block save attempts when details have no usable notes, so a `200 OK` provisional details response is not treated as save-ready.
2. Rework the cache-promotion verification case. The current unique `Creed Aventus Test <id>` query produced no candidates, so it cannot prove positive-result cache promotion.
3. Investigate production search latency separately from details latency. Common typo-repair searches timed out under the guide's 15-second client budget but passed with 45 seconds.

## Correction - 2026-06-12 (verifier was the fault, not the engine)

Re-investigation showed two of the three "Verified Issues" above were artifacts of the verifier (`scripts/verify_production.py`), not production defects:

- **Issue 2 (cache promotion) was a broken check.** `test_live_search_and_caching` queried a synthetic `"Creed Aventus Test <random>"` string. That matches no catalog entry, so the first pass returns zero candidates, nothing is cached, and the promotion assertion can never pass. It is also unverifiable for any novel query while Railway is 403'd by Fragrantica: a cold live result caches as a Basenotes-only row with no `frag_url`, and `_can_skip_live_search_with_cache` deliberately refuses to fast-path a row that lost its Fragrantica identity. The cache fast-path itself is healthy: a real FG-linked identity (`Creed Aventus`, `Xerjoff Naxos`) is served `live_search_skipped` / `cache_mode: precheck` in **~0.2-0.6s** on every pass. The check now asserts that production-true behavior and measures the timing.
- **Issue 1 (search "timeout") was a too-tight client budget.** The cold spell-repair path (first pass -> SERP suggest -> second pass -> designer-catalog crawl) legitimately approaches ~17s; the verifier's 15s default was below that and reported a working path as hung. The default is now 30s, which clears the real latency while still flagging a genuinely stuck request.

After both fixes the verifier reports **6/6** against `https://srt-scent-engine-production.up.railway.app` (see `production_verification_results.json`). Issue 3 (a `200 OK` details response can carry no usable notes) is unchanged and remains a downstream save-gating concern, not an engine defect: the engine already signals it honestly via `source_coverage.complete=false`, empty `raw.notes`, and an `enrichment.status` of `pending`/`processing` with `requires_worker=true`.
