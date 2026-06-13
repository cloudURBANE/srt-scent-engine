# THOMBRONY Search Fix Findings

Date: 2026-06-13

Production target: `https://srt-scent-engine-production.up.railway.app`

## Scope

Focused user-facing/API testing only. Browser scenario tests were skipped per repo instructions. The relevant frontend path calls:

```text
GET /api/fragrances/search?q=<query>
```

## Production Reproduction Before Fix

Targeted production calls against the public search endpoint reproduced the user-visible failure:

| Query | Status | Result count | Notes |
| --- | ---: | ---: | --- |
| `THOMBRONY` | 200 | 0 | No candidates returned. |
| `thombrony` | 200 | 0 | Same failure lowercase. |
| `Thom Browne` | 200 | 0 | Clear designer query also returned no rows. |
| `Creed Aventus` | 200 | 3 | Cached control returned immediately with `live_search_skipped=true`. |

The broad production verifier still passed `6/6`, so the issue was not a total API outage. It was specific to cold discovery for a brand/designer that was not already in the system cache.

## Root Cause

Two code paths could produce the observed behavior:

1. `SERP_API_PROVIDER` was too strict. If production still carried a stale legacy value such as `serper`, Decodo credentials were ignored and all structured discovery was disabled.
2. The Decodo path only accepted Fragrantica perfume-page URLs. For brand/designer searches like `Thom Browne` or typo/collapsed input like `THOMBRONY`, Google can return a Fragrantica designer page first. The engine dropped designer URLs and never used them to discover the designer's perfume URLs, so the public search endpoint returned an empty `results` array.

## Fix

Changed `fragrance_parser_full_rewrite_fixed.py` to:

- Keep Decodo enabled when Decodo credentials exist even if `SERP_API_PROVIDER` still contains a deprecated provider value such as `serper` or `scrapedo`.
- Parse canonical Fragrantica designer URLs.
- Add Decodo-backed designer discovery: query the structured provider for a designer page, then query the structured provider for perfume URLs under that designer slug, e.g. `site:fragrantica.com/perfume/Thom-Browne`.
- Add a focused cold-search fallback that emits Fragrantica perfume rows from the top resolved designer page. This avoids direct Railway-to-Fragrantica designer crawling and preserves existing perfume URL ranking behavior.

## Verification

Focused tests passed locally:

```powershell
python test_decodo_scraper_api.py
python test_search_relevance.py
```

Production baseline verifier passed against the currently deployed service:

```powershell
python scripts\verify_production.py --url https://srt-scent-engine-production.up.railway.app --timeout 45 --json-output production_verification_results.json
```

Result: `6/6` checks passed.

The targeted `THOMBRONY` production call still depends on this commit being deployed. After deploy, the expected behavior is non-empty Thom Browne fragrance rows with `fg_url` populated from Fragrantica perfume URLs.
