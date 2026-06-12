# Scrape.do Serper Replacement Findings

Date: June 12, 2026

Scope: high-level feasibility test for replacing the current Serper-backed Google SERP URL discovery path with Scrape.do in this search engine. Browser scenario tests were intentionally skipped.

## Documents Reviewed

- Getting Started: https://scrape.do/documentation/
- Async API: https://scrape.do/documentation/async-api/
- Async plugin jobs: https://scrape.do/documentation/async-api/plugins/
- Google Scraper API: https://scrape.do/documentation/google-scraper-api/
- Google Search API: https://scrape.do/documentation/google-scraper-api/search/
- Google transient errors: https://scrape.do/documentation/google-scraper-api/transient-errors/
- API & Response: https://scrape.do/documentation/api-response/
- Request Costs: https://scrape.do/documentation/request-costs/

## Current Serper Usage

Primary production usage is in `fragrance_parser_full_rewrite_fixed.py`:

- `SerperKeyPool` reads `SERPER_API_KEYS` and legacy `SERPER_API_KEY`.
- `SerperClient.enabled()` currently gates on `SERP_API_PROVIDER=serper` and at least one configured Serper key.
- `SerperClient._post()` sends `POST https://google.serper.dev/search` with `X-API-KEY`.
- `SerperClient.search_fragrantica_urls()` expects response field `organic`.
- `SerperClient.search_parfumo_urls()` also uses the same Serper client for `site:parfumo.com/Perfumes`.
- `_search_core()` uses `SerperClient.discover_fragrances()` as the first-pass Fragrantica URL discovery provider when enabled.
- `ParfumoEngine.discover_urls()` uses `SerperClient.search_parfumo_urls()` before brand crawl fallback.
- FastAPI also exposes Serper-specific pool diagnostics/admin endpoints in `api.py`.
- `scripts/diag_parfumo_resolve.py` contains a standalone diagnostic Serper call.

This means Scrape.do can probably be introduced behind the same provider abstraction, but it is not a literal endpoint swap.

## Scrape.do API Shape

The tested synchronous endpoint was:

```text
GET https://api.scrape.do/plugin/google/search?token=<token>&q=<query>&gl=us&hl=en
```

Observed response shape:

- HTTP 200 for all tested queries.
- JSON content type.
- Top-level SERP field is `organic_results`, not Serper's `organic`.
- Organic entries include `title`, `link`, and `position`.
- Cost header `Scrape.do-Request-Cost` was present and reported `10` for each Google Search plugin request.

Relevant docs say the Google Search API uses token query authentication, returns structured JSON, costs 10 credits per `/plugin/google/search` request, and recommends retrying transient `502` or empty-result responses once.

## Live Test Results

The test token was used only via process environment for live calls. It was not written to the repo.

| Case | Query | Status | Latency | Accepted perfume URLs after existing canonicalization |
| --- | --- | ---: | ---: | ---: |
| Fragrantica Naxos | `xerjoff naxos site:fragrantica.com/perfume` | 200 | 1.77s | 9 |
| Fragrantica Aventus | `creed aventus site:fragrantica.com/perfume` | 200 | 1.07s | 9 |
| Fragrantica Tempio | `casamorati 1888 tempio d acqua site:fragrantica.com/perfume` | 200 | 1.49s | 9 |
| Fragrantica Q | `dolce gabbana q site:fragrantica.com/perfume` | 200 | 1.15s | 9 |
| Parfumo Khamrah | `site:parfumo.com/Perfumes "Lattafa" "Khamrah"` | 200 | 1.33s | 10 organic results returned |

First accepted Fragrantica URLs matched the expected fragrance page for all Fragrantica cases:

- Naxos: `https://www.fragrantica.com/perfume/Xerjoff/XJ-1861-Naxos-30529.html`
- Aventus: `https://www.fragrantica.com/perfume/Creed/Aventus-9828.html`
- Tempio d'Acqua: `https://www.fragrantica.com/perfume/Casamorati-1888/Tempio-d-Acqua-128356.html`
- Q by Dolce & Gabbana: `https://www.fragrantica.com/perfume/Dolce-Gabbana/Q-by-Dolce-Gabbana-78873.html`

The engine's current `FragranticaEngine.canonical_url()` already normalizes `beta.fragrantica.com` to `www.fragrantica.com`, so mixed beta/www Fragrantica results did not break URL acceptance in this high-level test.

## Timeout Check

The current Serper client has `MAX_TIMEOUT = 1.5`. With Scrape.do forced through a 1.5 second client timeout:

| Case | Result |
| --- | --- |
| Naxos | success, 10 organic results |
| Aventus | `ReadTimeout` |
| Tempio | success, 10 organic results |
| Q | success, 10 organic results |

This is the biggest compatibility risk found. Scrape.do works, but the current Serper latency budget is too tight for reliable drop-in replacement. A Scrape.do provider should probably use a larger timeout, retry-once handling for documented transient failures, and keep the search path's existing deadline behavior in mind.

## Compatibility Gaps

- Request method/auth changes from Serper `POST` + `X-API-KEY` header to Scrape.do `GET` + `token` query parameter.
- Response field changes from `organic` to `organic_results`.
- Existing key-pool logic is Serper-specific. Scrape.do likely needs separate env vars such as `SCRAPEDO_API_TOKEN` / `SCRAPEDO_API_TOKENS`, separate diagnostics labels, and separate quota/error handling.
- Current status handling retires Serper keys on 401/402/403 and some 400 credit errors, and cools down 429. Scrape.do docs list Google plugin `400`, `500`, and `502` behaviors differently; those should not be copied blindly.
- Scrape.do Google plugin cost is 10 credits/request, which is materially different from the Serper cost assumptions in the current comments.
- Existing source labels and tests are Serper-named (`serper_fragrantica_search`, `serper` discovery). Replacement should either rename these or preserve them temporarily behind a generic provider name to avoid breaking downstream diagnostics.
- Async Scrape.do exists and supports Google plugins, but the app's first-pass live search currently expects synchronous URL discovery. Async looks more relevant for batch warmups/enrichment than immediate `/api/fragrances/search`.

## Recommendation

Scrape.do passed the high-level feasibility test for the core Serper job: site-scoped Google URL discovery for Fragrantica and Parfumo. I would treat it as a viable replacement candidate, but not a safe drop-in endpoint swap.

Recommended implementation path:

1. Add a generic SERP provider layer or rename `SerperClient` to a neutral provider while preserving the existing public methods during transition.
2. Add a `ScrapeDoClient` path gated by `SERP_API_PROVIDER=scrapedo` and `SCRAPEDO_API_TOKEN`.
3. Parse both `organic_results` and `organic` during the transition to simplify tests and rollback.
4. Use Scrape.do-specific timeout and retry behavior; do not keep the hard 1.5s Serper cap without more latency testing.
5. Keep all existing URL canonicalization, dedupe, and identity scoring logic. The live results work with those pieces.
6. Update tests around provider enablement, response parsing, cache behavior, retry behavior, and diagnostics.
7. Update deployment docs and admin endpoints so they no longer expose Serper-specific naming once the migration is complete.

Confidence: medium-high that Scrape.do can replace Serper for URL discovery. I am not 95-100% certain it can replace Serper accurately across the whole application until the provider is implemented and run through the existing search relevance tests plus a small live smoke matrix under production-like timeouts.
