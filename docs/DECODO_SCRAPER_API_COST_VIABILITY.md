# Decodo Scraper API Cost and Viability

Date: June 12, 2026

This note evaluates Decodo Web Scraping API as a Serper replacement for the
fragrance search stack. It separates confirmed local behavior from the live
credential smoke and the remaining gates before migration.

## Live Test Status

Initial live testing was blocked in this shell because none of these
environment variables were set:

- `DECODO_API_BASIC_TOKEN`
- `DECODO_API_USERNAME`
- `DECODO_API_PASSWORD`

The attempted command failed before any network request was made:

```powershell
python scripts\diag_decodo_scraper_api.py --live --json-output decodo_scraper_smoke.json
```

Result:

```text
RuntimeError: Set DECODO_API_BASIC_TOKEN or DECODO_API_USERNAME/DECODO_API_PASSWORD for live checks.
```

No live Decodo credits were consumed by that failed preflight.

After credentials were supplied, a narrow live smoke passed on June 12, 2026:

```powershell
python scripts\diag_decodo_scraper_api.py --live --case fragrantica_naxos --image-query "xerjoff naxos fragrance bottle" --timeout 20
```

Result:

```text
PASS fragrantica_naxos: accepted=9, first=https://www.fragrantica.com/perfume/Xerjoff/XJ-1861-Naxos-30529.html
PASS images: candidates=12
```

The full URL-only matrix also passed after the request-schema fix:

```powershell
python scripts\diag_decodo_scraper_api.py --live --all-cases --skip-images --timeout 20
```

Result:

```text
PASS fragrantica_naxos: accepted=9
PASS fragrantica_aventus: accepted=9
PASS fragrantica_tempio: accepted=9
PASS parfumo_khamrah: accepted=9
```

Follow-up decision-gate testing on June 12, 2026 with live Decodo credentials
found that the default 8 second diagnostic budget is not clean enough to treat
as a production-ready gate yet:

```powershell
python scripts\diag_decodo_scraper_api.py --live --skip-images --json-output decodo_scraper_url_smoke.json
```

Result:

```text
PASS fragrantica_naxos: latency=4037ms accepted=9
```

The default cheap smoke then timed out on Google Images at the 8 second request
budget:

```powershell
python scripts\diag_decodo_scraper_api.py --live --json-output decodo_scraper_smoke.json
```

Result:

```text
PASS fragrantica_naxos: latency=1881ms accepted=9
FAIL images: latency=8344ms candidates=0 error=ReadTimeout
```

A repeat default cheap smoke showed the same budget risk in the opposite
direction: image search passed, but Fragrantica URL discovery timed out:

```text
FAIL fragrantica_naxos: latency=8317ms accepted=0 error=ReadTimeout
PASS images: latency=4771ms candidates=12
```

With a 20 second diagnostic timeout, the same paths passed:

```powershell
python scripts\diag_decodo_scraper_api.py --live --timeout 20 --json-output decodo_scraper_smoke_timeout20.json
python scripts\diag_decodo_scraper_api.py --live --all-cases --timeout 20 --json-output decodo_scraper_full_timeout20.json
```

Full 20 second matrix result:

```text
PASS fragrantica_naxos: latency=2263ms accepted=9
PASS fragrantica_aventus: latency=2942ms accepted=9
PASS fragrantica_tempio: latency=2531ms accepted=9
PASS parfumo_khamrah: latency=3290ms accepted=9
PASS images: latency=5053ms candidates=12
```

The exact expected Parfumo Khamrah canonical URL appeared in the accepted list,
but it was second behind `https://www.parfumo.com/Perfumes/Lattafa/khamrah-waha`.
The image response returned usable `imageUrl`, `source`, and source `link`;
Decodo omitted width and height in this live response.

Implementation note from the live smoke: `proxy_pool` must not be sent with
`target: "google_search"`. Decodo returned HTTP 400 for that payload because
`proxy_pool` customization is only supported by the Universal Web Scraping
target. The Google Search target template uses Premium proxies by default.

## Local Verification Completed

These checks passed after the research pass:

```powershell
python test_decodo_scraper_api.py
python scripts\diag_decodo_scraper_api.py
python -m py_compile fragrance_parser_full_rewrite_fixed.py test_decodo_scraper_api.py scripts\diag_decodo_scraper_api.py
python test_search_relevance.py
python scripts\test_identity_fix.py
git diff --check
```

They verify local request shaping, credential sourcing from environment only,
provider routing, parsed-response normalization, image candidate mapping,
Serper fallback behavior, and the Parfumo brand-crawl gating affected by
structured provider selection.

## Current Public Pricing

Primary source:

- https://decodo.com/scraping/web/pricing

Decodo lists Web Scraping API prices by request type. For the current
implementation, the relevant rows are Premium Proxies and Premium proxies + JS.
The current code does not set `headless: "html"`, so it is intended to use the
Premium Proxies non-JS rate unless live results prove JavaScript rendering is
required. For `google_search` target templates, Premium proxy use is implicit;
the request must not include `proxy_pool`.

| Monthly plan | Standard | Standard + JS | Premium | Premium + JS | Rate limit |
| ---: | ---: | ---: | ---: | ---: | ---: |
| $19 | $0.50 / 1K, 38K req | $0.75 / 1K, 25K req | $1.00 / 1K, 19K req | $1.50 / 1K, 12K req | 10 req/s |
| $49 | $0.30 / 1K, 163K req | $0.65 / 1K, 75K req | $0.90 / 1K, 54K req | $1.25 / 1K, 39K req | 25 req/s |
| $99 | $0.14 / 1K, 707K req | $0.60 / 1K, 165K req | $0.85 / 1K, 116K req | $1.20 / 1K, 82K req | 50 req/s |

Important accounting detail: these are marginal rates if the plan credits are
used. If monthly usage is far below the included request budget, effective cost
per 1K is the monthly plan price divided by actual usage.

## Current Serper Pricing Baseline

Primary source:

- https://serper.dev/

Serper lists top-up credits, not monthly subscriptions:

| Top-up | Credits | Listed rate |
| ---: | ---: | ---: |
| $50 | 50K | $1.00 / 1K |
| $375 | 500K | $0.75 / 1K |
| $1,250 | 2.5M | $0.50 / 1K |
| $3,750 | 12.5M | $0.30 / 1K |

At low volume, Decodo's $19 plan lowers the cash entry point but not the
marginal premium request cost. At scale, Serper can be cheaper per Google query
unless Decodo succeeds in places where Serper fails or reduces other
operational cost.

## Product Fit

Use Decodo Web Scraping API, not raw residential/static ISP proxies, as the
primary candidate for Google SERP replacement.

Reasons:

- Decodo docs say Web Scraping API handles proxy rotation, browser rendering,
  anti-bot protection, retries, and parsed outputs.
- The `google_search` target is parseable.
- Decodo docs and SDK pages expose `google_tbm`; their Google Images guide says
  Google Images is selected through the Google Search target with TBM `isch`.
- The repo's previous raw Decodo proxy test reached search-engine gate pages,
  so raw proxy cost per successful result is currently unknown.

Static ISP proxies may still be useful for long-session, account-like, or
site-specific workflows. They are not the first-choice replacement for parsed
Google SERP data in this app.

## Request Model

Confirmed in `search_engine`:

- Main first-pass Fragrantica discovery: 1 Decodo `google_search` request per
  search query when `SERP_API_PROVIDER=decodo`.
- Parfumo fallback discovery: 1 Decodo `google_search` request when a fallback
  resolver asks Parfumo for candidates.
- Python Decodo image candidate search exists, but `search_image_candidates()`
  is not called anywhere in `search_engine` yet.

Also observed outside this repo in the web API server:

- The image pipeline currently calls Serper Images directly.
- The scent-facts path currently makes up to 11 Serper search requests per
  fragrance source search.

That means replacing only `search_engine` is a partial migration. Replacing the
whole web app's Serper usage also requires a Node/API-server Decodo integration.

## Per-User Cost Examples

Using the $19 Decodo plan's marginal rates:

| Scenario | Requests per user action | Premium non-JS | Premium + JS |
| --- | ---: | ---: | ---: |
| Search only | 1 | $0.0010 | $0.0015 |
| Search plus image cache miss | 2 | $0.0020 | $0.0030 |
| Search plus Parfumo fallback | 2 | $0.0020 | $0.0030 |
| Naive full migration with 11 fact-search calls | 13 to 14 | $0.0130 to $0.0140 | $0.0195 to $0.0210 |

Monthly examples at 2 requests per user action:

| User activity | Monthly requests | Premium non-JS | Premium + JS |
| --- | ---: | ---: | ---: |
| 50 actions/month | 100 | $0.10 | $0.15 |
| 300 actions/month | 600 | $0.60 | $0.90 |
| 1,000 actions/month | 2,000 | $2.00 | $3.00 |

The 11-query scent-facts pattern is the main cost trap. If that path is migrated
to Decodo, collapse those targeted queries or cache aggressively before enabling
it at scale.

## Migration Recommendation

Do not fully migrate production traffic yet. The implementation is locally sound
and the credential-backed Decodo smoke/full URL matrix passed, but staging
behavior still needs verification.

Recommended decision gate:

1. Export Decodo credentials only in the shell or deployment environment.
2. Run URL discovery first:

   ```powershell
   python scripts\diag_decodo_scraper_api.py --live --skip-images --json-output decodo_scraper_url_smoke.json
   ```

3. If URL discovery passes, run the default cheap smoke:

   ```powershell
   python scripts\diag_decodo_scraper_api.py --live --json-output decodo_scraper_smoke.json
   ```

4. If both pass, run the full matrix:

   ```powershell
   python scripts\diag_decodo_scraper_api.py --live --all-cases --json-output decodo_scraper_full.json
   ```

5. Enable `SERP_API_PROVIDER=decodo` in a staging deployment before production.

Use these pass criteria:

- Fragrantica URL discovery returns accepted canonical perfume URLs.
- The expected pages appear in the accepted URL list.
- Parfumo fallback URL discovery returns plausible canonical URLs.
- Image search returns usable `imageUrl`, source, and source link. Dimensions
  should be preserved when present, but the live Decodo Google Images response
  can omit width/height.
- Latency stays inside production search budgets.
- The no-JS request mode works. If not, add `headless: "html"` and budget at
  Premium + JS rates.

Current confidence: high enough for staging URL-discovery evaluation, but not
for immediate full migration. The live 20 second matrix passed, but the default
8 second budget produced read timeouts on both URL discovery and image search
across repeated cheap smokes. The Node image and scent-facts Serper paths are
not yet migrated, and production behavior still needs staging observation with
real user-shaped traffic.
