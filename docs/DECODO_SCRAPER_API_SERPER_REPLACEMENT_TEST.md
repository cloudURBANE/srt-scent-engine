# Decodo Scraper API Serper Replacement Test

Date: June 12, 2026

This covers Decodo's synchronous scraper API path, not the raw residential
proxy SERP scraping path in `DECODO_SERPER_REPLACEMENT_TEST.md`.

## Runtime Contract

Set the provider and credentials with environment variables:

```powershell
$env:SERP_API_PROVIDER = "decodo"
$env:DECODO_API_BASIC_TOKEN = "<base64 username:password>"
```

Alternatively:

```powershell
$env:SERP_API_PROVIDER = "decodo"
$env:DECODO_API_USERNAME = "<decodo username>"
$env:DECODO_API_PASSWORD = "<decodo password>"
```

The token/password must not be committed. The code reads credentials only from
environment variables.

## Required Request Shape

Endpoint:

```text
POST https://scraper-api.decodo.com/v2/scrape
```

Headers:

```text
Content-Type: application/json
Accept: application/json
Authorization: Basic <base64 username:password>
```

Normal Google Search payload:

```json
{
  "target": "google_search",
  "query": "xerjoff naxos site:fragrantica.com/perfume",
  "parse": true,
  "locale": "en-us"
}
```

Google Image Search payload:

```json
{
  "target": "google_search",
  "query": "xerjoff naxos fragrance bottle",
  "google_tbm": "isch",
  "parse": true,
  "locale": "en-us"
}
```

Do not send `proxy_pool` with `target: "google_search"`. Decodo returns HTTP
400 because `proxy_pool` customization is only supported by the Universal Web
Scraping target. Google Search target templates use Premium proxies by default.

Do not use Decodo's `google_images` target for the image enrichment path; this
implementation uses `google_search` plus `google_tbm: "isch"` so parsed image
metadata can be mapped.

## Response Mapping

`DecodoScraperClient` accepts parsed data under:

- `results[].content.results`
- nested `results.results` objects used by live `google_search` responses
- `content.results`
- direct `organic`, `organic_results`, `images`, `image_results`, or `items`
  arrays for fixture/backward compatibility

Search results are canonicalized through the existing Fragrantica and Parfumo
URL filters.

Image results are normalized to a Serper-compatible candidate shape:

```json
{
  "title": "Xerjoff Naxos perfume bottle",
  "imageUrl": "https://...",
  "imageWidth": 1200,
  "imageHeight": 900,
  "thumbnailUrl": "https://...",
  "source": "retailer.example",
  "link": "https://retailer.example/product",
  "position": 1,
  "source_provider": "decodo"
}
```

This keeps the existing image-ranking layer's expected URL and dimension fields
available while changing only the upstream provider.

## Non-Live Checks

These do not use Decodo credits:

```powershell
python test_decodo_scraper_api.py
python scripts/diag_decodo_scraper_api.py
```

## Cheap Live Smoke

Run one Fragrantica URL-discovery case and one image-search case:

```powershell
python scripts/diag_decodo_scraper_api.py --live --json-output decodo_scraper_smoke.json
```

Latest local smoke result with credentials, run on June 12, 2026:

```text
PASS fragrantica_naxos: accepted=9
PASS images: candidates=12
```

To avoid image-search spend during URL-discovery debugging:

```powershell
python scripts/diag_decodo_scraper_api.py --live --skip-images
```

## Full Matrix

Only run this after the cheap smoke passes:

```powershell
python scripts/diag_decodo_scraper_api.py --live --all-cases --json-output decodo_scraper_full.json
```

The matrix covers Fragrantica Naxos, Aventus, Tempio d'Acqua, and Parfumo
Khamrah URL discovery.

Latest URL-only matrix result with credentials, run on June 12, 2026:

```text
PASS fragrantica_naxos: accepted=9
PASS fragrantica_aventus: accepted=9
PASS fragrantica_tempio: accepted=9
PASS parfumo_khamrah: accepted=9
```

## Pass Criteria

Treat Decodo's scraper API as a viable Serper replacement only if:

- Each URL case returns at least one accepted canonical target URL.
- Expected fragrance/page substrings appear in the accepted URL list.
- Image search returns candidates with usable `imageUrl` and source link.
  Width/height are preserved when Decodo provides them, but live Google Image
  parsed results can omit dimensions.
- Latency stays within the production search budget after at least one small
  live smoke.

No browser scenario tests are required for this suite.
