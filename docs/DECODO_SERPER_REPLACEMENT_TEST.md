# Decodo Residential SERP Replacement Test

Date: June 12, 2026

This is a traffic-capped test suite for deciding whether Decodo residential
proxies can replace the current Serper.dev URL discovery path. It avoids browser
scenario tests and does not store proxy credentials in the repository.

## Current Decodo Setup From Docs

Decodo residential proxies are used as authenticated HTTP(S) proxies. Decodo's
residential quick-start and code examples show the same shape:

```text
http://<username>:<password>@gate.decodo.com:<port>
```

For the trial endpoint list, use one generated sticky port at a time, such as
`gate.decodo.com:10001`. The script reads credentials only from environment
variables.

Useful Decodo docs:

- https://help.decodo.com/docs/residential-proxy-quick-start
- https://help.decodo.com/docs/residential-proxy-endpoints-and-ports
- https://help.decodo.com/docs/code-integration

## Local Non-Live Checks

These checks validate parser behavior, Decodo proxy URL construction, masking,
and block detection without consuming trial bandwidth:

```powershell
python test_decodo_serp_proxy.py
python scripts/diag_decodo_serp_proxy.py
```

## Cheap Live Smoke

Run a single Google SERP case through one sticky Decodo endpoint:

```powershell
$env:DECODO_PROXY_HOST = "gate.decodo.com"
$env:DECODO_PROXY_PORT = "10001"
$env:DECODO_PROXY_USERNAME = "<decodo username>"
$env:DECODO_PROXY_PASSWORD = "<decodo password>"
python scripts/diag_decodo_serp_proxy.py --live --check-ip --json-output decodo_serp_smoke.json
```

Default live mode runs only `fragrantica_naxos`. It exits non-zero if the page is
blocked, the request errors, or the expected fragrance result is not found.

## Full Matrix

Only run this after the cheap smoke passes:

```powershell
python scripts/diag_decodo_serp_proxy.py --live --all-cases --json-output decodo_serp_full_google.json
```

The default matrix covers:

- Fragrantica: Xerjoff Naxos
- Fragrantica: Creed Aventus
- Fragrantica: Casamorati 1888 Tempio d'Acqua
- Fragrantica: Dolce & Gabbana Q
- Parfumo: Lattafa Khamrah

To test Bing with the same parser:

```powershell
python scripts/diag_decodo_serp_proxy.py --live --search-engine bing --all-cases --json-output decodo_serp_full_bing.json
```

## Traffic Controls

The script has conservative defaults:

- No live network calls unless `--live` is passed.
- One case by default in live mode.
- `--max-response-bytes 900000` per SERP response.
- `--max-total-bytes 2500000` across a run.

For a 100 MB trial, keep the first validation to the cheap smoke. The full
Google matrix should still be small, but blocked or unusually large pages should
stop early rather than drain the trial.

## Observed Trial Smoke Results

The initial trial smoke was intentionally stopped before the full matrix:

| Port | Engine | Case | Result |
| ---: | --- | --- | --- |
| 10001 | Google | Fragrantica Naxos | Proxy IP check passed, but Google returned an enable-JavaScript search gate with zero result URLs. |
| 10002 | Google | Fragrantica Naxos | Same Google gate; classified as blocked/unusable. |
| 10001 | Bing | Fragrantica Naxos | Bing returned a Turnstile challenge page; no accepted target URLs. |
| 10002 | Bing | Fragrantica Naxos | Same Bing challenge shape; no accepted target URLs. |

Do not run `--all-cases` on this Decodo residential trial unless a single-case
smoke first returns an unblocked page with at least one accepted canonical target
URL. Based on the smoke, direct search-engine HTML scraping through these
residential endpoints is not yet a viable Serper.dev replacement.

## Pass Criteria

Treat Decodo as a viable Serper replacement candidate only if:

- The IP check succeeds through Decodo.
- Google or Bing returns HTML that is not detected as a block page.
- Each full-matrix case has at least one accepted canonical target URL.
- The expected fragrance/page appears in the accepted URL list.
- Latency is acceptable under the live search budget.

This is still a replacement feasibility test, not a production swap. If the
matrix passes, the next code change should introduce a provider abstraction so
the app can choose `serper` or `decodo_proxy` by environment while preserving
current canonicalization, dedupe, caching, and resolver scoring.
