# Deploying to Railway

## Files added (engine untouched)

| file                 | purpose                                            |
|----------------------|----------------------------------------------------|
| `api.py`             | FastAPI wrapper — imports the engine, exposes HTTP  |
| `db.py`              | Postgres layer for the enrichment queue + detail cache |
| `requirements.txt`   | Python deps (engine deps + fastapi/uvicorn + psycopg) |
| `Procfile`           | start command                                      |
| `railway.toml`       | Railway build/deploy config + `/health` healthcheck |
| `API_CONTRACT.md`    | endpoint contract for the frontend                 |
| `test_enrichment.py` | self-test for the enrichment pipeline              |

## Steps

1. **Push** this directory to a Git repo (GitHub/GitLab).
2. In Railway: **New Project → Deploy from repo**, pick this repo.
3. Railway auto-detects Python (Nixpacks), installs `requirements.txt`, and
   runs the start command from `railway.toml` / `Procfile`.
4. **Set the env var** `FRONTEND_ORIGINS` to your deployed frontend origin(s),
   comma-separated — e.g. `https://your-frontend.up.railway.app`. Without it,
   CORS only allows `http://localhost:5173` (Vite dev).
5. On the **API** service, set `API_INITIAL_TIMEOUT=5.5`. This is the
   recommended first-pass budget for live Basenotes search; strong cached exact
   hits still return before this budget applies.
6. Under **Settings → Networking**, click **Generate Domain** to get the public
   URL. That URL is the API base URL.
7. On the **frontend** service, set `VITE_FRAGRANCE_API_URL` to that URL.
8. Verify: `GET https://<your-domain>/health` → `{"ok": true}`.

The Python API and the Vite frontend are **separate Railway services** in the
same project.

## Enrichment pipeline (durable storage)

The enrichment job queue and the durable Fragrantica detail cache live in
Postgres. To enable them:

1. In the Railway project: **New → Database → Add PostgreSQL**.
2. On the **API** service, reference the database's `DATABASE_URL` (Railway
   exposes it as a shared variable — add it to the API service's variables).
3. Set `ENRICHMENT_WORKER_TOKEN` on the **API** service to a long random secret.
   This authorizes the offline worker against `/api/enrichment/jobs/*`. **Never**
   put this token on the frontend service.
4. Redeploy. On startup `db.init_db()` creates the `enrichment_jobs` and
   `fg_detail_cache` tables idempotently (`CREATE TABLE IF NOT EXISTS`) — no
   manual migration step.

If `DATABASE_URL` is unset the API still runs: enrichment enqueue is skipped,
`/api/enrichment/status` reports `enabled: false`, and the worker endpoints
return `503`. The existing bundled JSON detail cache keeps working unchanged.

## Notes

- `$PORT` is provided by Railway; the start command binds to it.
- Each request creates its own default scraper. Cloudflare-clearance sessions
  for Basenotes/Fragrantica may be reused, but calls through those shared
  sessions are serialized per host to avoid concurrent session mutation.
- Search/detail are blocking; FastAPI runs the sync endpoints in its threadpool.
  The API caps live upstream work per process with
  `API_LIVE_SEARCH_MAX_CONCURRENT` (default `6`) and
  `API_DETAIL_FETCH_MAX_CONCURRENT` (default `8`). Cache hits bypass those caps;
  saturated uncached requests return `503` with `Retry-After: 2` instead of
  piling up resolver threads. For heavy traffic, scale via Railway replicas or
  add `--workers N` to the start command.
- **Capacity ceiling (readiness gap E9):** the deploy is ONE uvicorn worker —
  a single GIL-bound process with anyio's ~40-thread pool for sync routes. Fine
  at MVP traffic. **Scale-out trigger:** sustained CPU-bound search latency
  (p95 climbing while the 503 saturation guards are NOT firing). **Plan:**
  prefer `--workers N` on one service first (shared filesystem caches stay
  coherent); Railway replicas only after re-examining the in-process state —
  per-IP rate-limit windows (rate_limit.py), the live-work semaphores, and the
  clearance-session reuse are all per-process and would multiply per replica.
- Per-IP rate limits protect the cost-bearing endpoints (search / image-search
  / details / requeue) with `RATE_LIMIT_*_PER_MIN` env knobs — see
  rate_limit.py. `0` disables a rule. Fail-open by design.
- Error tracking: set `SENTRY_DSN` on the Railway service to enable Sentry
  (inert when unset). `SENTRY_TRACES_SAMPLE_RATE` optionally enables tracing.
- Verified DB TLS: set `DATABASE_SSL_CA` (PEM content or file path) to upgrade
  the Postgres connection to `sslmode=verify-full`; unset logs a boot warning
  and connects unverified (encrypted, but MITM-able). Railway's official
  `postgres-ssl` image issues its server certificate only for `localhost`, so a
  service connecting through Railway's proxy must also set
  `DATABASE_SSL_MODE=verify-ca`. That still verifies the private CA chain but,
  necessarily, cannot verify the proxy hostname. Keep the default
  `verify-full` for providers whose certificate matches the connection host.

## Search provider env

Recommended API service variable:

```bash
API_INITIAL_TIMEOUT=5.5
```

Optional structured URL discovery variables for the Decodo-backed path:

```bash
SERP_API_PROVIDER=decodo  # optional; leaving it unset also defaults to Decodo
DECODO_API_BASIC_TOKEN=<base64 username:password>
```

`DECODO_AUTH_TOKEN`, `DECODO_SCRAPER_BASIC_TOKEN`, and `DECODO_BASIC_TOKEN` are
accepted aliases for the token variable, and all credential names are matched
case-insensitively (e.g. `Decodo_auth_token` works).

Alternatively, provide username/password credentials and let the engine build the
Basic token:

```bash
SERP_API_PROVIDER=decodo
DECODO_API_USERNAME=<decodo-user>
DECODO_API_PASSWORD=<decodo-password>
```

`SERP_API_PROVIDER` defaults to Decodo when unset. If no Decodo credentials are
configured, structured search is disabled and the engine falls back to its
non-provider search paths.

The former Serper pool diagnostics/admin endpoints now return deprecation
notices and no longer manage credentials at runtime.

When Decodo is configured, the query spell-repair pass also harvests its
structured SERP results, so typo'd queries ("creed avantus") are corrected even
on datacenter hosts where the Google/Bing HTML scrape is dead. Repair only runs
after a clearly bad first pass; its budget is `API_SPELL_REPAIR_BUDGET`
(default `4.0` seconds). Below ~1.2s of remaining budget the engine skips the
structured call rather than spend a request that cannot finish.

The first-pass Fragrantica leg runs Decodo URL discovery under
`API_FG_TIMEOUT` (default `4.5` seconds, clamped to `API_INITIAL_TIMEOUT`).
Decodo URL discovery measures ~2-4s per call, so do not set this below ~4s: a
tighter cap makes the structured leg read-time-out on slow calls, returning
zero first-pass links while still spending the provider credit. The same
~1.2s minimum-viable-budget gate applies — a starved leg is skipped, not
started. Provider failures are logged as `[SYS] Decodo ... failed (<error>)`
so a read timeout, an auth failure, and an empty SERP are distinguishable in
deploy logs.

### Bing redundancy + universal egress recovery

Two reliability additions reuse the same Decodo credentials (no new secrets):

- **Bing fallback.** When Google SERP discovery returns no usable
  `fragrantica.com/perfume` URL for a query, the engine retries the same scoped
  query against Decodo's `bing_search` target. Bing is an independent index that
  often surfaces the page when Google is rate-limited or omits it. The second
  call only happens on the (cold) queries that would otherwise be empty, and is
  gated by the same minimum-viable-budget rule.

- **Universal egress recovery (`/details`).** The deployed datacenter IP is
  Cloudflare-blocked from Fragrantica, so a direct page fetch returns a
  challenge/empty body and cold details collapse to Basenotes-only. When the
  direct fetch fails or hits a challenge, the engine re-fetches the *same URL's*
  HTML through Decodo's `universal` target (premium-proxy egress with JS
  rendering) and feeds it to the normal parser. Recovered details set
  `diagnostics.fg.fetched_via = "decodo_universal"` and clear the
  `fetch_errors.fg` flag. A recovered body is still run through challenge
  detection, so a challenge shell is never accepted as a real page.

  Set `DECODO_DISABLE_UNIVERSAL_FALLBACK=1` to turn the recovery off (cost cap /
  debugging the direct path). It is enabled by default.

The Decodo client also now sends a bounded `(connect, read)` request timeout and
reserves a small safety margin against the overall search deadline, so an
in-flight provider call started near the `API_SEARCH_TOTAL_BUDGET` ceiling can no
longer overshoot it (the cause of cold queries running ~20s against an 18s
budget).

## Basenotes / Chromium requirement

The Basenotes scraper passes Cloudflare clearance by driving a real Chromium
process via `DrissionPage.ChromiumPage`. The default Nixpacks Python image does
**not** ship a Chromium binary, so on Railway clearance silently fails and the
search engine falls back to zero Basenotes rows.

`nixpacks.toml` installs Chromium via **Nix** (not apt) and sets the env vars the
engine looks for. Ubuntu's apt `chromium`/`chromium-browser` packages are
snap-transition shims that can't run in Railway's container, so we pin a real
Nix-managed binary on `PATH`:

```toml
[phases.setup]
nixpkgsArchive = "336eda0d07dc5e2be1f923990ad9fdb6bc8e28e3"
nixPkgs = ["python311", "gcc", "chromium", "xvfb-run"]

[variables]
BASENOTES_CHROMIUM_HEADLESS = "1"
# The web service should NOT spawn Chromium while serving traffic (failed mints
# leak helper processes and pressure memory). Offline workers/local runs opt back
# in by overriding this.
DISABLE_CHROMIUM_MINT = "1"
```

`BASENOTES_CHROMIUM_PATH` is intentionally left unset — the mint helpers fall
back to `shutil.which("chromium")`, which resolves to the Nix binary. Set it
explicitly only if you need to point at a non-default Chromium.

### Post-deploy validation

After Railway redeploys, run diagnostics with the worker bearer token:

```bash
curl -H "Authorization: Bearer $ENRICHMENT_WORKER_TOKEN" \
  "https://srt-scent-engine-production.up.railway.app/api/diagnostics/basenotes?q=xerjoff"
curl -H "Authorization: Bearer $ENRICHMENT_WORKER_TOKEN" \
  "https://srt-scent-engine-production.up.railway.app/api/diagnostics/basenotes?q=xerjoff&mint=1"
curl "https://srt-scent-engine-production.up.railway.app/api/fragrances/search?q=xerjoff"
```

Diagnostics are private by default because they can expose runtime/process/cache
state and trigger upstream probes. For a short local debugging window only, set
`PUBLIC_DIAGNOSTICS=1` to bypass the bearer-token check.

Expected:

- `chromium_binary.which` is non-null.
- `imports.DrissionPage.available` is `true`.
- `mint_attempt.success` is `true` (or reports a concrete non-missing-binary
  failure).
- `engine_search_for_q.row_count` is non-zero for `xerjoff`.
- `/api/fragrances/search?q=xerjoff` returns a populated `results` array from
  the Python engine (not just the Express fallback catalog row).

## Local run

```bash
pip install -r requirements.txt
uvicorn api:app --reload --port 8000
# http://localhost:8000/health
```

## Public launch checklist

Steps to make the engine public-ready alongside the web app. The web app's
operator checklist lives in the sCAST repo (`docs/USER_LAUNCH_SETUP.md`).

### Required env (API service)

| Variable | Value / notes |
| --- | --- |
| `FRONTEND_ORIGINS` | The **production SPA origin** (comma-separated), e.g. `https://app.example.com`. Without it CORS only allows `http://localhost:5173`. |
| Decodo credentials | The SERP/Decodo proxy creds the engine scrapes through. **Verify them live** after deploy — a stale/expired cred degrades cold search to Basenotes-only / empty results with no hard error. |
| `DATABASE_URL` | Postgres for the enrichment queue + durable detail cache. |
| `ENRICHMENT_WORKER_TOKEN` | Long random secret authorizing the offline worker against `/api/enrichment/jobs/*`. **Never** put it on the frontend service. |
| `DB_POOL_MAX_SIZE` | Pin to **10–15**. Bounds connections against the shared/managed Postgres so a burst can't exhaust its limit. |

### No rate limiting by design

The public search/detail endpoints stay **keyless** so browser calls work for
logged-out visitors; request quota is enforced in the web layer, not here. Do
not add engine-side rate limiting.

### Pending ops (run after deploy)

- **E-1** — run the wardrobe-completeness heal sweep.
- **E-5** — bring up the local/offline enrichment worker.
- **E-8** — live-validate the search-budget fix on a **cold** (never-cached)
  search. Cached tests don't count — use the `engine-live-verify` skill (the
  cold-Decodo-discovery canary) and confirm a populated `results` array with a
  real family, not an Express-fallback catalog row.
