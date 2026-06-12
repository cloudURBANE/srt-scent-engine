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

## Basenotes / Chromium requirement

The Basenotes scraper passes Cloudflare clearance by driving a real Chromium
process via `DrissionPage.ChromiumPage`. The default Nixpacks Python image does
**not** ship a Chromium binary, so on Railway clearance silently fails and the
search engine falls back to zero Basenotes rows.

`search_engine/nixpacks.toml` installs the OS packages and sets the env vars the
engine looks for:

```toml
[phases.setup]
aptPkgs = ["chromium", "chromium-driver"]

[variables]
BASENOTES_CHROMIUM_HEADLESS = "1"
BASENOTES_CHROMIUM_PATH = "/bin/chromium-browser"
```

`_mint_basenotes_clearance()` reads `BASENOTES_CHROMIUM_PATH` and, when set,
passes it to `ChromiumOptions.set_browser_path()` so DrissionPage does not have
to auto-discover the binary.

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
