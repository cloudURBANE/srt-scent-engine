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
- Each request creates its own scraper session (`engine.get_scraper()`), so
  there is no shared mutable state between requests.
- Search/detail are blocking; FastAPI runs the sync endpoints in its threadpool.
  If you expect heavy concurrency, scale via Railway replicas or add
  `--workers N` to the start command.

## Search provider env

Recommended API service variable:

```bash
API_INITIAL_TIMEOUT=5.5
```

Optional Fragrantica URL discovery variables for the Serper-backed path:

```bash
SERP_API_PROVIDER=serper
# Single key, or a comma/space/newline-separated POOL of free-account keys.
SERPER_API_KEYS=<key1>,<key2>,<key3>
# Legacy singular var still works and is merged into the pool.
SERPER_API_KEY=<secret>
```

Leaving `SERP_API_PROVIDER` unset (or providing no keys) preserves the current
non-Serper behavior.

**Key pool / auto-rotation.** When multiple keys are supplied the engine drains
one key until it returns 401/402/403 (out of credits → retired for the process)
or 429 (rate-limited → short cooldown, retried later), then rotates to the next
key automatically — no redeploy. Inspect live health at
`GET /api/diagnostics/serper-pool` (masked keys). Refill without a redeploy via
`POST /api/admin/serper-pool/keys` (worker bearer token) with body
`{"keys": "k1,k2"}`. State is in-memory only, so a restart re-tests every key.

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

After Railway redeploys, run:

```bash
curl "https://srt-scent-engine-production.up.railway.app/api/diagnostics/basenotes?q=xerjoff"
curl "https://srt-scent-engine-production.up.railway.app/api/diagnostics/basenotes?q=xerjoff&mint=1"
curl "https://srt-scent-engine-production.up.railway.app/api/fragrances/search?q=xerjoff"
```

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
