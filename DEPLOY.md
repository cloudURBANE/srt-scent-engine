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
5. Under **Settings → Networking**, click **Generate Domain** to get the public
   URL. That URL is the API base URL.
6. On the **frontend** service, set `VITE_FRAGRANCE_API_URL` to that URL.
7. Verify: `GET https://<your-domain>/health` → `{"ok": true}`.

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

## Local run

```bash
pip install -r requirements.txt
uvicorn api:app --reload --port 8000
# http://localhost:8000/health
```
