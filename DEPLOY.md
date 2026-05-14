# Deploying to Railway

## Files added (engine untouched)

| file               | purpose                                            |
|--------------------|----------------------------------------------------|
| `api.py`           | FastAPI wrapper — imports the engine, exposes HTTP  |
| `requirements.txt` | Python deps (engine deps + fastapi/uvicorn)         |
| `Procfile`         | start command                                      |
| `railway.toml`     | Railway build/deploy config + `/health` healthcheck |
| `API_CONTRACT.md`  | endpoint contract for the frontend                 |

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
