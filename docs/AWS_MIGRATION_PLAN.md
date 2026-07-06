# Railway → AWS Migration Plan (engine)

Date: 2026-07-06. Companion to `sCAST/docs/AWS_MIGRATION_PLAN.md` (the web app's
Vercel-removal plan). The web app can move first — the two migrations are independent
as long as env vars are re-pointed at cutover.

---

## 1. Current state (audited)

- **FastAPI monolith** (`api.py`, `app` at `api.py:355`) served by a single uvicorn
  process on Railway via Nixpacks: `railway.toml` (healthcheck `/health`, on-failure
  restart), `Procfile`, `nixpacks.toml` (Python 3.11 + **gcc + chromium + xvfb-run**,
  `BASENOTES_CHROMIUM_HEADLESS=1`, `DISABLE_CHROMIUM_MINT=1`). No Dockerfile exists.
- **Postgres ("viaduct" DB)** via `DATABASE_URL` (`db.py:42`) — enrichment job queue,
  `fragrance_records`, `fg_detail_cache`, worker accounts, mobile sessions. Tables
  auto-create via `db.init_db()` (`db.py:88-232`); psycopg pool max 15. This is a
  Railway Postgres today and **must be migrated with its data** (the FG detail cache
  and job history are valuable).
- **On-disk JSON caches** in `fg_cache/` (identity/detail/parfinity), shipped in the
  repo and selectable via `FG_CACHE_PATH`/`FG_DETAIL_CACHE_PATH`/`PARFINITY_CACHE_PATH`.
- **In-process state**: Cloudflare clearance cookie sessions, bounded semaphores
  (live search ≤6, detail fetch ≤8 → 503+Retry-After when saturated), striped locks,
  boot-time Basenotes warmup thread (`api.py:333-352`). Replicas do NOT share any of
  this — the service is designed to run as **one instance**.
- **Offline enrichment worker** (`scripts/enrichment_worker.py`) runs on the owner's
  Windows machine, polling the cloud API (55s page timeouts, 20–40s Decodo renders).
  It targets `DEFAULT_API_BASE_URL = https://srt-scent-engine-production.up.railway.app`
  (`enrichment_worker.py:69`, `run_worker.ps1:70`) unless `SCENT_API_BASE_URL` is set.
- **CORS fails closed**: `FRONTEND_ORIGINS` env (comma-separated); default is
  localhost only (`api.py:385-398`). Prod must set it to the SPA origin.
- **Timeouts**: cold search budget 18s (`API_SEARCH_TOTAL_BUDGET`, `api.py:181`) —
  fine behind App Runner (120s) or an ALB, **not** behind a 29s API Gateway.
- No CI/CD; Railway deploys on git push.

**Compute conclusion:** long-running single container with a real Chromium, persistent
DB pool, background threads, and >18s requests → **container service (App Runner or
ECS Fargate), never Lambda.**

## 2. Target architecture

```
Route 53 (engine.scentbeam.com or similar)
   └── App Runner service "srt-scent-engine"  (max 1 instance)
         └── Docker image: python3.11-slim + chromium + xvfb + uvicorn api:app
RDS Postgres (db.t4g.micro, 20GB)  ← pg_dump/restore from Railway Postgres
Offline Windows worker  ── unchanged, SCENT_API_BASE_URL → new URL
```

- **App Runner, min=max=1 instance**, 1 vCPU / 2 GB (bump to 4 GB if
  `/api/diagnostics/memory` shows Chromium-mint pressure; keep
  `DISABLE_CHROMIUM_MINT=1` on the web tier exactly as `nixpacks.toml` does today).
  Health check `GET /health`. Fixed instance count preserves the in-memory clearance
  sessions and concurrency-cap semantics.
- **RDS Postgres** replaces Railway Postgres. Alternative: a second Supabase project
  (cheaper at small scale) — either works; RDS keeps everything in one AWS account.
  Migration: `pg_dump` Railway → `pg_restore` RDS, then set `DATABASE_URL`.
  `init_db()` is idempotent, so a restore-then-boot is safe.
- **`fg_cache/` stays baked into the image** (App Runner has no EFS). The durable
  detail cache is the DB (`fg_detail_cache`), so image-local JSON going stale between
  deploys is acceptable — same situation as Railway today. If disk-cache writes ever
  matter, that's the trigger to move to ECS Fargate + EFS.
- ECS Fargate + ALB (idle timeout ≥60s) is the drop-in fallback if App Runner limits
  bite; the same Dockerfile serves both.

## 3. Dockerfile (to be added — replaces `nixpacks.toml`)

```dockerfile
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
      chromium xvfb gcc ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt   # NOT requirements-dev.txt
COPY . .
ENV BASENOTES_CHROMIUM_HEADLESS=1 \
    DISABLE_CHROMIUM_MINT=1 \
    BASENOTES_CHROMIUM_PATH=/usr/bin/chromium \
    FRAGRANTICA_CHROMIUM_PATH=/usr/bin/chromium \
    PORT=8000
EXPOSE 8000
CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT}"]
```

(Verify the Chromium binary path DrissionPage resolves; Debian's package installs
`/usr/bin/chromium`. Test the clearance-mint diagnostics before cutover.)

## 4. Environment variables to carry over

- **Core**: `DATABASE_URL` (→ RDS), `PORT`, `DB_POOL_MAX_SIZE`, `DB_POOL_ACQUIRE_TIMEOUT`.
- **CORS/self**: `FRONTEND_ORIGINS=https://scentbeam.com` (fails closed if missed!),
  `PUBLIC_BASE_URL=<new engine URL>` (mobile magic-links).
- **Auth**: `ENRICHMENT_WORKER_TOKEN` (worker + diagnostics bearer token).
- **Decodo**: `DECODO_API_BASIC_TOKEN` (or username/password pair), locale/fanout/
  fallback flags, and the residential-proxy set (`DECODO_PROXY_*`) if used.
- **Email**: `RESEND_API_KEY`, `RESEND_FROM`, `ADMIN_ALERT_EMAIL`.
- **Tuning**: any `API_*` budget overrides and `ENRICHMENT_*` sweep settings currently
  set on Railway (audit the Railway dashboard before decommissioning — several
  defaults live only there).
- Drop the `RAILWAY_*` vars (diagnostics-only); optionally set `SOURCE_VERSION`/
  `GIT_COMMIT` in the image build for `/api/diagnostics/runtime`.

## 5. Migration steps

1. **Add the Dockerfile** (above) on a branch; build locally, run with a copy of prod
   env, smoke: `/health`, cold `GET /api/fragrances/search?q=...`, `POST
   /api/fragrances/details`, `/api/diagnostics/memory`, a Basenotes clearance mint
   (`POST /api/diagnostics/basenotes/clearance`).
2. **AWS**: ECR repo, push image; RDS Postgres (t4g.micro, private or
   publicly-accessible-with-strict-SG depending on App Runner networking choice);
   Secrets Manager for the §4 secrets.
3. **Data**: put the Railway service in a quiet window (pause the Windows worker),
   `pg_dump --no-owner` Railway DB → restore into RDS.
4. **Deploy** App Runner service (env from §4, `DATABASE_URL`→RDS), health check
   `/health`, max 1 instance.
5. **Verify cold path** per the `engine-live-verify` skill: cold Decodo discovery on a
   never-cached search — cached tests are void.
6. **Re-point consumers**:
   - Windows worker: set `SCENT_API_BASE_URL=<new URL>` (env or `run_worker.ps1`),
     restart, confirm job claim/complete round-trip.
   - Web app: `FRAGRANCE_ENGINE_URL` (Express) and/or `VITE_FRAGRANCE_API_URL` (SPA
     build) → new URL.
   - Custom domain (e.g. `engine.scentbeam.com`) via App Runner + Route 53 so future
     moves don't require touching consumers again.
7. **Decommission** Railway service + Postgres after a quiet week. Repo cleanup PR:
   delete `railway.toml`, `nixpacks.toml`, `Procfile` (or keep Procfile for local
   reference), update `DEPLOY.md`, update the hardcoded default URL in
   `scripts/enrichment_worker.py:69` and `run_worker.ps1:70`.
8. **CI/CD (optional but recommended)**: GitHub Actions on `main` — `run_tests.py`,
   build image, push to ECR, `aws apprunner start-deployment` (OIDC role).

## 6. Risks

| Risk | Severity | Mitigation |
|---|---|---|
| CORS lockout (default is localhost-only) | High | Set `FRONTEND_ORIGINS` before cutover; verify from the real SPA origin |
| Worker still pointed at Railway after cutover | High | Set `SCENT_API_BASE_URL`; watch `/api/enrichment/status` for job flow |
| Chromium clearance mint fails in the new image | Medium | Pre-cutover diagnostics test (step 1); keep Railway warm for rollback |
| DB restore misses in-flight jobs | Medium | Pause worker + quiet window during dump; jobs are idempotent by `fg_url` |
| Memory spikes (known issue, `MEMORY_LEAK_FIX_PLAN.md`) | Medium | 2–4 GB instance; `/api/diagnostics/memory` monitoring; `DISABLE_CHROMIUM_MINT=1` |
| Egress IP changes (Decodo account allowlists?) | Low | Check Decodo dashboard for IP allowlisting before cutover |

## 7. Rough monthly cost

App Runner 1 vCPU/2 GB always-on ~$25–50 + RDS db.t4g.micro ~$13 + ECR/secrets ~$2.
Roughly on par with the Railway service + Railway Postgres it replaces.
