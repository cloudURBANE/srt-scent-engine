# AWS migration — engine repo notes

**The Python fragrance engine is NOT migrating.** It stays on **Railway**,
deployed via Railway's own Git integration exactly as documented in
[`../../DEPLOY.md`](../../DEPLOY.md). AWS S3 + CloudFront is only for the sCAST
**frontend** SPA.

## What did NOT change for this repo

- **Hosting**: still Railway (Nixpacks, `railway.toml` / `Procfile`, `/health`
  healthcheck). No AWS resources.
- **Deploy mechanism**: still Railway's auto-deploy on push. **No deploy
  workflow was added** — do not add one.
- **How the browser reaches it**: the SPA still calls the engine **directly** at
  `VITE_FRAGRANCE_API_URL`
  (`https://srt-scent-engine-production.up.railway.app`). This call is **not**
  proxied through CloudFront, so the frontend move does not touch it.
- **Env / secrets**: all engine vars (`DATABASE_URL`, `DECODO_*`, `API_*`,
  `FRONTEND_ORIGINS`, `ENRICHMENT_WORKER_TOKEN`, Chromium/Basenotes vars, etc.)
  stay on Railway, unchanged.

## What DID change for this repo

A **GitHub Actions CI workflow** was added at
[`../../.github/workflows/ci.yml`](../../.github/workflows/ci.yml). This repo
previously had **no CI**, so nothing tested the engine automatically. The new
workflow runs on **push to `main` and on pull requests**:

- Sets up Python 3.11+ and installs `requirements.txt` (+ `requirements-dev.txt`
  if present).
- Runs the test entrypoint — `python -m pytest` when pytest is available (there
  is `test_enrichment.py`), otherwise the self-test scripts.
- Runs a `python -m compileall` smoke as a floor so the job always does something
  meaningful.
- Is **non-flaky by design**: no live network scraping in CI — tests that hit
  Basenotes/Fragrantica are guarded/skipped so datacenter Cloudflare blocks and
  Decodo egress never make the build red.

CI is **test/lint only**. It does **not** deploy — Railway continues to own
engine deploys.

## Source of truth for the frontend move

The frontend migration (target architecture, env mapping, cutover, rollback,
security) is documented in the **sCAST repo**:

- `sCAST/docs/aws-migration/README.md` — target architecture
- `sCAST/docs/aws-migration/ENV_MAPPING.md` — env var mapping
- `sCAST/docs/aws-migration/CUTOVER.md` — cutover + rollback runbook
- `sCAST/docs/aws-migration/SECURITY.md` — OIDC / OAC / TLS / auth model

Only one cross-cutting fact matters to this repo: the engine keeps serving the
browser directly via `VITE_FRAGRANCE_API_URL` and is unaffected by CloudFront.
