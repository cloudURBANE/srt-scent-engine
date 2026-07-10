# Launch Readiness — 2026-07-10 (engine repo pointer)

**The canonical, full launch-readiness audit lives in the sCAST repo:**
`sCAST/docs/LAUNCH_READINESS_2026-07-10.md`. It covers both repos — scoreboard
with readiness percentages and confidence, verified-done ledger, remaining
blockers, ready-to-USE vs ready-to-SELL analysis, launch sequence, and the
owner confirmation checklist. This file is only a pointer plus the
engine-local facts, so the canonical doc has one home and cannot drift.

## Engine status snapshot (verified today against this repo)

Closed since the 2026-07-09 gap audit (all evidence-checked):

- **E1** Sentry — `sentry-sdk[fastapi]` pinned, DSN-gated init in `api.py`.
- **E2** per-IP rate limits — `rate_limit.py` middleware on search/details/requeue.
- **E3** verified DB TLS — `DATABASE_SSL_CA` → `sslmode=verify-full` in `db.py`.
- **E4** (code half) `/readyz` DB-pinging probe; `railway.toml` healthchecks it.
- **E5** (doc half) `docs/DR_RUNBOOK.md` written.
- **E6** dependabot (pip + actions) + pip-audit CI step.
- **E8** repo hygiene (diag scripts under `scripts/`, run outputs dropped).
- **X2** shared `source_coverage` contract fixtures, asserted by both CI suites.
- CI green on `main` (latest run 2026-07-09).

## Still open on the engine side (see canonical doc for the full plan)

1. **Branch protection on `main` requiring `ci`** (L5 / E4 op half) — GitHub
   settings; Railway otherwise deploys a red main.
2. **Restore drill not performed** (L3 / E5) — runbook exists, RPO/RTO TBD
   until one timed drill is run.
3. **Uptime monitor on `/readyz`** (L2 / X1) — nothing external watches the
   engine today.
4. **E7** stale pinned Chromium (Feb-2024 nixpkgs) — mitigated by
   `DISABLE_CHROMIUM_MINT=1` on the web service (verify the flag in Railway
   config); refresh the pin deliberately.
5. **E9** capacity ceiling undocumented — single GIL-bound uvicorn worker;
   record the scale-out trigger before it matters.

Cost containment relevant to launch economics (verified): per-IP rate limits
on all cost-bearing endpoints, `DECODO_DAILY_REQUEST_CAP` spend kill-switch,
CORS allowlist via `FRONTEND_ORIGINS`, bearer-gated worker/diagnostics
endpoints.
