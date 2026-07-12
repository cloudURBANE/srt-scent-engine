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

**Reconciled 2026-07-12** — items 1, 2, 3, and 5 below have since closed
(evidence in the canonical sCAST doc §2/§3 and the commits cited here). Only
E7 remains.

1. ✅ **Branch protection on `main` requiring the `test` check** (L5 / E4 op
   half) — verified via the GitHub protection API (canonical doc L5).
2. ✅ **Restore drill performed** (L3 / E5) — 2026-07-10 scratch Railway drill:
   full row parity, 19.4s restore, ~8 min end-to-end, healthy `/readyz`
   (PR #148, re-recorded in #151; `docs/DR_RUNBOOK.md`).
3. ✅ **Uptime monitor on `/readyz`** (L2 / X1) — sCAST
   `.github/workflows/readiness-monitor.yml` probes the engine every five
   minutes with the exact JSON contract (sCAST PR #598, web probe added in
   #600).
4. ⛔ **E7** stale pinned Chromium (Feb-2024 nixpkgs) — still open. Mitigated
   by `DISABLE_CHROMIUM_MINT=1` on the web service (verify the flag in Railway
   config); refresh the pin deliberately, with a live mint verification
   (`engine-live-verify`) after the deploy.
5. ✅ **E9** capacity ceiling documented — `docs/CAPACITY.md` records the
   single-worker ceiling and the scale-out trigger (PR #150).

Cost containment relevant to launch economics (verified): per-IP rate limits
on all cost-bearing endpoints, `DECODO_DAILY_REQUEST_CAP` spend kill-switch,
CORS allowlist via `FRONTEND_ORIGINS`, bearer-gated worker/diagnostics
endpoints.
