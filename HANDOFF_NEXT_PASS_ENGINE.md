# Next-Pass Handoff — `search_engine` (Python FastAPI fragrance engine)

> **For:** the agent continuing work in **`search_engine/`** (the FastAPI engine the SPA calls at
> `VITE_FRAGRANCE_API_URL`). The companion file for the web app is
> `huge_monorepo/HANDOFF_NEXT_PASS_WEBAPP.md` — do not do web-app work from here.
>
> **Compiled:** 2026-06-15 · **Branch at compile time:** `feat/recompute-derived-metrics-backfill`.
> **Prod base URL:** `https://srt-scent-engine-production.up.railway.app`.

---

## 0. Working protocol (read first)

1. **Toolchain (Windows):** venv at `.venv`; call the interpreter directly —
   `.venv\Scripts\python.exe -m pytest`, `… -m uvicorn api:app --reload --port 8000`,
   `… scripts\<name>.py` (`dev-commands`).
2. **Never read the giant files whole.** `fragrance_parser_full_rewrite_fixed.py` (~452 KB),
   `api.py` (~215 KB), `db.py` (~84 KB), `test_*.py` (80-110 KB) are token traps — `rg -n "symbol"`
   to locate, then read only the slice (`repo-map`, `token-efficient-navigation`).
3. **Git is hook-enforced here.** `core.hooksPath = .githooks` (`prepare-commit-msg`). On a fresh
   clone run `git config core.hooksPath .githooks` once. Cut a **fresh short-lived branch from updated
   `main`**, rebase don't back-merge, never `git merge main` into a feature branch (`git-guardrails`).
   The current branch `feat/recompute-derived-metrics-backfill` already carries the recompute work —
   do not pile unrelated changes onto it.
4. **Cross-service contract:** the SPA gates detail "completeness" on `source_coverage`
   (`basenotes && fragrantica && (complete || derived_metrics ∈ {complete,completed,full})`). Do not
   change that response shape on this side without the SPA side (`cross-service-contract`).
5. **Verify offline first.** The sandbox usually has **no PyPI and no outbound network to Railway**, so
   `api`-importing suites (`test_search_relevance.py`, `test_enrichment.py`, `test_decodo_scraper_api.py`)
   may not run locally. `python -m py_compile` + the offline `test_*` that don't import `fastapi`, plus a
   read-only repro (`_diag_enrich_verify.py`), are your local gate. Anything network-bound is **post-deploy
   validation**, and must be called out as not-locally-verified.

**Authoritative source docs (read before editing):** `ENRICHMENT_FIX_HANDOFF.md` (2026-06-15, verified
against the live DB — supersedes `FRAGRANCE_ENRICHMENT_INVESTIGATION.md`), `SENIOR_DEV_FOLLOW_UP_DIAGNOSTICS.md`
(infra/pipeline), `COLD_SEARCH_LIVE_TEST_FINDINGS.md` (search latency), `ENRICHMENT_RUNBOOK.md`,
`API_CONTRACT.md`, `DEPLOY.md`.

---

## 1. What just landed (do NOT redo — verify, then build on it)

On `feat/recompute-derived-metrics-backfill`:
- `d607d5d` — **offline derived-metrics recompute sweep** (`api.py` +106, `db.py` +41, tests +119):
  the bulk offline recompute of `derived_metrics_json` from stored `fg_raw_json` (Fix 1 of the handoff).
- `fea9c13` — **offline heal CLI** `scripts/heal_offline.py` (+ `ENRICHMENT_RUNBOOK.md`, tests): the
  Unknown-family / concentration backlog healer.

**First action: confirm scope.** Read `scripts/heal_offline.py` + the new `api.py`/`db.py` sweep and the
runbook to see exactly which of {family recompute, concentration backfill, wardrobe projection} they
cover, and whether they've been **run against prod** (the backlog only drains when the sweep actually
executes — code landing ≠ data healed). Then pick up the **still-open** items below.

---

## 2. Master index — what is left

> **Status reconciliation — 2026-07-12 (verified against the current tree).**
>
> - **E-2 ❌ / E-4 ✅** — already marked below; unchanged.
> - **E-3 ✅ closed as moot** — `_persist_detail_record` (api.py) persists
>   `derived_metrics` on the detail write, and stored-payload incompleteness now
>   self-heals via the auto-requeue path (`test_auto_requeue_incomplete.py`) plus
>   the offline recompute sweep. No eager-compute change needed.
> - **E-6 ✅ code half closed** — pool default 15, bounded acquisition
>   (`DB_POOL_ACQUIRE_TIMEOUT`, 5s) and `PoolTimeout` → retryable 503 landed with
>   the audit B3 closure (`test_audit_gap_closures.py`). The Railway
>   `DB_POOL_MAX_SIZE` env value itself remains an ops check.
> - **E-9 ✅ done** — the hard-coded `THOMBRONY` alias is retired from
>   `fragrance_parser_full_rewrite_fixed.py`; the string survives only in a
>   comment describing the general collapsed-alias fix.
> - **E-1 / E-5 (OPERATE)** — the code and runbooks are shipped; whether the prod
>   backlog is drained is live-DB state. Check `GET /api/enrichment/completeness`
>   (see `ENRICHMENT_RUNBOOK.md` and the `wardrobe-completeness-heal` skill)
>   before re-running anything.
> - **E-7 / E-8 (VERIFY)** — remain post-deploy/ops checks (Railway env +
>   `engine-live-verify`); not verifiable from a sandbox.
> - **E-10** — long-term infra track, unchanged.
>
> Do not re-open the ✅ items; the table below is historical context.

| ID | Title | Type | Priority | Network? | Primary file(s) |
|---|---|---|---|---|---|
| E-1 | Run the heal/recompute against prod to drain the 89-job / Unknown-family backlog | OPERATE | High | yes | `scripts/heal_offline.py`, `GET /api/enrichment/completeness` |
| E-2 | ~~Root Cause B: worker drops `_derived_metrics`~~ — **MISDIAGNOSIS, do not implement** (`/complete` recomputes server-side) | ❌ VOID | — | no | (none) |
| E-3 | Eager-compute `derived_metrics` on `/details` write — *live-fetch path already does; marginal* | 🟢 FIX | Low | no (code) | `api.py` (`_persist_detail_record`), `db.py` |
| E-4 | ~~Project families/metrics into the wardrobe tables~~ — **already shipped in `heal_offline.py`** | ✅ DONE | — | no | `scripts/heal_offline.py` |
| E-5 | Launch the local enrichment worker (residential IP) to clear the 14 scrape-needed rows | OPERATE | Medium | yes (local) | `scripts/run_worker.ps1`, `scripts/enrichment_worker.py` |
| E-6 | Railway DB pool size vs hobby-tier cap | 🟢 FIX (env) | High | ops | `db.py` (`DB_POOL_MAX_SIZE`), Railway env |
| E-7 | Verify Decodo egress credentials in prod env | 🟠 VERIFY | High | ops | Railway env (`DECODO_API_*`) |
| E-8 | Post-deploy validation of the cold-search budget fix; consider lowering the ceiling | 🟠 VERIFY | Medium | yes | `api.py` (`API_SEARCH_TOTAL_BUDGET`), `scripts/verify_production.py` |
| E-9 | Retire the hard-coded `THOMBRONY` alias once the general fix is confirmed | 🟢 CLEANUP | Low | no | `fragrance_parser_full_rewrite_fixed.py` |
| E-10 | (Architectural) non-datacenter egress for live Fragrantica | 🟣 LONG-TERM | — | infra | `fragrance_parser_full_rewrite_fixed.py`, `DEPLOY.md` |

**Suggested order:** E-2 + E-3 (stop the bleed at the source) → E-1 + E-5 (drain the backlog) →
E-4 (project to wardrobe) → E-6 + E-7 (ops) → E-8 → E-9. E-10 is infra, separate track.

---

## 3. Cards

### E-1 · Drain the family/concentration backlog (OPERATE)
The 2026-06-15 audit found **89 pending `enrichment_jobs`** and ~**102 of 116** NULL-metric
`fragrance_records` that resolve to a real family **offline, zero scraping**. With the recompute sweep
(`d607d5d`) + heal CLI (`fea9c13`) landed, the remaining work is to **run** them against the live DB and
confirm the drop. After the run:
- `SELECT count(*) FROM fragrance_records WHERE derived_metrics_json IS NULL AND fg_raw_json <> '{}'::jsonb`
  should fall to **0** (only the ~14 empty-raw rows remain).
- `GET /api/enrichment/completeness` (Bearer worker token) `missing_field_counts.family` should drop ~102.
- Re-run the read-only `_diag_enrich_verify.py` for the current counts **before** acting (numbers churn).
Endpoints `/api/enrichment/completeness` (GET) and `/api/enrichment/heal` (POST) are `_require_worker_token`-guarded.

### E-2 · ~~Root Cause B — the worker never persists derived metrics~~ ❌ MISDIAGNOSIS — DO NOT IMPLEMENT
**Verified against the live code 2026-06-15: this "fix" is a no-op and a hot-loop regression risk.**
The premise — worker pops `_derived_metrics` so completion writes NULL — is wrong. The `/complete`
endpoint **recomputes the metrics server-side** from the payload's `frag_cards`, completely independent of
the worker's popped blob:
- `api.py:5311` (`complete_enrichment_job`): `temp_details.derived_metrics = build_derived_metrics(temp_details)`
  is rebuilt from `payload.frag_cards`, then persisted into `cache_row["derived_metrics"]` (`api.py:5341`).
- The worker's `_derived_metrics` is **never sent** to the server (`CompleteJobRequest` has no such field);
  the pop at `enrichment_worker.py:1818` only feeds a local log summary (`_payload_fact_summary`).
- `build_derived_metrics` always returns a dict, so a worker-completed row with non-empty `frag_cards`
  persists **NON-NULL** metrics. Threading `_derived_metrics` through `complete_job` would change nothing
  (the server ignores it) while adding risk to the regression-sensitive worker hot loop.
**The real cause is Root Cause A:** the raw-upsert path (`db.upsert_fragrance_details`) deliberately
invalidates `derived_metrics_json` to NULL on every fresh raw, and only the read-time `/details` healer
(`_fill_missing_derived_metrics_once`) recomputes it — so never-opened rows stay Unknown. **That is already
addressed** by the shipped `POST /api/enrichment/recompute-metrics` (commit `d607d5d`) + `heal_offline.py`
(`fea9c13`); the remaining work is purely **operational (E-1): run them against prod.** Do not "fix" the
worker.

### E-3 · Eager `derived_metrics` on `/details` write 🟢
Today, a successful live `/details` fetch persists raw to `fragrance_records` but writes
`derived_metrics_json = NULL` (computed lazily on next read via `_fill_missing_derived_metrics_once`,
`api.py:3889-4002`). The completeness audit then reads NULL and re-enqueues the row even though the
accords to derive `family` are already in `fg_raw_json` — wasted Decodo/proxy budget (SENIOR doc §5: ~112
redundant rows). **Fix:** compute `derived_metrics` eagerly on write in the `/details` persist path
(`_persist_detail_record` / `_details_from_fragrance_record` reconstruction at `api.py:3826-3871`), so the
metrics blob is populated at ingest. Pairs with E-2 — together they stop NULLs reappearing. Keep writes
idempotent. **Verify:** a fresh `/details` save leaves `derived_metrics_json` non-NULL; completeness audit
doesn't re-queue it.

### E-4 · ~~Project families/metrics into the wardrobe tables~~ ✅ ALREADY SHIPPED
**Done — `scripts/heal_offline.py` already carries this.** The `heal_wardrobe` step
(`scripts/heal_offline.py:254`) projects healed family + concentration from `fragrance_records` into both
`user_fragrances.fragrance_data` and `global_fragrances.profile_data` by **exact** `_norm_key` brand+name
match (skips ambiguous, never overwrites existing values). Do **not** write a new `project_families.py` — it
would duplicate this. The remaining gap is the ~144 wardrobe fragrances with **no** engine record (Creed
Royal Oud, Chanel Coco, …): projection can't reach them; they need the `seed` step + the live worker (E-5),
after which a re-run of `heal_offline` projects them. This too is **operational (run the tool), not code.**
The original card text is preserved below for context.

`fragrance_records` is the engine cache; the **wardrobe** tables store family *statically*
(`user_fragrances.fragrance_data->>'family'`, `global_fragrances.profile_data->>'family'`). Healing
`fragrance_records` does **not** auto-fix them (prior audit: ~88/138 user, ~104/191 global Unknown — re-audit
first). **Do NOT** run `scripts/enrich_database_metrics.py` for this — it's an online re-scraper that's
403/429-blocked on the datacenter IP. Instead write a small **offline projector** mapping
`fragrance_records.derived_metrics_json` → wardrobe `fragrance_data`/`profile_data` by brand+name, reusing
`derive_families` (`enrichment_facts.py:23-170`) and the existing `_apply_derived_facts` helper, **without**
`engine.get_scraper()`. Idempotent, merge-safe (don't overwrite existing values). First check whether
`scripts/heal_offline.py` already covers this projection before writing a new script.

### E-5 · Run the local enrichment worker for the ~14 truly-empty rows (OPERATE)
~14 NULL rows have empty `fg_raw_json '{}'` and genuinely need a scrape. The worker is **architected to run
locally** (residential IP) because datacenter IPs are Cloudflare-blocked on Fragrantica — running it on
Railway crashes on boot at the `_validate_fragrantica_session` clearance preflight
(`scripts/enrichment_worker.py:1969-1975`, `SystemExit`). Launch locally:
```powershell
.\scripts\run_worker.ps1 -Mode pending -- --limit 50 --delay 45
```
Only meaningful **after E-2** lands, or the worker will scrape and still write NULL metrics.

### E-6 · Railway DB pool vs hobby-tier cap 🟢 (env) — code default now 15
**Code default lowered:** `DEFAULT_DB_POOL_MAX_SIZE` in `db.py` is now **15** (was 40), so a fresh deploy
no longer over-subscribes the hobby/basic-tier Railway Postgres **20**-connection hard cap (which surfaced
as `fe_sendauth` / "too many connections" hangs under load). **Still set `DB_POOL_MAX_SIZE` explicitly on
the Railway API service env** to pin it for that tier (10–15), and raise it only on a tier that allows more
connections.

### E-7 · Verify Decodo egress credentials 🟠
Live `/details` for uncached fragrances falls back to Decodo's premium proxy (`decodo_universal`) when the
direct datacenter fetch hits Cloudflare. If `DECODO_API_BASIC_TOKEN` (or `DECODO_API_USERNAME`/`_PASSWORD`)
is expired/unset on Railway, the recovery path **fails silently** → BN-only / empty profiles. Confirm the
credentials are present and valid in the Railway env. See `DECODO_UNIVERSAL_RECOVERY.md`.

### E-8 · Cold-search budget fix — post-deploy validation + tuning 🟠
The wall-clock `Deadline` fix (`API_SEARCH_TOTAL_BUDGET`, default 18s) for hanging bare-designer/brand
queries (`Thom Browne`, `Tom Ford`, `Maison Francis Kurkdjian`) is **in the tree** but was **not re-tested
against the live Fragrantica-blocked runtime** (sandbox had no prod network). Run from a machine with prod
access:
```bash
python scripts/verify_production.py --url https://srt-scent-engine-production.up.railway.app --timeout 30
curl -s "https://srt-scent-engine-production.up.railway.app/api/fragrances/search?q=Thom%20Browne" | head -c 400
curl -s ".../api/fragrances/search?q=Tom%20Ford" | head -c 400
curl -s ".../api/fragrances/search?q=Maison%20Francis%20Kurkdjian" | head -c 400
```
Expect non-empty `results` (or a fast empty JSON envelope), `total_search` ≤ ~18s, **no empty body**. Once
the live spell-repair path is confirmed fast (~1s), consider lowering `API_SEARCH_TOTAL_BUDGET` to ~12s for
better perceived latency. Files: `api.py` (`_ARGS.search_total_budget`),
`fragrance_parser_full_rewrite_fixed.py` (`search_once`, `_designer_provider_fallback_results`, `class Deadline`).
Full detail: `COLD_SEARCH_LIVE_TEST_FINDINGS.md`.

### E-9 · Retire the `THOMBRONY` hard-coded alias 🟢 (after E-8 confirms)
The hard-coded collapsed-brand alias masked one input; the general `Deadline` fix is the real cure. Once E-8
confirms `Thom Browne` resolves in-budget in prod, remove the alias (or keep it only as a fast-path
optimization, not as "the fix") in `fragrance_parser_full_rewrite_fixed.py`.

### E-10 · Non-datacenter egress for live Fragrantica 🟣 (long-term, infra)
Issue 2 from the cold-search findings: the Railway datacenter IP is Cloudflare-blocked, so cold,
never-warmed identities come back **Basenotes-only / degraded** (`fragrantica_unreachable: true`,
`fg_url: null`). This is the constraint the whole Decodo/enrichment design works around — **not a code
defect.** Closing it fully needs a residential/non-blocked egress for the live FG fetch path, or leaning
harder on the offline worker (E-5) to backfill FG identity for cold designers. Separate infra track;
honestly surfaced by the API today, so not urgent for correctness.

---

## 4. Repo-integrity note (carried forward)
A prior session flagged that an edit to the 412 KB `fragrance_parser_full_rewrite_fixed.py` truncated the
`main()` tail (CLI loop + `if __name__ == "__main__"` guard) and restored it from `HEAD`. Before committing
any change to that file, `git diff HEAD -- fragrance_parser_full_rewrite_fixed.py` and confirm the `main()`
region reads intact.
