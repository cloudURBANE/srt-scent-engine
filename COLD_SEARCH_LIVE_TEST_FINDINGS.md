# Cold-Search Live Test Findings & Fix

**Date:** 2026-06-13
**Branch / HEAD:** `feat/serper-fg-discovery` @ `b0de0f7`
**Production target tested:** `https://srt-scent-engine-production.up.railway.app`
**Method:** Live HTTP testing of the public search API exactly as the web frontend calls it
(`GET /api/fragrances/search?q=…`), focusing on fragrances/brands unlikely to be in cache
("cold" searches). Cold vs. cached was read from each response's
`diagnostics` (`live_search_skipped`, `cache_source`, `timing.total_search`).

---

## 1. Summary

The recent commit history (Decodo migration, "cold search fallback", "Thom Browne cold search
fallback", "Prefer direct brand fallback before designer lookup", spelling repair, etc.) has been
fighting one underlying user-visible failure: **searches for fragrances that are not already cached
can hang long enough to return nothing to the user.**

Rigorous live testing reproduced this. The failure is **not** random and **not** a total outage —
it is specific to a class of cold queries:

- **House + fragrance** cold queries work and are fast (3–4 s): `Marc-Antoine Barrois Ganymede`,
  `Maison Crivelli Hibiscus Mahajad`, `Nishane Hacivat`, `Parfums Dusita Oudh Infini`, etc.
- **Bare designer/brand** cold queries (no specific fragrance) **hang past the client timeout and
  return an empty body**: `Thom Browne` (3/3), `Tom Ford`, `Maison Francis Kurkdjian`,
  `Xerjoff` (1/2 — succeeded on retry at 7.4 s), and even a nonsense query.

The hard-coded `THOMBRONY` alias the team added works, but the **natural** query a real user types
(`Thom Browne`) was still broken. The hard-code masked one input; it did not fix the class.

**Root cause:** the cold-query fallback chain in `search_once` stacks several independent
multi-second budgets **with no overall wall-clock ceiling**, so a bare designer/brand query that
misses every cache and the catalog runs ≈30 s and exceeds the client/gateway timeout.

**Fix applied (this change):** thread a single overall `Deadline` through the whole live-search
fallback chain (env `API_SEARCH_TOTAL_BUDGET`, default 18 s on the API; unbounded for CLI/scripts so
nothing else changes). When the live engine can't finish in time it now bails in-budget and the API's
existing fast DB fallback (`_cache_search_fallback`, which already resolves brand queries from
`aggregate_db`) serves the request instead of hanging.

---

## 2. Live test matrix (what a user actually gets)

| # | Query (as a user types it) | Class | Result | Cold/Cached | Notes |
|---|---|---|---|---|---|
| 1 | `Creed Aventus` | popular | 3 results | cached (`live_search_skipped`) | control, ~0.2 s |
| 2 | `THOMBRONY` | collapsed alias | 9 results | cached | hard-coded alias path works |
| 3 | **`Thom Browne`** | **bare designer** | **EMPTY BODY (hang)** | cold | **reproduced 3/3** |
| 4 | `Thom Browne Vetyver` | designer + note | 9 results | cached | warmed by earlier calls |
| 5 | `Bortnikoff Musk Deer` | niche house + frag | 2 results | cold, 1.75 s | brand-fuzzy match |
| 6 | `Areej Le Dore Siberian Musk` | niche house + frag | 3 results | cold, 4.28 s | 1 row BN-only (`fg_url:null`) |
| 7 | `creed avantus` | typo | 6 results | cold, 1.02 s | spell-repair OK |
| 8 | `Marc-Antoine Barrois Ganymede` | niche + frag | 2 results | cold, 3.33 s | OK |
| 9 | `Maison Crivelli Hibiscus Mahajad` | niche + frag | 1 result | cold, 3.67 s | OK |
| 10 | `Nishane Hacivat` | niche + frag | 2 results | cold, 3.96 s | OK |
| 11 | `Parfums Dusita Oudh Infini` | niche + frag | 1 result | cold, 4.06 s | OK |
| 12 | **`Goldfield and Banks Bohemian Lime`** | brand("and") + frag | **EMPTY BODY (hang)** | cold | reworded variant worked |
| 13 | `Goldfield Banks Bohemian Lime` | brand + frag | 1 result | cold, 2.81 s | **`fragrantica_unreachable:true`**, BN-only |
| 14 | `Goldfield and Banks` | bare brand | 2 results | cold, 3.64 s | BN-only, FG unreachable |
| 15 | `zzzqqq nonexistent fragrance xyzzy` | nonsense control | **EMPTY BODY (hang)** | cold | proves empties are timeouts, not `results:[]` |
| 16 | **`Xerjoff`** | bare brand | **EMPTY then 15 results (7.4 s on retry)** | cold | intermittent — latency-bound |
| 17 | **`Tom Ford`** | bare designer | **EMPTY BODY (hang)** | cold | reproduced |
| 18 | `Bortnikoff` | bare niche brand | 15 results | cold, 5.60 s | completes (has BN brand rows) |
| 19 | **`Maison Francis Kurkdjian`** | bare designer | **EMPTY BODY (hang)** | cold | reproduced |

**Key reads from the data**

- An empty body is **not** an empty result set. Every responding request — even nonsense — returns the
  full JSON envelope (`{"query":…,"results":[…],"diagnostics":…}`). An empty body therefore means the
  request never returned within the timeout: a genuine hang. (Confirmed by the nonsense control #15 and
  by `Xerjoff` #16 returning empty once and 15 rows on retry.)
- The failures cluster on **bare designer/brand** queries that produce no direct Basenotes fragrance
  hit and fall into the slow fallback chain. Queries that yield Basenotes brand rows quickly
  (Bortnikoff, Xerjoff-on-retry) complete; ones that don't (Thom Browne, Tom Ford, MFK) hang.

---

## 3. Issues found

### Issue 1 — CRITICAL: cold bare-designer/brand searches hang past the client timeout (empty body)

- **Severity:** Critical (user-visible: search "spins" and returns nothing).
- **Reproduced live:** `Thom Browne` (3/3), `Tom Ford`, `Maison Francis Kurkdjian`, `Xerjoff` (1/2),
  nonsense control — all returned an empty body; see matrix rows 3, 15, 16, 17, 19.
- **Root cause:** In `search_once`, when the first pass needs repair, the fallback stages run
  **serially with independent budgets and no shared ceiling**:
  - spell repair — `API_SPELL_REPAIR_BUDGET` = **4.0 s**
  - designer catalog crawl — `catalog_budget` = **3.5 s**
  - structured designer-provider discovery — budget
    `max(2.4, spell_repair_budget + fg_timeout + 8.0)` = **16.5 s**

  Plus the initial `_search_core` pass (observed up to ~7.4 s). Worst case ≈ **30 s+**, which exceeds
  the client/gateway request timeout, so the connection returns nothing. The team's own
  `THOMBRONY_SEARCH_FIX_FINDINGS.md` already recorded the symptom in logs:
  `"[SYS] Decodo Fragrantica designer discovery failed (ReadTimeout) … read timeout=8.0"`.
- **Why the `THOMBRONY` "fix" didn't close it:** that change added a hard-coded collapsed-brand alias
  plus a brand-label-first ordering. It makes the *one* string `THOMBRONY` resolve, but the natural
  `Thom Browne` (and every other bare designer) still walks the full unbounded chain.
- **Related files:**
  - `fragrance_parser_full_rewrite_fixed.py` — `search_once()` (the fallback chain),
    `_designer_provider_fallback_results()` (the 16.5 s leg), `_search_core()` (initial pass legs),
    `build_parser()` (budget defaults), `class Deadline`.
  - `api.py` — `search()` handler (`/api/fragrances/search`), `_ARGS` budget block,
    `_cache_search_fallback()` (the fast DB fallback that should serve these when live bails).
  - `DEPLOY.md` — documents `API_INITIAL_TIMEOUT`, `API_FG_TIMEOUT`, `API_SPELL_REPAIR_BUDGET`
    (the per-leg budgets that stack).
  - `docs/THOMBRONY_SEARCH_FIX_FINDINGS.md`, `docs/SEARCH_PROVIDER_PRODUCTION_FINDINGS.md`
    (prior partial diagnoses of the same symptom).
- **Status:** **FIXED** in this change (see §4). Verified offline (see §5).

### Issue 2 — HIGH (architectural constraint): production cannot reach Fragrantica → cold results are Basenotes-only / degraded

- **Severity:** High (data completeness), but **honestly surfaced** by the API today.
- **Reproduced live:** `Goldfield and Banks` / `Goldfield Banks Bohemian Lime` returned
  `"fragrantica_unreachable": true` with a `warning` and every row `fg_url: null`; several
  Areej Le Doré / Xerjoff rows were also BN-only.
- **Root cause:** the deployed Railway runtime's datacenter IP is blocked by Fragrantica's Cloudflare,
  so live Fragrantica URL discovery returns zero links. The engine compensates with the `aggregate_db`
  identity cache + Decodo structured SERP + the offline enrichment worker. For a cold identity that is
  not yet cached and not resolvable via Decodo in-budget, the result is BN-only ("degraded, not
  complete"), and `/details` for those ids will be BN-only.
- **Related files:** `fragrance_parser_full_rewrite_fixed.py` (`_designer_provider_fallback_results`,
  `structured_search_provider`, FG fetch path), `api.py` (search `diagnostics.warning`,
  `source_coverage`, enrichment enqueue), `scripts/enrichment_worker.py`,
  `db.py`, `DEPLOY.md` (Decodo / Basenotes-Chromium notes), `PARFUMO_FALLBACK_RESOLVER_DESIGN.md`.
- **Status:** Not a code defect — it is the constraint the whole Decodo/enrichment design exists to
  work around. **Not changed here.** It is the reason Issue 1's correct end-state for a never-warmed
  designer may be "fast best-effort rows" rather than "complete FG-linked rows." Closing it fully needs
  a non-blocked egress (residential proxy) or relying on the offline enrichment worker to backfill FG
  identity — out of scope for a search-path latency fix.

### Issue 3 — MEDIUM (known, downstream): `/details` can return `200 OK` with no usable notes

- **Severity:** Medium. Documented previously in `docs/SEARCH_PROVIDER_PRODUCTION_FINDINGS.md`
  ("Issue 3 … remains a downstream save-gating concern, not an engine defect").
- **Note:** `POST /api/fragrances/details` could not be re-exercised in this pass (the test channel
  available here is GET-only and no browser was connected), so this is carried forward from the
  team's prior finding rather than re-reproduced. The engine already signals it honestly via
  `source_coverage.complete=false`, empty `raw.notes`, and `enrichment.status` = pending/processing.
- **Related files:** `api.py` (`/api/fragrances/details`, enrichment enqueue), `enrichment_facts.py`,
  `derived_metrics_adapter.py`, `scripts/enrichment_worker.py`.
- **Status:** Not changed here; recommend the frontend gate "save" on `source_coverage.complete`.

---

## 4. The fix (Issue 1)

Single concept: **bound the entire live search with one wall-clock deadline** so the runaway cold
fallback chain returns in time and the existing DB fallback can answer brand queries.

**Changed files**

1. `fragrance_parser_full_rewrite_fixed.py`
   - `build_parser()`: new `--search-total-budget` (default **`None` = unbounded**, preserving all
     existing CLI/script/test behavior unchanged).
   - `search_once()`: creates `overall = Deadline(getattr(args,"search_total_budget",None))` and gates
     every fallback stage on it — spell-repair seconds are clamped via `overall.timeout(...)`, the
     catalog crawl deadline is clamped to remaining time, and the designer-provider stage is skipped
     once `overall` is expired.
   - `_designer_provider_fallback_results(query, args, overall_deadline=None)`: new optional param;
     its internal 16.5 s budget is clamped to the overall remaining time, and it returns early when no
     time is left. (Existing 2-positional-arg callers, incl. `test_search_relevance.py`, are unaffected.)

2. `api.py`
   - `_ARGS.search_total_budget = _env_float("API_SEARCH_TOTAL_BUDGET", 18.0)` — the only place a
     concrete ceiling is set. 18 s clears the team's verified ~17 s cold spell-repair path while
     cutting the runaway >30 s designer stack. Tunable per-deploy via the env var (a tighter value
     such as 12 s is likely safe in practice — live spell-repair completed in ~1 s — and would improve
     perceived latency; left at 18 s here to avoid regressing the documented worst case without a live
     re-test).

3. `test_search_total_budget.py` (new) — offline regression tests (see §5).

**Why this is safe / backward-compatible:** with `search_total_budget=None` (CLI default and every
non-API caller — `scripts/enrichment_worker.py`, `scripts/enrich_database_metrics.py`,
`scripts/warm_fg_cache.py`), `Deadline(None)` never expires and `timeout(cap)` returns `cap`
unchanged, so `search_once` is byte-for-byte identical to before. Only the HTTP API opts into the
bound.

**Expected production behavior after deploy:** `Thom Browne` (and any bare designer/brand) returns
within ≤18 s instead of hanging. Because `aggregate_db` already holds Thom Browne rows (proven by the
`THOMBRONY`/`Thom Browne Vetyver` cache hits), a bounded-empty live pass now falls through to
`_cache_search_fallback`, which returns those brand rows fast — the user gets results instead of an
empty screen.

---

## 5. Verification

**Done here (offline):**

- `python -m py_compile` passes for `fragrance_parser_full_rewrite_fixed.py` and `api.py`.
- Module imports cleanly; CLI default `search_total_budget` is `None` (unbounded).
- `test_search_total_budget.py` — **4/4 pass**:
  - `overall_budget_bounds_chain` — a query whose every fallback stage is slow returns in **3.00 s**
    (budget 3 s), not the ~30 s sum; spell-repair seconds clamped to the overall budget.
  - `catalog_crawl_is_clamped` — slow catalog crawl returns in **2.01 s** (overall budget) instead of
    its own 8 s `catalog_budget`.
  - `designer_fallback_skips_when_expired` — no provider call once the overall deadline is exhausted.
  - `unbounded_budget_does_not_clamp` — regression: with `None`, the provider is still queried
    (existing behavior preserved).
- Backward-compat audit: every other caller of `search_once` / `_designer_provider_fallback_results`
  uses unbounded args; the one 2-positional test call still type-checks.

**Not possible from this environment (call out explicitly):**

- The sandbox has **no outbound network to Railway** and **no PyPI** (so `fastapi` can't be installed
  and the `api`-importing suites — `test_search_relevance.py`, `test_decodo_scraper_api.py`,
  `test_enrichment.py` — could not be executed here). The live API was reached only through the
  approved fetch channel for read-only `GET` testing.
- Therefore the fix has **not** been re-tested against the live deployment, and the change has **not**
  been deployed. Post-deploy validation is required (below).

**Recommended post-deploy validation (run from a machine with prod access):**

```bash
python scripts/verify_production.py --url https://srt-scent-engine-production.up.railway.app --timeout 30
python run_tests.py            # full suite, in an env with deps installed
# Cold designer regression (should now return rows in-budget, never an empty body):
curl -s "https://srt-scent-engine-production.up.railway.app/api/fragrances/search?q=Thom%20Browne"   | head -c 400
curl -s "https://srt-scent-engine-production.up.railway.app/api/fragrances/search?q=Tom%20Ford"      | head -c 400
curl -s "https://srt-scent-engine-production.up.railway.app/api/fragrances/search?q=Maison%20Francis%20Kurkdjian" | head -c 400
```

Expect: non-empty `results` (or a fast empty JSON envelope), `total_search` ≤ ~18 s, and **no empty
response body**.

---

## 6. Repo-integrity note (please review)

While editing the 412 KB `fragrance_parser_full_rewrite_fixed.py`, the file's `main()` tail (the CLI
interactive loop, the `except KeyboardInterrupt`, and the `if __name__ == "__main__": main()` guard)
was truncated by a write artifact. It was **restored from the last commit (`HEAD`)**, which matches an
older staged snapshot, so the CLI loop is intact and the file compiles. The uncommitted working
changes prior to this session were in the cold-search code paths (not `main()`), so this restoration
is believed lossless — but please `git diff HEAD -- fragrance_parser_full_rewrite_fixed.py` and confirm
the `main()` region reads as you intend before committing.

---

## 7. Recommendations

1. **Deploy and run the post-deploy validation in §5.** This is the only way to confirm the fix on the
   real Fragrantica-blocked runtime.
2. **Consider lowering `API_SEARCH_TOTAL_BUDGET` to ~12 s** once the live spell-repair path is
   confirmed fast — better perceived latency, same protection.
3. **Retire the hard-coded `THOMBRONY` alias** after the general fix is confirmed in prod, or keep it
   only as a fast-path optimization, not as the fix.
4. **Frontend:** gate "save"/treat-as-complete on `source_coverage.complete` (Issue 3) so a partial
   `200 OK` detail isn't treated as save-ready.
5. **Longer term (Issue 2):** route live Fragrantica fetches through a non-datacenter egress, or lean
   harder on the offline enrichment worker to backfill FG identity for cold designers so cold results
   stop being BN-only.
