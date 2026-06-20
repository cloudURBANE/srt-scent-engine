# Brand Search Engine Audit — SRT Scent Engine

**Date:** 2026-06-20
**Branch:** `claude/brand-search-audit-2rjemv`
**Repo:** `cloudURBANE/srt-scent-engine`
**Scope:** AUDIT ONLY. No source files were modified except this document. No commit/push.
**Method:** Static trace of the search path (`api.py` → `fragrance_parser_full_rewrite_fixed.py` → `db.py`) plus **offline reproduction** of the scoring/tokenization logic (engine imported and exercised directly; the existing `test_search_relevance.py` suite — 68 tests — and `test_search_total_budget.py` — 6 tests — were run locally and pass). Live network (Railway / Decodo / Fragrantica / Postgres) was **not** reachable from this environment; live HTTP repro commands are given for the fix team.

---

## 0. The single most important framing

The four symptoms split cleanly into **two unrelated failure classes**, and conflating them will send the fix team in the wrong direction:

- **Symptoms 1, 2, 3 (JPG 2 results, `torino 21` 1 result, `boss bottled night` 0 results) are DATA-COVERAGE failures, NOT scoring/normalization bugs.** I reproduced the scoring offline: every one of these queries scores its *correct* target at **1.0** and passes `candidate_relevance_ok`. The rows simply are not in the engine's durable store (`fragrance_records` / `fg_detail_cache`), and live discovery (Decodo→Fragrantica) is blocked/slow on the Railway runtime (documented in `COLD_SEARCH_LIVE_TEST_FINDINGS.md`). The relevance gate is working correctly; the candidates never reach it.
- **Symptom 4 (slowness) is an architecture/caching failure:** brand-only queries are structurally excluded from the fast cache path and re-run the full bounded live chain (up to `API_SEARCH_TOTAL_BUDGET = 18s`) every time, even when the answer is already in `fragrance_records`.

So the high-leverage fix is **populate and serve brand catalogs from the DB**, plus **let brand-only queries hit the cache fast path**. Loosening scoring would not fix symptoms 1–3 and would risk regressing the 68 passing relevance tests.

---

## 1. Executive summary of root causes (ranked by confidence)

| # | Root cause | Confidence | Symptoms it explains |
|---|---|---|---|
| RC1 | **`fragrance_records` (the engine's durable identity store) holds only a sparse handful of rows per brand.** Brand breadth depends on the brand-sweep + enrichment worker, which need live Decodo/Fragrantica — blocked/slow in prod. So `jean paul gaultier` finds ~2 stored rows, `boss bottled night` 0, `torino 21`'s exact product 0. | **High** | 1, 2, 3 |
| RC2 | **The SQL identity search is a single contiguous `%query%` substring with no token splitting and no brand-alias expansion** (`db.py:1466` `search_fragrance_records`, term built at `db.py:1213` `_identity_search_term`). `jean paul gaultier` only matches rows whose `house`/`concat(house,name)` literally contains the whole phrase; a row stored under house `JPG` or `Gaultier` is missed. | **High** | 1, 2 |
| RC3 | **In production the bundled JSON identity cache is DISABLED** (`_ALLOW_BUNDLED_FG_SEARCH_CACHE` / `_ALLOW_BUNDLED_FG_DETAIL_CACHE` default to `False` when `DATABASE_URL` is set — `api.py:116-122`). The shipped `fg_cache/fg_identity_cache_v2.json` actually contains **28 JPG** and **12 Xerjoff** identity rows (with real Fragrantica URLs) that prod never reads. | **High** | 1, 2 |
| RC4 | **Brand-only queries can never hit the strong/warm fast cache path.** `_strong_cache_candidate_ok` (`api.py:1172-1183`) requires the query tokens to intersect the candidate's **name** tokens; a brand-only query shares tokens only with the **brand**, so it always falls through to the full live search → repeated high latency even when rows are cached. | **High** | 4 (and worsens 1) |
| RC5 | **Hard result cap of 15** (`--max-results` default, `fragrance_parser_full_rewrite_fixed.py:11389`; used as `_ARGS.max_results` everywhere). Even with full coverage a major designer (40+ frags) is truncated to 15. | **High** (cap is real; only bites once coverage exists) | 1 |
| RC6 | **No durable brand-search result cache + in-process Decodo caches are unbounded, TTL-less, and reset on every redeploy** (`fragrance_parser_full_rewrite_fixed.py:3690-3694`). Repeat brand queries re-pay live cost. | **High** | 4 |
| RC7 | **`torino 21`: the `21` token is not over-fuzzed/dropped by scoring** (verified: `Torino 21` scores 1.0, `Torino` alone scores 0.8 and is *rejected*). The lone "Torino" result is a coverage/identity-completeness artifact — the exact "Torino 21" identity isn't stored and live discovery returns the broader/parent row. | **Medium-High** | 2 |
| RC8 | **Single uvicorn worker, sync endpoint on a 40-thread pool, live-search semaphore = 6, DB pool max = 15.** Not the cause of the reported symptoms but a real throughput ceiling under concurrency (`Procfile`, `railway.toml`, `api.py:188-196`, `db.py:56`). | **Medium** | 4 (under load) |

---

## 2. Per-symptom analysis

### Symptom 1 — `jean paul gaultier` returns only 2 results

**Code path**
1. `api.py:2432` `search()` — query `jean paul gaultier`.
2. Strong-cache precheck `_strong_cache_search` (`api.py:2449`, def `api.py:1186`) → calls `_fragrance_record_search` etc., each gated by `_strong_cache_candidate_ok` (`api.py:1172`). **Brand-only fails this gate** (RC4, proven below), so no fast-path hit. Same for the warm tier (`api.py:2482`).
3. Live search `engine.search_once` (`api.py:2529`, def `…:11106`). `expand_query_aliases('jean paul gaultier')` returns it unchanged (already canonical). First pass `_search_core` calls Decodo/Fragrantica which return little/nothing in prod; `needs_repair` → brand-only path: `query_is_brand_only` true, catalog crawl + `_designer_provider_fallback_results` attempt to fetch the FG designer page — blocked/slow.
4. On empty live results, `_cache_search_fallback` (`api.py:2542`, def `…:1065`) → `_fragrance_record_search` → `db.search_fragrance_records` (`db.py:1466`).

**Root cause (evidence)**
- `db.search_fragrance_records` builds the term via `_identity_search_term` (`db.py:1213`): for a >2-char multi-word query it returns `ilike`, `"%jean paul gaultier%"`. The WHERE clause (`db.py:1511-1514`) matches `house ILIKE '%jean paul gaultier%'` OR `concat_ws(' ', house, name) ILIKE …`. This **does** match every row whose `house` is literally "Jean Paul Gaultier" — so the gate is not the problem; **the rows don't exist**. The two that return ("Gaultier Divine Elixir", "Gaultier Divine Le Parfum") match because `concat_ws(house,name)` = "Jean Paul Gaultier Gaultier Divine …" contains the phrase. Only ~2 JPG identities are in `fragrance_records` (RC1).
- Offline proof scoring is NOT the limiter:
  - `relevance_score('jean paul gaultier', UnifiedFragrance('Le Male','Jean Paul Gaultier'))` → **1.0**; `candidate_relevance_ok` → **True**.
  - `query_is_brand_only('jean paul gaultier', 'Jean Paul Gaultier')` → **True** (so the catalogue is meant to survive; `candidate_relevance_ok` short-circuits to True at `…:3046`).
- The shipped `fg_cache/fg_identity_cache_v2.json` has **28** JPG rows with FG URLs, but `_identity_cache_search` is disabled in prod (RC3).
- Even fully populated, `_ARGS.max_results = 15` (RC5) caps the response.

**Recommended fix direction**
- **Coverage (primary):** make the brand-sweep able to populate `fragrance_records` without live FG — seed from the bundled `fg_identity_cache_v2.json` on boot/first-brand-query (it already carries real FG URLs), and/or run the offline enrichment worker brand-sweep to backfill. Verify `_maybe_spawn_brand_sweep` (`api.py:1324`) actually persists rows for designer houses.
- **SQL (RC2):** when `query_is_brand_only`/`canonical_brand_query` resolves a known house, search `fragrance_records` by **canonical house + all alias forms** (`IdentityTools.brand_forms`, `…:758`) rather than the raw `%phrase%`, e.g. `house ILIKE ANY(brand_forms)`.
- **Bundled cache (RC3):** allow the bundled identity cache as a read-through tier in prod for brand-only queries (or import it into `fragrance_records` once), so the 28 JPG / 12 Xerjoff rows are served while the DB warms.
- **Cap (RC5):** raise `max_results` for brand-only queries (e.g. 40–60) — the frontend has a "BRAND ONLY" mode that expects a full house listing.
- **Test to add:** an integration-style test that seeds `fragrance_records` with N JPG rows (some under house alias `JPG`/`Gaultier`) and asserts `_cache_search_fallback('jean paul gaultier', 50)` returns **all** of them, not 2.

### Symptom 2 — `torino 21` returns only 1 result ("Torino" by Xerjoff)

**Code path:** same as Symptom 1; relevant scoring at `relevance_score` (`…:1191`), `candidate_relevance_ok` (`…:3021`), `fuzzy_token_coverage` (`…:1159`).

**Root cause (evidence)** — the `21` token is **not** dropped or over-fuzzed:
- `TextSanitizer.normalize_identity` keeps digits (`…:469` regex `[^a-z0-9\s]`), so `21` survives tokenization.
- `expand_query_aliases('torino 21')` → unchanged.
- Offline scoring:
  - `relevance_score('torino 21', ('Torino 21','Xerjoff'))` → **1.0**, `candidate_relevance_ok` → **True**.
  - `relevance_score('torino 21', ('Torino','Xerjoff'))` → **0.80**, `candidate_relevance_ok` → **False** (below `MIN_RESULT_SCORE = 0.72`? it's above 0.72 but the *distinctive-coverage* check at `…:3082` fails: `fuzzy_token_coverage({torino,21},{torino}) = 0.5 < 0.86`).
- So if the exact "Torino 21" identity were stored, it would rank first and pass. The single "Torino" the user sees is the closest stored/discoverable identity (likely the parent/line page or a row stored without the numeric suffix). This is a **coverage/identity-completeness** problem (the `21` flanker isn't a separate record), not a token-dropping bug.
- One subtlety to flag for the fix team: the digit-2 token gets **no fuzzy credit** (`fuzzy_token_coverage` only fuzzes tokens with `len >= 4`, `…:1168/1170`); only an exact `21==21` match counts. That is *correct* here (we don't want `21` fuzzing to `28`), but it means a record stored as "Torino21" (no space) would tokenize to `torino21` and **fail** to match `{torino,21}`. Check how the Casamorati/Xerjoff "Torino21" name is actually stored.

**Recommended fix direction**
- Confirm the canonical identity: is the product "Torino21" (one token) or "Torino 21"? Normalize numeric-suffix flankers consistently at parse time (e.g. split letter/number boundaries: `torino21` → `torino 21`) in `TextSanitizer.normalize_identity` or `identity_tokens`, so query and stored identity tokenize the same way. (Low-risk: only affects letter↔digit boundaries.)
- Ensure discovery/enrichment captures numbered flankers as distinct records.
- **Test to add:** `relevance_score`/`candidate_relevance_ok` parametrized over `('torino 21' ↔ 'Torino 21')`, `('torino 21' ↔ 'Torino21')`, `('torino 21' ↔ 'Torino')` asserting only the first two pass.

### Symptom 3 — `boss bottled night` returns "No Olfactory Matches Found" (0 results)

**Code path:** same as Symptom 1.

**Root cause (evidence)** — **pure coverage gap**, scoring is correct:
- `expand_query_aliases('boss bottled night')` → unchanged (the test `test_query_expander_prepends_omitted_house_for_known_lines` at `test_search_relevance.py:2015` asserts it is *not* re-prepended because "boss" is already a Hugo Boss alias, `BRAND_ALIASES` `…:629-630`).
- Offline scoring:
  - `relevance_score('boss bottled night', ('Boss Bottled Night','Hugo Boss'))` → **1.0**, `candidate_relevance_ok` → **True**.
  - `relevance_score('boss bottled night', ('Bottled Night','Hugo Boss'))` → **1.0**, **True**.
- `query_is_brand_only('boss bottled night','Hugo Boss')` → **False** (correct: there's a name remainder), so it's treated as brand+name.
- The shipped identity cache has **0** "boss" rows; if `fragrance_records` also has none and live discovery is blocked, the fallback chain returns `[]` → the frontend's "No Olfactory Matches Found". `needs_repair`/`MIN_USEFUL_TOP_SCORE` (`…:11242`) then suppresses weak FG-only rows, finalizing zero.

**Recommended fix direction**
- This mainstream designer fragrance must be in the durable store. Backfill Hugo Boss via the offline enrichment worker / brand-sweep, or seed from a mainstream catalog. The query path is correct; the data is missing.
- Consider a **mainstream-designer seed list** imported into `fragrance_records` so popular houses (Hugo Boss, Dior, Chanel, JPG, Versace) are never empty even cold.
- **Test to add:** seed a Hugo Boss "Boss Bottled Night" row and assert `_cache_search_fallback('boss bottled night', 15)` returns it; add a guard test that the alias `boss → Hugo Boss` resolves in `brand_forms`.

### Symptom 4 — search feels slower than expected

**Code path / evidence**
- **RC4 (biggest perceived-latency hit for brand queries):** `_strong_cache_candidate_ok` (`api.py:1181-1183`) requires `query_tokens ∩ name_tokens`. Offline: for `jean paul gaultier` vs a "Le Male" row, name tokens are `[]` after brand-strip → intersection empty → **fast path declined**. Every brand-only search therefore runs the full live chain bounded by `API_SEARCH_TOTAL_BUDGET = 18s` (`api.py:178`) before falling back to DB — even when DB already has the rows.
- **Cold live chain cost** is documented in `COLD_SEARCH_LIVE_TEST_FINDINGS.md`: stacked per-leg budgets (spell-repair 4s + catalog 3.5s + designer-provider up to ~16.5s), now ceilinged at 18s. 18s is still a long worst case; the doc itself recommends lowering to ~12s.
- **No durable result cache:** Decodo caches are in-process class dicts (`fragrance_parser_full_rewrite_fixed.py:3690-3694`), unbounded, **TTL-less**, lost on redeploy. There is no query→results cache; "warm"/"strong" tiers read `fragrance_records`/`fg_detail_cache` only.
- **Concurrency ceiling (RC8):** single uvicorn worker (`Procfile`/`railway.toml`, no `--workers`); sync `def search` runs on FastAPI's 40-thread pool; `_LIVE_SEARCH_SEMAPHORE` = 6 (`API_LIVE_SEARCH_MAX_CONCURRENT`, `api.py:188`); DB pool max 15 (`db.py:56`). Under bursty load, the 6-slot live gate serializes cold searches.

**Recommended fix direction**
- **Fix RC4:** add a brand-only fast path — when `query_is_brand_only` (or `canonical_brand_query` resolves a house), serve directly from `fragrance_records` by canonical house (RC2 fix) and **skip live** when ≥1 cached row exists, optionally kicking a background refresh (mirror `_spawn_warm_refresh`). This is the owner's explicit ask ("brand results cached so repeat queries are fast").
- Add a small **in-process LRU/TTL brand-result cache** keyed on the canonical brand key (`_brand_sweep_key`, `api.py:1308`) with a short TTL (see §3).
- Lower `API_SEARCH_TOTAL_BUDGET` to ~12s once cold spell-repair is confirmed fast.
- **Test to add / baseline:** `test_search_total_budget.py` already proves the chain is bounded (ran: **6/6 pass**, e.g. `overall_budget_bounds_chain` returns in 3.00s under a 3s budget). Extend it with a "brand-only query served from cache without acquiring the live semaphore" assertion.

---

## 3. Caching findings + recommended brand-search cache design

**What exists today**
| Cache | Where | Keyed on | TTL | Invalidation | Survives redeploy |
|---|---|---|---|---|---|
| `fragrance_records` (aggregate identity) | Postgres, `db.search_fragrance_records` (`db.py:1466`) | substring `%query%` | `FRAGRANCE_RECORD_TTL_HOURS=168` staleness filter (`api.py:212`, `_fragrance_record_is_stale` `:863`) | upsert on each search (`_persist_search_results` `:906`) | yes |
| `fg_detail_cache` (durable detail) | Postgres, `db.search_detail_cache` (`db.py:1696`) | substring `%query%` | none (detail-level) | enrichment worker overwrite | yes |
| Bundled JSON identity/detail | `fg_cache/*.json` | name/url | none | manual | yes, but **disabled in prod** (RC3) |
| Decodo SERP/URL caches | `DecodoScraperClient._cache/_designer_cache/_brand_perfume_cache` (`…:3690-3694`) | `normalize_identity(query)` | **none** | none (unbounded) | **no** (in-process) |
| Strong/warm tiers | read `fragrance_records`/detail (`api.py:1186`, `:2482`) | name-token-intersecting | n/a | n/a | n/a |

**Gaps:** (a) no query→results cache for brand searches; (b) brand-only queries are structurally locked out of the fast tiers (RC4); (c) Decodo caches are unbounded and lost on redeploy (memory-leak risk and cold-after-deploy).

**Recommended brand-search cache design**
- **Tier 1 (durable, source of truth):** keep `fragrance_records`, but add a **`brand_key` column or index** and a brand-scoped query so a brand-only search reads all rows for the house in one indexed lookup (pairs with RC2 alias-aware SQL). The brand-sweep already has a durable `brand_sweeps` claim table (`db.py:224`, `claim_brand_sweep` `:540`) — reuse `found_count` to know a house is fully swept.
- **Tier 2 (hot, in-process):** a bounded `TTLCache`/LRU keyed on `_brand_sweep_key(brand)` → list of serialized results.
  - **Key:** alias-collapsed canonical brand key (`IdentityTools.canonical_brand_query` `…:934` → fallback `normalize_identity`), so `jpg`, `JPG`, `jean paul gaultier` share one entry.
  - **TTL:** 15–60 min (configurable env, e.g. `BRAND_SEARCH_CACHE_TTL_SECONDS`). Bound size (e.g. 256 brands) to avoid the documented memory-leak class.
  - **Invalidation:** drop the entry when the brand-sweep completes (`mark_brand_sweep_done` `db.py:571`) or when `_persist_search_results` writes new rows for that brand; otherwise expire by TTL.
  - **Population:** on a brand-only fast path, fill from `fragrance_records`; serve hits without touching `_LIVE_SEARCH_SEMAPHORE`.
- **Decodo caches:** convert to bounded TTL caches to cap memory and avoid stale-forever entries.

---

## 4. Latency baseline

**Obtained offline (this environment):**
- `python3 test_search_total_budget.py` → **6/6 PASS**. Measured: `overall_budget_bounds_chain` returns in **3.00s** under a 3s budget (vs the unbounded ~30s sum); `catalog_crawl_is_clamped` **2.01s** under a 2s budget. Confirms the 18s wall-clock ceiling is enforced and the chain cannot run away.
- `python3 -m pytest test_search_relevance.py` → **68/68 PASS in 4.56s** (scoring/identity logic baseline; proves symptoms 1–3 are not scoring regressions).

**Configured budgets (prod):** `API_INITIAL_TIMEOUT=5.5`, `API_FG_TIMEOUT=4.5`, `API_SPELL_REPAIR_BUDGET=4.0`, `API_SEARCH_TOTAL_BUDGET=18.0`, `API_DESIGNER_PROVIDER_RESERVE=7.0` (`api.py:156-186`). Cold worst case ≈ 18s; cached fast path = tens of ms (`COLD_SEARCH_LIVE_TEST_FINDINGS.md` §2: cached ~0.2s, niche cold 3–4s).

**Live baseline NOT obtainable here** (no network to Railway/Decodo/Postgres). Exact repro/measure commands for the fix team:
```bash
# Per-query latency + cold/cached attribution (reads diagnostics.timing.total_search)
for q in "jean paul gaultier" "torino 21" "boss bottled night" "creed aventus"; do
  echo "== $q =="
  curl -s -w "\nHTTP %{http_code} | total %{time_total}s\n" \
    "https://srt-scent-engine-production.up.railway.app/api/fragrances/search?q=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$q")" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print('results',len(d.get('results',[])),'| diag',d.get('diagnostics',{}).get('timing'),d.get('diagnostics',{}).get('cache_source'),d.get('diagnostics',{}).get('live_search_skipped'))"
done

# Full prod verification harness (already in repo)
python scripts/verify_production.py --url https://srt-scent-engine-production.up.railway.app --timeout 30

# Full offline suite (deps: fastapi, beautifulsoup4, python-multipart, pytest)
python run_tests.py
```
Record `diagnostics.timing.total_search`, `cache_source`, and `live_search_skipped` for warm vs cold of each query before and after the fix.

---

## 5. Concurrency & state findings

- **Process model:** single uvicorn process, **no `--workers`** (`Procfile`, `railway.toml`). One event loop + FastAPI's default ~40-thread pool for sync endpoints. `search()` and `details` are sync `def`, so they run in the threadpool.
- **Live-search backpressure:** `_LIVE_SEARCH_SEMAPHORE = BoundedSemaphore(6)` (`api.py:196`, env `API_LIVE_SEARCH_MAX_CONCURRENT`), acquired with a 0.25s queue timeout (`api.py:189`); on failure the request degrades to cache (`api.py:2521`) or 503 if both empty. Brand-sweep and warm-refresh threads **share** this semaphore (`api.py:1366`, warm refresh similar), so speculative work cannot starve user requests — good.
- **DB pool:** `psycopg_pool.ConnectionPool(min_size=1, max_size=DB_POOL_MAX_SIZE=15, autocommit=True)` (`db.py:244-249`). Sized under Railway Postgres's 20-connection cap. With a 40-thread pool a burst of DB-touching requests can exceed 15 and block on pool checkout — a latency tail under load.
- **Shared mutable state / race surface:**
  - Decodo class-dict caches (`…:3690-3694`) guarded by `_cache_lock` (`…` class-level `threading.Lock`). Unbounded growth = memory-leak class flagged in `docs/MEMORY_LEAK_FIX_PLAN.md`.
  - Brand-sweep inflight set `_brand_sweep_inflight` + `_brand_sweep_lock` (`api.py:1300-1302`), capped at `_BRAND_SWEEP_MAX_INFLIGHT=1`; durable once-ever via `claim_brand_sweep` (`db.py:540`). Looks race-safe (lock around the set, DB claim is the real guard).
  - Warm-refresh inflight set + lock (`api.py:1237-1239`), cap 2.
- **In-flight dedup:** there is **no** request-level dedup of identical concurrent search queries (unlike the monorepo image pipeline). Two simultaneous `jean paul gaultier` requests both run live. A brand-result cache (§3) plus a per-canonical-brand inflight guard would dedup these.
- **No obvious deadlock**, but the **brand-only-bypasses-fast-path** issue (RC4) means brand queries disproportionately consume the 6 live slots.

---

## 6. Dirty-data findings + sanitization gaps

**Where sanitization happens**
- **Search serialization:** `_search_result_to_dict` (`api.py:559`) → `_recover_candidate_identity` (`api.py:491`) recovers name/house from canonical FG/BN URL slugs and calls `IdentityTools.strip_house_from_name` (`api.py:536`) to drop a leading house echo. `_candidate_has_display_identity` (`api.py:541`) drops `"Unknown"`/sentinel/house-echo rows. `TextSanitizer.clean` (`…:451`) unescapes HTML, strips zero-width chars, collapses whitespace; `normalize_identity` (`…:462`) does NFKD accent-fold.
- **Enrichment/detail blob:** `scripts/dirty_data.py` is the canonical taxonomy/normalizer for **accords/family/concentration/gender/year** (Title-case accords, `Chypere→Chypre`, drop junk accord tokens via `_ACCORD_JUNK_RE` `:49`, year string→int, flag `name_has_year`/`name_equals_brand`/`name_self_dup` `:199-204`). It is used by `db_audit_export.py` (audit) and `sanitize_wardrobe_accords.py` (write).
- **Repair scripts:** `heal_duplicate_house_names.py` (strips house prefix from `name` across `fg_detail_cache`, `fragrance_records`, `enrichment_jobs`), `purge_junk_globals.py` (deletes brand==name skeleton stubs — but targets `global_fragrances`, the **monorepo/Supabase** catalog, a *different DB* from the engine's `fragrance_records`).

**Gaps (where dirty artifacts can leak into a search response)**
1. **`dirty_data.py` does not run on the search path.** Its name/brand hygiene checks (`name_has_year`, `name_equals_brand`, `name_self_dup`, `:199-204`) are **audit-only flags**, never applied as a write/serialization filter. So a `fragrance_records` row whose `name` already contains the year (e.g. "Boss Bottled Night 2010") or a duplicated house ("Hugo Boss Hugo Boss …") will serialize as-is — `_recover_candidate_identity` only fixes *house echo at the start of the name*, not embedded years or mid-string duplication.
2. **Encoding/whitespace junk on name/brand** relies entirely on `TextSanitizer.clean`; if a record was stored *before* a clean fix (legacy rows), the stored value is served verbatim — `_search_result_to_dict` re-cleans via `_recover_candidate_identity` only when identity "needs recovery" (`_identity_needs_recovery`), not unconditionally. A row with a stray trailing whitespace/HTML entity in `name` that still "looks real" bypasses recovery.
3. **Two-DB confusion:** the engine search reads `fragrance_records`; the dirty-data purge/sanitize scripts mostly target the monorepo `global_fragrances`/wardrobe DB. Cleaning one does not clean the other (see `search_engine/CLAUDE.md` `wardrobe-completeness-heal` two-DB trap). Owner-reported "dirty artifacts" could be coming from un-sanitized `fragrance_records` rows the cleanup scripts never touch.

**Recommended fix direction**
- Apply the **safe** `dirty_data` name/brand normalizers (whitespace collapse, strip embedded year, drop self-duplication, HTML-unescape) as a final read-time pass inside `_search_result_to_dict`/`_recover_candidate_identity` (presentation-only, idempotent — same guarantees `dirty_data.py:9` already promises).
- Add a `fragrance_records`-scoped sanitize sweep (mirror `heal_duplicate_house_names.py`) covering `name_has_year` and encoding junk.
- **Test to add:** feed a `UnifiedFragrance` with name `"Hugo Boss Boss Bottled Night 2010 "` (leading house echo + embedded year + trailing space) and assert `_search_result_to_dict` yields `name="Boss Bottled Night"`, `house="Hugo Boss"`.

---

## 7. How to verify after fix (regression-proof)

**A. Offline (must stay green — these are the regression guard):**
```bash
pip install fastapi beautifulsoup4 python-multipart pytest   # sandbox deps
python -m pytest test_search_relevance.py -q                  # baseline: 68 passed (~4.6s)
python test_search_total_budget.py                            # baseline: 6/6 PASS, chain bounded
python run_tests.py                                           # full suite (needs DB-less stubs/mocks already in repo)
```
Extend these existing tests rather than adding new files where possible:
- `test_multi_token_brand_only_query_keeps_whole_house` (`test_search_relevance.py:200`) — add a DB-fallback variant asserting **all** seeded JPG rows return (not just synthetic single rows).
- `test_brand_alias_query_keeps_house_catalog` (`:895`), `test_brand_only_query_gets_catalog_labels` (`:977`), `test_multi_token_bare_brand_seeds_catalog_labels` (`:999`) — add coverage assertions on `_cache_search_fallback` count.
- `test_bundled_identity_cache_rescues_deploy_repros` (`:1224`) — add `boss bottled night` and `torino 21` repros.
- `test_api_cache_fallback_uses_long_anchor_variants` (`:433`) — extend with a brand-only query served from the new fast path **without** acquiring the live semaphore.

**B. Live (run from a host with prod access):**
```bash
BASE=https://srt-scent-engine-production.up.railway.app
# Symptom 1: expect a FULL JPG catalogue (>> 2), not 2
curl -s "$BASE/api/fragrances/search?q=jean%20paul%20gaultier" | python3 -c "import sys,json;d=json.load(sys.stdin);print('JPG results:',len(d['results']));[print(' -',r['house'],'|',r['name']) for r in d['results'][:20]]"
# Symptom 2: expect the exact 'Torino 21' identity present (and 'Torino' parent not the only row)
curl -s "$BASE/api/fragrances/search?q=torino%2021" | python3 -c "import sys,json;d=json.load(sys.stdin);print([ (r['house'],r['name']) for r in d['results']])"
# Symptom 3: expect >=1 result (Boss Bottled Night), never an empty body / 'no matches'
curl -s "$BASE/api/fragrances/search?q=boss%20bottled%20night" | python3 -c "import sys,json;d=json.load(sys.stdin);print('results:',len(d['results']), d['results'][:3])"
# Symptom 4: repeat a brand query twice; second call must be fast + cache-served
for i in 1 2; do curl -s -w "  total %{time_total}s\n" "$BASE/api/fragrances/search?q=jean%20paul%20gaultier" -o /dev/null; done
curl -s "$BASE/api/fragrances/search?q=jean%20paul%20gaultier" | python3 -c "import sys,json;d=json.load(sys.stdin);print('cache_source',d['diagnostics'].get('cache_source'),'live_skipped',d['diagnostics'].get('live_search_skipped'),'timing',d['diagnostics'].get('timing'))"
```
Pass criteria: Symptom 1 returns ≥ ~20 JPG rows; Symptom 2 includes "Torino 21"; Symptom 3 ≥ 1 row and **no empty body**; Symptom 4 second call `live_search_skipped: true` / `cache_source` set and `total` well under 1s. Also run `python scripts/verify_production.py --url $BASE --timeout 30`.

**C. Dirty-data:** `python scripts/db_audit_export.py` before/after; expect fewer `name_has_year` / `name_self_dup` / `accord_junk` flags. Spot-check a search response's `name`/`house` for stray whitespace/HTML/embedded years.

---

## 8. Appendix — key file:line references

**Search endpoint & flow**
- `api.py:2432` `search()` — `GET /api/fragrances/search` handler
- `api.py:2449` strong-cache precheck; `api.py:2482` warm-cache tier; `api.py:2529` live `search_once`; `api.py:2542` `_cache_search_fallback`
- `api.py:1186` `_strong_cache_search`; `api.py:1172` `_strong_cache_candidate_ok` (**RC4** name-token intersection at `:1181-1183`)
- `api.py:1224` `_can_skip_live_search_with_cache` (requires `frag_url`)
- `api.py:1065` `_cache_search_fallback`; `api.py:880` `_fragrance_record_search`
- `api.py:970/998/1040` `_json_cache_search` / `_identity_cache_search` / `_db_detail_cache_search`
- `api.py:559` `_search_result_to_dict`; `api.py:491` `_recover_candidate_identity`; `api.py:541` `_candidate_has_display_identity`
- `api.py:906` `_persist_search_results`
- `api.py:1324` `_maybe_spawn_brand_sweep`; `api.py:1308` `_brand_sweep_key`

**Budgets / caching flags / concurrency**
- `api.py:116-122` `_ALLOW_BUNDLED_FG_SEARCH_CACHE` / `_ALLOW_BUNDLED_FG_DETAIL_CACHE` (**RC3**, disabled when `DATABASE_URL` set)
- `api.py:129/136` `_STRONG_CACHE_MIN_SCORE=0.95` / `_WARM_CACHE_MIN_SCORE=0.85`
- `api.py:156-186` API budgets (`API_INITIAL_TIMEOUT`/`FG_TIMEOUT`/`SPELL_REPAIR_BUDGET`/`SEARCH_TOTAL_BUDGET=18`/`DESIGNER_PROVIDER_RESERVE=7`)
- `api.py:188-196` `_LIVE_SEARCH_MAX_CONCURRENT=6` semaphore; `api.py:212/863` `FRAGRANCE_RECORD_TTL_HOURS=168` staleness

**Engine scoring / tokenization**
- `…:11106` `search_once`; `…:10795` `_search_core`; `…:10965` `_designer_provider_fallback_results`
- `…:11389` `--max-results` default **15** (**RC5**); `…:11380` `build_parser`
- `…:561-676` `IdentityTools` (`STOPWORDS`, `BRAND_ALIASES` incl. JPG `:618-619`, Hugo Boss `:629-630`; `QUERY_LINE_HOUSE_HINTS` incl. `bottled night→Hugo Boss` `:666`)
- `…:758` `brand_forms`; `…:880` `expand_query_aliases`; `…:934` `canonical_brand_query`; `…:984` `catalog_brand_keys`
- `…:1055` `query_is_brand_only`; `…:1132` `query_name_tokens`; `…:1159` `fuzzy_token_coverage` (digit/short-token: no fuzz for `len<4`, `:1168/1170`)
- `…:1191` `relevance_score` (brand-only → 1.0 at `:1202`); `…:3007` `filter_relevant_candidates`; `…:3021` `candidate_relevance_ok` (brand-only short-circuit `:3046`; distinctive-coverage `:3082`)
- `…:1731` `needs_repair`; `…:1272` `MIN_USEFUL_TOP_SCORE=0.72`
- `…:451` `TextSanitizer.clean`; `…:462` `normalize_identity` (digits kept, `:469`)
- `…:3690-3694` `DecodoScraperClient` in-process caches (**RC6**, unbounded, TTL-less)

**DB**
- `db.py:1466` `search_fragrance_records` (**RC2** `%query%` ILIKE, `:1511-1523`); `db.py:1213` `_identity_search_term`
- `db.py:1696` `search_detail_cache`; `db.py:244-249` connection pool; `db.py:56` `DEFAULT_DB_POOL_MAX_SIZE=15`
- `db.py:224` `brand_sweeps` table; `db.py:540` `claim_brand_sweep`; `db.py:571` `mark_brand_sweep_done`

**Deploy / docs**
- `Procfile`, `railway.toml` (single uvicorn worker, no `--workers`); `nixpacks.toml` (Chromium disabled while serving, `DISABLE_CHROMIUM_MINT=1`)
- `COLD_SEARCH_LIVE_TEST_FINDINGS.md` (live cold-search hang + 18s ceiling); `API_CONTRACT.md` (`/search` shape); `docs/MEMORY_LEAK_FIX_PLAN.md` (unbounded cache class)

**Dirty-data**
- `scripts/dirty_data.py:49` `_ACCORD_JUNK_RE`; `:129` `audit_row` (name/brand flags `:199-204`); `:90` `normalize_row`
- `scripts/heal_duplicate_house_names.py` (house-prefix strip); `scripts/purge_junk_globals.py` (targets `global_fragrances`, NOT engine `fragrance_records`)

**Tests (baselines run this audit)**
- `test_search_relevance.py` — 68/68 pass (4.56s). Brand-relevant: `:200`, `:895`, `:977`, `:999`, `:1224`, `:433`.
- `test_search_total_budget.py` — 6/6 pass; chain bounded at the configured ceiling.
