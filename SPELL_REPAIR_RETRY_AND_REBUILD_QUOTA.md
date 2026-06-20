# Spell-repair retry fix + the per-user "3 rebuilds/day" quota

_2026-06-20. Engine: `search_engine` (FastAPI). Companion to the perf work on
`perf/identity-cache-memo`._

## TL;DR

Two asks were on the table:

1. **Speed up end-to-end search** with no regression.
2. **"Google retry logic doesn't work in prod; each user should get 3 rebuilds
   a day from improper querying."** Confirmed symptom: **misspelled queries
   return nothing in prod.**

The reported prod failure is an **under-firing** bug (the retry never resolves),
not an over-firing one. A per-user 3/day quota is a _cost cap_ that only matters
once retries fire **and** become expensive — it would not have fixed "returns
nothing." So this pass fixes the firing (engine) and **recommends** where the
quota belongs (the web API, not the engine). See the layer recommendation below.

## Root cause of "misspelled queries return nothing"

The "Google retry" for an improper query is the engine's spell-repair leg:
`QueryRepair.suggest()` → structured Decodo SERP discovery
(`fragrance_parser_full_rewrite_fixed.py`). On the Fragrantica-blocked datacenter
runtime, the **only** leg that actually corrects a typo is the structured Decodo
call inside `suggest()`; the plain Google/Bing HTML scrapes return JS-shell pages
with no result links.

`search_once()` runs, in order, under one overall wall-clock deadline
(`API_SEARCH_TOTAL_BUDGET`, 18s):

1. first-pass `_search_core` (~5.5s),
2. **early** structured designer/brand discovery
   (`_designer_provider_fallback_results`, gated on `designer_provider_reserve`),
3. spell repair (`QueryRepair.suggest`, budget `API_SPELL_REPAIR_BUDGET`=4s),
4. designer-catalog crawl, then a final designer pass.

For a **genuine typo** there is no real brand, so step 2 resolves nothing — but
its budget was `min(~16.5s, overall.remaining())`, i.e. it could spend nearly the
whole remaining budget on doomed brand-URL lookups. That left step 3 below the
`STRUCTURED_MIN_BUDGET` (~1.2s) floor, so `suggest()` skipped its Decodo leg, the
HTML scrapes returned nothing, and the query came back empty. This is the
"decreasing Decodo read-timeouts = budget-starvation" signature.

## Fix (shipped on this branch)

Add a symmetric reserve so the early designer leg cannot starve spell repair —
mirroring how the catalog crawl already reserves budget for the final designer
pass.

- New knob `spell_repair_reserve` (CLI `--spell-repair-reserve`, default **0.0** =
  prior behavior byte-for-byte; env `API_SPELL_REPAIR_RESERVE`, prod default
  **5.0s**).
- `_designer_provider_fallback_results(..., reserve_after=)` holds that slice back
  from its own budget (`usable = remaining - reserve_after`; yields entirely when
  there's no runway for both).
- `search_once` passes `reserve_after=spell_repair_reserve` to the **early**
  designer leg only (the final designer pass keeps the full remainder — nothing
  runs after it).

Result with prod budgets (18s total, ~5.5s first pass → ~12.5s left): the early
designer leg gets ~7.5s (real brands resolve in ~2-4s, so no cold-brand
regression), and spell repair is **guaranteed** its ~4s → the Decodo leg fires
and a typo self-corrects. Set `API_SPELL_REPAIR_RESERVE=0` to instantly revert
without a redeploy.

Tests: `test_search_total_budget.py` (`test_designer_reserve_after_holds_back_budget`,
`test_designer_reserve_after_skips_when_no_runway`,
`test_search_once_reserves_spell_repair_budget`, `test_zero_spell_reserve_unchanged`).

## Perf (this branch, no regression)

The live search path (`_search_core`) constructed a fresh `IdentityCache` per
search — the same disk-read-and-parse constructor the api.py loader was already
memoized to avoid (commit 73f6cee), but on the **live** path it was still
un-memoized (and runs twice for a spell-repaired query). Now it reuses a
process-shared, thread-safe instance per resolved path (`_shared_identity_cache`),
which also removes the `save()` file-replace race between concurrent searches.
Tests: `test_shared_identity_cache.py`.

## Recommendation: where the per-user "3 rebuilds/day" quota belongs

**The web API (`huge_monorepo`), not the engine.** Rationale:

- The engine's `/api/fragrances/search` is **stateless** and carries **no user
  identity** — there is no per-user, per-day counter anywhere in `search_engine`,
  and adding user state + a durable daily counter to a stateless search service is
  exactly the kind of change that risks regressions on the hot path.
- `huge_monorepo` already has user sessions + Postgres, so a 3/day cap is a small,
  isolated addition there: count a user's "rebuild" events (a search that
  triggered the expensive spell-repair/Decodo retry) per UTC day; past 3, call the
  engine in a **cheap mode** (no paid retry) and surface a "couldn't find it"
  state instead of spending another Decodo credit.

Suggested contract (no breaking change to the existing response shape): the engine
already returns `diagnostics.fallback_source` / timing and could expose a boolean
like `diagnostics.paid_retry_used`; `huge_monorepo` increments the per-user daily
counter when that's true. To _suppress_ the paid retry once a user is over quota,
pass the existing budget knobs per request (e.g. effectively
`API_SPELL_REPAIR_RESERVE`/`spell_repair_budget=0` semantics) — wiring a request
param would be a deliberate, reviewed `cross-service-contract` change and is left
as the follow-up, since the prod symptom that was actually reported ("returns
nothing") is fixed by making the retry fire reliably above.

**Net:** engine = make the retry reliable (done). `huge_monorepo` = own the 3/day
per-user budget (follow-up, scoped above).
