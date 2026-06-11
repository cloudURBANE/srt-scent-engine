# Fragrance Data Completeness Research And Fix Plan

Date: 2026-06-11  
Workspace: `C:\Users\urban\my_project_workspace\search_engine`  
Adjacent app checked: `C:\Users\urban\my_project_workspace\huge_monorepo`

## Executive Summary

The current data gaps are not one single parser bug. They come from three separate layers that do not share one canonical completeness contract:

1. The Python engine can parse or derive many of the needed facts, but its canonical `UnifiedDetails` model only has `notes`, `gender`, `description`, `bn_consensus`, `frag_cards`, `pros_cons`, `reviews`, and `derived_metrics`. It has no first-class `family`, `environment`, `season`, `time`, or `concentration` fields. `concentration` is attached dynamically with `setattr`.
2. The durable engine cache stores concentration only inside JSON blobs (`raw_identity_json` / `fg_raw_json`), not as a column. Existing `fg_detail_cache` rows have zero concentration coverage, so responses hydrate as `None` unless another layer fills it.
3. The app/profile layer writes defaults: `Unknown Family` in the API-server profile builder and `Universal` season/environment in the frontend/vectorizer path. Those defaults then persist as if they were facts.

The fix should be a unified self-healing enrichment loop that treats every displayed metric as a typed field with provenance, confidence, and a retry policy. Do not keep adding one-off repair scripts that write some fields flat, some nested, and some only in source caches.

## Verified Live Coverage

I ran read-only aggregate SQL against the configured Postgres database. No row-level fragrance data or secrets were printed.

### `user_fragrances` coverage

Total rows: 123

| Field | Missing / Default Count |
|---|---:|
| year | 59 |
| gender blank or `Unisex / Unspecified` | 56 |
| concentration blank or `Unknown` | 70 |
| environment blank or `Universal` | 123 |
| derived_metrics | 0 |
| derived_metrics.wear_profile | 1 |
| derived_metrics.main_accords | 1 |
| family key absent | 0 |

Family is technically present on every row, but 80 / 123 are `Unknown Family`.

### `global_fragrances` coverage

Total rows: 177

| Field | Missing / Default Count |
|---|---:|
| brand | 1 |
| year | 177 |
| gender blank or `Unisex / Unspecified` | 177 |
| concentration blank or `Unknown` | 145 |
| environment blank or `Universal` | 177 |
| derived_metrics | 153 |
| derived_metrics.wear_profile | 153 |
| derived_metrics.main_accords | 153 |
| family key absent | 0 |

Family is present on every row, but 100 / 177 are `Unknown Family`. `profile_data.product` only has `brand`, `name`, and sometimes `perfumer`; year/gender/concentration are currently top-level profile fields when present, not product fields.

### Engine durable cache coverage

`fragrance_records`: 126 rows

| Field | Missing / Default Count |
|---|---:|
| year | 20 |
| gender blank or `Unisex / Unspecified` | 107 |
| concentration blank or `Unknown` | 126 |
| derived_metrics | 116 |
| wear_profile | 116 |
| main_accords | 116 |
| canonical_fg_url | 2 |
| bn_url | 85 |

`fg_detail_cache`: 103 rows

| Field | Missing / Default Count |
|---|---:|
| quality_status = complete | 102 |
| quality_status = partial | 1 |
| year | 21 |
| concentration blank or `Unknown` | 103 |
| frag_cards | 0 |
| wear card | 1 |
| main accords card | 0 |
| gender card | 84 |

`enrichment_jobs`: 35 completed, 89 pending.

## How The Engine Is Supposed To Extract Each Field

### Year

Search candidates carry `UnifiedFragrance.year` as a string. API responses coerce it to an integer when possible via `api._coerce_year`.

Primary sources:

- Fragrantica search/card parsing in `fragrance_parser_full_rewrite_fixed.py`.
- `heal_missing_gender_and_year()` parses launch year from description phrases like `launched in 2012`, then any valid 1700-2099 year, then a last-resort year in the fragrance name.
- Parfumo fallback parses year from `og:description`.

Persistence:

- `fg_detail_cache.year` column.
- `fragrance_records.year` column.
- App rows can store flat `year`; `global_fragrances.profile_data.product.year` is currently absent for all rows in the live audit.

Risk:

- Global rows never got year copied into the app profile schema.
- Existing complete cache rows may have enough description/status evidence but still have null year because the worker/API completion path did not heal them historically.

### Gender

Primary sources:

- Fragrantica status payload's `gender` distribution becomes a `Gender` frag card.
- `heal_missing_gender_and_year()` maps dominant `Gender` label to `Feminine`, `Masculine`, or `Unisex`.
- If the card is missing, it parses description text (`for women`, `for men`, `women and men`, `unisex fragrance`) and then name markers (`pour homme`, `femme`, etc.).
- Parfumo fallback parses gender from page description.

Persistence:

- `fragrance_records.gender` column.
- `fg_detail_cache` has no gender column, but complete job payloads pass gender into aggregate `fragrance_records`.
- App rows store flat `gender` when updated.

Risk:

- 84 / 103 `fg_detail_cache` rows have no `Gender` card. That is not necessarily fatal because description/name fallback can still work, but `fg_detail_cache` does not store description, so old cache rows may not have enough raw evidence to re-heal gender.
- `fragrance_records` has 107 / 126 default gender values even though `fg_detail_cache` has complete parsed cards for most rows. The aggregate write path needs a backfill from cache + `heal_missing_gender_and_year()`.

### Concentration

Concentration is not parsed from Fragrantica detail by the main parser. It is a separate best-effort resolver.

Primary sources, in order:

- Parfinity catalog lookup (`parfinity.lookup_concentration`).
- Explicit tokens in query/name (`EDP`, `Eau de Parfum`, `Extrait`, etc.).
- Brand prior and pillar prior from `concentration_grabber.SemanticScentEngine`.
- Slow SERP consensus in the local worker (`SemanticScentEngine.analyze()`).

Current implementation:

- `scripts/enrichment_worker.py` resolves concentration and writes it to the `/complete` payload top-level `concentration` and `raw_identity.concentration`.
- `api.py` accepts worker concentration and stores it inside `fg_detail_cache.raw_identity_json` and `fragrance_records.fg_raw_json`.
- `api._details_from_fragrance_record()` hydrates concentration back from `fg_raw.concentration` or `raw_identity.concentration`.
- `scripts/enrich_concentration.py` backfills app `user_fragrances` and matching `global_fragrances`, but not durable engine cache rows.

Observed problem:

- Live `fg_detail_cache` has 103 / 103 missing concentration and `fragrance_records` has 126 / 126 missing concentration.
- Therefore existing rows were completed before concentration was attached, completed through a path that did not apply concentration, or never reprocessed after that code landed.
- `scripts/enrich_database_metrics.py` treats `Unknown` concentration as complete in `_base_fields_complete()`, so cheap failed attempts can make rows stop healing in that sweep.

### Environment / Season / Time

The Python engine does not store a field called `environment`.

Closest engine metric:

- Fragrantica `When to wear` / `Season` card is parsed into `frag_cards`.
- `derived_metrics_adapter._wear_profile()` converts that into:
  - `primary_seasons`
  - `primary_time`
  - raw season/time votes

App defaults:

- `FragranceCapture.tsx` writes `season: 'Universal'` when saving a fragrance.
- `WardrobeContext.tsx` normalizes missing `season` to `Universal`.
- `scentVectorizer.calculateContext()` pushes `Universal` when vector weather is empty or when woodiness/freshness rules say it is broad.

Observed problem:

- Live audit shows every user and global row is blank/Universal for environment/season. That means the saved app row is not using engine `derived_metrics.wear_profile.primary_seasons` / `primary_time` as the canonical display source.

Recommendation:

- Do not call this field `environment` unless it is explicitly defined. Use a normalized `wear_profile` object with `seasons`, `time_of_day`, `weather`, `occasion`, `source`, and `confidence`.
- If the UI must display a one-word environment chip, derive it from `wear_profile` at read time and treat `Universal` as a low-confidence fallback, not a completed field.

### Family / Families

The Python engine does not have a first-class `family` field on `UnifiedDetails`.

Current sources:

- `derived_metrics.main_accords.top_accords` from Fragrantica `Main accords`.
- App API-server profile builder chooses `finalFamily` from dataset match, engine fallback family, or `"Unknown Family"`.
- `FragranceCapture.tsx` sends `detail.family` to the profile endpoint only if the engine detail has `family`; the engine response does not currently include it.
- `scripts/enrich_database_metrics.py` tries to read `details.family`, but `UnifiedDetails` has no such dataclass field. In normal Python this can raise `AttributeError` on that branch unless something dynamically added it.

Observed problem:

- Family key exists on all app rows, but 80 / 123 user rows and 100 / 177 global rows are `Unknown Family`.
- That is consistent with app fallback behavior, not with the engine having failed to parse main accords: `fg_detail_cache` has `Main accords` for 103 / 103 rows.

Recommendation:

- Replace `Unknown Family` persistence with a derivation from `derived_metrics.main_accords.top_accords`.
- Define a mapping from accords to canonical family/families, e.g. `woody`, `amber`, `floral`, `fresh`, `citrus`, `aromatic`, `spicy`, `aquatic`, `green`, `leather`, `gourmand`, `chypre`, `fougere`, `musk`.
- Store both `primary_family` and `families[]` with provenance. Do not persist `Unknown Family`; absence is cleaner.

## Main Root Causes

### 1. No Single Canonical Completeness Contract

Completeness is checked differently in different places:

- API source coverage checks Fragrantica/Basenotes/derived metrics.
- `scripts/enrich_database_metrics.py` checks `gender`, `year`, and `concentration`, but counts `Unknown` as complete.
- App rows have persisted `Unknown Family` and `Universal` defaults, which makes a value present but not accurate.

### 2. Durable Engine Cache Does Not Backfill Existing Concentration

The worker now has a concentration resolver, but existing cache rows have no concentration in `raw_identity_json`. Nothing in `scripts/enrich_concentration.py` updates `fg_detail_cache` or `fragrance_records`.

### 3. App Save Path Ignores Wear Profile For Season/Environment

The engine already exposes `derived_metrics.wear_profile`. The app save path still writes `season: 'Universal'`, so every saved row loses the actual `When to wear` signal.

### 4. Family Is Treated As A Dataset Field, Not An Engine-Derived Field

The engine derives main accords reliably when Fragrantica cards are present. The app then persists `Unknown Family` because it expects `detail.family`, which the engine does not provide.

### 5. Global Profile Shape Is Incomplete

`global_fragrances.profile_data.product` currently has only `brand`, `name`, and sometimes `perfumer`. Year, gender, and concentration live top-level when present, but all 177 audited global rows are missing year/gender and 145 are missing concentration.

## Proposed Target Contract

Create one canonical enrichment payload per fragrance identity:

```json
{
  "identity": {
    "name": "...",
    "house": "...",
    "year": 1995,
    "gender": "Unisex",
    "concentration": "Eau de Parfum"
  },
  "taxonomy": {
    "primary_family": "Woody",
    "families": ["Woody", "Aromatic"],
    "accords": ["woody", "aromatic", "citrus"]
  },
  "wear_profile": {
    "primary_seasons": ["Spring", "Summer"],
    "primary_time": "Day",
    "weather": ["Warm", "Mild"],
    "occasion": ["Casual", "Daytime"]
  },
  "provenance": {
    "year": {"source": "fragrantica_description", "confidence": 80},
    "gender": {"source": "fragrantica_gender_votes", "confidence": 90},
    "concentration": {"source": "parfinity_catalog", "confidence": 95},
    "families": {"source": "main_accords_mapping", "confidence": 85},
    "wear_profile": {"source": "fragrantica_when_to_wear_votes", "confidence": 90}
  }
}
```

This can live inside `fragrance_records` first as JSONB, then be projected into `user_fragrances` / `global_fragrances`.

## Implementation Plan

### Phase 1 - Build A Read-Only Completeness Auditor

Add `scripts/audit_fragrance_completeness.py`.

Requirements:

- Read `user_fragrances`, `global_fragrances`, `fg_detail_cache`, `fragrance_records`.
- Normalize identity as `house|name|year?` plus source URLs.
- Report coverage for:
  - name
  - house
  - year
  - gender
  - concentration
  - primary_family
  - families
  - main_accords
  - notes
  - wear_profile.primary_seasons
  - wear_profile.primary_time
  - environment/season display value
  - derived_metrics group coverage
- Treat these as incomplete:
  - blank/null
  - `Unknown`
  - `Unknown Family`
  - `Unisex / Unspecified`
  - `Universal` when it is the only environment/season signal and no wear votes support it
- Output aggregate counts plus a CSV/JSON list of incomplete identities for the worker.

### Phase 2 - Add Canonical Derivers

Create a small shared Python module, for example `enrichment_facts.py`, with pure functions:

- `derive_gender(selected, details)`: wraps current heal logic and returns value + provenance.
- `derive_year(selected, details, raw_identity)`: wraps current year healing with source tag.
- `derive_concentration(house, name, description, allow_serp)`: wraps worker/script tiering.
- `derive_families(details, existing_family)`: maps `derived_metrics.main_accords.top_accords` to `primary_family` and `families[]`.
- `derive_wear_profile(details, existing_context)`: uses `derived_metrics.wear_profile` first, then vectorizer context as fallback.
- `is_fact_complete(value, field)`: one common completeness predicate.

Keep this pure and testable. The worker, API cache hydration, and backfill scripts should call the same functions.

### Phase 3 - Fix Persistence

Engine DB:

- Add a JSONB `facts_json` or reuse `fg_raw_json.raw_identity` carefully for facts + provenance.
- Backfill `fragrance_records` from `fg_detail_cache` by reconstructing `UnifiedDetails`, running `build_derived_metrics()`, `heal_missing_gender_and_year()`, concentration resolver, family derivation, and wear derivation.
- Update `db.complete_job()` / `_upsert_completed_fragrance_record()` so every future completed job writes these facts consistently.

App DB:

- Update both flat and nested shapes consistently:
  - `user_fragrances.fragrance_data.year/gender/concentration/family/families/context/season`
  - `global_fragrances.profile_data.year/gender/concentration/family/families/context`
  - optionally also `profile_data.product.year/gender/concentration` if the frontend expects product identity there.
- Stop writing `Unknown Family` as a durable value.
- Stop writing `season: 'Universal'` on save when engine `wear_profile` exists. Prefer `primary_seasons[0]`, or store `season` as blank and let the display layer show `Universal` only as a fallback.

### Phase 4 - Backfill In Priority Order

1. Use existing `fg_detail_cache` rows first. They have cards and accords for almost all complete entries.
2. Use `fragrance_records` rows next.
3. Use Parfinity cache for concentration and notes where available.
4. Requeue pending jobs for rows with no Fragrantica detail or missing status cards.
5. Use slow SERP concentration only for rows still `Unknown` after cheap tiers.

Important: do not mark a field complete just because a fallback wrote `Unknown` or `Universal`.

### Phase 5 - Self-Healing Loop

Add a scheduled or operator-run sweep:

- Run the auditor.
- For each incomplete identity, determine the cheapest repair action:
  - local derivation from existing metrics
  - concentration-only backfill
  - cache reconstruction from `fg_detail_cache`
  - worker requeue
  - manual review for conflicts
- Write only fields with better provenance or higher confidence than existing values.
- Store `last_checked_at`, `last_enriched_at`, `field_status`, and `provenance` so the same row does not churn forever.

## Specific Code Change Targets

### Search engine repo

- `fragrance_parser_full_rewrite_fixed.py`
  - Consider adding explicit optional fields to `UnifiedDetails`: `concentration`, `family`, `families`, `wear_profile`, `provenance`.
  - Or keep dataclass stable and centralize dynamic fields in `facts_json`.
- `derived_metrics_adapter.py`
  - Keep `wear_profile` and `main_accords` as the canonical source for season/time/family derivation.
- `scripts/enrichment_worker.py`
  - Continue concentration resolution, but ensure all completed payloads include facts/provenance.
- `api.py`
  - Include derived `family/families/wear_profile` in detail responses if the frontend needs them directly.
  - Ensure hydration from aggregate DB produces the same facts as live fetch.
- `db.py`
  - Persist facts consistently during `complete_job()` and `upsert_fragrance_details()`.
- `scripts/enrich_concentration.py`
  - Expand beyond app tables or replace with a unified `enrich_facts.py`.
- `scripts/enrich_database_metrics.py`
  - Change completeness logic so `Unknown`, `Unknown Family`, and unsupported `Universal` are incomplete.
  - Remove unsafe `details.family` assumption unless the engine model is updated.

### Adjacent app repo

- `artifacts/api-server/src/services/scentEngineCore.ts`
  - Replace `|| "Unknown Family"` persistence with absent/null plus derived families from engine accords.
- `artifacts/api-server/src/services/scentVectorizer.ts`
  - Keep `Universal` as a display/context fallback, not proof that environment is known.
- `artifacts/scent-cast/src/components/FragranceCapture.tsx`
  - Replace `season: 'Universal'` with engine-derived season when available.
- `artifacts/scent-cast/src/context/WardrobeContext.tsx`
  - Keep guest display fallback if needed, but do not write it as canonical enrichment.

## Recommended Tests

No browser scenario tests are needed for the first implementation pass.

Add focused unit/integration tests:

- `derived_metrics_adapter._wear_profile()` maps `When to wear` votes to `primary_seasons` and `primary_time`.
- Family derivation maps main accords to `primary_family` and `families[]`.
- Completeness predicate marks `Unknown`, `Unknown Family`, default gender, and unsupported `Universal` incomplete.
- Worker `/complete` payload with concentration stores concentration in durable cache and aggregate records.
- Existing `fg_detail_cache` row can reconstruct app facts without live scraping.
- App profile builder does not persist `Unknown Family` when engine accords exist.
- Fragrance capture does not overwrite engine season with `Universal`.

## Open Questions For The Implementing Agent

1. Should `environment` be renamed in the DB/UI to `wear_profile`, or do we need to keep a legacy `environment` string for compatibility?
2. Should `global_fragrances.profile_data.product` be expanded with year/gender/concentration, or should those remain top-level under `profile_data`?
3. What confidence threshold should allow concentration SERP results to overwrite existing `Unknown` values? Current worker accepts `>= 50`; the app fast resolver uses `>= 75`.
4. Should Parfumo fallback facts be allowed to complete year/gender/family when Fragrantica is unavailable, even though Fragrantica-derived status metrics remain partial?

## Bottom Line

The engine already has strong raw signals for accords and wear profile when Fragrantica details are present. The database is weak because those signals are not being projected into the canonical app fields, and because historical cache rows predate concentration persistence. Fix the contract first, then backfill from existing cache before spending worker/browser time on re-scraping.

## Implementation Update - 2026-06-11

Completed in `search_engine`:

- Added `enrichment_facts.py` with pure helpers for:
  - completeness checks that treat `Unknown`, `Unknown Family`, `Unisex / Unspecified`, and unsupported `Universal` as incomplete;
  - family derivation from `derived_metrics.main_accords.top_accords`;
  - wear-profile extraction from `derived_metrics.wear_profile`.
- Updated `scripts/enrich_database_metrics.py` so:
  - base-field completeness no longer treats `Unknown` concentration as complete;
  - default gender is incomplete;
  - `details.family` is no longer accessed, because `UnifiedDetails` has no first-class `family` field;
  - app/profile payloads get `family`, `primary_family`, `families`, `accords`, `family_meta`, `wear_profile`, `context.wear_profile`, and an engine-derived `season` when available.
- Updated `db.complete_job()` / aggregate completion handling so worker/script top-level `concentration` and `concentration_meta` are merged into `fg_detail_cache.raw_identity_json` and `fragrance_records.fg_raw_json`.
- Expanded `scripts/enrich_concentration.py` so resolved concentration backfills missing/`Unknown` values in durable engine cache tables (`fg_detail_cache`, `fragrance_records`) in addition to app rows.
- Updated API detail serialization to expose derived `family`, `families`, and `wear_profile` from existing `derived_metrics` without pretending these are native parser fields.
- Added focused non-browser checks in `test_enrichment.py` for completeness defaults, family/wear projection, API detail projection, and durable concentration merge.

Verification:

- `python -m py_compile enrichment_facts.py db.py api.py scripts/enrich_database_metrics.py scripts/enrich_concentration.py test_enrichment.py`
- `python test_enrichment.py`
  - Passed.
  - DB-backed lifecycle checks were skipped because `DATABASE_URL` was not set in the shell.
- `python test_parfinity.py`
  - Passed.

## Implementation Update 2 - 2026-06-11 (self-healing sweep)

Completed in `search_engine`:

- Added a granular per-fragrance fact contract in `enrichment_facts.py`:
  `FACT_FIELDS` (name, house, year, gender, concentration, family,
  main_accords, wear_profile, performance_score, value_score,
  community_interest_score, notes, reviews), `record_fact_status()`, and
  `missing_facts()` over `fragrance_records`-shaped rows.
- Added `db.list_fragrance_records()` / `db.count_fragrance_records()` /
  `db.get_jobs_by_keys()` for paged audits with batched job-state annotation.
- Added two token-authed management endpoints (`ENRICHMENT_WORKER_TOKEN`):
  - `GET /api/enrichment/completeness` -- read-only granular audit; per
    fragrance it lists exactly which facts are missing and the durable job
    state for that identity.
  - `POST /api/enrichment/heal` -- the self-healing sweep; idempotently
    reopens an enrichment job (via `recover_or_enqueue_job`) for every
    fragrance missing any audited fact. Supports `dry_run`, `fields` filter,
    `priority` (default 5, below interactive recoveries at 10),
    `limit`/`offset` paging, and `max_requested_count` churn protection.
- Wired the worker's auto-approve loop to call the heal sweep whenever the
  queue is empty (`ENRICHMENT_HEAL_SWEEP_MINUTES`, default 30, `0` disables;
  `ENRICHMENT_HEAL_SWEEP_MAX_REQUESTED`, default 8), paging through the table
  across idle ticks -- so a deployed API + running worker continuously detect
  and repair incomplete fragrances with no operator action.
- Added focused checks in `test_enrichment.py` for the granular fact audit,
  endpoint auth, dry-run/field-filter/churn-guard behavior, and the worker's
  throttled, paged idle-tick sweep.

Still left:

- Run a real DB dry-run/audit with `DATABASE_URL` configured to quantify post-change coverage (`GET /api/enrichment/completeness` now does this remotely).
- Backfill existing historical rows:
  - run concentration tier-1 backfill against app rows and engine cache rows;
  - run metrics/profile projection from existing `fg_detail_cache` rows before re-scraping.
- Decide the app compatibility shape for legacy `environment` versus canonical `wear_profile`.
- Update the adjacent app repo so save/profile paths stop persisting `Unknown Family` and unsupported `Universal`.
- Decide whether `global_fragrances.profile_data.product` should duplicate year/gender/concentration or keep those facts only top-level.
- Add DB-backed tests for the new `scripts/enrich_concentration.py` engine-cache updates once a disposable test database is available.
