---
name: wardrobe-completeness-heal
description: >
  Use when the owner reports a fragrance shows no year / concentration /
  accords / metric cards, or "the app isn't improving even though drains
  ran". Covers the two-DB heal trap (engine viaduct vs Supabase wardrobe),
  the recovery passes, and how to verify a fragrance on the account.
---

# Wardrobe completeness heal

## The two-DB heal trap (read this before running anything)

There are **two databases** and the heal only helps if it writes to the right one:

- **Engine viaduct** — the engine cache (`fragrance_records`,
  `fg_detail_cache`, `enrichment_jobs`). This is the `DATABASE_URL` Railway
  injects for the engine service (`railway run -s lively-adaptation`).
  It has **no** `user_fragrances` table.
- **Supabase wardrobe** — the DB the app actually reads:
  `user_fragrances.fragrance_data`, `global_fragrances.profile_data`. These
  store family/concentration **statically**; healing the engine cache does
  NOT auto-fix them.

**The trap:** launching a drain via `railway run` from a shell that doesn't
already export `DATABASE_URL` makes `heal_offline.py` project against the
viaduct — which silently no-ops for the app. Every drain "succeeds", the
catalog (`global_fragrances`) still moves via the API upload path, but the
owner's own rows never improve. That is exactly the "app isn't improving even
though drains ran" report.

**The fix is the committed wrapper** `scripts/heal_app_wardrobe.py`: it remaps
the injected viaduct DSN to `ENGINE_DATABASE_URL` (read-only fold source) and
points `DATABASE_URL` at the Supabase wardrobe from `.env` /
`huge_monorepo/.env`:

```powershell
railway run -s lively-adaptation -- <abs venv python> scripts/heal_app_wardrobe.py --dry-run
```

Check the `engine_db+<N>` fold count in the output: N must be the full engine
row count (~1300+), **not 0** — that proves the viaduct facts were folded.

## Why tiles read "Unknown" (root cause)

- **Family is derived, not stored** — computed from
  `fragrance_records.derived_metrics_json -> main_accords` via
  `enrichment_facts.derive_families`. Every raw upsert deliberately NULLs
  `derived_metrics_json`; only a worker `/complete` or a read-time `/details`
  hydrate refills it.
- **Concentration has no column** — rides in `fg_raw_json.concentration`.
- **Wardrobe copies are static** — refreshed only when that detail is re-opened.

## Recovery passes (idempotent; full detail in ENRICHMENT_RUNBOOK.md)

1. **Offline heal** (no network; fills only Unknown values, re-run is a no-op):
   `scripts/heal_offline.py --dry-run` then without `--dry-run` — heals engine
   cache family+concentration and projects into wardrobe rows by **exact**
   brand+name match. Against the app wardrobe, run it through
   `heal_app_wardrobe.py` as above.
2. **Seed** the wardrobe rows with no engine record (can't be projected —
   must be scraped): `heal_offline.py --steps seed --dry-run` then execute.
3. **Live worker drain** from a **residential IP** (Fragrantica blocks the
   Railway datacenter IP; on Railway the worker exits at the clearance
   preflight): `scripts/enrichment_worker.py --process-pending --limit 5
   --delay 90 --jitter 20` (never lower the delay for bulk runs).
4. **Re-project**: re-run pass 1 so freshly scraped facts land on the
   matching wardrobe rows. Loop 3–4 until drained.

## Verify on the account

```sql
-- engine cache: should trend to ~0 (only genuinely empty-raw rows remain)
SELECT count(*) FROM fragrance_records
WHERE derived_metrics_json IS NULL AND fg_raw_json <> '{}'::jsonb;

-- wardrobe Unknowns should fall after passes 1 + 4
SELECT count(*) FILTER (WHERE fragrance_data->>'family'        IS NULL OR lower(fragrance_data->>'family')        IN ('','unknown','unknown family')) AS fam_unknown,
       count(*) FILTER (WHERE fragrance_data->>'concentration' IS NULL OR lower(fragrance_data->>'concentration') IN ('','unknown'))                 AS conc_unknown
FROM user_fragrances;
```

Then spot-check the reported fragrance in the app (or
`GET /api/fragrances/details`): year, concentration, accords, and metric
cards should render; `source_coverage.complete` honest (the SPA's self-heal
gates on it — do not fake it true).

Scoped single-record heal for a specific complaint:

```powershell
.\.venv\Scripts\python.exe scripts\heal_offline.py --record-key "fg:https://www.fragrantica.com/perfume/<House>/<Name>-<id>.html"
```
