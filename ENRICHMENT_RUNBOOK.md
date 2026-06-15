# Enrichment runbook — healing "Unknown" family & concentration

This is the operational runbook for clearing the "Unknown family / Unknown
concentration" backlog and keeping it cleared. It runs from a machine with a
**residential IP** (Fragrantica blocks the Railway datacenter IP), e.g. the dev
laptop.

## Why fragrances read "Unknown" (root cause)

- **Family is derived, not stored.** It is computed from
  `fragrance_records.derived_metrics_json -> main_accords` via
  `enrichment_facts.derive_families`. Every raw upsert deliberately NULLs
  `derived_metrics_json` (so a stale blob is never served), and only a live
  worker `/complete` or a read-time `/details` hydrate refills it — so the cached
  backlog stays Unknown until something recomputes it.
- **Concentration has no column.** It rides in `fg_raw_json.concentration` /
  `raw_identity.concentration`; missing = the source page had no tag or it was
  never resolved.
- **The wardrobe tables keep a static copy.** `user_fragrances.fragrance_data`
  and `global_fragrances.profile_data` store family/concentration directly; they
  only refresh when a user re-opens that fragrance's detail. Healing the engine
  cache does **not** auto-fix them.

The live `/complete` path already persists **both** family (recomputed
server-side, `api.py`) and concentration (`db.py`) — so a fresh worker scrape is
correct. The problem was purely an **un-run backlog**, in three layers:

| Layer | Rows (prod, 2026-06-15) | Fix |
|---|---|---|
| Engine cache, NULL metrics, raw present | 102 / 126 | `heal_offline.py` metrics (offline) |
| Engine cache, missing concentration | 126 / 126 (25 offline-resolvable) | `heal_offline.py` concentration (offline) |
| Wardrobe rows that match an engine record | ~46 | `heal_offline.py` wardrobe projection (offline) |
| Wardrobe rows with **no** engine record | ~144 | `seed` + live worker scrape |
| Engine cache, empty raw | 14 | live worker scrape |

## Prerequisites

```powershell
cd c:\Users\urban\my_project_workspace\search_engine
# DATABASE_URL is read from the env, else auto-loaded from huge_monorepo\.env
$env:ENRICHMENT_WORKER_TOKEN = "<worker token>"
$env:SCENT_API_BASE_URL = "https://srt-scent-engine-production.up.railway.app"
```

## Step 1 — offline heal (no network, idempotent, safe to re-run)

Fixes the engine cache (family + concentration) and projects into the wardrobe
tables by **exact** brand+name match. Every write only fills values that are
currently Unknown; re-running is a no-op.

```powershell
# Preview everything first (writes nothing)
.\.venv\Scripts\python.exe scripts\heal_offline.py --dry-run

# Execute
.\.venv\Scripts\python.exe scripts\heal_offline.py
```

Scope to specific records (used for the 5-fragrance verification):

```powershell
.\.venv\Scripts\python.exe scripts\heal_offline.py --record-key "fg:https://www.fragrantica.com/perfume/Chanel/Bleu-de-Chanel-9099.html"
```

## Step 2 — queue the fragrances that have no engine record yet

The ~144 wardrobe fragrances absent from the engine cache (Creed Royal Oud,
Chanel Coco, …) can't be projected — they must be scraped. `seed` enqueues them
with the same `id:brand|name` key the SPA's `/details` path uses, so it never
duplicates existing jobs.

```powershell
.\.venv\Scripts\python.exe scripts\heal_offline.py --steps seed --dry-run   # preview count
.\.venv\Scripts\python.exe scripts\heal_offline.py --steps seed             # enqueue
```

## Step 3 — run the live worker (residential IP) to drain the queue

The worker resolves → scrapes Fragrantica → `/complete`, which persists family +
concentration into the engine cache. Pacing defaults are conservative; raise the
delay for a long bulk, never lower it.

```powershell
# rehearse one job (mutates nothing)
.\.venv\Scripts\python.exe scripts\enrichment_worker.py --process-pending --limit 1 --dry-run --debug
# process a gentle batch
.\.venv\Scripts\python.exe scripts\enrichment_worker.py --process-pending --limit 5 --delay 90 --jitter 20
# or hands-free with the idle heal sweep
.\.venv\Scripts\python.exe scripts\enrichment_worker.py --dashboard --auto-approve
```

## Step 4 — re-project the freshly scraped facts into the wardrobe

After the worker has populated the engine cache, re-run the offline heal so the
newly available family/concentration land on the matching wardrobe rows.

```powershell
.\.venv\Scripts\python.exe scripts\heal_offline.py
```

Loop Steps 3–4 until the queue is drained. The whole sequence is idempotent.

## Verification

```sql
-- engine cache: should trend to ~0 (only the genuinely empty-raw rows remain)
SELECT count(*) FROM fragrance_records WHERE derived_metrics_json IS NULL AND fg_raw_json <> '{}'::jsonb;

-- wardrobe Unknown family / concentration should fall after Steps 1 + 4
SELECT count(*) FILTER (WHERE fragrance_data->>'family'        IS NULL OR lower(fragrance_data->>'family')        IN ('','unknown','unknown family')) AS fam_unknown,
       count(*) FILTER (WHERE fragrance_data->>'concentration' IS NULL OR lower(fragrance_data->>'concentration') IN ('','unknown'))                 AS conc_unknown
FROM user_fragrances;
```
