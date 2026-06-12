# Enrichment Queue Failure Plan

Date: 2026-06-12

This is the current plan for the Fragrantica enrichment queue failures observed
around the auto-approve run:

- `fg_url_missing_after_resolution`
- `fg_resolver_blocked`
- `Fragrantica rate-limited`

The goal is not to make every Basenotes row pretend to have Fragrantica data.
The goal is to make every queue outcome explicit: completed from Fragrantica,
completed from Parfumo partial fallback, requeued because Fragrantica was
blocked, or ignored because no Fragrantica/usable fallback page exists.

## Current Queue Snapshot

Read-only commands run from this workspace:

```powershell
.\.venv\Scripts\python.exe _inspect_failed_jobs.py
.\.venv\Scripts\python.exe _inspect_failed_jobs2.py
.\.venv\Scripts\python.exe test_enrichment.py
.\.venv\Scripts\python.exe scripts\test_identity_fix.py
```

Results:

- Terminal failed jobs returned by the live API: `22`
- Failure bucket: `22 / 22` are `resolver returned no Fragrantica URL`
- Parser-empty failures: `0`
- Failed rows with `fg_url`: `1`
- Failed rows with `bn_url`: `22`
- Failed rows with `query`: `0`
- Failure count distribution: all `failure_count = 3`
- Pending rows: `84`
- Newer items from the dashboard log, including `Elysium Pour Femme`,
  `Fame Blooming Pink`, `Fleurs Des Comores`, and `Frosted Pink`, are pending
  retry-history rows in the current API snapshot, not terminal failed rows.

The terminal failed jobs are:

| House | Name | Current bucket |
|---|---|---|
| Clive Christian | 1872 Twist Geranium | resolver miss; needs single-job verification before patch/ignore |
| Clive Christian | 1872 Twist Tarocco Orange | likely recoverable by current unique-subset catalog logic |
| Clive Christian | 1872 Twist Vetiver | recoverable by current unique-subset catalog logic |
| Jean Paul Gaultier | Le Beau Paradise Garden | catalog has exact match in diagnostics; requeue after current code deploy |
| 4160 Tuesdays | Lightning Ridge | no catalog match observed; verify, then likely ignore/no-FG |
| Violette Market | Lightning Storm | no catalog match observed; verify, then likely ignore/no-FG |
| Testament London | Longevity | no catalog match observed; verify, then likely ignore/no-FG |
| Le Jardin Retrouve | Lys | recoverable by current stopword/subset logic |
| Clive Christian | Miami Poolside | no FG page; recoverable as Parfumo partial when fallback is enabled |
| Yves Saint Laurent | Myslf Intense | designer catalog returned zero items; needs slug/alias or block verification |
| Chanel | No 55 | no catalog match observed; verify before ignore |
| Chanel | No 5 Eau Premiere | designer catalog returned zero items; needs slug/block verification |
| Jean Charles Brosseau | Ombre Rose Loriginal | likely garbled apostrophe/title case; needs URL or alias verification |
| Maitre Parfumeur Et Gantier | Parfum Dhabit | likely garbled apostrophe/title case; needs URL or alias verification |
| Body Shop | Poivre Rose Pink Pepper | likely recoverable through `The Body Shop` house alias |
| The Fireplace By Martin Margiela | Replica | likely recoverable through recombined identity plus Maison Margiela/Replica alias |
| Le Jardin Retrouve | Rose The | garbled/accented name; verify exact FG presence before alias |
| Jean Patou | Sublime | likely house rename/alias issue; verify exact FG designer before changing |
| Hermes | Terre Dhermes Eau Intense Vetiver | likely recoverable through Hermes elision/name repair |
| D S Durga | The Greatest Cologne Of All Time | likely recoverable through `D.S. & Durga` house alias |
| Yves Saint Laurent | Pour Homme Haute Concentration | designer catalog returned zero items; needs slug/block verification |
| Jean Desprez | Bal A Versailles Le Parfum Du Jour | likely accent/title issue; verify exact FG presence before alias |

## High-Level Root Causes

1. The resolver is currently dependent on Fragrantica designer-page crawling when
   Serper is disabled. If Fragrantica blocks or rate-limits the catalog page,
   URL discovery becomes unavailable even for rows that probably exist.

2. The worker already distinguishes `fg_resolver_blocked` from a real miss, but
   the generic failure path was still reporting it through `/fail` as a retryable
   failure. `db.fail_job()` increments `failure_count` for retryable failures, so
   a rate-limit window could still push jobs toward terminal failure.

3. Several terminal failed jobs are old resolver misses that the current local
   code appears able to handle now. The local tests cover:
   - stopword-only names like `Lys`/`The Lys`
   - unique subset catalog matches with exact year
   - generic house suffixes like `Lattafa` vs `Lattafa Perfumes`
   - swapped line/house imports like `The Fireplace By Martin Margiela`/`Replica`
   - Parfumo fallback for FG-absent rows like `Miami Poolside`

4. Some rows are probably legitimate no-Fragrantica-coverage rows. Those should
   not remain in `failed`; they should be moved to `ignored` with a note after a
   single-item verification pass.

5. Some rows are not safe to auto-ignore yet. Names like `Jean Patou - Sublime`,
   `Hermes - Terre Dhermes Eau Intense Vetiver`, and `Chanel - No 5 Eau
   Premiere` are plausible real Fragrantica entries. They need exact URL/alias
   verification before any ignore action.

## Code Fix Applied

`scripts/enrichment_worker.py` now treats `fg_resolver_blocked` as a blocked
external dependency, not a failed product identity:

- dry run: reports that it would requeue without consuming failure budget
- live run: calls `client.requeue_job(...)`
- does not call `client.fail_job(...)`
- still returns `False` so auto-approve can apply the existing Fragrantica
  cooldown

Regression added in `test_enrichment.py`:

- blocked resolver miss requeues
- blocked resolver miss does not call `fail_job`
- blocked resolver miss does not call `ignore_job`
- queue diagnostics separate terminal failed rows from pending retry-history rows
- queue diagnostics only mark conservative no-Fragrantica product-line rows as
  ignored candidates

Verification:

```powershell
.\.venv\Scripts\python.exe test_enrichment.py
.\.venv\Scripts\python.exe scripts\test_identity_fix.py
```

Both passed.

Additional hardening added:

- `--queue-diagnostics [DIR]` exports failed, pending-with-failures, ignored,
  and conservative ignored-candidate rows to timestamped JSON/CSV files.
- The management menu includes the same read-only export action.
- The dashboard now surfaces a latest terminal failure reason and a latest
  pending retry-history reason instead of only showing a combined count.

## Execution Plan

1. Deploy the worker/API code containing the blocked-requeue fix.

2. Pause auto-approve while Fragrantica is actively rate-limiting. Do not keep
   cycling jobs through `fg_resolver_blocked`.

3. Requeue the 22 terminal failed jobs after deploy, but process them slowly.
   The current local resolver should clear a subset of them without manual URL
   patches.

4. For any row that fails again with `fg_url_missing_after_resolution`, run a
   single-job verification pass:

```powershell
$base = "https://srt-scent-engine-production.up.railway.app"
$h = @{ Authorization = "Bearer $env:ENRICHMENT_WORKER_TOKEN"; "Content-Type" = "application/json" }
Invoke-RestMethod "$base/api/enrichment/jobs?status=failed&limit=100" -Headers $h |
  Select-Object -ExpandProperty jobs |
  Select-Object id, house, name, failure_count, last_error, fg_url, bn_url
```

5. If exact FG URL is verified, patch the job:

```powershell
Invoke-RestMethod "$base/api/enrichment/jobs/<JOB_ID>" -Method Patch -Headers $h `
  -Body '{"fg_url":"https://www.fragrantica.com/perfume/<verified>.html"}'
```

6. If no FG page exists but Parfumo exists and has useful facts, let the Parfumo
   fallback complete the row as partial instead of ignoring it.

7. If neither FG nor useful fallback coverage exists, ignore the job:

```powershell
Invoke-RestMethod "$base/api/enrichment/jobs/<JOB_ID>/ignore" -Method Post -Headers $h `
  -Body '{"note":"No verified Fragrantica perfume page; retired after resolver triage 2026-06-12"}'
```

8. Add only verified aliases. Do not add broad aliases because the identity
   matcher already has strong false-positive guardrails, and broad aliasing is
   the easiest way to pollute cached fragrance facts.

## Follow-Up Code Work

- Use the durable queue diagnostic command whenever a failed/retry batch looks
  ambiguous; it exports failed, pending-with-failures, ignored, and ignored
  candidates into timestamped JSON/CSV files.
- Add a `blocked_at`/`blocked_count` style field or equivalent event log if we
  need blocked history without abusing `failure_count`.
- Consider enabling Serper for workers. With Serper disabled, every unknown URL
  depends on Fragrantica designer-page availability.
- If the dashboard needs more detail than the latest reason lines, add a
  drill-down view backed by the queue diagnostic export instead of widening the
  main auto-approve frame.

## Acceptance Criteria

- `fg_resolver_blocked` no longer increments `failure_count`.
- No terminal failed rows remain just because a rate-limit window blocked the
  designer catalog.
- Every current failed row lands in one of three explicit states:
  `completed`, `completed` from Parfumo partial, or `ignored` with a reason.
- Pending rows with retry history stop cycling once Fragrantica clearance is
  healthy or are triaged individually if they remain true misses.
