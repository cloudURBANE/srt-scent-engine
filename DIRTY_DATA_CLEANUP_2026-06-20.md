# Dirty-data audit and cleanup — 2026-06-20

## Verified live result

The app database contained 380 fragrance rows. The original taxonomy detected
65 rows with at least one issue. The expanded, report-only identity checks detect
73 rows because they add six missing-brand rows and eight owner/catalog-scoped
duplicate-identity rows; those are newly visible issues, not data regressions.

Two guarded additive updates filled three scalar facts in
`global_fragrances`. No rows were inserted, deleted, merged, or renamed.

| Category | Before | After | Resolved |
|---|---:|---:|---:|
| `gender_missing` | 31 | 29 | 2 |
| `concentration_unknown` | 20 | 19 | 1 |
| All other categories | unchanged | unchanged | 0 |

Updated rows:

- `13fbe6c2-db45-4b47-8757-6e57041fb22a` — Christian Dior / Sauvage Elixir:
  added `gender=Masculine`.
- `d47d3615-58a4-4cc6-8ea5-c46ecad1c4aa` — Hermès / Twilly d'Hermes:
  added `gender=Feminine` and replaced the explicit unknown concentration with
  `Eau de Toilette`.

Both updates were supported by two agreeing live donor rows and guarded by UUID
plus exact prior field/family values. An initial stricter transaction failed its
Unicode identity guard and rolled back in full. The successful transaction
matched exactly two rows. The post-change regression gate reported three
resolved issue instances and zero new issue instances.

## Parallel category decisions

### Enrichment completeness

The subagent proposed four targets. Independent live verification accepted only
the two scalar updates above. Accord replacements were rejected because they are
not additive. Millésime Impérial was rejected because the target family
(`Citrus`) conflicted with both proposed donors (`Aquatic`). All 16 unknown-family
rows and the remaining sparse records lacked sufficiently strong evidence.

### Taxonomy and formatting

The one lowercase accord is mechanically normalizable, but changing its stored
list was rejected under the additive-only constraint. Five deprecated
`Oriental` values and two unexpected `Vanilla` family values remain report-only;
automatic remapping would change semantics.

### Identity integrity

Year-bearing names and the two name-equals-brand rows remain unchanged. The
first duplicate proposal was rejected because it grouped different users'
wardrobes. The implemented detector scopes user duplicates by owner and global
duplicates to the catalog, producing eight review-only flags. Six blank-brand
catalog rows are now visible. No fuzzy match, merge, delete, rename, or inferred
brand repair was applied.

## Reusable commands

```powershell
# Explicitly DB-read-only live export; custom basenames get matching audit JSON.
.\.venv\Scripts\python.exe scripts\db_audit_export.py --out audit_runs\monthly

# Compare snapshots and fail if a row/category issue was introduced.
.\.venv\Scripts\python.exe scripts\dirty_data_progress.py `
  audit_runs\before.audit.json audit_runs\after.audit.json `
  --out audit_runs\delta.json --fail-on-new
```

The exporter now includes a deterministic `snapshot_id` and per-category row
keys. Generated exports remain ignored by Git.
