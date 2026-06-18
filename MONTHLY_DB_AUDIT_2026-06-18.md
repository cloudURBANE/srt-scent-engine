# Monthly DB dirty-data audit — 2026-06-18

**Scope:** the live app DB (Supabase `postgres`, the only DB the phone reads) —
all fragrances in `user_fragrances` (152 rows, 7 users) + `global_fragrances`
(204 rows). Engine `fragrance_records` (127) is the upstream cache and is not
user-facing; it was not swept here.

**Tooling (new, reusable — run these any month):**
- `python scripts/db_audit_export.py` → writes `db_export_fragrances.xlsx` (open
  in Excel; rows with issues are tinted, `issues` column lists every flag) +
  `.csv` + `db_audit_report.json`. **Read-only.**
- `python scripts/sanitize_wardrobe_accords.py [--commit]` → applies the *safe*
  fixes below. Dry-run by default.
- Shared detection/normalization logic: `scripts/dirty_data.py`.

---

## What was fixed this run (safe, auto-applied, forward-focused)

Applied to the live DB via `sanitize_wardrobe_accords.py --commit`. These are
presentation-only, idempotent normalizations — **no semantic change, no data
loss, fully consistent with what the engine projection already writes**, so they
cannot cause a regression. Total **230 rows** updated (`updated_at` bumped so the
SPA ETag busts and clients refetch).

| Fix | Rows | Example |
|---|---|---|
| `year` numeric-string → `int` | 176 | `"2021"` → `2021` |
| accord casing → Title Case (+ junk/dup drop) | 45 | `['citrus','musk']` → `['Citrus','Musk']` |
| family casing → Title Case | 19 | `woody` → `Woody` |
| family typo `Chypere` → `Chypre` | (in 19) | `Fruity Chypere` → `Fruity Chypre` |

**Forward-focused guarantee:** the same normalization (`dirty_data.normalize_row`)
is now wired into `heal_offline._sanitize_wardrobe_blob`, which runs on **every**
wardrobe row on every drain/heal — regardless of engine match or derived-metrics
junk. So this dirt cannot re-accumulate; any future write that reintroduces
lowercase/string-year is corrected on the next sweep. Locked by
`test_heal_offline_projection.py` (3 new checks, all green).

**Result:** rows with ≥1 issue dropped **249 → 72**. Owner wardrobe (the primary
surface) dropped **33 → 11** flagged rows.

---

## What was NOT auto-fixed (flagged only — needs a source or a human decision)

These are genuine enrichment gaps or data-entry artifacts, not formatting dirt.
Auto-"fixing" them would be a semantic guess and risk a regression, so they are
reported, not rewritten.

### Enrichment gaps (resolvable via the recovery ladder, not by this audit)
- **`concentration_unknown` — 35 rows** (all `global_fragrances`). Concentration
  is resolved off-page; run `enrich_concentration.py --engine-gap --online
  --tier1-only` then heal. Costs Decodo budget — deliberately left as an op.
- **`family_unknown` — 17 rows** / **`accords_empty` — 15 rows**. Need a
  (re)scrape: reopen/seed drain per the `wardrobe-completeness-heal` ladder, then
  the projection fills them. Owner examples: *Liam Grey*, *Casamorati Fiero/Mefisto*,
  *Guilty Intense Pour Femme*, *Myslf Intense*.
- **`gender_missing` — 51 rows** (49 `global_fragrances`, 2 owner). Same — needs
  source. Owner: *Chanel No 5* (known variant-ambiguous), *invictus* (malformed).

### Semantic / known residue (intentionally untouched)
- **`family_deprecated: Oriental` — 6 rows** (e.g. *Lost Cherry*, *Un Jardin Sur
  Le Nil*). Fragrantica renamed Oriental→Amber, but remapping silently moves the
  family bucket — a semantic change. **Decide once, then remap deliberately**;
  not auto-applied.
- **`name_has_year` — 3 rows** (*Miss Dior 2021*, *Love In White 2024*, *Virgin
  Island Water 2007*). Ambiguous — some flankers legitimately carry a year.
- **`name_equals_brand` / malformed entries** — `kouros | kouros`,
  `invictus | invictus`, `Xerjoff | Xerjoff`. Data-entry artifacts with no real
  FG page (see skill "Don'ts"); fix is a manual name/brand correction.
- **Brand typo `Lataffa`** (should be `Lattafa`) on *Liam Grey*. Single instance;
  a brand-typo map could fix it but needs care not to mis-correct real brands.
- **Degraded `global_fragrances` search rows** — lowercase names like
  `Royal | sapphire`, `Guess | blue`, `| thom browne`, and possible mojibake
  (`Millésime Impérial`). Low stakes (shared catalog, not owner-facing); candidates
  for a future cleanup pass with proper re-resolution.

---

## How to view the live DB "like an Excel sheet"

```sh
cd search_engine
python scripts/db_audit_export.py        # all users + global catalog
python scripts/db_audit_export.py --owner-only   # just the owner's wardrobe
# -> open db_export_fragrances.xlsx (filter on the `issues` column to triage)
```

No Railway token needed — it reads the Supabase DSN from `huge_monorepo/.env`.
