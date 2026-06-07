# Task: Fix enrichment resolver identity-match bugs + unblock 3 stuck jobs

You are implementing two things in the **`search_engine`** workspace
(`C:\Users\urban\my_project_workspace\search_engine`):

1. **Code fix** — two bugs in the resolver's identity matcher that make it
   reject Fragrantica URLs it has already fetched.
2. **Data remediation** — unblock the three enrichment jobs that are currently
   cycling pending→failed on the live engine.

Read this whole file before changing anything. The diagnosis below is verified
against the live engine with valid Fragrantica clearance — trust it, but
reproduce before and after with the harness in §4.

---

## 1. Background

`scripts/enrichment_worker.py` resolves a Fragrantica detail URL for each queued
fragrance, then fetches + caches the detail. Resolution lives in
`fragrance_parser_full_rewrite_fixed.py::search_once` →
`_search_core`.

FG's native search SERP is dead, and **Serper is disabled in this environment**
(`SerperClient.enabled() == False`, no `SERPER_API_KEY`). So the only live
FG-URL discovery path is the **designer-page crawl**
(`SearchSniper.attach_from_designer_catalog`): it fetches
`fragrantica.com/designers/<Brand>.html`, scrapes the `<a href>` perfume links,
and accepts a link when `Orchestrator.identity_score(candidate, catalog_item) >=
Orchestrator.CATALOG_ACCEPT` (0.76).

Three jobs fail repeatedly with `last_error = "resolver returned no Fragrantica
URL"` (code `fg_url_missing_after_resolution`, non-retryable). They re-enter the
queue because the SPA re-enqueues on every `/details` view, and the auto-approve
dashboard hides the reason (`_run_auto_approve` redirects stdout,
`enrichment_worker.py:~1405`).

**Verified fact:** for two of the three, the crawl *does* fetch the right
designer page and the correct URL *is present on it* — the matcher then scores
it `0.0` and discards it. This is a matcher bug, not a discovery failure.

| Job (name / house) | Real Fragrantica URL | Designer page | URL present? | `identity_score` |
|---|---|---|---|---|
| Man / Calvin Klein | `https://www.fragrantica.com/perfume/Calvin-Klein/Man-1656.html` | `Calvin-Klein` → 200, 445 links | **yes** | `0.0` |
| Ramz Gold / Lattafa | `https://www.fragrantica.com/perfume/Lattafa-Perfumes/Ramz-Lattafa-Gold-70368.html` | `Lattafa` → 200, 793 links | **yes** | `0.0` |
| Miami Poolside / Clive Christian | **none — not on Fragrantica** (2019 travel-exclusive LE; only on Basenotes/Parfumo) | `Clive-Christian` → 200, 133 links | no | n/a |

---

## 2. Root causes (verified)

All in `fragrance_parser_full_rewrite_fixed.py`, class `IdentityTools` /
`Orchestrator`.

### Bug A — single-word names that are stopwords collapse to an empty token set
- `STOPWORDS` (line ~485) includes `"man"`, `"woman"`, `"men"`, `"women"`, etc.
- `IdentityTools.name_tokens(name, brand)` (line ~533) =
  `tokenized(name) - brand_tokens(brand)`, and `tokenized` drops STOPWORDS.
- For Calvin Klein **"Man"**: `name_tokens("Calvin Klein Man","Calvin Klein")`
  removes `calvin`,`klein` (brand) and `man` (stopword) → **`set()`**. The
  catalog item "Man" → also `set()`.
- In `Orchestrator.identity_score` (line ~7488), when either token set is empty
  it falls back to `seq_ratio(a_name_raw, b_name_raw)` =
  `seq_ratio("Calvin Klein Man", "Man")` ≈ low, `< 0.80` → returns `0.0`.
- **Verified:** `identity_score(UF("Calvin Klein Man","Calvin Klein"),
  CI("Man","Calvin Klein"))` = `0.0`; with the name pre-stripped to "Man" it is
  `1.0`. The whole failure is the stopword eating the only distinctive token.

### Bug B — brand hard gate rejects "X" vs "X Perfumes"
- `identity_score` brand gate (lines ~7495-7503) calls
  `IdentityTools.compatible_brand(a_brand, b_brand)`.
- `compatible_brand("Lattafa", "Lattafa Perfumes")` (line ~754) = False:
  `brand_forms("Lattafa") == {"lattafa"}`, `brand_forms("Lattafa Perfumes") ==
  {"lattafa perfumes"}`, no intersection, and the normalized strings differ.
- So `brand_ok` is False and `identity_score` returns `0.0` **before names are
  compared**. The queued house is "Lattafa"; FG's canonical brand (from the URL)
  is "Lattafa Perfumes".
- **Verified:** if the gate passed, the names both reduce to `{ramz, gold}`
  (FG embeds the house mid-name as "Ramz Lattafa Gold", which `name_tokens`
  correctly strips) and would score `0.96`. The gate is the sole blocker.

This is a whole *class* of misses: any `<House>` vs `<House> Perfumes /
Parfums / Fragrances / Paris / Beauty` mismatch fails the gate.

---

## 3. What to implement

### Fix A — don't let a stopword-only name collapse to nothing
In `Orchestrator.identity_score`, when `name_tokens(...)` yields an empty set
for one or both sides, recompute that side's tokens **keeping stopwords but
still removing brand tokens**, and compare those before falling through to
`seq_ratio`. Equivalent acceptable approaches:
- Add an `IdentityTools.name_tokens_keep_stopwords(name, brand)` helper
  (`tokenized` minus brand tokens, without STOPWORDS removal) and, in
  `identity_score`, if `a_name` or `b_name` is empty, recompute *both* with the
  keep-stopwords variant and run the same jaccard/coverage/exact logic on them.
- Do **not** globally remove "man"/"woman" from STOPWORDS — that regresses
  multi-word names ("Eternity for Men" etc.). The fix must only kick in when the
  normal path produced an empty set.

Target: `identity_score(UF("Calvin Klein Man","Calvin Klein"),
CI("Man","Calvin Klein")) >= 0.76` (expect ~0.96), while
`identity_score` for "Man" vs "CK Free", "Eternity for Men", "CK One" stays
`< 0.76`.

### Fix B — make the brand gate tolerant of generic house suffixes
In `IdentityTools.compatible_brand` (and/or `compatible_catalog_brand`), treat
brands as compatible when their token sets match after dropping a small set of
generic house-suffix tokens: `perfumes`, `parfums`, `parfum`, `fragrances`,
`fragrance`, `beauty`, `paris`, `cosmetics`, `perfume`. Implementation options:
- Strip those suffix tokens from each brand's normalized form before the
  existing `brand_forms` intersection / equality check, **or**
- Accept when one brand's token set is a non-empty subset of the other's
  (so "Lattafa" ⊂ "Lattafa Perfumes").
- Keep it conservative — it must not make unrelated brands compatible (e.g.
  "Lattafa" must NOT match "Lattafa Pride" only by coincidence; subset of
  distinctive tokens is fine, but "Armani" must not match "Giorgio Beverly
  Hills"). Prefer the suffix-drop approach over broad subset matching if unsure.

Target: `compatible_brand("Lattafa", "Lattafa Perfumes") == True` and
`identity_score(UF("Ramz Gold","Lattafa"),
CI("Ramz Lattafa Gold","Lattafa Perfumes")) >= 0.76`.

### Constraints
- Minimal, surgical edits to `IdentityTools` / `Orchestrator`. No signature
  changes to `search_once` / `_search_core`.
- No new dependencies. Match the file's existing style (staticmethods,
  `TextSanitizer.normalize_identity`, etc.).
- Do not touch the image pipeline, DB layer, or API routes.

---

## 4. Reproduce + regression-guard (must pass)

Use the project venv: `.\.venv\Scripts\python.exe`.

There are throwaway probe scripts already in `scripts/` from the diagnosis you
can use or adapt (delete them when done if you prefer): `diag_resolve.py`
(runs the full live resolver with debug trace for the 3 names),
`diag_score.py` (unit-level `identity_score` checks + designer-slug reachability).

**Before coding**, capture the baseline:
```powershell
.\.venv\Scripts\python.exe scripts\diag_score.py     # CK Man / Ramz scoring
.\.venv\Scripts\python.exe scripts\diag_resolve.py   # full resolver, expect 3x FAILED
```

**Add unit assertions** (a small `scripts/test_identity_fix.py` or inline in
`diag_score.py`) covering at minimum:
- `identity_score("Calvin Klein Man"/"Calvin Klein", "Man"/"Calvin Klein") >= 0.76`
- `identity_score("Ramz Gold"/"Lattafa", "Ramz Lattafa Gold"/"Lattafa Perfumes") >= 0.76`
- `compatible_brand("Lattafa","Lattafa Perfumes") is True`
- **Regression (must stay BELOW 0.76 / behavior unchanged):**
  - "Man"/"Calvin Klein" vs "CK Free"/"Calvin Klein" → `< 0.76`
  - "Man"/"Calvin Klein" vs "Eternity For Men"/"Calvin Klein" → `< 0.76`
  - "Sauvage"/"Dior" vs "Sauvage"/"Christian Dior" → still `>= 0.76`
  - two clearly different houses stay incompatible (e.g.
    `compatible_brand("Lattafa","Armani") is False`).

**After coding**, `diag_resolve.py` should now resolve CK Man and Ramz Gold to
their correct URLs (Miami Poolside will still fail — that is correct, it is not
on Fragrantica).

Note: `diag_resolve.py` hits live Fragrantica and can be rate-limited if run
repeatedly in quick succession (designer pages are large). If the designer fetch
returns `status=None`, wait ~30s and retry — that is throttling, not your bug.
The `diag_score.py` unit checks do not depend on live fetches for the scoring
assertions (only the slug-reachability section does).

---

## 5. Data remediation (live engine)

Unblock the three jobs on the deployed engine.

- Base URL: `https://srt-scent-engine-production.up.railway.app`
- Auth: `Authorization: Bearer <token>` — read the token from the
  **`ENRICHMENT_WORKER_TOKEN`** env var. Do NOT hardcode it in any file or
  commit it. (The previous token was shared in chat and should be rotated.)

Steps:
1. List the failed jobs to get their real `id`s (do not assume IDs):
   `GET /api/enrichment/jobs?status=failed` → match rows by `name`/`house`.
2. For the two that exist on FG, attach the verified URL — this moves the job
   back to `pending` and clears `failure_count`:
   - `PATCH /api/enrichment/jobs/{id}`  body
     `{"fg_url": "https://www.fragrantica.com/perfume/Calvin-Klein/Man-1656.html"}`  (Calvin Klein Man)
   - `PATCH /api/enrichment/jobs/{id}`  body
     `{"fg_url": "https://www.fragrantica.com/perfume/Lattafa-Perfumes/Ramz-Lattafa-Gold-70368.html"}`  (Lattafa Ramz Gold)
3. For Miami Poolside (no FG page), stop the churn:
   - `POST /api/enrichment/jobs/{id}/ignore`  body
     `{"note": "No Fragrantica perfume page (2019 travel-exclusive LE; only on Basenotes/Parfumo)"}`
4. Verify: re-GET the jobs and confirm CK Man / Ramz Gold are `pending` (or
   `completed` after the next worker pass) and Miami Poolside is ignored.

PowerShell example (token from env, never inline):
```powershell
$h = @{ Authorization = "Bearer $env:ENRICHMENT_WORKER_TOKEN"; "Content-Type" = "application/json" }
$base = "https://srt-scent-engine-production.up.railway.app"
# 1. find IDs
Invoke-RestMethod "$base/api/enrichment/jobs?status=failed" -Headers $h |
  Select-Object id, name, house, failure_count, last_error
# 2. attach (substitute the real id)
Invoke-RestMethod "$base/api/enrichment/jobs/<CK_MAN_ID>" -Method Patch -Headers $h `
  -Body '{"fg_url":"https://www.fragrantica.com/perfume/Calvin-Klein/Man-1656.html"}'
```

---

## 6. Acceptance criteria
- [ ] Bug A fixed: CK "Man" scores `>= 0.76` against the catalog "Man"; the
      stopword-only-name fallback only triggers when the normal path is empty.
- [ ] Bug B fixed: `compatible_brand("Lattafa","Lattafa Perfumes")` is True;
      Ramz Gold scores `>= 0.76`.
- [ ] All regression assertions in §4 pass (Dior Sauvage still resolves;
      generic-name collisions and unrelated brands stay rejected).
- [ ] `diag_resolve.py` resolves CK Man and Ramz Gold to the correct URLs.
- [ ] The 3 live jobs are remediated (2 patched, 1 ignored) and no longer cycle.
- [ ] No token committed; throwaway diag scripts either kept intentionally or
      removed.

## Notes / out of scope
- The highest-leverage *systemic* fix is enabling Serper for the worker
  (`SERPER_API_KEY`), which returns canonical FG URLs directly and bypasses this
  matcher entirely. That is an ops/config change, not part of this task, but
  worth flagging to the owner.
- The dashboard masking the failure reason (`_run_auto_approve` stdout redirect)
  is a separate UX issue; optional follow-up: surface `exc.code` in the live
  stream line.
