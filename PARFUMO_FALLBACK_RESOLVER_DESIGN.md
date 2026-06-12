# Parfumo Fallback Resolver — Feasibility & Design

**Status:** implemented (gated, dormant by default). §6.A–§6.D are wired:
`engine.ParfumoEngine` + structured-provider Parfumo discovery (§6.A), the worker
resolve/fetch fallback (§6.B/C), and the additive API completion path (§6.D).
The whole path is inert unless `PARFUMO_FALLBACK_ENABLED` is truthy; `db.py` is
untouched (no schema migration). The read-only POC
`scripts/diag_parfumo_resolve.py` remains as the regression harness.

**Problem:** some enrichment jobs have no Fragrantica page, so the FG resolver
returns no URL and the job cycles `pending → failed` forever (see
`ENRICHMENT_RESOLVER_FIX_TASK.md`). The canonical example is **Clive Christian /
Miami Poolside** (a 2019 travel-exclusive LE that is *not* on Fragrantica but
*is* on Parfumo). We want Parfumo as a **fallback** source for factual fields
when, and only when, FG resolution yields nothing.

---

## 1. Feasibility — verified live (2026-05-29)

All checks were run from this environment with the project venv against the live
site. Verbatim findings:

| Check | Result |
|---|---|
| Reachability via `engine.get_scraper()` (plain cloudscraper, **no clearance mint**) | **200 OK**, full HTML. Parfumo did **not** challenge the datacenter UA. |
| Clearance needed? | **No.** `RoutedScraper._for_url` only mints for `*.fragrantica.com` / `*.basenotes.com`; Parfumo falls through to the default cloudscraper, which is sufficient. |
| robots.txt compliance | We fetch only `/Perfumes/…`, `/Parfums/…`, and public brand grids — none disallowed. We never touch `/api/`, `/app_v5/api/`, `/s_perfumes.php`, or logged-in endpoints. |
| Field richness | name, house, year, gender, rating + count, main accord, full notes pyramid, perfumer, production status — all present on public pages. |
| Soft-404 hazard | A missing slug returns **HTTP 200** with a `<title>404</title>` shell (no `og:url`, no breadcrumb). Must be detected by content, not status code. |

**Conclusion:** technically and compliantly feasible as a fallback. The POC
resolves all three positive controls and rejects all three near-misses.

> Note: `WebFetch`/generic bots get `403` from Parfumo; the project's
> cloudscraper does not. Discovery that relies on a structured search provider is
> therefore strongly preferred over hammering Parfumo directly.

---

## 2. URL patterns (confirmed)

- Perfume page: `https://www.parfumo.com/Perfumes/<Brand>/<slug>`
- Alias: `https://www.parfumo.com/Parfums/<Brand>/<slug>` — **self-canonicalizes**.
  Fetching `/Parfums/Clive_Christian/town-country` returns
  `og:url = …/Perfumes/Clive_Christian/town-country`. So we read `og:url` from
  the fetched page and treat that as canonical; no manual `/Parfums`→`/Perfumes`
  rewrite needed.
- Brand grid: `https://www.parfumo.com/Perfumes/<Brand>` (paginated
  `?current_page=N`); also `https://www.parfumo.com/PerfumeBrand/<Brand>`.
- **Brand slug**: keeps case, spaces → underscores (`Clive_Christian`,
  `Calvin_Klein`, `Lattafa`).
- **Perfume slug**: two coexisting styles — lowercase-hyphen
  (`ramz-lattafa-gold`) and Mixed_Case_Underscore (`Ramz_Lattafa_Silver`,
  `CK_One`). **No numeric ID** (unlike Fragrantica's `-1656.html`), so the slug
  *is* the identity — slug-guessing is unreliable (names embed house/flankers),
  which is why search-provider discovery is primary.
- Path shape regex used: `^/(?:Perfumes|Parfums)/[^/]+/[^/?#]+$`.

---

## 3. URL discovery strategy

Layered, fallback-only, rate-limited:

1. **Primary - Decodo structured SERP, Parfumo-scoped.**
   Query: `site:parfumo.com/Perfumes "<house>" "<name>"`. Reuses the existing
   `DecodoScraperClient` provider abstraction used for FG, but with a Parfumo
   site scope. Collect Parfumo perfume URLs, canonicalize, dedupe, and cache per
   normalized query.
2. **Fallback — capped single-house brand crawl.** Only when the provider is
   unavailable/empty *and* explicitly enabled. Walk `/Perfumes/<Brand>` for at
   most N pages (default 6), collect public perfume links, score each. Brand
   grids use image tiles (empty anchor text), so each candidate must still be
   fetched + scored. **Expensive** — Miami Poolside is on page 5 of Clive
   Christian; Lattafa has hundreds of entries across many pages. Keep N small,
   keep `--delay`, treat as last resort.
3. **Canonicalization** of every confirmed hit: read `og:url` (collapses
   `/Parfums` and `?query` junk), force `https://www.parfumo.com`, strip
   trailing slash.

---

## 4. Identity matcher + thresholds

**Reuses the engine's existing scorer** — no parallel logic to drift:
`score_record()` builds an `engine.UnifiedFragrance` probe (queued house/name/
year) and an `engine.CatalogItem` (parsed Parfumo identity), then calls
`engine.Orchestrator.identity_score(probe, catalog)`. This inherits, for free:
- the brand hard-gate + generic-suffix tolerance (`Lattafa` ≡ `Lattafa Perfumes`),
- the stopword-only-name keep-stopwords fallback (so `"Man"` still matches),
- the year hard-gate (Δ>1yr ⇒ 0.0; the same gate that exposed the year-parse
  bug during POC bring-up — see §7).

**One Parfumo-specific normalization** before scoring: flatten flanker parens,
`"Ramz Lattafa (Gold)"` → `"Ramz Lattafa Gold"`, so the engine tokenizer sees
the words (`_normalize_parfumo_name`).

**Proposed thresholds** (Parfumo is fallback ⇒ favor precision; slightly stricter
than `Orchestrator.CATALOG_ACCEPT = 0.76`):

| Band | Decision |
|---|---|
| `score ≥ 0.82` **and** `(best − runner_up) ≥ 0.06` | **accept** |
| `0.70 ≤ score < 0.82`, or top-two within the 0.06 sibling margin | **manual_review** |
| `score < 0.70` | **reject** |

The sibling margin is the **collision guard**: if two discovered Parfumo entries
score near-identically (e.g. an EDT vs EDP flanker), don't auto-accept — flag for
a human. Live POC scores: Ramz Gold **0.96**, CK Man **0.90**, Miami Poolside
**0.94**; all near-misses **0.00**.

---

## 5. Parseable fields (live samples, no review text)

| Field | Source on page | Example (Ramz Gold / Miami Poolside) |
|---|---|---|
| canonical URL | `meta[property=og:url]` | `…/Perfumes/Lattafa/ramz-lattafa-gold` |
| name | breadcrumb last item (`ld+json BreadcrumbList`) + `og:title` | `Ramz Lattafa (Gold)` / `The Art Of Travel Collection - Miami Poolside` |
| house | breadcrumb position 3 | `Lattafa` / `Clive Christian` |
| year | regex `released in (\d{4})` from `og:description` (may be "unknown") | — / `2019` |
| gender | `og:description` ("for women" / "for women and men" / "for men") | Feminine / Unisex |
| rating + count | `itemprop=ratingValue` / `ratingCount` | `7.3` / `94` |
| main accord | `og:description` ("The scent is …") | `fruity-sweet` / `synthetic-aquatic` |
| notes pyramid | `.notes_list` → `.clickable_note_img`, bucketed by "Top/Heart/Base Notes" headers; flanker pages give a **flat** list | top/heart/base populated / flat list |
| perfumer | `a[href*="/Perfumers/"]` | (none) / (none); CK Be → `Rene Morgenthaler` |
| production status | `og:description` ("still in production" / "discontinued") | — / `in_production` |
| community bars (future) | `.barfiller_*` (scent/durability/sillage/bottle/pricing) | available, not yet parsed |

JSON-LD carries **only** `BreadcrumbList` (no `Product`/`aggregateRating`
schema), so facts come from HTML/meta, not a single structured blob.

---

## 6. Integration points (IMPLEMENTED — gated by `PARFUMO_FALLBACK_ENABLED`)

All edits run only when `PARFUMO_FALLBACK_ENABLED` is truthy *and* FG resolution
returned nothing. The capped brand crawl is independently gated by
`PARFUMO_BRAND_CRAWL_ENABLED` (off by default).

**A. `fragrance_parser_full_rewrite_fixed.py` — DONE.** Added `ParfumoEngine`
(parallel to `FragranticaEngine`) holding the §1–§5 logic promoted verbatim from
the POC: `canonical_url`, `is_perfume_url`, `brand_slug`, `parse_fields`,
`fetch_record`, `discover_urls`, `score_record`, and a banded `resolve()`
returning a `ParfumoVerdict` (`accept` / `manual_review` / `reject` /
`not_found`). Thresholds `ACCEPT=0.82` / `MANUAL_REVIEW=0.70` /
`SIBLING_MARGIN=0.06` live on the class. Discovery is Decodo-first via
`structured_search_provider().search_parfumo_urls(house, name)`; the Parfumo
cache is separate from the FG cache,
then the off-by-default brand crawl. `enabled()` / `brand_crawl_enabled()` read
the env gates. Discovery is capped at `ParfumoEngine.MAX_CANDIDATES = 5` so a
noisy SERP can't stretch one claimed job into tens of page fetches. No signature
changes to `search_once` / `_search_core`. Scoring reuses
`Orchestrator.identity_score` and reproduces the POC numbers exactly
(Ramz 0.96, CK Man 0.90, Miami 0.94, all near-misses 0.00). Gender uses the
engine's own vocabulary (`Feminine` / `Masculine` / `Unisex`, matching FG's
`heal_missing_gender_and_year`); an undetectable gender is left `""` and the API
defaults it to `"Unisex / Unspecified"` (the "unknown" value). Year is parsed
from the og:description ("released in YYYY", else a bare 19xx/20xx), left blank
when absent (never guessed), and is honored both in the identity year-gate and in
the persisted row (`_coerce_year` → int / NULL).

**B. `scripts/enrichment_worker.py::resolve_candidate` — DONE.** Before the final
`WorkerError("fg_url_missing_after_resolution")` raise, it calls the new
`_try_parfumo_fallback(scraper, job)`: gated by `ParfumoEngine.enabled()`, needs
both a `house` and `name`, runs `ParfumoEngine.resolve`, and **only on
`accept`** returns a candidate with `frag_url=""`,
`resolver_source="parfumo_fallback"`, and the parsed `ParfumoRecord` stashed on
`candidate.parfumo_record` (side channel). `manual_review` / `reject` /
`not_found` / disabled / any exception → returns `None`, so the FG path raises as
before (never crashes the job).

**C. `scripts/enrichment_worker.py::fetch_payload` — DONE.** When
`candidate.resolver_source == "parfumo_fallback"`, it short-circuits to
`_build_parfumo_payload` (no `fetch_selected_details`): `source="parfumo"`,
`parfumo_url`, `fg_url=None`, parsed `notes` / `gender` / `year`, with
`accords` / `rating` / `rating_count` / `status` / `perfumers` in `raw_identity`,
and `quality_status="partial"` (no `frag_cards`). `process_job`'s parse log and
dry-run line were taught to handle the Parfumo payload shape.

**D. API completion path — DONE (was DRAFTED & DORMANT).** This was the
gating change. The `/complete` endpoint now has an additive Parfumo branch
(`api.py`): when a payload carries `source="parfumo"` (or a `parfumo_url`), it
skips the `fg_url` and `frag_cards` requirements, requires a public
`parfumo_url` plus at least one factual field, and persists a terminal
`quality_status="partial"` row via the existing `db.complete_job`. Everything is
inert until such a payload arrives, so the FG contract is unchanged for every
current caller. Specifics of why this is safe and end-to-end:

- **No schema migration.** `frag_cards_json` is `NOT NULL` but `Json({})`
  satisfies it; `"partial"` is already in `db.VALID_QUALITY_STATUSES`. `db.py`
  is untouched.
- **Keying.** The canonical Parfumo URL is stored in the `canonical_fg_url` PK
  slot (the cache's generic identity key — unique, never collides with a real FG
  URL). `parfumo_url` is also stashed in `raw_identity_json` for attribution.
- **Never masquerades as FG.** `_apply_fg_detail_cache_db` only hydrates
  `quality_status="complete"`, so a Parfumo `partial` is never mistaken for FG
  status metrics.
- **Displayable.** `db.complete_job` also writes the identity-keyed
  `fragrance_records` aggregate (name/house/year/gender/notes), which the
  cache-search path reads — the same retrieval route the Parfinity seed uses.
  No FG URL is required to surface it.
- **Churn stops.** Marking the job `completed` is enough: `db.enqueue_job` (the
  per-`/details` path) does **not** resurrect a completed job. Only the explicit
  manual-refresh (`db.requeue_or_enqueue_job`) resurrects it, and once B/C are
  wired that simply re-resolves Parfumo and idempotently overwrites the same row.

So B/C (worker wiring) can now land safely on top of D. New request field:
`CompleteJobRequest.parfumo_url`. Helpers added: `_canonical_parfumo_url`,
`_parfumo_payload_has_facts`, `_build_parfumo_cache_row`, plus the branch in
`complete_enrichment_job`. (The local canonicalizer should move into
`engine.ParfumoEngine` when §6.A lands.)

---

## 7. Regression cases & acceptance criteria

Run: `python scripts/diag_parfumo_resolve.py --cases` → currently **ALL PASS**.

| Case | House / queued name | Expect | POC score |
|---|---|---|---|
| Positive | Lattafa / "Ramz Gold" | accept | 0.96 |
| Positive | Calvin Klein / "Man" | accept | 0.90 |
| Positive (no FG page) | Clive Christian / "Miami Poolside" | accept | 0.94 |
| Near-miss | Calvin Klein / "Man" vs *Eternity for Men* | reject | 0.00 |
| Near-miss | Calvin Klein / "Man" vs *CK One* | reject | 0.00 |
| Unrelated house | Lattafa / "Ramz Gold" vs *CK Be* | reject (brand gate) | 0.00 |

**Acceptance criteria for productionizing:**
- [x] Parfumo fallback runs **only** after the FG resolver returns no URL (§6.B,
  after both FG search passes, before the terminal raise).
- [x] All three positive controls accept; all near-misses reject or manual-review
  (POC `--cases` ALL PASS; `ParfumoEngine.score_record` reproduces the POC scores).
- [x] Sibling collisions (EDT vs EDP near-ties) route to `manual_review`, not
  auto-accept — and the worker only completes on `accept`, so a `manual_review`
  never auto-writes.
- [x] Stored rows carry `source="parfumo"` + `parfumo_url`; no `fg_url` invented
  (`_build_parfumo_payload` sends `fg_url=None`; §6.D keys on the parfumo URL).
- [x] A Parfumo partial completes the job (no re-enqueue churn) — §6.D.
- [x] No review text is ingested (`_build_parfumo_payload` sends no reviews;
  §6.D writes empty `pros_cons`/`reviews`).
- [x] Rate limiting + caching in place; brand crawl off by default
  (`ParfumoEngine.resolve` `delay`, Decodo response cache,
  `PARFUMO_BRAND_CRAWL_ENABLED`).

**Remaining before turning the flag on in prod:** re-verify `robots.txt`; smoke
the live `resolve()` once with Decodo credentials set (the POC proves parse+score;
provider discovery needs one live confirmation); decide where
`manual_review` verdicts surface (currently they fall through to a non-retryable
FG failure for a human to inspect).

---

## 8. Compliance & rate-limit risks

- **robots.txt:** we use only public `/Perfumes`, `/Parfums`, and brand grids.
  Re-verify `robots.txt` on each deploy; if Parfumo later disallows `/Perfumes`,
  the fallback must hard-disable.
- **Volume:** fallback-only (FG-miss subset), Decodo-first (one structured query,
  not page hammering), per-fetch `--delay`, response caching. Brand crawl is
  off by default and page-capped.
- **Anti-bot fragility:** cloudscraper works **today** with no challenge; Parfumo
  could add Cloudflare like FG/BN. The resolver must degrade cleanly (treat any
  non-200/parse failure as "no Parfumo result", never crash the job).
- **Attribution:** persist `source="parfumo"` + `parfumo_url` on every stored
  field set so the UI can attribute and so we never silently mix it with FG data.
- **Known parser rough edges (POC):** the perfumer anchor sometimes concatenates a
  role label (`"Ann GottliebProduct developer"`); the production `ParfumoEngine`
  should split role suffixes. Year is "unknown" on some pages (left blank, not
  guessed). Community `.barfiller_*` bars are available but not yet parsed.

---

## 9. POC reference

`scripts/diag_parfumo_resolve.py` — read-only; no DB/job/engine mutation.

```
# regression suite (seeded URLs when provider discovery is off):
python scripts/diag_parfumo_resolve.py --cases

# one fragrance (Decodo if configured, else needs --url/--brand-crawl):
python scripts/diag_parfumo_resolve.py --house "Lattafa" --name "Ramz Gold"

# score a specific candidate URL directly:
python scripts/diag_parfumo_resolve.py --house "Clive Christian" \
    --name "Miami Poolside" \
    --url https://www.parfumo.com/Perfumes/Clive_Christian/the-art-of-travel-collection-miami-poolside
```
