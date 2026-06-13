# Decodo Universal Recovery + Bing Redundancy — Findings & Change

**Date:** 2026-06-13
**Branch:** `claude/eager-albattani-db8vie`
**Scope:** Make cold (never-cached) fragrance queries resolve reliably by attacking
the root cause, not the symptom.

---

## 1. The high-level diagnosis (why the same issue keeps coming back)

Every recent commit — "Thom Browne cold search fallback", "designer fallback",
"deadline budget", "avoid caching empty lookups" — was fighting **one root
cause**, not many bugs:

> **Production (Railway's datacenter IP) is Cloudflare-blocked from Fragrantica.**
> On a cold query the engine cannot just read the fragrance page, so it has to
> *reconstruct* identity + notes from Google-SERP URL discovery plus page fetches
> that frequently time out or get a challenge. The "tighten vs. loosen the
> budget" tug-of-war was rearranging deck chairs around an unreliable source.

**Why cached tests are null and void (correct intuition):** a cached query skips
the entire live discovery + fetch chain — the only part that is actually fragile.
Green checks on a warmed identity (`Creed Aventus`, `Thom Browne Vetyver`) prove
nothing about the cold path. Any honest test must run on a **never-seen**
house/fragrance with the cache bypassed (see §5).

## 2. What the Decodo research actually showed

Pulled from Decodo's official code (`Decodo/SERP-Scraping-API`,
`Decodo/Web-Scraping-API`):

- **There is no dedicated "AI Overview" / "AI Mode" target and no structured
  `ai_overview` response field.** The dashboard template "Google Ads with AI
  Overview" maps to the `google_ads` target; AI text only appears if you render
  with `headless: html` and scrape the HTML block yourself — fragile and
  credit-heavy. Betting identity resolution on this would *recreate* the
  unreliability we are trying to escape, so it was **not** built.
- **The clean, documented, stable pieces** are `google_search` and `bing_search`
  (both return a parsed `organic` array) and the **`universal`** target
  (`{"target":"universal","url":<any URL>}`), which fetches any page's rendered
  HTML through Decodo's un-blocked premium egress.

The real lever is therefore: **fetch the Fragrantica page itself *through Decodo*,
bypassing the Cloudflare block** — plus Bing as a redundant URL-discovery index.

## 3. What changed (code)

All in `fragrance_parser_full_rewrite_fixed.py`, reusing existing Decodo creds
(no new secrets):

1. **Universal egress recovery in `FragranticaEngine.fetch_details`.** When the
   direct fetch returns nothing or a challenge page, re-fetch the same URL via
   `DecodoScraperClient.fetch_url_html()` (`universal` target, `headless: html`)
   and run the *identical* parser on the recovered HTML. Recovered details set
   `parse_diagnostics.fg.fetched_via = "decodo_universal"` and clear
   `fetch_errors.fg`. The recovered body is still passed through
   `_response_has_challenge`, so a challenge shell is never accepted.
   Kill-switch: `DECODO_DISABLE_UNIVERSAL_FALLBACK=1`.

2. **Bing redundancy in `_search_fragrantica_urls_for_google_query`.** If Google
   discovery yields no usable `fragrantica.com/perfume` URL, retry the scoped
   query against `bing_search`. Fires only on otherwise-empty cold queries; the
   shared URL-collection logic is factored into `_collect_fragrantica_urls`.

3. **Overshoot clamp (code-only tightening).** `_viable_budget` reserves a
   `DEADLINE_SAFETY` (0.4s) margin so an in-flight call finishes before the
   overall deadline, and `_request_timeout` returns a bounded `(connect, read)`
   tuple (connect capped at `CONNECT_TIMEOUT`=3.05s) instead of a single float
   applied to both phases. This stops cold queries overshooting an 18s budget to
   ~20s, without loosening the budget (so the niche brand-only rows are
   recovered by the universal-egress fix above, not by a longer deadline).

## 4. Verification (offline, this environment)

- `python run_tests.py` → **6/6** files pass (incl. the new
  `test_cold_path_decodo_recovery.py`).
- `python test_search_total_budget.py` → **4/4** pass (deadline bounds intact).
- New `test_cold_path_decodo_recovery.py` (cache cleared + mock-call asserted so a
  cache hit can't pass a check):
  - Bing recovers the FG URL when Google is empty; Bing is *not* called when
    Google succeeds (no wasted credit).
  - A blocked direct fetch **and** a challenge page are both recovered through the
    universal target, and notes parse out of the recovered HTML.
  - A challenge body returned by Decodo is rejected (no false recovery).
  - The budget skips a near-deadline call and caps connect time.

**Not possible here:** no Decodo credentials and outbound egress is firewalled
(403), so cold queries could **not** be run live against Decodo/Railway from this
sandbox. The change is offline-verified only; run §5 against prod after deploy.

## 5. Cold-validation methodology (run after deploy)

Cached results invalidate the test. Force cold queries:

```bash
# 1) Pick houses/fragrances the DB has NEVER seen. Rotate them every run so a
#    prior run cannot warm the next. Read diagnostics.cache_source /
#    live_search_skipped to CONFIRM the response was cold, not cached.
BASE=https://srt-scent-engine-production.up.railway.app

for q in "Imaginary Authors Cape Heartache" "Cola Addict" "Bortnikoff Vobla" \
         "Zoologist Bee" "Nishane Hacivat"; do
  echo "== $q =="
  curl -s "$BASE/api/fragrances/search?q=$(python -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$q")" \
    | python -c "import sys,json;d=json.load(sys.stdin);g=d.get('diagnostics',{});print('rows',len(d.get('results',[])),'cold',not g.get('live_search_skipped'),'t',g.get('timing',{}).get('total_search'))"
done
```

Expect after this change: non-empty `results`, `cold=True`, `total_search` ≤ ~18s
with **no ~20s overshoot**, and — once a cold detail is opened — the diagnostics
show `fg.fetched_via = "decodo_universal"` with FG-linked rows instead of
`fragrantica_unreachable` / Basenotes-only.

```bash
python scripts/verify_production.py --url $BASE --timeout 45
```

## 6. What is intentionally NOT done

- No AI-Overview/AI-Mode resolver (see §2 — most fragile, least supported path).
- Budget left at `API_SEARCH_TOTAL_BUDGET=18s` (tightened, not loosened).
- The `THOMBRONY` hard-coded alias is left in place; it can be retired once the
  universal-egress path is confirmed in prod.
