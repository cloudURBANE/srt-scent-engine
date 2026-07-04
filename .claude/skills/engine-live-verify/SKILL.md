---
name: engine-live-verify
description: >
  Post-deploy / cold-search canary for the engine. Use after any engine
  deploy or redeploy, when validating a Decodo/SERP change, or on any report
  of empty results, Basenotes-only profiles, or Unknown-family tiles on a
  cold (never-cached) search. Cached tests are null and void — this skill is
  the cold-Decodo-discovery canary.
---

# Engine live verify — the cold-search canary

## Why cached tests prove nothing

Production (Railway's datacenter IP) is Cloudflare-blocked from Fragrantica.
A cached query skips the entire live discovery + fetch chain — the only part
that is actually fragile. Green checks on a warmed identity (`Creed Aventus`,
`Thom Browne Vetyver`) prove nothing about the cold path. Any honest
verification must run on **never-seen** houses with the cache confirmed cold
via `diagnostics.cache_source` / `live_search_skipped`.

The recovery path under test: when the direct fetch is blocked or returns a
challenge page, the engine re-fetches the same URL through Decodo's
`universal` target (premium egress) and re-parses; recovered details carry
`parse_diagnostics.fg.fetched_via = "decodo_universal"`. Bing SERP redundancy
(`DECODO_ENABLE_BING_FALLBACK=1`, off by default) covers Google-empty
discovery. Kill-switch: `DECODO_DISABLE_UNIVERSAL_FALLBACK=1`.
Full background: `DECODO_UNIVERSAL_RECOVERY.md`, `COLD_SEARCH_LIVE_TEST_FINDINGS.md`.

## Procedure (run after every deploy)

```bash
BASE=https://srt-scent-engine-production.up.railway.app

# 0) Basic liveness + read-only endpoint sweep
python scripts/verify_production.py --url $BASE --timeout 45

# 1) Cold-search sweep. Pick houses/fragrances the DB has NEVER seen and
#    ROTATE them every run so a prior run cannot warm the next.
for q in "Imaginary Authors Cape Heartache" "Bortnikoff Vobla" \
         "Zoologist Bee" "Nishane Hacivat"; do
  echo "== $q =="
  curl -s "$BASE/api/fragrances/search?q=$(python -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$q")" \
    | python -c "import sys,json;d=json.load(sys.stdin);g=d.get('diagnostics',{});print('rows',len(d.get('results',[])),'cold',not g.get('live_search_skipped'),'t',g.get('timing',{}).get('total_search'))"
done
```

### Pass criteria

- Non-empty `results` (or a **fast** empty JSON envelope — never an empty body).
- `cold=True` for every probe (otherwise the probe is void — pick fresher names).
- `total_search` ≤ ~18s (`API_SEARCH_TOTAL_BUDGET`), **no ~20s overshoot**
  (the overshoot clamp reserves a 0.4s `DEADLINE_SAFETY` margin).
- Opening a cold detail: diagnostics show `fg.fetched_via = "decodo_universal"`
  with FG-linked rows — not `fragrantica_unreachable: true` / Basenotes-only.

## Triage when a probe fails

| Symptom | First check |
|---|---|
| Empty body / hang > 18s | Deadline regression — `api.py` (`API_SEARCH_TOTAL_BUDGET`), `Deadline` in `fragrance_parser_full_rewrite_fixed.py`; see `COLD_SEARCH_LIVE_TEST_FINDINGS.md` |
| Basenotes-only / `fragrantica_unreachable` | Decodo creds on Railway: `DECODO_API_BASIC_TOKEN` (or `DECODO_API_USERNAME`/`_PASSWORD`) expired/unset → the universal recovery **fails silently**. See `DECODO_UNIVERSAL_RECOVERY.md` |
| Unknown family on a fresh detail | Metrics not derived at ingest — check `derived_metrics_json` on the row; if a backlog, this is the `wardrobe-completeness-heal` skill's territory |
| Cold probe returns instantly with rows | It wasn't cold — `live_search_skipped` true; rotate to a genuinely unseen house |

## Offline gate (sandbox, before deploy)

No Decodo creds / egress in the sandbox — cold queries cannot be run live from
here; say so explicitly in any report. The local gate is:

```bash
python run_tests.py                      # includes test_cold_path_decodo_recovery.py
python test_search_total_budget.py       # deadline bounds intact
```
