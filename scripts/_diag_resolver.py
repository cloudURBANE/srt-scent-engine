"""Read-only resolver diagnosis for failed enrichment jobs.

For each failed job: print the query variants the worker would try, then
enumerate the house's Fragrantica designer catalog and show the top-scored
items via the engine's own identity_score. Shows exactly which FG-side name
each job should match and the score shortfall. No queue mutations.
"""
import io
import contextlib
import re
import sys

import requests

import fragrance_parser_full_rewrite_fixed as engine
sys.path.insert(0, "scripts")
import enrichment_worker as w

env = open(r"..\huge_monorepo\ScentCast.env", encoding="utf-8").read()
tok = re.search(r"ENRICHMENT_WORKER_TOKEN=(.+)", env).group(1).strip().strip('"').strip("'")
base = "https://srt-scent-engine-production.up.railway.app"
h = {"Authorization": f"Bearer {tok}"}

jobs = requests.get(f"{base}/api/enrichment/jobs", params={"status": "failed", "limit": 100}, headers=h, timeout=60).json()["jobs"]
only = sys.argv[1:] if len(sys.argv) > 1 else None
if only:
    jobs = [j for j in jobs if any(pat.lower() in f"{j.get('house','')} {j.get('name','')}".lower() for pat in only)]
print(f"diagnosing {len(jobs)} failed job(s)\n")

scraper = engine.get_scraper()
fg = engine.get_fragrantica_scraper(scraper.default_scraper, mint_clearance=not engine._chromium_mint_disabled())
if not engine._validate_fragrantica_session(fg):
    raise SystemExit(f"clearance preflight failed: {getattr(engine, '_FRAGRANTICA_LAST_MINT_ERROR', None)}")

for j in jobs:
    house = w._canonical_house(str(j.get("house") or ""))
    name = w._canonical_name(str(j.get("house") or ""), str(j.get("name") or ""))
    year = w._coerce_year(j.get("year"))
    print("=" * 78)
    print(f"JOB {j.get('house')} - {j.get('name')}  (year={j.get('year')!r}, fg_url={j.get('fg_url')!r})")
    print(f"  canonical: house={house!r} name={name!r}")
    print(f"  query variants: {w._query_variants(j)}")
    probe = engine.UnifiedFragrance(name=name, brand=house, year=year)
    probe_noyear = engine.UnifiedFragrance(name=name, brand=house, year="")
    scored = []
    try:
        for label in engine.IdentityTools.catalog_brand_keys(house, name):
            deadline = engine.Deadline(20.0)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                items = engine.SearchSniper.catalog_candidates_for_brand(
                    scraper, label, deadline, slug_limit=8, page_timeout=8.0)
            print(f"  catalog label {label!r}: {len(items)} items")
            for it in items:
                scored.append((engine.Orchestrator.identity_score(probe, it),
                               engine.Orchestrator.identity_score(probe_noyear, it), it))
    except Exception as exc:
        print(f"  catalog enumeration FAILED: {exc}")
        continue
    scored.sort(key=lambda t: max(t[0], t[1]), reverse=True)
    for sc, sc_ny, it in scored[:6]:
        print(f"    {sc:.2f} (noyear {sc_ny:.2f})  {it.name!r} [{it.brand}] year={it.year!r} {it.url}")
    if not scored:
        print("    (no catalog items at all)")
