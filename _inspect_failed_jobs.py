"""Read-only inspection of the remote enrichment queue. Safe to run while the worker is live."""
import collections
import re

import requests

env = open(r"..\huge_monorepo\ScentCast.env", encoding="utf-8").read()
tok = re.search(r"ENRICHMENT_WORKER_TOKEN=(.+)", env).group(1).strip().strip('"').strip("'")
base = "https://srt-scent-engine-production.up.railway.app"
h = {"Authorization": f"Bearer {tok}"}

r = requests.get(f"{base}/api/enrichment/jobs", params={"status": "failed", "limit": 100}, headers=h, timeout=60)
r.raise_for_status()
jobs = r.json().get("jobs", [])
print(f"failed jobs returned: {len(jobs)}\n")

resolver_fail = [j for j in jobs if "resolver returned no Fragrantica URL" in str(j.get("last_error"))]
parser_fail = [j for j in jobs if "parser produced no cards" in str(j.get("last_error"))]
other = [j for j in jobs if j not in resolver_fail and j not in parser_fail]

def label(j):
    return f"{j.get('house') or '?'} - {j.get('name') or j.get('query') or '?'}"

def summarize(group, title):
    print(f"=== {title} ({len(group)}) ===")
    with_fg = sum(1 for j in group if j.get("fg_url"))
    with_bn = sum(1 for j in group if j.get("bn_url"))
    with_query = sum(1 for j in group if j.get("query"))
    fc = collections.Counter(j.get("failure_count") for j in group)
    rq = collections.Counter(j.get("requested_count") for j in group)
    print(f"  has fg_url: {with_fg} | has bn_url: {with_bn} | has query: {with_query}")
    print(f"  failure_count dist: {dict(sorted(fc.items(), key=lambda x: str(x[0])))}")
    print(f"  requested_count dist: {dict(sorted(rq.items(), key=lambda x: str(x[0])))}")
    houses = collections.Counter((j.get("house") or "<none>") for j in group)
    print(f"  top houses: {houses.most_common(10)}")
    for j in group[:20]:
        print(f"   - {label(j)!r:60s} fg_url={'Y' if j.get('fg_url') else '-'} bn={'Y' if j.get('bn_url') else '-'} fails={j.get('failure_count')}")
    print()

summarize(resolver_fail, "resolver returned no Fragrantica URL")
summarize(parser_fail, "parser produced no cards (empty frag_cards)")
if other:
    summarize(other, "other")

# Duplicates check
keys = collections.Counter(label(j) for j in jobs)
dups = {k: v for k, v in keys.items() if v > 1}
print(f"duplicate identities among failed: {dups}")
