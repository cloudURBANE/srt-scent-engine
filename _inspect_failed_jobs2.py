"""Read-only: failure timing clusters + pending queue snapshot."""
import collections
import re

import requests

env = open(r"..\huge_monorepo\ScentCast.env", encoding="utf-8").read()
tok = re.search(r"ENRICHMENT_WORKER_TOKEN=(.+)", env).group(1).strip().strip('"').strip("'")
base = "https://srt-scent-engine-production.up.railway.app"
h = {"Authorization": f"Bearer {tok}"}

failed = requests.get(f"{base}/api/enrichment/jobs", params={"status": "failed", "limit": 100}, headers=h, timeout=60).json()["jobs"]
pending = requests.get(f"{base}/api/enrichment/jobs", params={"status": "pending", "limit": 100}, headers=h, timeout=60).json()["jobs"]

print("=== failed_at by minute (burst detection) ===")
buckets = collections.Counter()
for j in failed:
    ts = str(j.get("failed_at") or "")[:16]  # YYYY-MM-DDTHH:MM
    kind = "resolver" if "resolver returned" in str(j.get("last_error")) else "parser"
    buckets[(ts, kind)] += 1
for (ts, kind), n in sorted(buckets.items()):
    print(f"  {ts}  {kind:8s} x{n}")

print(f"\n=== pending ({len(pending)}) ===")
for j in pending[:30]:
    print(f"  - {j.get('house') or '?'} - {j.get('name') or j.get('query') or '?'} (req={j.get('requested_count')}, fails={j.get('failure_count')})")
