"""One-shot triage of the remote enrichment queue (read-only unless --apply).

Moves product lines with no Fragrantica pages (Bath & Body Works mists,
Victoria's Secret Pink line, Calvin Klein body mists) to 'ignored' so they
stop burning resolver attempts. Garbled-name rows (Verveine d'Été, Lattafa,
Initio, …) are NOT touched here — the worker's HOUSE_ALIASES/NAME_ALIASES
now repair those at resolve time.

Safe alongside a live worker: skips anything in 'processing'.
"""
import re
import sys

import requests

env = open(r"..\huge_monorepo\ScentCast.env", encoding="utf-8").read()
tok = re.search(r"ENRICHMENT_WORKER_TOKEN=(.+)", env).group(1).strip().strip('"').strip("'")
base = "https://srt-scent-engine-production.up.railway.app"
h = {"Authorization": f"Bearer {tok}"}
APPLY = "--apply" in sys.argv


def norm(s):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (s or "").lower())).strip()


def should_ignore(j):
    hn, nn = norm(j.get("house")), norm(j.get("name"))
    if hn in ("bath and body works", "bath body works"):
        return True  # body-mist line; not catalogued on Fragrantica
    if hn in ("victorias secret", "victoria s secret") and nn.startswith("pink"):
        return True  # VS Pink body-mist line
    if hn == "calvin klein" and "body mist" in nn:
        return True
    return False


rows = []
for st in ("pending", "failed"):
    r = requests.get(f"{base}/api/enrichment/jobs", params={"status": st, "limit": 100}, headers=h, timeout=60)
    r.raise_for_status()
    page = r.json().get("jobs", [])
    # Page past the 100-row cap when the API supports `offset`; older deploys
    # ignore the param and return the same first page, so stop on repeats.
    seen = {j["id"] for j in page}
    offset = len(page)
    while len(page) == 100:
        nxt = requests.get(
            f"{base}/api/enrichment/jobs",
            params={"status": st, "limit": 100, "offset": offset},
            headers=h, timeout=60,
        ).json().get("jobs", [])
        if not nxt or nxt[0]["id"] in seen:
            break
        seen.update(j["id"] for j in nxt)
        rows.extend(j for j in page)
        page = nxt
        offset += len(nxt)
    rows.extend(page)

targets = [j for j in rows if should_ignore(j)]
print(f"scanned {len(rows)} rows; would ignore {len(targets)}  (apply={APPLY})\n")
for j in targets:
    print(f"  {j['id']}  [{j.get('status')}]  {j.get('house')} - {j.get('name')}  req={j.get('requested_count')} fails={j.get('failure_count')}")

if not APPLY:
    print("\n(dry run -- rerun with --apply to execute)")
    sys.exit(0)

print("\napplying...")
for j in targets:
    resp = requests.post(
        f"{base}/api/enrichment/jobs/{j['id']}/ignore",
        json={"note": "no Fragrantica page for this product line (body-mist triage 2026-06-11)"},
        headers=h, timeout=30,
    )
    print(f"  ignore {j.get('house')} - {j.get('name')}: {resp.status_code}")
print("done.")
