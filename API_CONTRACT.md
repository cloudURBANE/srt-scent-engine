# Fragrance Engine API — Frontend Contract

A thin HTTP wrapper around the existing fragrance engine. The engine, parser
selectors, resolver/search/fallback behavior, and `derived_metrics_adapter.py`
are **unchanged** — this layer only exposes their existing entry points over
HTTP.

## Base URL

Railway assigns the public URL at deploy time (**Settings → Networking →
Generate Domain**). It looks like:

```
https://<service-name>-production.up.railway.app
```

The frontend reads it from `VITE_FRAGRANCE_API_URL`.

CORS is restricted to the origins in the `FRONTEND_ORIGINS` env var
(comma-separated). Defaults to `http://localhost:5173` (Vite dev) when unset.
Set it on Railway to your deployed frontend origin.

---

## `GET /health`

Liveness probe. Used by Railway's healthcheck and the frontend.

**Response `200`**
```json
{ "ok": true }
```

---

## `GET /api/fragrances/search`

Run the dual-source resolver and return ranked candidates.

**Query params**

| param | required | description                       |
|-------|----------|-----------------------------------|
| `q`   | yes      | Search query, e.g. `silver mountain water` |

**Response `200`**
```json
{
  "query": "silver mountain water",
  "results": [
    {
      "id": "<opaque token>",
      "name": "Silver Mountain Water",
      "house": "Creed",
      "year": 1995,
      "gender": "Unisex / Unspecified",
      "source_url": "https://www.fragrantica.com/..."
    }
  ]
}
```

Notes:
- `id` is an **opaque token** — pass it back verbatim to `/api/fragrances/details`.
  Do not parse it.
- `gender` is the engine's default placeholder at search time; the engine only
  resolves the real gender during the detail fetch. The detail response carries
  the resolved value.
- `year` is an integer when the engine has a clean year, otherwise `null`.

**Errors:** `400` empty query · `502` resolver failure.

---

## `POST /api/fragrances/details`

Fetch the full detail bundle for a fragrance returned by `/search`.

**Request body** (JSON)
```json
{
  "id": "<opaque token from a search result>",
  "source_url": "https://www.fragrantica.com/..."
}
```

Send `id` (preferred — it carries both source URLs, so the engine keeps its
dual-source behavior). `source_url` alone is accepted as a fallback but yields
single-source detail.

**Response `200`**
```json
{
  "name": "Silver Mountain Water",
  "house": "Creed",
  "year": 1995,
  "gender": "Unisex / Unspecified",
  "derived_metrics": {
    "headline": { "crowd_consensus_score": 74, "label": "...", "summary": "..." },
    "community_interest_score": { "score": 77, "...": "..." },
    "value_score": { "dominant_label": "Way Overpriced", "...": "..." },
    "performance_score": { "longevity_label": "Moderate", "sillage_label": "Moderate", "...": "..." },
    "...": "..."
  },
  "raw": {
    "description": "...",
    "bn_consensus": { "pos": [980, 78], "neu": [180, 14], "neg": [100, 8] },
    "frag_cards": { "Longevity": [ { "label": "...", "count": "..." } ] },
    "pros_cons": ["..."],
    "reviews": [ { "text": "...", "source": "Fragrantica" } ],
    "notes": {
      "has_pyramid": true,
      "top": ["Bergamot"], "heart": ["..."], "base": ["..."], "flat": []
    },
    "source_urls": { "bn_url": "...", "frag_url": "..." }
  }
}
```

- `derived_metrics` is the bundle produced by `derived_metrics_adapter.py`,
  passed through verbatim. If the adapter raised, the engine sets it to `None`
  and the API returns `"derived_metrics": null` — the raw detail is still
  returned. It is never fabricated.
- Any metric group inside `derived_metrics` may itself be `null` when its
  source data is missing — render defensively.

- `source_coverage` is a read-only honesty signal describing which sources
  actually backed the bundle. When Fragrantica is unreachable the response is
  Basenotes-only and `source_coverage.complete` is `false` — the frontend must
  not present that as complete. Shape:
  ```json
  {
    "basenotes": true,
    "fragrantica": true,
    "fragrantica_cached": true,
    "fragrantica_cache_source": "db",
    "basenotes_linked": true,
    "fragrantica_linked": true,
    "derived_metrics": "full",
    "complete": true
  }
  ```
  `fragrantica_cache_source` is `"db"` (durable cache), `"json"` (bundled file
  cache), or `null` (live data, or no Fragrantica contribution at all).

**Errors:** `400` missing/invalid `id` and no usable `source_url` · `502`
detail fetch failure.

---

## Enrichment pipeline

When `/api/fragrances/details` returns a partial result (a Fragrantica URL is
linked but no live or cached Fragrantica data is available), the API enqueues
an enrichment job in durable Postgres storage. An offline worker (a later pass)
fetches the missing Fragrantica detail from an environment that is not blocked,
and uploads the parsed payload back — which is durably cached so future detail
requests hydrate from the database.

### `GET /api/enrichment/status` (public)

Aggregate job counts by status. No job content, no auth.

```json
{ "enabled": true, "counts": { "pending": 12, "processing": 1, "completed": 40, "failed": 2, "ignored": 3 } }
```

`enabled` is `false` when `DATABASE_URL` is unset; `counts` is then `{}`.

### Worker endpoints (protected — **not for the frontend**)

These require `Authorization: Bearer <ENRICHMENT_WORKER_TOKEN>`. The token is
for the offline worker only and must never be shipped to the frontend. Missing
or invalid token → `401`. `ENRICHMENT_WORKER_TOKEN` / `DATABASE_URL` unset →
`503`.

| Endpoint | Purpose |
|----------|---------|
| `GET /api/enrichment/jobs?status=pending&limit=20` | List jobs, priority-first (`limit` ≤ 100). |
| `POST /api/enrichment/jobs/{id}/claim` | Claim a pending (or stale-processing) job under a lease. |
| `POST /api/enrichment/jobs/{id}/complete` | Upload parsed Fragrantica detail; rejects empty `frag_cards`. |
| `POST /api/enrichment/jobs/{id}/fail` | Record a failure (`retryable: true` returns the job to `pending`). |
| `POST /api/enrichment/jobs/{id}/ignore` | Permanently retire a bad job. |

---

## Example frontend flow (Vite)

```js
const BASE = import.meta.env.VITE_FRAGRANCE_API_URL;

// 1. search
const s = await fetch(
  `${BASE}/api/fragrances/search?q=${encodeURIComponent("silver mountain water")}`
).then(r => r.json());

// 2. pick a result, fetch detail (echo its opaque id back)
const pick = s.results[0];
const d = await fetch(`${BASE}/api/fragrances/details`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ id: pick.id, source_url: pick.source_url }),
}).then(r => r.json());

console.log(d.derived_metrics?.headline?.crowd_consensus_score);
```
