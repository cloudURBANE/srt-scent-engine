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

**Errors:** `400` missing/invalid `id` and no usable `source_url` · `502`
detail fetch failure.

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
