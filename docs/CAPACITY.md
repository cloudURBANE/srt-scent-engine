# Engine capacity ceiling & scale-out plan (readiness gap E9)

**Status:** documented 2026-07-10. Nothing here needs action at beta scale
(10–20 users). The point of this file is that the scale-out decision gets made
from a written plan, not improvised during an incident.

## Current topology (what actually serves traffic)

- One Railway service running **one uvicorn worker**:
  `uvicorn api:app --host 0.0.0.0 --port $PORT` (`railway.toml`, `Procfile`) —
  no `--workers`, no replicas.
- CPU-bound work (parsing, relevance scoring) is GIL-bound inside that single
  process. Sync routes run on anyio's default worker-thread pool (~40
  threads). The pool has no lifespan teardown — harmless on SIGTERM, noted in
  the 2026-07-09 gap audit.
- Postgres connections come from one pool sized by `DB_POOL_MAX_SIZE`.
- **In-process state (load-bearing for the plan below):** the per-IP rate
  limiter (`rate_limit.py` — `threading.Lock` + per-IP deques) and the Decodo
  daily request budget (`DECODO_DAILY_REQUEST_CAP`, default 2,500/day) live in
  process memory. They are correct **only while exactly one process serves
  traffic**.

## Why one worker is fine right now

Cold searches are dominated by network waits on Decodo/scrape targets, not
CPU, so the GIL rarely binds at beta traffic. Warm searches are DB reads.
10–20 beta users leave the process mostly idle; low hundreds of casual users
are likely still fine for warm reads.

## Scale-out triggers (watch these; any one firing = revisit)

1. **CPU-bound latency:** p95 of `/api/fragrances/search` degrades while
   Decodo and the DB are healthy — i.e. latency grows with concurrency, not
   with upstream slowness.
2. **Sustained CPU:** Railway service CPU pinned high (≳80%) across normal
   traffic, not just during a drain/heal batch.
3. **Traffic-correlated incidents:** the 5-minute readiness monitor
   (sCAST `readiness-monitor.yml`) opens engine incidents that correlate with
   traffic peaks rather than deploys.
4. **Legitimate 429s:** real users hitting the per-IP limits — that is a
   *limit-sizing* problem (`RATE_LIMIT_*_PER_MIN`), not a capacity problem;
   fix the env, not the topology.

## The plan, in lever order

1. **Vertical first (state-safe, zero code):** raise the Railway plan/limits
   for the service. No effect on in-process state. Buys headroom for the
   thread pool and GC; does not help pure-GIL CPU saturation much.
2. **`uvicorn --workers N` (same container):** real CPU parallelism, but
   **every in-process structure duplicates per worker** — each worker gets its
   own per-IP rate-limit map and its own Decodo daily budget, so effective
   per-IP limits and the daily Decodo cap multiply by N. Either divide the env
   caps by N (crude, workable) or do lever 4 first. In-flight dedup caches
   also split, so duplicate concurrent cold searches become possible.
3. **Railway replicas:** same multiplication caveats as lever 2 plus
   load-balancer spread. Do not turn on replicas before lever 4.
4. **Externalize the shared state (prerequisite for 2/3 done properly):** move
   the rate limiter and the Decodo daily budget to a shared store. Postgres is
   already present and sufficient at this scale (a counters table with
   `ON CONFLICT` upserts); Redis is the conventional choice if one is ever
   added to the engine. This is the same trade the sCAST side documented for
   its Redis-optional single replica (gap S8/H1) — revisit both together.

## Explicit non-goals at this scale

- No async rewrite of the sync scrape/parse paths — the thread pool is not the
  bottleneck at beta traffic.
- No Kubernetes/queue architecture — Railway vertical + workers covers the
  next order of magnitude.
