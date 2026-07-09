# Engine DB — Backup / Restore / DR Runbook (readiness gap E5)

The sCAST repo's `docs/DR_RUNBOOK.md` covers only the Supabase wardrobe DB.
This runbook covers the **engine's own Postgres** (the Railway-attached
instance `DATABASE_URL` points at). The `wardrobe-completeness-heal` doctrine
treats the two databases as separate systems; so does DR.

## What lives here (and how much it matters)

| Data | Table(s) | Loss impact |
| --- | --- | --- |
| Enrichment job queue | `worker_jobs` (+ status/lease columns) | Low–medium: pending jobs are re-enqueued by the SPA self-heal loop over time |
| Durable FG detail cache | `fg_detail_cache` | Medium: rebuilding needs live FG access, which Railway cannot do (Cloudflare 403); until re-warmed, coverage falls back to the bundled `fg_cache/*.json` |
| Mobile accounts + magic links | `mobile_*`, `magic_links` | High for the (small) mobile-account population: bcrypt hashes are not recoverable |
| Enrichment metrics/state | misc. state tables | Low: observability, re-derivable |

The schema itself needs **no backup**: `db.init_db()` bootstraps it idempotently
on boot (`CREATE TABLE IF NOT EXISTS`), so a restore only has to bring data.

## Backup

- **Platform layer:** Railway Postgres supports scheduled backups on paid
  plans — verify in the Railway dashboard (service → Backups) that a **daily**
  schedule is enabled and note the retention. _Status: TBD — verify, this is
  the ops step the drill below confirms._
- **Logical layer (works regardless of plan):** a `pg_dump` from any machine
  with the `DATABASE_URL` (e.g. via `railway run` or the public proxy):

  ```bash
  pg_dump "$DATABASE_URL" --format=custom --no-owner \
    --file "engine_$(date -u +%Y%m%dT%H%M%SZ).dump"
  ```

  Store outside Railway (local disk + any offsite copy). Cadence: weekly
  minimum; before any risky data operation (bulk sweep, schema experiment).

## Restore

1. Provision a scratch Postgres (Railway new database, or local Docker).
2. `pg_restore --no-owner --dbname "$SCRATCH_URL" engine_<stamp>.dump`
3. Point a **local** engine at it (`DATABASE_URL=$SCRATCH_URL uvicorn api:app`)
   and check `/readyz` returns `{"ok": true, "db": "ok"}` plus a spot-read of
   `fg_detail_cache` row counts.
4. Only after 1–3 verify, repoint the production service's `DATABASE_URL` (or
   restore in place via Railway's backup restore).

## Drill (not yet performed)

- [ ] One timed end-to-end restore into a scratch DB. Record: dump size,
      restore wall-clock (→ **RTO**), dump timestamp vs. incident time
      (→ **RPO**), and the exact commands used.
- [ ] Write the measured RPO/RTO here. Until this box is checked, the numbers
      are unknown and this runbook is aspiration, not capability.

**RPO:** TBD (target: ≤24h via daily platform backup)
**RTO:** TBD (target: ≤1h)
**Owner:** repo owner (single-operator project)

## Related

- Readiness probe `/readyz` (api.py) fails the platform healthcheck when this
  DB is unreachable — a botched restore cannot silently take traffic.
- Secrets rotation for `DATABASE_URL` and friends: see the sCAST repo's
  `docs/SECRETS_ROTATION.md` (covers both services).
