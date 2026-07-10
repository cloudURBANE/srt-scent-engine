# Engine DB — Backup / Restore / DR Runbook (readiness gap E5)

The sCAST repo's `docs/DR_RUNBOOK.md` covers only the Supabase wardrobe DB.
This runbook covers the **engine's own Postgres** (the Railway-attached
instance `DATABASE_URL` points at). The `wardrobe-completeness-heal` doctrine
treats the two databases as separate systems; so does DR.

## What lives here (and how much it matters)

| Data | Table(s) | Loss impact |
| --- | --- | --- |
| Enrichment job queue | `enrichment_jobs` (+ status/lease columns) | Low–medium: pending jobs are re-enqueued by the SPA self-heal loop over time |
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

## Drill

- [x] One timed end-to-end restore into a scratch DB. Record: dump size,
      restore wall-clock (→ **RTO**), dump timestamp vs. incident time
      (→ **RPO**), and the exact commands used.
- [x] Write the measured RPO/RTO here.

### 2026-07-10 drill result

- Source: a live, CA-verified `pg_dump --format=custom --no-owner` of the
  production Railway Postgres service.
- Target: temporary Railway environment `dr-drill-20260710-1145`, containing
  only a scratch Postgres service. The environment was deleted after the
  verification passed.
- Dump size: **6,315,337 bytes** (about 6.0 MiB).
- Measured dump + clean restore: **19.4 seconds**.
- Verification: all **9** public tables matched the source by name; every table
  matched by row count (**5,061 rows** total); a local engine pointed at the
  restored DB returned `/readyz` HTTP 200 with `{"ok":true,"db":"ok"}`.
- Operator wall-clock: about **8 minutes** from scratch provisioning through
  verification and teardown, including tooling setup.
- Issue found and corrected: this runbook named the queue table `worker_jobs`;
  the code and both source/restored databases use `enrichment_jobs`.

Commands used (credentials supplied from Railway variables and never printed):

```bash
pg_dump "$PRODUCTION_URL?sslmode=verify-ca&sslrootcert=$RAILWAY_CA" \
  --format=custom --no-owner --file engine.dump
pg_restore --clean --if-exists --no-owner \
  --dbname "$SCRATCH_URL?sslmode=require" engine.dump
```

**Measured RPO:** effectively zero for this on-demand dump (snapshot taken at
drill start). The standing RPO remains **≤24h only after a daily platform backup
schedule is verified**.

**Measured RTO:** **19.4s** for dump + restore; about **8 minutes** end-to-end
including scratch provisioning and verification (target: ≤1h).
**Owner:** repo owner (single-operator project)

## Related

- Readiness probe `/readyz` (api.py) fails the platform healthcheck when this
  DB is unreachable — a botched restore cannot silently take traffic.
- Secrets rotation for `DATABASE_URL` and friends: see the sCAST repo's
  `docs/SECRETS_ROTATION.md` (covers both services).
