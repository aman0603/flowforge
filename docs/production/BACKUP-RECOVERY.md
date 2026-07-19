# Backup & Disaster Recovery (Phase 13)

PostgreSQL is the single source of truth. Redis and Kafka are recoverable/
regenerable. Protecting the database protects the system.

## What Holds State

| Store | Role | Backup priority |
|---|---|---|
| PostgreSQL | Source of truth: workflows, runs, tasks, outbox | **Critical** |
| Kafka | Event stream (derived from outbox) | Optional (replayable from outbox) |
| Redis | Ephemeral coordination (leases/heartbeats) | None — rebuilds itself |

## PostgreSQL Backups

- **Continuous:** enable WAL archiving / PITR (managed service or `pg_basebackup`
  + archive) for point-in-time recovery.
- **Periodic:** scheduled logical dumps as a secondary:

  ```bash
  pg_dump "$DB_URL" -Fc -f flowforge-$(date +%F).dump
  ```

- Store off-site/encrypted. Test restores regularly.

## Restore

```bash
pg_restore -d "$DB_URL" --clean --if-exists flowforge-YYYY-MM-DD.dump
```

Then start services; `schema.sql` is idempotent, and the outbox re-drives any
unpublished events on startup.

## Recovery Objectives (targets)

| Metric | Target | Basis |
|---|---|---|
| RPO | ≤ minutes | WAL/PITR interval |
| RTO | ≤ 1 hour | DB restore + service restart |

## Disaster Scenarios

| Disaster | Recovery |
|---|---|
| DB data loss | Restore from PITR/dump; outbox re-publishes unsent events. |
| Kafka loss | Recreate topic; unpublished outbox events re-drive; downstream reprocesses idempotently. |
| Redis loss | Restart; leases/heartbeats rebuild; recovery reclaims stale tasks. |
| Full region loss | Restore DB in new region, redeploy services, repoint DNS/LB. |

## Why Recovery Is Safe

- **Transactional outbox:** events are committed with state changes, so no event
  is lost even if Kafka was down at write time.
- **Idempotent consumers:** replayed events are deduplicated.
- **Lease-based reclamation:** in-flight tasks orphaned by a crash are re-run
  exactly-effectively-once via DB-guarded claims.

See CHAOS.md for the validated failure/recovery matrix.

## DR Drill Checklist

1. Restore latest backup to a staging DB.
2. Boot services against it; confirm `/readyz` = 200.
3. Confirm outbox drains and no duplicate side effects downstream.
4. Record actual RTO/RPO; adjust backup cadence if needed.
