# Operations Runbook

Day-2 operations for FlowForge: startup/shutdown, health verification, common
failures, recovery procedures, and incident response. Deeper material lives in
[production/OPERATIONS.md](production/OPERATIONS.md),
[production/TROUBLESHOOTING.md](production/TROUBLESHOOTING.md), and
[production/CHAOS.md](production/CHAOS.md).

## Service Startup / Shutdown

- **Startup order:** `db, redis, kafka` → `scheduler, recovery` → `worker,
  publisher, app` (see [deployment.md](deployment.md)).
- **Shutdown:** send `SIGTERM`; processes drain gracefully. Workers return queued
  tasks to `READY` and wait `WORKER_SHUTDOWN_GRACE_PERIOD_MS`.

```bash
docker compose up -d              # start
docker compose stop worker        # graceful stop of one service
docker compose down               # stop all (keeps pgdata volume)
```

## Health Verification

```bash
curl -fsS http://localhost:8080/healthz     # liveness
curl -fsS http://localhost:8080/readyz      # readiness (503 if DB down)
grpcurl -plaintext -d '{"readiness":true}' localhost:9090 \
  flowforge.health.HealthService/Check       # gRPC service health
curl -fsS http://localhost:9091/metrics | grep flowforge_   # metrics present
```

## Observability

| Signal | Where |
|---|---|
| Metrics | Prometheus `:9090`; per-process `/metrics` on `:9091` |
| Dashboards | Grafana `:3000` (FlowForge Overview) |
| Traces | Jaeger `:16686` (enable `OTEL_DISABLED=false`) |
| Logs | JSON on stdout (ship via log agent) |
| Alerts | `deploy/prometheus/alerts.yml` |

### Key metrics to watch

- `flowforge_tasks_{claimed,started,completed,failed,timed_out}_total`
- `flowforge_worker_queue_depth`
- `flowforge_outbox_{published,failed,retried,cleaned}_total`
- `flowforge_tasks_recovered_total`
- `flowforge_http_request_duration_seconds` (p95), `flowforge_grpc_request_duration_seconds`

### Provisioned alerts (`alerts.yml`)

| Alert | Condition |
|---|---|
| `FlowForgeServiceDown` | `up == 0` for 1m (critical) |
| `FlowForgeHighTaskFailureRate` | >25% failures over 5m |
| `FlowForgeWorkerQueueBacklog` | queue depth >1000 for 10m |
| `FlowForgeOutboxPublishFailures` | publish failure rate >0 for 5m |
| `FlowForgeHighHTTPLatency` | p95 > 1s for 5m |

## Common Failures & Recovery

FlowForge recovers from dependency outages automatically; prefer letting
built-in recovery run over manual DB edits. See
[production/CHAOS.md](production/CHAOS.md) for the full failure matrix.

| Failure | Symptom | Recovery |
|---|---|---|
| **Worker crash** | tasks stuck `RUNNING`, then reclaimed | Recovery loop reclaims after stale timeout; another worker re-runs (idempotent). Check OOM/logs. |
| **Scheduler down** | no new claims | Restart; stateless, resumes from DB. |
| **Recovery down** | stale tasks not reclaimed | Restart; workers' own recovery loop also covers this. |
| **Publisher down** | outbox backlog grows | Restart; leases expire and another publisher reclaims; events persist. |
| **Kafka outage** | outbox backlog, publish failures | Restore Kafka; publisher retries with backoff; no event loss. |
| **PostgreSQL outage** | `/readyz` 503; processing paused | Restore DB; pool reconnects; no task loss. |
| **Redis outage** | lease/heartbeat coordination degraded | Postgres remains source of truth; `SKIP LOCKED` still guards claims; restore Redis. |

### Database recovery

- Restore from PITR/backup, then restart services. `schema.sql` is idempotent and
  the outbox re-drives unpublished events. Full procedure:
  [production/BACKUP-RECOVERY.md](production/BACKUP-RECOVERY.md).

### Redis recovery

- Restart Redis. Leases/heartbeats rebuild from live activity; stale tasks are
  reclaimed by the recovery loop. No durable state is lost.

### Kafka recovery

- Restart brokers (recreate topic if lost). Unpublished outbox rows publish once
  Kafka is reachable; idempotent consumers dedupe any duplicates.

### Publisher / Worker / Scheduler / Recovery recovery

- All are stateless: restart the process. In-flight leases expire; DB-guarded
  claims and fencing tokens prevent double execution.

## Routine Tasks

- **Scale workers/consumers:** `docker compose up --scale worker=N` or adjust
  replicas. Re-check the DB connection budget ([production/SCALING.md](production/SCALING.md)).
- **Profiling:** set `PPROF_ENABLED=true` on one instance, capture via
  `go tool pprof http://host:9091/debug/pprof/profile`, then disable. See
  [production/PERFORMANCE.md](production/PERFORMANCE.md).
- **Outbox retention:** published events prune automatically per
  `OUTBOX_RETENTION`.
- **Inspect DLQ:** `GET /api/v1/dead-letter?limit=&offset=`.

## Operational Checklist

- [ ] All services `up` in Prometheus targets.
- [ ] `/readyz` = 200 on all API instances.
- [ ] No firing alerts.
- [ ] Outbox backlog near zero; no sustained publish failures.
- [ ] Worker queue depth within expected range.
- [ ] DB pool in-use well below max.

## Incident Response

1. Check `/readyz` across instances to isolate app vs dependency.
2. Inspect metrics/traces for the failing stage (claim → execute → publish).
3. Correlate with recent deploys or dependency alerts.
4. For dependency outages, rely on built-in recovery; avoid manual DB mutation.
5. Capture a pprof profile if the symptom is latency/CPU.
6. After resolution, verify backlog drains and no duplicate side effects
   downstream.
