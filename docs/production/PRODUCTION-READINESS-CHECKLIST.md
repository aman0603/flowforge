# Production Readiness Checklist (Phase 13)

Gate before promoting FlowForge to production. Grouped by concern; each item
links to the relevant runbook.

## Configuration (CONFIGURATION.md)

- [ ] `ENV=production`, `LOG_LEVEL=info`.
- [ ] `DB_URL`, `REDIS_ADDR`, `KAFKA_BROKERS` point at managed/HA instances.
- [ ] Secrets injected via env/secret manager, not committed (`.env` gitignored).
- [ ] `Config.Validate()` passes at startup (no fail-fast errors in logs).

## Reliability (READINESS-AUDIT.md)

- [ ] DB pool sized within DB connection budget (see SCALING.md).
- [ ] HTTP timeouts active (ReadHeader/Idle/MaxHeaderBytes).
- [ ] `/healthz` and `/readyz` wired to orchestrator probes.
- [ ] Graceful shutdown verified (SIGTERM drains, recovery reclaims).

## Security (SECURITY.md)

- [ ] `GRPC_TLS_ENABLED=true` with valid cert/key (+ CA for mTLS).
- [ ] `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` set for public endpoints.
- [ ] `MAX_REQUEST_BODY_BYTES` set on write endpoints.
- [ ] Container runs as non-root; image scanned.
- [ ] Default dev credentials overridden.

## Performance (PERFORMANCE.md)

- [ ] Load test run against staging; latency/throughput acceptable.
- [ ] `PPROF_ENABLED=false` in normal operation.
- [ ] Worker pool/queue tuned for target throughput.

## Resilience (CHAOS.md)

- [ ] Chaos suite passes: `go test -tags chaos ./...`.
- [ ] Dependency-outage behavior validated (Kafka/DB/Redis) in staging.
- [ ] Idempotent consumers confirmed under duplicate delivery.

## Observability (OPERATIONS.md)

- [ ] Metrics scraped; dashboards live.
- [ ] `OTEL_DISABLED=false` with OTLP endpoint; traces flowing.
- [ ] JSON logs shipped to aggregation.
- [ ] Alerts configured (outbox backlog, readiness, failure rate, pool, stale
      reclamations).

## Scalability (SCALING.md)

- [ ] Horizontal scaling verified for workers/consumers.
- [ ] DB connection budget re-checked at max replica count.
- [ ] Kafka partitions ≥ target consumer count.

## Backup & DR (BACKUP-RECOVERY.md)

- [ ] PITR/WAL archiving enabled; periodic dumps scheduled.
- [ ] Restore tested; RTO/RPO measured.
- [ ] DR drill completed.

## Deployment (DEPLOYMENT.md)

- [ ] Rolling update strategy (one replica at a time) confirmed.
- [ ] Migrations applied safely (additive, idempotent).
- [ ] Rollback plan documented.
