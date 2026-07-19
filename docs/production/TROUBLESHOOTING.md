# Troubleshooting (Phase 13)

Symptom-oriented guide. See OPERATIONS.md for signals and CHAOS.md for the
failure model.

## Service won't start

| Symptom | Likely cause | Fix |
|---|---|---|
| Exits immediately, config error | `Config.Validate()` failed | Check log; e.g. `GRPC_TLS_ENABLED=true` without cert/key. |
| Cannot connect to DB | Bad `DB_URL` / DB down | Verify DSN, network, credentials. |
| Schema apply fails | `SCHEMA_PATH` missing/invalid | Ensure `schema.sql` present and readable. |

## `/readyz` returns 503

- Database unreachable → check DB health, network, pool exhaustion.
- Expected during dependency outages; instance recovers automatically. LB should
  route away until 200.

## Tasks not progressing

| Symptom | Cause | Fix |
|---|---|---|
| Tasks stuck `pending` | No workers / scheduler down | Scale workers; check scheduler. |
| Tasks stuck `running`, then reclaimed | Worker crashed | Recovery reclaims after stale timeout; check worker logs/OOM. |
| Slow throughput | Pool/queue saturation | Raise `DB_MAX_OPEN_CONNS`, `WORKER_POOL_SIZE`, add workers. |

## Events not delivered

| Symptom | Cause | Fix |
|---|---|---|
| Outbox backlog growing | Kafka down / publisher stalled | Restore Kafka; check publisher logs. Events persist and retry. |
| Duplicate events downstream | At-least-once + reclaim | Expected; consumers must be idempotent (they are). |
| Events stuck with errors | Retry exhaustion | Inspect `last_error`; fix root cause; poison events surface via metrics. |

## Connection pool exhaustion

- Symptom: latency spikes, `in-use == max`.
- Total connections = `DB_MAX_OPEN_CONNS` × replica count. Keep under DB limit.
- Fix: lower per-instance max or scale DB.

## gRPC failures

| Code | Cause | Behavior |
|---|---|---|
| `Unavailable` | Target down / network partition | Retried with backoff (max 2s). |
| `DeadlineExceeded` | Slow call | Bounded by `GRPC_REQUEST_TIMEOUT`. |
| TLS handshake error | Cert mismatch / mTLS | Verify cert/key/CA on both ends. |

## Rate limiting (429)

- Clients receive `429` + `Retry-After` when over `RATE_LIMIT_RPS`.
- Limit is per-instance; total = limit × replicas. Tune accordingly.

## Body too large (413)

- Write endpoint body exceeds `MAX_REQUEST_BODY_BYTES`. Increase limit or shrink
  payload.

## High latency / CPU

1. `PPROF_ENABLED=true` on one instance.
2. Capture `go tool pprof` CPU/heap (see PERFORMANCE.md).
3. Disable pprof afterward.
