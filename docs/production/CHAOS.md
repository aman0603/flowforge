# Resilience & Chaos Testing (Phase 13, Loop 13.4)

FlowForge's failure model and the chaos test suite that validates recovery and
idempotency. The automated suite is infra-free and gated behind the `chaos`
build tag so the default test run stays fast:

```bash
go test -tags chaos ./...
```

## Automated Chaos Suite

`internal/outbox/chaos_test.go` (build tag `chaos`) injects failures into the
transactional-outbox publisher using in-memory fault-injecting fakes:

| Test | Injected failure | Asserted behavior |
|---|---|---|
| `TestChaosDuplicateDeliveryOnCrashAfterAck` | Crash between Kafka ack and outbox mark | Event is re-claimed and re-published (at-least-once), never lost; eventually marked |
| `TestChaosRetryExhaustion` | Kafka always fails | Errors recorded (drives retry/DLQ); event never marked published |
| `TestChaosTransientKafkaThenRecovery` | N transient Kafka failures then success | Exactly one successful publish; event marked; recovers automatically |

These encode the core invariant: **at-least-once delivery with
duplicate-tolerant consumers**, never silent data loss.

## Failure Scenario Matrix

For each production failure, the expected behavior, recovery mechanism, and
acceptable impact:

| Scenario | Cause | Expected behavior | Recovery | Acceptable impact |
|---|---|---|---|---|
| Worker crash | OOM / node loss | Claimed/running tasks become stale | Recovery service reclaims after `RUNNING_STALE_TIMEOUT`; another worker re-runs (idempotent) | Delayed task; no loss |
| Scheduler crash | Process exit | No new claims scheduled | Restart (compose `restart:`); stateless, resumes from DB | Scheduling pause |
| Publisher crash | Process exit | Claimed outbox events stall | Claim lease expires; another publisher reclaims | Possible duplicate publish |
| Kafka outage | Broker down | Publish fails | Outbox retries with backoff; events persist in DB | Event delivery lag |
| PostgreSQL outage | DB down | Reads/writes fail; `/readyz` → 503 | Retry on reconnect; pool re-establishes; no traffic routed while unready | Full processing pause |
| Redis outage | Cache down | Lease/heartbeat coordination degraded | Postgres remains source of truth; `SKIP LOCKED` still guards claims | Reduced coordination, correctness preserved |
| Network partition | Split network | gRPC calls fail with Unavailable | Retryable codes retried with backoff; DB claim guards prevent double-exec | Latency / partial degradation |
| Slow DB queries | Lock contention / load | Elevated latency | Connection pool caps + per-attempt timeouts; backpressure via bounded queue | Throughput drop |
| High-latency gRPC | Overload | Slow internal calls | Per-attempt deadline (`GRPC_REQUEST_TIMEOUT`) + capped retries | Tail latency |
| Node restart | Deploy / reboot | Graceful shutdown drains in-flight work | Signal-context cancellation; leases expire; recovery reclaims | Brief unavailability |
| Duplicate message | At-least-once Kafka | Consumer detects already-processed | Idempotent consumer (dedup) treats as success | None |

## Design Invariants Validated

- **Idempotency:** task execution and event consumption tolerate re-delivery.
- **Crash safety:** a crash before persistence leaves work reclaimable; after
  persistence, downstream duplicates are tolerated.
- **Source of truth:** PostgreSQL holds durable state; Redis is coordination
  only, so a Redis outage never corrupts state.
- **Backpressure:** bounded worker pool + queue prevent unbounded resource use.

## Manual / Infra Chaos (docker compose)

With the full stack running, exercise real outages:

```bash
docker compose up --build --scale worker=3
docker compose stop kafka      # observe outbox backlog + recovery on restart
docker compose stop db         # observe /readyz 503 and no task loss
docker compose kill worker     # observe stale-task reclamation by recovery
docker compose start kafka db worker
```

Verify via Grafana (task/outbox metrics), Jaeger (trace continuity), and the
`/readyz` endpoint that the system recovers without manual data repair.
