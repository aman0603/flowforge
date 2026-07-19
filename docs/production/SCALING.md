# Scaling Guide (Phase 13)

FlowForge scales horizontally. Every service is stateless with respect to
in-memory data — PostgreSQL is the source of truth and `SELECT ... FOR UPDATE
SKIP LOCKED` coordinates claims, so adding instances never causes double
execution.

## What to Scale

| Bottleneck | Scale | Notes |
|---|---|---|
| API request volume | API replicas + LB | Stateless; rate limit is per-instance. |
| Task throughput | Workers | Most common lever; scale `worker`. |
| Event publish lag | Publishers | Outbox claims are lease-guarded. |
| Event consumption lag | Event consumers | Cap at Kafka partition count. |
| Scheduling latency | Schedulers | Idempotent; 1–N. |

## Vertical Tuning (per instance)

- `WORKER_POOL_SIZE` — concurrent task executors.
- `WORKER_QUEUE_CAPACITY` — buffer before backpressure.
- `WORKER_CLAIM_BATCH_SIZE` — tasks claimed per poll.
- `DB_MAX_OPEN_CONNS` / `DB_MAX_IDLE_CONNS` — DB concurrency.

## Connection Budget (critical)

Total DB connections ≈ `DB_MAX_OPEN_CONNS` × (sum of all replicas across all
services). This must stay under the PostgreSQL `max_connections` limit.

Example: 4 workers + 2 API + 1 scheduler + 1 recovery + 2 publishers = 10
instances × 25 = **250 connections**. Size the DB or use a pooler (PgBouncer)
accordingly.

## Kafka Partitions

Consumer parallelism is capped by partition count. To scale consumers beyond
current partitions, add partitions first.

## Redis

Coordination only. A single Redis (or a small cluster) serves many instances;
it is not the throughput bottleneck. Correctness does not depend on Redis (see
CHAOS.md).

## Scaling Signals

- Workers: rising claim latency / queue depth at capacity → add workers.
- Consumers: growing Kafka lag → add consumers (up to partitions).
- DB: `in-use == max`, rising query latency → scale DB / add pooler.

## Autoscaling

Scale workers/consumers on task-queue depth and Kafka lag; scale API on
CPU/RPS. Always re-check the DB connection budget when raising max replicas.
