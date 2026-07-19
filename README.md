# FlowForge

**A distributed, DAG-based workflow engine written in Go.**

FlowForge runs workflows defined as directed acyclic graphs (DAGs) of tasks
across a pool of stateless workers. Task and workflow state lives in PostgreSQL,
workers coordinate through Redis leases, and lifecycle events are published to
Kafka via a transactional outbox. It handles crash recovery, retries with
backoff, dead-letter handling, and ships with metrics, tracing, and structured
logs.

I built FlowForge to explore how a real workflow engine holds together — the
kind of correctness problems (double execution, lost events, partial failure)
that only show up once work is distributed across processes.

---

## Table of Contents

- [Why FlowForge?](#why-flowforge)
- [Design Principles](#design-principles)
- [Non-Goals](#non-goals)
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Repository Layout](#repository-layout)
- [Installation](#installation)
- [Local Development](#local-development)
- [Docker Setup](#docker-setup)
- [Running FlowForge](#running-flowforge)
- [Example Workflow](#example-workflow)
- [API Overview](#api-overview)
- [gRPC Overview](#grpc-overview)
- [Kafka Overview](#kafka-overview)
- [Configuration](#configuration)
- [Observability](#observability)
- [Testing](#testing)
- [Failure Handling](#failure-handling)
- [Distributed Systems Concepts Demonstrated](#distributed-systems-concepts-demonstrated)
- [Documentation](#documentation)
- [Future Improvements](#future-improvements)

---

## Why FlowForge?

Workflow engines look simple from the outside: run a graph of tasks in
dependency order. The hard parts are all in the failure modes:

- A worker claims a task, then crashes mid-execution. Who reclaims it, and how do
  you stop the original worker from finishing later and corrupting state?
- A task and its "completed" event must both be recorded, or neither. How do you
  avoid publishing an event for a database change that rolled back?
- Two workers poll for work at the same instant. How do you guarantee only one
  runs a given task?

FlowForge is my attempt to answer those questions with a design that leans on
established patterns — database-backed queues with `SELECT ... FOR UPDATE SKIP
LOCKED`, fencing tokens for lease safety, and the transactional outbox pattern —
rather than hand-rolled coordination. It is a learning project first, but the
correctness properties are real and tested.

## Design Principles

- **PostgreSQL is the source of truth.** Redis and Kafka are supporting
  infrastructure; if either is wiped, the system can be rebuilt from Postgres.
  Correctness never depends on Redis being available.
- **Assume at-least-once, design for idempotency.** Nothing assumes
  exactly-once delivery or execution. Consumers dedupe by event ID; task
  transitions are guarded so a replayed action is a no-op.
- **Claiming is a database operation.** Task claiming uses `FOR UPDATE SKIP
  LOCKED` so concurrent workers can safely pull work without distributed locks
  in the hot path. Redis leases handle liveness, not correctness.
- **Fencing tokens over trust.** Every lease carries a monotonic token; a stale
  worker that wakes up after its lease expired cannot complete a task that has
  been reclaimed.
- **Stateless processes.** Workers, the scheduler, recovery, and the publisher
  hold no durable in-memory state, so any of them can be scaled or restarted.
- **Clear ownership per transport.** REST for external clients, gRPC for
  internal synchronous calls, Kafka for async events. Each boundary has one job.

## Non-Goals

FlowForge deliberately does **not** try to be:

- **A general-purpose task queue.** It models DAGs with dependencies, not
  fire-and-forget jobs.
- **A multi-tenant SaaS platform.** There is no authentication, authorization,
  or tenant isolation built in — it expects to run behind a trusted boundary.
- **A sandbox for arbitrary code.** Executors are compiled into the worker
  binary; there is no dynamic code loading or untrusted-payload execution.
- **A drop-in replacement for Temporal, Airflow, or Argo.** It shares ideas with
  them but is intentionally smaller in scope.
- **Kubernetes-native.** The reference deployment is Docker Compose; running on
  an orchestrator is possible but not provided.

## Overview

A **workflow** in FlowForge is a definition consisting of tasks and their
dependencies. When a workflow is instantiated as a **run**, tasks with no
unmet dependencies become `READY`, are claimed by workers, executed, and — on
completion — unlock their dependents. Every state transition is persisted to
PostgreSQL (the single source of truth) and emitted as an event through a
transactional outbox to Kafka for downstream consumers.

See [Design Principles](#design-principles) for the reasoning behind these
choices.

## Features

- **DAG workflows** with dependency resolution and cycle detection.
- **Durable state machine** for workflow runs and task runs in PostgreSQL.
- **Concurrent worker pools** with bounded, backpressure-aware task claiming
  (`FOR UPDATE SKIP LOCKED`).
- **Distributed leasing** via Redis with fencing tokens and heartbeats.
- **Lease-aware crash recovery** — stale `CLAIMED`/`RUNNING` tasks are safely
  reclaimed by a recovery service.
- **Retries with exponential backoff** and a **dead-letter queue** for tasks
  that exhaust their retries.
- **Transactional outbox → Kafka** for at-least-once event delivery, keyed by
  workflow run for per-workflow ordering.
- **gRPC internal services** (scheduler, recovery, health) with optional
  TLS/mTLS.
- **Observability** — Prometheus metrics, OpenTelemetry tracing (Jaeger), and
  structured logs.
- **Operational guards** — connection pooling, HTTP timeouts, opt-in rate
  limiting and request body limits, non-root containers, and health/readiness
  probes (all off by default).

## Architecture

FlowForge runs as a set of independent, horizontally scalable processes sharing
three backing stores with clear ownership boundaries:

| Store | Owns |
|---|---|
| **PostgreSQL** | Durable workflow/task state; transactional outbox (source of truth) |
| **Redis** | Ephemeral coordination — worker heartbeats, task leases |
| **Kafka** | Asynchronous event stream (workflow lifecycle events) |

```
                 ┌────────────┐
  clients ─────► │  API (REST)│  create workflows / runs, query status
                 └─────┬──────┘
                       │ writes state + outbox (same TX)
                       ▼
                 ┌────────────┐        ┌───────────────┐
                 │ PostgreSQL │◄──────►│  Scheduler    │ (gRPC) claim / promote
                 │  (truth)   │        └───────────────┘
                 └─────┬──────┘        ┌───────────────┐
                       ▲               │  Recovery     │ (gRPC) reclaim stale
             leases /  │               └───────────────┘
             heartbeats│  ┌────────────┐
                 ┌─────┴──┐│  Workers   │ claim → execute → persist
                 │ Redis  ││ (stateless)│
                 └────────┘└─────┬──────┘
                                 │ state change emits outbox row
                                 ▼
                 ┌────────────┐ poll+publish ┌────────┐   ┌───────────┐
                 │ Publisher  │─────────────►│ Kafka  │──►│ Consumers │
                 └────────────┘              └────────┘   └───────────┘

  Telemetry: every process → Prometheus (metrics) + Jaeger (OTLP traces)
```

**Boundary invariants** (validated in [docs/architecture.md](docs/architecture.md)):

- REST owns **external** APIs; gRPC owns **synchronous internal** communication.
- Kafka owns **asynchronous** events; Redis owns **ephemeral** coordination;
  PostgreSQL owns **durable** state.
- Workers are stateless. The **publisher never mutates** workflow state. The
  **scheduler and recovery services never execute** tasks.

See [docs/architecture.md](docs/architecture.md) and
[docs/diagrams/](docs/diagrams/) for full diagrams.

## Technology Stack

| Concern | Technology |
|---|---|
| Language | Go 1.26 |
| Durable store | PostgreSQL 16 (`github.com/lib/pq`) |
| Coordination | Redis 7 (`github.com/redis/go-redis/v9`) |
| Event streaming | Kafka (`github.com/segmentio/kafka-go`) |
| Internal RPC | gRPC + Protobuf (`google.golang.org/grpc`) |
| Metrics | Prometheus (`client_golang`) via OpenTelemetry |
| Tracing | OpenTelemetry → OTLP → Jaeger |
| Logging | Zap (structured JSON) |
| Packaging | Docker, Docker Compose |

## Repository Layout

```
flowforge/
├── cmd/                    # Entrypoints (one binary per process)
│   ├── flowforge/          #   REST API / control plane
│   ├── worker/             #   Task execution engine
│   ├── scheduler/          #   gRPC claim + retry-promotion service
│   ├── recovery/           #   gRPC stale-task recovery service
│   ├── publisher/          #   Transactional outbox → Kafka relay
│   └── event-consumer/     #   Reference idempotent Kafka consumer
├── internal/
│   ├── api/                # HTTP server, handlers, routing
│   ├── config/             # Env-driven configuration + validation
│   ├── dag/                # DAG validation + cycle detection
│   ├── grpcutil/           # gRPC server/client, TLS, retry, health
│   ├── model/              # Domain models + event envelopes
│   ├── outbox/             # Outbox publisher + Kafka producer
│   ├── proto/              # Generated protobuf/gRPC code
│   ├── recovery/           # Recovery service (local + gRPC)
│   ├── repository/         # PostgreSQL repository (all SQL)
│   ├── scheduler/          # Scheduler service (local + gRPC)
│   ├── telemetry/          # Metrics, tracing, logging, middleware
│   └── worker/             # Worker pool, coordinator, executors
├── proto/flowforge/        # .proto contracts (common/health/scheduler/recovery)
├── deploy/                 # Prometheus + Grafana provisioning
├── docs/                   # Architecture, API, gRPC, deployment, diagrams
├── scripts/                # loadtest.sh, gen-proto.sh
├── schema.sql              # Database schema (applied on startup)
├── docker-compose.yml      # Full local stack
└── Dockerfile              # Multi-stage, non-root image
```

## Installation

**Prerequisites:** Go 1.26+, Docker + Docker Compose (for the full stack).

```bash
git clone https://github.com/aman0603/flowforge.git
cd flowforge
go mod download
go build ./...
```

## Local Development

Common commands:

```bash
go build ./...            # compile all packages/binaries
go test ./...             # unit tests (fast; integration/chaos excluded)
go test -race ./...       # race detector
gofmt -l .                # formatting check
go vet ./...              # static analysis

# Integration tests (require a real Postgres)
TEST_DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable" \
  go test -tags integration ./...

# Chaos / failure-injection tests (infra-free)
go test -tags chaos ./internal/outbox/...

# Benchmarks
go test -bench=. ./internal/dag/... ./internal/grpcutil/...
```

Regenerate gRPC code after editing `.proto` files:

```bash
./scripts/gen-proto.sh    # requires protoc, protoc-gen-go, protoc-gen-go-grpc
```

## Docker Setup

The full stack (Postgres, Redis, Kafka, all FlowForge services, Prometheus,
Grafana, Jaeger) runs via Docker Compose:

```bash
cp .env.example .env       # set DB_URL, POSTGRES_*, secrets
docker compose up --build
```

Scale stateless services:

```bash
docker compose up --build --scale worker=4 --scale publisher=2
```

**Exposed ports:**

| Service | Port |
|---|---|
| API (HTTP) | `8080` |
| Scheduler (gRPC) | `9091` → `9090` |
| Recovery (gRPC) | `9092` → `9090` |
| Prometheus | `9090` |
| Grafana | `3000` (admin/admin) |
| Jaeger UI | `16686` |
| PostgreSQL | `5432` |
| Redis | `6379` |
| Kafka | `9092` |

> `event-consumer` is a reference consumer and is not part of the compose stack;
> run it manually against `KAFKA_BROKERS`.

## Running FlowForge

Each binary is a separate process. Locally without Docker, start the backing
stores yourself, then run the API:

```bash
export DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable"
go run ./cmd/flowforge     # API on :8080, applies schema.sql on startup
go run ./cmd/scheduler     # gRPC :9090
go run ./cmd/recovery      # gRPC :9090
go run ./cmd/worker        # executes tasks
go run ./cmd/publisher     # relays outbox → Kafka
```

Workers run in-process (local) scheduler/recovery clients by default. Set
`SCHEDULER_ADDR` / `RECOVERY_ADDR` to use the standalone gRPC services.

## Example Workflow

**1. Create a workflow definition** (a two-task DAG: `build` → `deploy`):

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "ci-pipeline",
    "description": "build then deploy",
    "tasks": [
      { "name": "build",  "task_type": "SLEEP", "config": {"duration_ms": 500}, "max_retries": 3 },
      { "name": "deploy", "task_type": "SLEEP", "config": {"duration_ms": 200}, "dependencies": ["build"] }
    ]
  }'
# → 201 { "id": "<definition-id>", ... }
```

**2. Start a run:**

```bash
curl -X POST http://localhost:8080/runs \
  -H 'Content-Type: application/json' \
  -d '{ "workflow_definition_id": "<definition-id>", "input": {} }'
# → 201 { "id": "<run-id>", "status": "PENDING", ... }
```

**3. Inspect progress:**

```bash
curl http://localhost:8080/runs/<run-id>
curl http://localhost:8080/api/v1/runs/<run-id>/history
```

The built-in `SLEEP` executor is a reference implementation; add your own by
implementing the `Executor` interface in `internal/worker/executor.go`.

## API Overview

The REST API is the external control plane. Full reference:
[docs/api.md](docs/api.md).

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/workflows` | Create a workflow definition |
| `POST` | `/runs` | Start a workflow run |
| `GET` | `/runs/{id}` | Run details + tasks |
| `GET` | `/api/v1/runs/{run_id}/history` | Full run + attempt history |
| `GET` | `/api/v1/tasks/{task_run_id}/attempts` | Attempts for a task |
| `GET` | `/api/v1/dead-letter` | Dead-letter queue (paginated) |
| `GET` | `/health`, `/healthz`, `/readyz` | Health / liveness / readiness |
| `GET` | `/metrics` | Prometheus metrics |

## gRPC Overview

Internal synchronous communication uses gRPC (all RPCs unary). Full reference:
[docs/grpc.md](docs/grpc.md).

- **`SchedulerService`** — `ClaimTasks`, `PromoteRetries`
- **`RecoveryService`** — `RecoverTask`, `RecoverStaleTasks`
- **`HealthService`** — `Check` (liveness/readiness)

Contracts live in [`proto/flowforge/`](proto/flowforge/). TLS/mTLS is
configurable via `GRPC_TLS_*` (disabled by default).

## Kafka Overview

Workflow lifecycle events are delivered through a **transactional outbox**:
state changes and their events are committed in the same PostgreSQL transaction,
then the `publisher` relays them to Kafka at-least-once. Events are keyed by
`workflow_run_id` for per-workflow ordering.

Event types include `WorkflowStarted`, `WorkflowCompleted`, `WorkflowFailed`,
`TaskStarted`, `TaskCompleted`, `TaskFailed`, `TaskTimedOut`, `RetryScheduled`,
`RetryExhausted`, `DLQCreated`, `TaskRecovered`, `RetryPromoted`.

`cmd/event-consumer` is a reference **idempotent** consumer: it dedupes by event
ID, commits offsets only after processing, and skips malformed envelopes.

## Configuration

All configuration is via environment variables with sensible defaults; the
optional guards (TLS, rate limiting, body limits, profiling) are **off by
default**. See [.env.example](.env.example) for the full annotated list.

Key groups: core (`PORT`, `DB_URL`, `ENV`), connection pool (`DB_MAX_OPEN_CONNS`
…), worker/outbox tuning, Kafka/Redis, security (`GRPC_TLS_*`, `RATE_LIMIT_*`,
`MAX_REQUEST_BODY_BYTES`), and observability (`OTEL_*`, `PPROF_ENABLED`).

## Observability

- **Metrics** — Prometheus on `METRICS_ADDR` (`:9091/metrics`) for every
  process. Dashboards provisioned in Grafana (`deploy/grafana/`).
- **Tracing** — OpenTelemetry → OTLP → Jaeger (enable with
  `OTEL_DISABLED=false`). Trace context propagates across HTTP, gRPC, and Kafka.
- **Logging** — structured JSON via Zap in the service layer, with correlation
  IDs (`X-Request-ID`) propagated through requests. Some worker subsystems still
  use the standard library logger.

Alerting rules live in `deploy/prometheus/alerts.yml`.

## Testing

```bash
go test ./...                              # unit tests, no external deps
go test -race ./...                        # race detector
go test -tags chaos ./internal/outbox/...  # failure injection, infra-free
go test -bench=. ./internal/dag/... ./internal/grpcutil/...

# integration tests need a real Postgres
TEST_DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable" \
  go test -tags integration ./...
```

The suite is layered: fast unit tests by default, integration tests behind
`-tags integration`, and chaos/failure-injection tests behind `-tags chaos`.

## Failure Handling

The interesting parts of FlowForge are what happens when things go wrong:

| Failure | How it's handled |
|---|---|
| **Task throws / times out** | Marked failed, moved to `RETRY_WAIT` with exponential backoff; the scheduler promotes it back to `READY` when due. |
| **Retries exhausted** | Task is dead-lettered (`dead_letter_tasks`), workflow marked `FAILED`, queryable via the API. |
| **Worker crashes mid-task** | Its Redis lease expires; a recovery loop reclaims the stale task and resets it to `READY`. |
| **Zombie worker wakes up** | Fencing tokens: its guarded writes no-op because the task's token has moved on, so it can't corrupt a reclaimed task. |
| **DB commit succeeds but Kafka write fails** | Avoided entirely — events are written to an outbox table in the same transaction and published separately, at-least-once. |
| **Duplicate event delivery** | Consumers dedupe by event ID and commit offsets only after processing. |
| **Two workers claim the same task** | Prevented by `SELECT ... FOR UPDATE SKIP LOCKED` — claiming is a single atomic SQL statement. |
| **Process restart** | Every process is stateless; in-flight tasks are drained on shutdown or recovered afterwards. |

See [docs/diagrams/retry.md](docs/diagrams/retry.md) and
[docs/diagrams/recovery.md](docs/diagrams/recovery.md) for the flows.

## Distributed Systems Concepts Demonstrated

- **Database-backed work queue** with `FOR UPDATE SKIP LOCKED` for lock-free
  concurrent claiming.
- **Fencing tokens** to make leases safe against stale/zombie workers.
- **Lease-based liveness** in Redis, kept separate from correctness (which lives
  in Postgres).
- **Transactional outbox** to solve the dual-write problem between a database
  and a message broker.
- **At-least-once delivery with idempotent consumers** instead of pretending
  exactly-once exists.
- **Idempotent, guarded state transitions** so retries and replays are safe.
- **Graceful shutdown and crash recovery** for stateless processes.
- **Backpressure** via bounded worker pools and queues.

## Documentation

| Doc | Purpose |
|---|---|
| [docs/architecture.md](docs/architecture.md) | System architecture, components, and boundaries |
| [docs/api.md](docs/api.md) | REST API reference |
| [docs/grpc.md](docs/grpc.md) | Internal gRPC services |
| [docs/deployment.md](docs/deployment.md) | Running locally with Docker Compose |
| [docs/diagrams/](docs/diagrams/) | System, workflow, retry, recovery, and event-flow diagrams |

## Future Improvements

Things I'd tackle next if I kept building this:

- Make the `internal/worker` unit tests hermetic (they currently need Redis) and
  add a CI pipeline.
- Standardize REST route prefixes (some endpoints predate the `/api/v1/` scheme)
  and return `404` instead of `500` for missing runs.
- Batch outbox publishing to Kafka for higher throughput.
- Add retention/pruning for the dead-letter table.
- Add more executor types beyond the built-in `SLEEP` reference.
- A dockerized end-to-end test asserting a workflow completes and emits the
  expected Kafka events.
