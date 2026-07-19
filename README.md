# FlowForge

**A distributed, DAG-based workflow orchestration engine written in Go.**

FlowForge executes workflows defined as directed acyclic graphs (DAGs) of tasks
across a horizontally scalable fleet of stateless workers. It provides durable
state, exactly-effectively-once execution semantics, at-least-once event
delivery, crash recovery, retries with backoff, dead-letter handling, and
first-class observability.

> **Status:** v1.0 — feature complete, with production hardening, security, and
> operational documentation in place.

---

## Table of Contents

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
- [Benchmarks](#benchmarks)
- [Testing](#testing)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

A **workflow** in FlowForge is a definition consisting of tasks and their
dependencies. When a workflow is instantiated as a **run**, tasks with no
unmet dependencies become `READY`, are claimed by workers, executed, and — on
completion — unlock their dependents. Every state transition is persisted to
PostgreSQL (the single source of truth) and emitted as an event through a
transactional outbox to Kafka for downstream consumers.

Design goals:

- **Durability** — no task or event is lost across crashes.
- **Horizontal scalability** — every component is stateless and scales out.
- **Correctness under failure** — leasing, fencing tokens, and lease-aware
  recovery prevent double execution and lost work.
- **Observability** — structured logs, Prometheus metrics, and OpenTelemetry
  traces across HTTP, gRPC, and Kafka boundaries.

## Features

- **DAG workflows** with dependency resolution and cycle detection.
- **Durable state machine** for workflow runs and task runs in PostgreSQL.
- **Concurrent worker pools** with bounded, backpressure-aware task claiming
  (`FOR UPDATE SKIP LOCKED`).
- **Distributed leasing** via Redis with fencing tokens and heartbeats.
- **Lease-aware crash recovery** — stale `CLAIMED`/`RUNNING` tasks are safely
  reclaimed by a recovery service.
- **Retries with exponential backoff** and a **dead-letter queue** for
  permanently failed tasks.
- **Transactional outbox → Kafka** for at-least-once event delivery with
  per-workflow ordering.
- **gRPC internal services** (scheduler, recovery, health) with optional
  TLS/mTLS.
- **Full observability** — Prometheus metrics, OTLP tracing (Jaeger), JSON logs.
- **Production hardening** — connection pooling, HTTP timeouts, rate limiting,
  request body limits, non-root containers, health/readiness probes.

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
├── docs/                   # Documentation (this audit)
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

All configuration is via environment variables with production-safe defaults;
every hardening feature is **off by default**. See [.env.example](.env.example)
and the full reference in
[docs/production/CONFIGURATION.md](docs/production/CONFIGURATION.md).

Key groups: core (`PORT`, `DB_URL`, `ENV`), connection pool (`DB_MAX_OPEN_CONNS`
…), worker/outbox tuning, Kafka/Redis, security (`GRPC_TLS_*`, `RATE_LIMIT_*`,
`MAX_REQUEST_BODY_BYTES`), and observability (`OTEL_*`, `PPROF_ENABLED`).

## Observability

- **Metrics** — Prometheus on `METRICS_ADDR` (`:9091/metrics`) for every
  process. Dashboards provisioned in Grafana (`deploy/grafana/`).
- **Tracing** — OpenTelemetry → OTLP → Jaeger (enable with
  `OTEL_DISABLED=false`). Trace context propagates across HTTP, gRPC, and Kafka.
- **Logging** — structured JSON via Zap, with correlation IDs (`X-Request-ID`).

Alerting rules live in `deploy/prometheus/alerts.yml`. See
[docs/operations.md](docs/operations.md).

## Benchmarks

Go benchmarks cover DAG validation (`BenchmarkValidateChain`,
`BenchmarkValidateWide`) and gRPC retry backoff (`BenchmarkNextBackoff`). A
system-level load-test helper is provided in `scripts/loadtest.sh`.

```bash
go test -bench=. -benchmem ./internal/dag/... ./internal/grpcutil/...
```

See [docs/benchmarking.md](docs/benchmarking.md) for methodology and how to
capture results.

## Testing

Layered test suite (see [docs/testing.md](docs/testing.md)):

- **Unit** — default `go test ./...` (no external dependencies).
- **Integration** — `-tags integration`, requires `TEST_DB_URL` (real Postgres).
- **Chaos** — `-tags chaos`, infra-free failure injection.
- **Benchmarks** — `go test -bench`.

## Documentation

| Doc | Purpose |
|---|---|
| [docs/architecture.md](docs/architecture.md) | System architecture + boundaries |
| [docs/api.md](docs/api.md) | REST API reference |
| [docs/grpc.md](docs/grpc.md) | gRPC service reference |
| [docs/deployment.md](docs/deployment.md) | Deployment guide |
| [docs/operations.md](docs/operations.md) | Operations runbook |
| [docs/benchmarking.md](docs/benchmarking.md) | Benchmark report |
| [docs/testing.md](docs/testing.md) | Test coverage report |
| [docs/release.md](docs/release.md) | Release checklist + audit findings |
| [docs/diagrams/](docs/diagrams/) | Architecture + sequence diagrams |
| [docs/adr/](docs/adr/) | Architecture Decision Records |
| [docs/production/](docs/production/README.md) | Production runbooks |

## Contributing

1. Fork and create a feature branch.
2. Follow existing code style; run `gofmt -l .`, `go vet ./...`, and
   `go test ./...` before submitting.
3. Add tests for new behavior. Keep changes focused.
4. Regenerate protobuf via `./scripts/gen-proto.sh` if you touch `.proto` files.
5. Open a pull request describing the change and its rationale.

See [CONTRIBUTING](docs/release.md#contributing) notes and the Definition of
Done in `AGENT.md`.

## License

See [LICENSE](LICENSE). *(No license file is currently present in the
repository — add one before public release; see
[docs/release.md](docs/release.md).)*
