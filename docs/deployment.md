# Deployment Guide

How to build, configure, and run FlowForge locally with Docker Compose. This
project targets a single-host Compose setup; running it on an orchestrator is
possible but not provided.

## Deployable Units

One container image, six binaries (`command:` selects which runs):

| Service | Binary | Serves | Scales |
|---|---|---|---|
| API | `flowforge` | HTTP `:8080`, metrics `:9091`, gRPC health | 2+ behind LB |
| Scheduler | `scheduler` | gRPC `:9090`, metrics | 1–N (idempotent) |
| Recovery | `recovery` | gRPC `:9090`, metrics | 1–2 |
| Worker | `worker` | metrics | N (throughput) |
| Publisher | `publisher` | metrics | 1–N |
| Event consumer | `event-consumer` | metrics | ≤ Kafka partitions |

## Development

```bash
go build ./...
export DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable"
go run ./cmd/flowforge     # applies schema.sql on startup
```

Bring up backing stores however you prefer (local installs or `docker compose up
db redis kafka`), then run the other binaries as needed.

Workers use in-process scheduler/recovery clients by default. Set
`SCHEDULER_ADDR` / `RECOVERY_ADDR` to route through the standalone gRPC services
instead.

## Tests & Codegen

```bash
go test ./...             # unit tests
go test -race ./...       # race detector
gofmt -l . && go vet ./... # formatting + static analysis

# integration tests (require a real Postgres)
TEST_DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable" \
  go test -tags integration ./...

# chaos / failure-injection tests (infra-free)
go test -tags chaos ./internal/outbox/...

# benchmarks
go test -bench=. ./internal/dag/... ./internal/grpcutil/...

# regenerate gRPC code after editing .proto files
./scripts/gen-proto.sh    # requires protoc, protoc-gen-go, protoc-gen-go-grpc
```

## Docker Compose

The full stack is defined in `docker-compose.yml`:

```bash
cp .env.example .env       # override POSTGRES_*, DB_URL, secrets
docker compose up --build
docker compose up --build --scale worker=4 --scale publisher=2
```

Compose provides:

- `restart: unless-stopped` on all services.
- `deploy.resources` CPU/memory limits + reservations per service.
- Per-service healthchecks (HTTP `/healthz` for API; disabled for non-HTTP
  services).
- `depends_on` health gating for correct startup order.
- Externalized credentials (`POSTGRES_*` / `DB_URL`) with dev-only defaults.

Included observability: Prometheus (`:9090`), Grafana (`:3000`, admin/admin),
Jaeger (`:16686`, OTLP `:4317`).

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

## Container Image

`Dockerfile` is a hardened multi-stage build:

- Build stage: `golang:1.26.5-alpine`, CGO disabled, stripped binaries.
- Run stage: `alpine:3.20`, `ca-certificates` + `wget`.
- Runs as **non-root** user `flowforge`.
- Built-in `HEALTHCHECK` on `/healthz` (overridden/disabled per non-HTTP service
  in Compose).
- Exposes `8080`; default command `./flowforge`.

```bash
docker build -t flowforge:$(git rev-parse --short HEAD) .
```

## Startup Order

Enforced by Compose `depends_on` healthchecks; replicate in any orchestrator:

```
db, redis, kafka  (healthy)
        │
   scheduler, recovery  (started)
        │
   worker, publisher, app
```

## Shutdown Order

Reverse of startup. All processes handle `SIGTERM`/`SIGINT` via
`signal.NotifyContext`:

1. `app`, `worker`, `publisher` — drain in-flight work.
   - Workers stop claiming, drain the queue back to `READY`, and wait up to
     `WORKER_SHUTDOWN_GRACE_PERIOD_MS` before cancelling active executions
     (leases then expire and recovery reclaims).
2. `scheduler`, `recovery` — stateless, exit on context cancel.
3. Infrastructure (`db`, `redis`, `kafka`).

## Health Checks

| Endpoint | Probe | Meaning |
|---|---|---|
| `GET /healthz` | Liveness | Process alive |
| `GET /readyz` | Readiness | DB reachable; safe for traffic (503 otherwise) |
| gRPC `HealthService/Check` | Internal | Liveness/readiness for gRPC services |

Readiness returning 503 lets a load balancer drop an instance during a
dependency outage without restarting it.

## Key Environment Variables

The full annotated list is in [.env.example](../.env.example). The ones you'll
usually set:

| Variable | Purpose |
|---|---|
| `DB_URL` | PostgreSQL connection string (required) |
| `REDIS_ADDR` | Redis address for leases/heartbeats |
| `KAFKA_BROKERS` | Kafka bootstrap brokers |
| `PORT` / `METRICS_ADDR` / `GRPC_ADDR` | Listen addresses |
| `SCHEDULER_ADDR` / `RECOVERY_ADDR` | Use standalone gRPC services (optional) |
| `DB_MAX_OPEN_CONNS` | Bound the connection pool |
| `GRPC_TLS_ENABLED` (+ cert/key/CA) | Enable gRPC TLS/mTLS (off by default) |
| `RATE_LIMIT_RPS` / `MAX_REQUEST_BODY_BYTES` | API guards (off by default) |
| `OTEL_DISABLED` / `OTEL_EXPORTER_OTLP_ENDPOINT` | Tracing to Jaeger |

## Updates & Migrations

- Deploy one replica at a time; graceful shutdown prevents task loss.
- `schema.sql` is idempotent DDL applied on API startup (`SCHEMA_PATH`). Keep
  migrations additive to stay rolling-deploy safe; for managed DBs apply schema
  out-of-band in CI before deploy.
