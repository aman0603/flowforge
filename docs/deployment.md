# Deployment Guide

How to build, configure, and deploy FlowForge across development, Docker Compose,
and production. This is the top-level guide; deeper production runbooks live in
[production/DEPLOYMENT.md](production/DEPLOYMENT.md),
[production/SCALING.md](production/SCALING.md), and
[production/CONFIGURATION.md](production/CONFIGURATION.md).

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

Wire orchestrator probes accordingly. Readiness returning 503 removes an
instance from rotation during dependency outages without restarting it.

## Production Configuration Highlights

Set before rollout (full list in [production/CONFIGURATION.md](production/CONFIGURATION.md)):

- `ENV=production`, `LOG_LEVEL=info`.
- Managed `DB_URL`, `REDIS_ADDR`, `KAFKA_BROKERS`.
- TLS: `GRPC_TLS_ENABLED=true` (+ cert/key, CA for mTLS).
- Limits: `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`, `MAX_REQUEST_BODY_BYTES`.
- Pool: `DB_MAX_OPEN_CONNS` within the DB connection budget (see
  [production/SCALING.md](production/SCALING.md)).
- Tracing: `OTEL_DISABLED=false`, `OTEL_EXPORTER_OTLP_ENDPOINT`.
- Keep `PPROF_ENABLED=false` unless actively profiling.

## Rolling Updates & Migrations

- Deploy one replica at a time; graceful shutdown prevents task loss.
- `schema.sql` is idempotent DDL applied on API startup (`SCHEMA_PATH`). Keep
  migrations additive to stay rolling-deploy safe; for managed DBs apply schema
  out-of-band in CI before deploy.
