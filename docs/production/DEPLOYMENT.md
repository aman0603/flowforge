# Deployment Guide (Phase 13)

How to build, configure, and deploy FlowForge to production.

## Services

FlowForge runs as independent, horizontally scalable processes sharing
PostgreSQL (source of truth), Redis (coordination), and Kafka (events):

| Service | Binary | Serves | Scale |
|---|---|---|---|
| API | `flowforge` | HTTP `:8080`, gRPC | 2+ behind LB |
| Scheduler | `scheduler` | gRPC | 1–N (idempotent claims) |
| Recovery | `recovery` | gRPC | 1–2 |
| Worker | `worker` | — | N (throughput) |
| Publisher | `publisher` | — | 1–N (outbox) |
| Event Consumer | `event-consumer` | — | N (per partition) |

## Container Image

The image (`Dockerfile`) is hardened:

- Multi-stage build; final stage on `alpine:3.20`.
- Runs as **non-root** user `flowforge` (least privilege).
- Built-in `HEALTHCHECK` hits `/healthz` (API); disabled for non-HTTP services.
- Single image, per-service `command:` selects the binary.

```bash
docker build -t flowforge:$(git rev-parse --short HEAD) .
```

## Local / Compose

```bash
cp .env.example .env      # set DB_URL, POSTGRES_*, secrets
docker compose up --build
docker compose up --scale worker=4 --scale event-consumer=3
```

Compose defines `restart: unless-stopped`, resource limits/reservations, and
per-service healthchecks. Credentials are externalized via `POSTGRES_*`/`DB_URL`
(dev defaults only; **override in production**).

## Production Checklist Before Rollout

1. Provision managed PostgreSQL, Redis, Kafka; set connection strings.
2. Set `ENV=production`, `LOG_LEVEL=info`.
3. Enable TLS: `GRPC_TLS_ENABLED=true` with cert/key (and CA for mTLS).
4. Set `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`, `MAX_REQUEST_BODY_BYTES`.
5. Tune `DB_MAX_OPEN_CONNS` to stay within DB connection budget across replicas.
6. Point `OTEL_EXPORTER_OTLP_ENDPOINT` at your collector; `OTEL_DISABLED=false`.
7. Leave `PPROF_ENABLED=false` unless actively profiling.
8. Wire orchestrator probes to `/healthz` (liveness) and `/readyz` (readiness).

## Health Probes

| Endpoint | Meaning | Probe |
|---|---|---|
| `GET /healthz` | Process alive | Liveness |
| `GET /readyz` | DB reachable, ready for traffic | Readiness |

Readiness returns 503 when dependencies are unavailable so load balancers stop
routing during outages — no requests hit an unready instance.

## Rolling Updates

Deploy one replica at a time. Graceful shutdown drains in-flight work within
`WORKER_SHUTDOWN_GRACE_PERIOD_MS`; unfinished leases expire and are reclaimed by
the recovery service, so a rollout never loses tasks.

## Database Migrations

`schema.sql` is idempotent DDL applied on startup (`SCHEMA_PATH`). For managed
DBs, apply it out-of-band before deploy in CI, or let the first instance apply
it. Additive-only migrations keep rolling deploys safe.
