# Configuration Reference (Phase 13)

All configuration is via environment variables (`internal/config`). Defaults are
production-safe: every hardening feature is **off by default** so behavior is
unchanged unless explicitly enabled. See `.env.example` for a copyable template.

## Core

| Variable | Default | Description |
|---|---|---|
| `ENV` | `development` | Deployment environment label. |
| `PORT` | `8080` | HTTP API listen port. |
| `GRPC_ADDR` | `0.0.0.0:9090` | gRPC listen address (scheduler/recovery/api). |
| `METRICS_ADDR` | `0.0.0.0:9091` | Prometheus metrics + optional pprof. |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error`. Logs are JSON. |
| `SCHEMA_PATH` | `schema.sql` | DDL applied on startup. |

## Datastores

| Variable | Default | Description |
|---|---|---|
| `DB_URL` | — (required) | PostgreSQL DSN. Source of truth. |
| `REDIS_ADDR` | `localhost:6379` | Redis coordination address. |
| `REDIS_PASSWORD` | empty | Redis auth. |
| `REDIS_DB` | `0` | Redis logical DB. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated brokers. |
| `KAFKA_TOPIC` | `flowforge.workflow-events.v1` | Event topic. |
| `KAFKA_CLIENT_ID` | per-service | Kafka client identifier. |

## Connection Pool (Loop 13.1)

| Variable | Default | Description |
|---|---|---|
| `DB_MAX_OPEN_CONNS` | `25` | Max open DB connections. |
| `DB_MAX_IDLE_CONNS` | `10` | Max idle DB connections. |
| `DB_CONN_MAX_LIFETIME` | `30m` | Max connection lifetime. |
| `DB_CONN_MAX_IDLE_TIME` | `5m` | Max idle time before close. |

## Observability / Profiling (Loop 13.2)

| Variable | Default | Description |
|---|---|---|
| `OTEL_DISABLED` | `true` | No-op tracing when true. |
| `OTEL_SERVICE_NAME` | per-service | Trace service name. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OTLP collector (e.g. `jaeger:4317`). |
| `PPROF_ENABLED` | `false` | Expose `/debug/pprof` on metrics server. |

## Security / Limits (Loop 13.3)

| Variable | Default | Description |
|---|---|---|
| `GRPC_TLS_ENABLED` | `false` | Enable TLS on gRPC. |
| `GRPC_TLS_CERT_FILE` / `_KEY_FILE` | — | Server certificate/key. |
| `GRPC_TLS_CA_FILE` | — | CA bundle; presence enables mTLS. |
| `RATE_LIMIT_RPS` | `0` (off) | Per-instance request rate. |
| `RATE_LIMIT_BURST` | `0` | Token-bucket burst. |
| `MAX_REQUEST_BODY_BYTES` | `0` (off) | Max write-endpoint body size. |

## gRPC Client Behavior

| Variable | Default | Description |
|---|---|---|
| `GRPC_REQUEST_TIMEOUT` | per-attempt deadline | Per-call timeout. |
| `GRPC_RETRY_MAX_ATTEMPTS` | capped | Retry attempts for retryable codes. |
| `GRPC_RETRY_BASE_DELAY` | — | Backoff base (max backoff 2s). |
| `SCHEDULER_ADDR` / `RECOVERY_ADDR` | — | Internal service targets. |

## Worker / Outbox

| Variable | Default |
|---|---|
| `WORKER_POOL_SIZE` | `16` |
| `WORKER_QUEUE_CAPACITY` | `32` |
| `WORKER_CLAIM_BATCH_SIZE` | `8` |
| `WORKER_HEARTBEAT_INTERVAL_MS` | `1000` |
| `WORKER_HEARTBEAT_TTL_MS` | `3000` |
| `WORKER_SHUTDOWN_GRACE_PERIOD_MS` | `10000` |
| `TASK_LEASE_TTL_MS` | `5000` |
| `TASK_LEASE_RENEW_INTERVAL_MS` | `1500` |
| `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL`, `OUTBOX_CLAIM_TIMEOUT`, `OUTBOX_MAX_RETRIES`, `OUTBOX_RETRY_BASE_DELAY`, `OUTBOX_RETENTION` | see `.env.example` |

`Config.Validate()` fails fast on invalid combinations (e.g. TLS enabled without
cert/key) at startup.
