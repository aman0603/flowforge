# Production Readiness Audit (Phase 13, Loop 13.1)

Findings from auditing the FlowForge codebase against production reliability
requirements. Items fixed in this loop are marked DONE; the rest are tracked to
later Phase 13 loops.

## Reliability

| Area | Finding | Status |
|---|---|---|
| DB connection pool | `sql.Open` used database/sql defaults → unlimited open connections; risks exhausting PostgreSQL `max_connections` across scaled instances. | **DONE** — `repository.NewWithPool` sets `SetMaxOpenConns/SetMaxIdleConns/SetConnMaxLifetime/SetConnMaxIdleTime`; configurable via `DB_*` env, safe defaults (25/10/30m/5m). |
| HTTP server timeouts | Only Read/Write timeouts (10s); missing IdleTimeout, ReadHeaderTimeout, MaxHeaderBytes → slow-loris / oversized-header exposure. | **DONE** — added IdleTimeout 60s, ReadHeaderTimeout 5s, MaxHeaderBytes 1 MiB. |
| Config validation | Load panics on a few invariants; no reusable, non-fatal validation. | **DONE** — added `Config.Validate()` returning errors; covers heartbeat/lease/pool/worker invariants. |
| `.env.example` | Referenced by AGENT.md but absent. | **DONE** — added documenting every env var with dev-safe defaults. |
| Graceful shutdown | API + worker drain correctly; scheduler/recovery/publisher use signal-context cancellation. | OK (verified) |
| Startup ordering | compose `depends_on` + healthchecks for infra; gRPC clients dial with timeout. | OK (documented) |
| Retry / timeout policies | Persisted retry schedules; gRPC retry/backoff configurable. | OK (Phases 8/11) |
| Body size limits (write endpoints) | No `MaxBytesReader` on JSON handlers. | Deferred → Loop 13.3 |
| HTTP rate limiting / DoS | None. | Deferred → Loop 13.3 |
| Circuit breakers / backpressure | Bounded worker pool + queue provide backpressure; no explicit breaker on gRPC. | Deferred → Loop 13.3 (evaluate) |

## Scalability (observations, actioned in later loops)

- Task claiming uses `FOR UPDATE SKIP LOCKED` → safe horizontal worker scaling.
- Outbox poll interval + batch size are the publisher throughput knobs.
- gRPC clients dial once per service; keepalive/pooling to review in 13.2/13.3.
- Kafka single-partition default topic — partition strategy documented in 13.5.

## Security (deferred → Loop 13.3)

- gRPC uses insecure credentials (intended for same-network dev); add opt-in TLS.
- Hardcoded dev credentials in `docker-compose.yml`; externalize via `.env`.
- No dependency vulnerability scan wired; add `govulncheck` guidance.

## Deployment (deferred → Loop 13.5)

- Dockerfile runs as root, no `HEALTHCHECK`.
- compose lacks `restart:` policies and `deploy.resources` limits.

## Config Reference Delta (this loop)

New env vars (all optional, defaulted):

- `DB_MAX_OPEN_CONNS` (default 25)
- `DB_MAX_IDLE_CONNS` (default 10)
- `DB_CONN_MAX_LIFETIME` (default 30m)
- `DB_CONN_MAX_IDLE_TIME` (default 5m)

Behavior is unchanged at defaults; these only bound resource usage.
