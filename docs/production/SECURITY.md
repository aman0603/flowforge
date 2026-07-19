# Security Hardening (Phase 13, Loop 13.3)

FlowForge's production security posture. All hardening features are **opt-in and
default OFF**, preserving prior dev behavior; enable them per environment.

## Threat Surface

| Surface | Risk | Mitigation |
|---|---|---|
| Public REST API | DoS via request floods | Token-bucket rate limiter (`RATE_LIMIT_RPS`) |
| Public REST API | Memory exhaustion via huge bodies | `http.MaxBytesReader` on write endpoints (`MAX_REQUEST_BODY_BYTES`) |
| Public REST API | Slowloris / oversized headers | Server timeouts + `MaxHeaderBytes` (Loop 13.1) |
| Internal gRPC | Eavesdropping / spoofing on shared networks | Opt-in TLS / mutual TLS (`GRPC_TLS_*`) |
| gRPC payloads | Memory exhaustion | 16 MiB recv/send cap (`WithGRPCDefaults`) |
| PostgreSQL / Redis / Kafka | Credential leakage | Externalized via env / `.env`; never commit real secrets |
| Error responses | Internal detail leakage | Details only returned when `ENV=development` |
| Dependencies | Known CVEs | `govulncheck` (see below) |

## Transport Security (gRPC TLS)

Internal gRPC defaults to insecure credentials for same-network dev. Enable TLS
via environment variables (read by both servers and clients):

```bash
GRPC_TLS_ENABLED=true
GRPC_TLS_CERT_FILE=/etc/flowforge/tls/server.crt   # server key pair
GRPC_TLS_KEY_FILE=/etc/flowforge/tls/server.key
GRPC_TLS_CA_FILE=/etc/flowforge/tls/ca.crt         # verify peer; enables mTLS on server
```

- Client-only TLS: set `GRPC_TLS_ENABLED=true` + `GRPC_TLS_CA_FILE` on clients.
- Mutual TLS: additionally supply a CA on the server (`ClientAuth =
  RequireAndVerifyClientCert`) and a client key pair on clients.
- Minimum protocol is TLS 1.2. Misconfiguration fails loudly (no silent
  downgrade) via `NewServerTLS` / `DialTLS`.

Generate a local dev CA + certs with your tool of choice (openssl/mkcert); do
not commit private keys.

## Rate Limiting (DoS protection)

A global token-bucket limiter fronts the HTTP handler chain:

```bash
RATE_LIMIT_RPS=100     # sustained requests/sec; 0 disables (default)
RATE_LIMIT_BURST=200   # bucket capacity; defaults to RPS when unset
```

Rejected requests receive `429 Too Many Requests` with a `Retry-After: 1`
header. Implemented with the standard library (no extra dependency). For
per-client limiting or distributed limits, place a reverse proxy / API gateway
in front.

## Input Validation & Size Limits

- Write endpoints (`POST /api/v1/workflows`, `POST /runs`) wrap the body in
  `http.MaxBytesReader` (`MAX_REQUEST_BODY_BYTES`, default 1 MiB).
- Workflow definitions are validated (`dag.Validate`) before persistence:
  non-empty, unique names, existing dependencies, no cycles/self-deps.

## Secrets Management

- `docker-compose.yml` reads `POSTGRES_USER/PASSWORD/DB` and `DB_URL` from the
  environment / `.env`, with dev-only defaults. In production supply these from
  a secret manager (Vault, AWS/GCP secret managers, K8s secrets) — never commit
  real credentials.
- DB URLs are redacted in logs (`telemetry.RedactDBURL`).
- `.env` is for local use; `.env.example` documents every variable.

## Dependency Vulnerability Scanning

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...
```

Run in CI and before releases; triage and upgrade flagged modules.

## Production Checklist (security)

- [ ] `GRPC_TLS_ENABLED=true` with valid certs (mTLS between services preferred).
- [ ] Secrets supplied from a secret manager, not `.env` in the image.
- [ ] `RATE_LIMIT_RPS` set to a sane ceiling (or gateway rate limiting in place).
- [ ] `ENV=production` so error details are not leaked.
- [ ] TLS/`sslmode=require` on PostgreSQL, AUTH on Redis, TLS/SASL on Kafka.
- [ ] `govulncheck ./...` clean.
