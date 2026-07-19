# FlowForge Production Documentation

Operational documentation produced in **Phase 13 — Production Hardening &
Scalability**. Every hardening feature ships **off by default**; enable per the
guides below.

## Guides

| Doc | Purpose |
|---|---|
| [PRODUCTION-READINESS-CHECKLIST.md](PRODUCTION-READINESS-CHECKLIST.md) | Go/no-go gate before promoting to production. |
| [CONFIGURATION.md](CONFIGURATION.md) | Full environment-variable reference. |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Build, image, compose, rollout, probes. |
| [OPERATIONS.md](OPERATIONS.md) | Monitoring, alerts, incident response. |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Symptom-oriented fixes. |
| [SCALING.md](SCALING.md) | Horizontal/vertical scaling & connection budget. |
| [PERFORMANCE.md](PERFORMANCE.md) | Benchmarks, pprof, load testing. |
| [SECURITY.md](SECURITY.md) | TLS/mTLS, rate limits, body limits, secrets. |
| [CHAOS.md](CHAOS.md) | Failure model + chaos test suite. |
| [BACKUP-RECOVERY.md](BACKUP-RECOVERY.md) | Backups and disaster recovery. |
| [READINESS-AUDIT.md](READINESS-AUDIT.md) | Baseline production-readiness audit. |

## Architecture Recap

- **PostgreSQL** — source of truth (workflows, runs, tasks, transactional outbox).
- **Redis** — ephemeral coordination (leases, heartbeats); correctness-independent.
- **Kafka** — async event stream via transactional outbox (at-least-once).
- **gRPC** — internal service comms (scheduler, recovery), TLS-capable.
- Services are stateless and horizontally scalable; claims are DB-guarded with
  `SKIP LOCKED`, and crash recovery is lease-based.
