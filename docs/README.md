# FlowForge Documentation

Comprehensive documentation for FlowForge, produced as part of the v1.0 release
audit. All documents reflect the current implementation as the source of truth.

## For Developers

| Doc | Purpose |
|---|---|
| [architecture.md](architecture.md) | System architecture, processes, boundaries, state machines |
| [api.md](api.md) | REST API reference (every endpoint) |
| [grpc.md](grpc.md) | gRPC service & message reference |
| [benchmarking.md](benchmarking.md) | Benchmark report + methodology |
| [testing.md](testing.md) | Test coverage report |
| [diagrams/](diagrams/) | Architecture + sequence diagrams |
| [adr/](adr/README.md) | Architecture Decision Records (ADR-001–010) |

## For Operators

| Doc | Purpose |
|---|---|
| [deployment.md](deployment.md) | Build, deploy, startup/shutdown, health checks |
| [operations.md](operations.md) | Monitoring, alerts, failure recovery, incidents |
| [production/](production/README.md) | Full production runbooks (config, security, scaling, chaos, backup/DR, checklist) |

## For Release / Review

| Doc | Purpose |
|---|---|
| [release.md](release.md) | v1.0 audit: findings, CI/CD, cleanup, architecture validation, release checklist |

## Diagrams

- [diagrams/system.md](diagrams/system.md) — overall system, deployment topology,
  component ownership
- [diagrams/sequences.md](diagrams/sequences.md) — all 12 sequence diagrams
- [diagrams/workflow-sequence.md](diagrams/workflow-sequence.md) — end-to-end
  workflow lifecycle
- [diagrams/retry.md](diagrams/retry.md) — retry, DLQ, and crash recovery
