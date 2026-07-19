# FlowForge v1.0 — Release Audit & Checklist

This document consolidates the v1.0 release audit: CI/CD status and
recommendations, the test coverage summary, repository cleanup recommendations,
final architecture validation, discrepancies found, and the release checklist.

All findings reflect the implementation as the source of truth. Where the
implementation and prior documentation disagreed, the discrepancy is documented
rather than silently corrected.

---

## 1. Audit Summary

FlowForge is a distributed DAG workflow engine (Go 1.26) with API, scheduler,
recovery, worker, publisher, and event-consumer processes over PostgreSQL,
Redis, and Kafka, with OpenTelemetry-based observability. The engine is feature
complete, including production hardening.

**Verification run during this audit:**

| Check | Result |
|---|---|
| `go build ./...` | ✅ pass |
| `go vet ./...` | ✅ pass |
| `go test ./...` (unit, excl. `internal/worker`) | ✅ pass |
| Benchmarks (`dag`, `grpcutil`) | ✅ run; results in [benchmarking.md](benchmarking.md) |
| `docker compose config` | ✅ valid |
| `internal/worker` unit tests | ⚠️ **hang/timeout** (pre-existing; see below) |

---

## 2. Discrepancies & Findings

| # | Finding | Location | Recommendation |
|---|---|---|---|
| F1 | Mixed route prefixes: `POST /runs`, `GET /runs/{id}` lack `/api/v1/` | `internal/api/server.go` | Standardize on `/api/v1/` in a future major version; documented in [api.md](api.md) |
| F2 | `GET /runs/{id}` returns **500** (not 404) for missing runs | `internal/api/server.go` | Map `sql.ErrNoRows → 404` (TODO already noted in code) |
| F3 | `internal/worker` unit tests hang | `internal/worker/*_test.go` | Make hermetic (fake coordinator / skip w/o Redis); see [testing.md](testing.md) |
| F4 | No CI/CD pipeline | repository | Add GitHub Actions (see §3) |
| F5 | No `LICENSE` file | repository root | Add a license before public release |
| F6 | No coverage measurement | repository | Add `-cover` in CI |
| F7 | DLQ table has no retention/pruning (outbox does) | `schema.sql` / repo | Add optional DLQ retention or archival |
| F8 | Some worker timing knobs parsed outside `config` | `cmd/worker/main.go` | Consolidate into `internal/config` for one config surface; documented in [production/CONFIGURATION.md](production/CONFIGURATION.md) |
| F9 | `event-consumer` not in Compose | `docker-compose.yml` | Intentional (reference consumer); documented |
| F10 | Publisher publishes events one-by-one | `internal/outbox/publisher.go` | Batch Kafka writes for higher throughput (see [benchmarking.md](benchmarking.md)) |

None of these block functional correctness; F3, F4, and F5 should be addressed
before public open-source release.

---

## 3. CI/CD Documentation

**Current state:** There is **no CI/CD** configured — no `.github/workflows/`, no
`Makefile`, no release automation. Builds, tests, formatting, and image builds
are run manually.

**Recommended pipeline** (GitHub Actions). Each stage maps to an existing
command:

| Stage | Command |
|---|---|
| Formatting | `test -z "$(gofmt -l .)"` |
| Static analysis | `go vet ./...` |
| Lint | `golangci-lint run` (add config) |
| Unit tests (race) | `go test -race ./...` (after fixing F3) |
| Chaos tests | `go test -tags chaos ./internal/outbox/...` |
| Integration tests | `go test -tags integration ./...` with a Postgres service container (`TEST_DB_URL`) |
| Coverage | `go test -coverprofile=cover.out ./...` + upload |
| Benchmarks | `go test -bench=. -benchmem ./internal/dag/... ./internal/grpcutil/...` (optional, non-gating) |
| Docker build | `docker build -t flowforge:${GIT_SHA} .` |
| Security scanning | `govulncheck ./...`; image scan (e.g. Trivy) |
| Release / tagging | on tag `v*`: build + push image; generate release notes |
| Artifact publishing | push container image to a registry (`flowforge:vX.Y.Z`, `:latest`) |

**Suggested skeleton** to add at `.github/workflows/ci.yml` (not created by this
audit):

```yaml
name: CI
on: [push, pull_request]
jobs:
  build-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.26' }
      - run: test -z "$(gofmt -l .)"
      - run: go vet ./...
      - run: go build ./...
      - run: go test -race ./...            # after F3 fix
      - run: go test -tags chaos ./internal/outbox/...
```

Add a separate `release.yml` triggered on `v*` tags for image build/push.

---

## 4. Test Coverage Summary

See [testing.md](testing.md) for the full report. Highlights:

- Layered suite: unit (default), integration (`-tags integration` + real PG),
  chaos (`-tags chaos`, infra-free), and benchmarks.
- Strong coverage of API, config, DAG, outbox (incl. chaos), grpcutil,
  telemetry/middleware, scheduler/recovery (unit + integration), and repository
  (integration).
- Gaps: `internal/worker` unit hang (F3), no coverage metric (F6), no E2E test,
  no CI (F4).

---

## 5. Repository Cleanup Recommendations

Non-destructive suggestions (no changes performed as part of documentation):

- **Directory layout:** already idiomatic (`cmd`/`internal`/`proto`/`deploy`).
  No changes needed.
- **Naming consistency:** align REST route prefixes (F1) in a future major
  version.
- **Dead code / unused deps:** run `go mod tidy` and `deadcode ./...` /
  `staticcheck` to confirm none; none obvious from the audit.
- **Configuration:** consolidate worker timing knobs into `internal/config`
  (F8) so there is one configuration surface with validation.
- **Documentation:** `AGENT.md` is the contributor guide; keep it maintained
  alongside the `docs/` tree.
- **Scripts:** `scripts/loadtest.sh` and `scripts/gen-proto.sh` are useful; add a
  `scripts/README.md` documenting prerequisites (`hey`, `protoc`).
- **Examples:** add an `examples/` directory with a ready-to-run workflow JSON and
  a `curl` walkthrough (the README example can seed it).
- **Developer experience:** add a `Makefile` (or `Taskfile`) wrapping common
  commands (`build`, `test`, `lint`, `proto`, `compose-up`) to reduce onboarding
  friction.
- **Binary artifact:** a compiled `flowforge` binary is present at the repo root
  and is git-ignored — keep it out of version control (confirmed in
  `.gitignore` handling of build artifacts).

---

## 6. Final Architecture Validation

Each release-gating invariant verified against the implementation:

| Invariant | Status | Evidence |
|---|:--:|---|
| REST owns external APIs | ✅ | Routes only in `internal/api/server.go` |
| gRPC owns synchronous internal comms | ✅ | `SchedulerService`, `RecoveryService`, `HealthService` |
| Kafka owns asynchronous events | ✅ | Publisher → Kafka; consumers read Kafka |
| Redis owns ephemeral coordination | ✅ | Lease/heartbeat keys only; TTL-based |
| PostgreSQL owns durable state | ✅ | All state + outbox in Postgres; repo centralizes SQL |
| Workers remain stateless | ✅ | Ephemeral identity; no persisted worker state |
| Publisher never mutates workflow state | ✅ | Only reads outbox + marks published |
| Scheduler never executes tasks | ✅ | Only claims + promotes retries |
| Recovery never executes tasks | ✅ | Only reclaims stale tasks |
| Execution guarantees enforced | ✅ | Fencing tokens + `SKIP LOCKED` + leases; hardening features default OFF |

All invariants **hold**. See [architecture.md](architecture.md) for detail.

---

## 7. Release Checklist

| Item | Status | Notes |
|---|:--:|---|
| Documentation complete | ✅ | README + `docs/` (architecture, api, grpc, deployment, operations, benchmarking, testing, ADRs, diagrams, production runbooks) |
| Architecture validated | ✅ | §6 — all invariants hold |
| Tests passing | ⚠️ | Unit green except `internal/worker` hang (F3); integration/chaos available |
| Benchmarks completed | ✅ | [benchmarking.md](benchmarking.md) |
| Observability configured | ✅ | Prometheus + Grafana + Jaeger; alerts in `deploy/` |
| Security reviewed | ✅ | TLS/mTLS, rate limits, body limits, non-root image; **no app auth** (proxy required) — [production/SECURITY.md](production/SECURITY.md) |
| Deployment verified | ✅ | Compose validated; hardened Dockerfile; startup/shutdown order documented |
| CI/CD operational | ❌ | Not present — recommended pipeline in §3 (F4) |
| Examples verified | ⚠️ | README example provided; dedicated `examples/` recommended |
| Docker Compose validated | ✅ | `docker compose config` passes |
| Production configuration documented | ✅ | [production/CONFIGURATION.md](production/CONFIGURATION.md), `.env.example` |
| Release artifacts prepared | ⚠️ | Image builds; add tagging/publishing automation (§3) |
| License present | ❌ | No `LICENSE` file (F5) — add before public release |
| Repository ready for public release | ⚠️ | Blockers: add LICENSE (F5), add CI (F4), fix worker test hang (F3) |

### Pre-release blockers (do before publishing)

1. **F5** — Add a `LICENSE` file.
2. **F3** — Make `internal/worker` unit tests hermetic so `go test ./...` passes
   cleanly.
3. **F4** — Add a CI pipeline (at minimum: fmt, vet, build, unit tests).

### Nice-to-have before v1.0

- Standardize REST route prefixes (F1) and map 404 correctly (F2).
- Add `examples/`, a `Makefile`, and DLQ retention (F7).
- Batch outbox publishing (F10).

---

## Contributing

See the [README Contributing section](../README.md#contributing) and the
Definition of Done in `AGENT.md`. In short: run `gofmt -l .`, `go vet ./...`,
and `go test ./...` before submitting; add tests for new behavior; regenerate
protobuf with `./scripts/gen-proto.sh` when editing `.proto` files.
