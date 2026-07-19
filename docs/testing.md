# Test Coverage Report

An audit of FlowForge's test suite: what is covered, how tests are layered, known
gaps, and recommendations. Coverage **percentages are not reported** because no
coverage tooling is wired into the project (see [Recommendations](#recommendations));
fabricating numbers would be misleading.

## Test Layers

FlowForge separates tests by build tag so the default run stays fast and
dependency-free:

| Layer | How to run | External deps |
|---|---|---|
| **Unit** | `go test ./...` | None |
| **Integration** | `go test -tags integration ./...` (needs `TEST_DB_URL`) | Real PostgreSQL |
| **Chaos** | `go test -tags chaos ./internal/outbox/...` | None (in-memory fault injection) |
| **Benchmarks** | `go test -bench=. ./...` | None |

30 test files in total.

## Coverage by Component

| Package | Unit | Integration | Chaos | Bench | Notes |
|---|:--:|:--:|:--:|:--:|---|
| `internal/dag` | ✅ | | | ✅ | Validation + cycle detection; chain/wide benchmarks |
| `internal/api` | ✅ | | | | Server routes + history endpoints |
| `internal/config` | ✅ | | | | Env parsing + validation |
| `internal/outbox` | ✅ | | ✅ | | Publisher + Kafka carrier; chaos failure injection |
| `internal/worker` | ✅¹ | ✅ | | | Worker, coordinator, executor; integration vs real PG |
| `internal/repository` | | ✅ | | | `postgres_test.go` (~2065 lines), outbox store; needs real PG |
| `internal/recovery` | ✅ | ✅ | | | gRPC handler/client; cross-service integration |
| `internal/scheduler` | ✅ | ✅ | | | gRPC handler/client; cross-service integration |
| `internal/grpcutil` | ✅ | | | ✅ | TLS, retry, health, backoff; backoff benchmark |
| `internal/telemetry` (+`grpcmw`,`httpmw`) | ✅ | | | | Tracing, logging, metrics server, middleware, rate limit |
| `cmd/event-consumer` | ✅ | | | | Consumer entrypoint |

¹ See the known issue below regarding the worker unit suite.

## Chaos / Failure-Injection Tests

`internal/outbox/chaos_test.go` (`//go:build chaos`) validates the outbox
publisher under simulated failures — all in-memory, no infrastructure:

- `TestChaosDuplicateDeliveryOnCrashAfterAck` — crash between Kafka ack and DB
  mark → event re-published (at-least-once), never lost.
- `TestChaosRetryExhaustion` — persistent Kafka failure → errors recorded, event
  not marked.
- `TestChaosTransientKafkaThenRecovery` — transient failures then success →
  exactly one successful publish.

See [production/CHAOS.md](production/CHAOS.md) for the full failure model.

## Benchmarks

`BenchmarkValidateChain`, `BenchmarkValidateWide` (`internal/dag`) and
`BenchmarkNextBackoff` (`internal/grpcutil`). Captured results and methodology
are in [benchmarking.md](benchmarking.md).

## Verification Status (this audit)

- `go build ./...` — **pass**
- `go vet ./...` — **pass**
- `go test ./...` (unit, excluding `internal/worker`) — **pass**
- Benchmarks — **run successfully**, results captured in [benchmarking.md](benchmarking.md)

## Known Issues / Gaps

1. **`internal/worker` unit suite hangs.** Running `go test ./internal/worker/`
   times out (reproduced at 25s and 60s). This is a **pre-existing**,
   infra-dependent hang unrelated to the documentation work — the suite appears
   to block on a coordinator/lease dependency without a real Redis. *Impact:*
   `go test ./...` does not complete cleanly out of the box. *Recommendation:*
   gate the affected tests behind `integration`/skip when Redis is absent, or
   inject a fake coordinator, so the default unit run is hermetic.
2. **No coverage measurement.** No `-cover` gating or CI job exists; true line
   coverage is unknown.
3. **Integration tests require manual setup** (`TEST_DB_URL`) and are not run in
   CI (no CI exists — see [release.md](release.md)).
4. **No end-to-end test** exercising the full stack (API → worker → outbox →
   Kafka → consumer). Covered indirectly by chaos + integration tests, but a
   dockerized E2E would strengthen release confidence.
5. **`cmd/` entrypoints** (except `event-consumer`) have no tests; low risk since
   they are thin wiring, but smoke tests would help.

## Recommendations

- Fix the `internal/worker` unit hang so `go test ./...` is hermetic and CI-safe.
- Add a CI pipeline (see [release.md](release.md)) running: `gofmt -l`,
  `go vet`, `go test -race ./...`, and `go test -tags chaos ./internal/outbox/...`.
- Add a coverage step (`go test -coverprofile=...`) and publish a real coverage
  number; consider a soft threshold.
- Add a spun-up-Postgres job (service container) to run the `integration` suite.
- Add a dockerized end-to-end smoke test asserting a workflow completes and emits
  the expected Kafka events.
