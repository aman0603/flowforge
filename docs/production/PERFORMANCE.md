# Performance & Benchmarking (Phase 13, Loop 13.2)

Repeatable performance tooling, profiling entry points, target metrics, and the
identified bottlenecks for FlowForge.

## Benchmarks

Pure, infra-free Go benchmarks cover the hot logic paths:

```bash
go test -run '^$' -bench . -benchmem ./internal/dag/...      # DAG validation / cycle detection
go test -run '^$' -bench . -benchmem ./internal/grpcutil/... # retry backoff policy
```

- `BenchmarkValidateChain` / `BenchmarkValidateWide` — DAG validation across
  linear (deep DFS) and wide fan-out shapes at n = 10/100/500.
- `BenchmarkNextBackoff` — exponential backoff computation.

Run all benchmarks:

```bash
go test -run '^$' -bench . -benchmem ./...
```

## Profiling (pprof)

pprof is **opt-in** via `PPROF_ENABLED=true`. When enabled, standard
`net/http/pprof` handlers are mounted on the metrics server (`METRICS_ADDR`,
default `:9091`) under `/debug/pprof/`.

```bash
PPROF_ENABLED=true ./flowforge
# CPU profile (30s)
go tool pprof http://localhost:9091/debug/pprof/profile?seconds=30
# Heap
go tool pprof http://localhost:9091/debug/pprof/heap
# Goroutines
curl http://localhost:9091/debug/pprof/goroutine?debug=1
```

Keep pprof disabled in untrusted environments — the endpoints expose internal
state and add overhead.

## Load Testing

`scripts/loadtest.sh` drives the REST API (uses `hey` if installed, else a curl
fallback). Requires a running stack; not part of CI.

```bash
BASE_URL=http://localhost:8080 REQUESTS=2000 CONCURRENCY=50 TARGET_PATH=/healthz \
  ./scripts/loadtest.sh
```

## Target Metrics (initial SLOs to validate)

| Metric | Target | Source |
|---|---|---|
| Workflow creation (POST /api/v1/workflows) p99 | < 100 ms | HTTP histogram |
| Run creation (POST /runs) p99 | < 100 ms | HTTP histogram |
| Task scheduling latency (eligible → claimed) | < 2 × poll interval | scheduler/worker |
| Worker task throughput | ≥ pool_size concurrent | worker pool |
| gRPC internal call p99 | < 50 ms intra-network | gRPC histogram |
| Kafka publish lag (outbox → published) | < 2 × outbox poll interval | outbox metrics |

Validate against the Prometheus histograms added in Phase 12 and the Grafana
overview dashboard.

## Identified Bottlenecks & Tuning Knobs

1. **DB claim contention** — many workers contend on `FOR UPDATE SKIP LOCKED`.
   Tune `WORKER_CLAIM_BATCH_SIZE`, `WORKER_POLL_INTERVAL`, and the DB pool
   (`DB_MAX_OPEN_CONNS`). Watch PostgreSQL lock waits.
2. **Connection pool caps** — total open connections = per-instance
   `DB_MAX_OPEN_CONNS` × instance count; keep below PostgreSQL `max_connections`.
3. **Outbox publish rate** — throughput bounded by `OUTBOX_POLL_INTERVAL` and
   `OUTBOX_BATCH_SIZE`; lower interval / larger batch for higher event rate.
4. **gRPC latency** — retries add tail latency; tune `GRPC_RETRY_MAX_ATTEMPTS`
   and `GRPC_REQUEST_TIMEOUT`. Message size capped at 16 MiB.
5. **Worker queue saturation** — `WORKER_QUEUE_CAPACITY` provides backpressure;
   if consistently full, scale workers horizontally rather than enlarging queues.

## Methodology

1. Establish a baseline with benchmarks (`-benchmem`) and record allocs/op.
2. Under load (`loadtest.sh`), capture a CPU profile with pprof.
3. Correlate with Prometheus/Grafana (latency histograms, DB pool gauges).
4. Change one knob at a time; re-measure. Prefer horizontal scaling over
   unbounded queue growth.
