# Benchmark Report

This report covers the Go micro-benchmarks in the repository and the system-level
load-test tooling. Where results are shown, they were captured on the hardware
noted below — treat them as **relative** indicators, not absolute SLAs. Re-run on
your target hardware.

## What is benchmarked

| Benchmark | Location | Measures |
|---|---|---|
| `BenchmarkValidateChain` | `internal/dag/dag_bench_test.go` | DAG validation on a linear chain (worst-case DFS depth) at n=10/100/500 |
| `BenchmarkValidateWide` | `internal/dag/dag_bench_test.go` | DAG validation on a wide (1 root + n−1 leaves) graph |
| `BenchmarkNextBackoff` | `internal/grpcutil/backoff_test.go` | Pure gRPC retry backoff computation |

> The repository does **not** commit captured benchmark result files, and there
> is no coverage tooling wired into CI (none exists yet — see
> [release.md](release.md)). Numbers below were generated ad hoc during this
> audit.

## How to run

```bash
# Micro-benchmarks
go test -bench=. -benchmem -run=^$ ./internal/dag/... ./internal/grpcutil/...

# Save a baseline for comparison (benchstat)
go test -bench=. -benchmem -run=^$ ./internal/dag/... | tee bench-base.txt
# ...after a change...
go test -bench=. -benchmem -run=^$ ./internal/dag/... | tee bench-new.txt
benchstat bench-base.txt bench-new.txt
```

## Captured results

Environment: `linux/amd64`, Intel Core 5 120U, Go 1.26.

### DAG validation (`internal/dag`)

| Benchmark | ns/op | B/op | allocs/op |
|---|--:|--:|--:|
| `BenchmarkValidateChain/n=10` | 3,674 | 1,800 | 18 |
| `BenchmarkValidateChain/n=100` | 46,954 | 25,288 | 126 |
| `BenchmarkValidateChain/n=500` | 307,207 | 208,488 | 544 |
| `BenchmarkValidateWide/n=10` | 3,394 | 2,152 | 14 |
| `BenchmarkValidateWide/n=100` | 41,384 | 28,168 | 35 |
| `BenchmarkValidateWide/n=500` | 267,044 | 219,304 | 55 |

Observations:
- Validation scales roughly **linearly** with task count (~0.6 µs/task), as
  expected for a DFS cycle check over a sparse DAG.
- Even a 500-task workflow validates in ~0.3 ms — validation is not a bottleneck
  relative to per-task DB I/O.
- Allocations grow with graph size (visited maps + adjacency). Wide graphs
  allocate fewer times than chains (shallower recursion).

### gRPC retry backoff (`internal/grpcutil`)

| Benchmark | ns/op | B/op | allocs/op |
|---|--:|--:|--:|
| `BenchmarkNextBackoff` | 0.16 | 0 | 0 |

The backoff computation is allocation-free and effectively free (sub-nanosecond),
confirming it adds no measurable overhead to the retry path.

## System-level load testing

`scripts/loadtest.sh` drives HTTP load against a running API. It prefers
[`hey`](https://github.com/rakyll/hey) and falls back to a bounded `curl` loop.

```bash
BASE_URL=http://localhost:8080 REQUESTS=5000 CONCURRENCY=50 TARGET_PATH=/healthz \
  ./scripts/loadtest.sh
```

Environment knobs: `BASE_URL`, `REQUESTS` (default 1000), `CONCURRENCY`
(default 25), `TARGET_PATH` (default `/healthz`). Output includes success/failure
counts, elapsed time, and approximate throughput.

## Dimensions to measure in a full performance run

The following are **not** yet captured as automated benchmarks; measure them in
a staging environment with the full stack (see
[production/PERFORMANCE.md](production/PERFORMANCE.md)):

| Dimension | How to measure |
|---|---|
| Scheduling throughput | tasks claimed/sec via `flowforge_tasks_claimed_total` rate under load |
| Workflow throughput | runs completed/sec; end-to-end run latency |
| Worker throughput | `flowforge_tasks_completed_total` rate at N workers |
| Kafka throughput | `flowforge_outbox_published_total` rate; consumer lag |
| Database latency | pg `pg_stat_statements`; pool in-use vs max |
| Redis latency | `redis-cli --latency`; lease renew failures |
| gRPC latency | `flowforge_grpc_request_duration_seconds` p50/p95/p99 |
| Memory / CPU | pprof heap/CPU profiles (`PPROF_ENABLED=true`) |

## Scaling observations

- Task throughput scales with worker count until the **DB connection budget** or
  DB write throughput saturates (see [production/SCALING.md](production/SCALING.md)).
- Consumer parallelism is capped by Kafka partition count.
- `SKIP LOCKED` claiming means added workers claim disjoint batches without lock
  contention, so scaling is near-linear until the DB is the bottleneck.

## Optimization opportunities (from the audit)

- **Batch outbox publishing:** the publisher currently publishes events one at a
  time (`publishOne`); batched Kafka writes could raise throughput under high
  event volume.
- **DAG validation allocations:** the visited/adjacency maps could be pooled for
  very large workflows if validation ever becomes hot.
- **Connection pooling:** consider PgBouncer in front of Postgres when total
  connections (across replicas) approach `max_connections`.
- **Benchmark automation:** capture baselines in CI with `benchstat` gating to
  catch regressions (currently manual).
