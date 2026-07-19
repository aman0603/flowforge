# AGENT.md

## Project Overview

FlowForge is a distributed workflow execution engine written in Go. Clients submit DAG-based workflows through REST APIs, and multiple workers execute eligible tasks concurrently.

When planning changes, treat the implementation as authoritative and preserve existing architecture and invariants unless the task explicitly changes them.

### Tech Stack

* Go
* PostgreSQL
* Redis
* Kafka
* gRPC
* Docker and Docker Compose
* Prometheus-compatible metrics

### Core Features

* DAG-based workflow execution
* Concurrent worker pools using goroutines and channels
* Priority scheduling
* Retries with exponential backoff and jitter
* Execution timeouts
* Dead-letter queues
* Distributed locking and idempotency
* Kafka-based event delivery
* Transactional outbox pattern
* gRPC-based internal communication

PostgreSQL is the durable source of truth. Redis is used for distributed coordination, and Kafka is used for asynchronous event delivery.

## Build and Test Commands

```bash
# Install dependencies
go mod download

# Build
go build ./...

# Run tests
go test ./...

# Run tests with race detection
go test -race ./...

# Format and validate
gofmt -w .
go vet ./...

# Start all services
docker compose up --build

# Start with multiple workers
docker compose up --build --scale worker=5
```

Before completing a change, run:

```bash
gofmt -w .
go vet ./...
go test ./...
```

Run `go test -race ./...` for concurrency-sensitive changes.

## Code Style Guidelines

* Write idiomatic Go and always use `gofmt`.
* Keep functions focused and packages aligned with clear responsibilities.
* Avoid global mutable state and unbounded goroutines.
* Pass dependencies explicitly through constructors.
* Keep interfaces small and define them near their consumers.
* Use `context.Context` for database, network, task execution, timeout, and cancellation operations.
* Never silently ignore errors.
* Wrap errors with useful context using `%w`.
* Do not use panics for expected runtime failures.
* Prefer the Go standard library or existing dependencies before adding new packages.

## Architecture Guidelines

* PostgreSQL is the durable source of truth for workflow and task state.
* Never assume exactly-once execution or message delivery.
* Design all task execution and event consumers to be idempotent.
* Validate all task state transitions explicitly.
* Use database transactions for related state changes that must succeed atomically.
* Use `FOR UPDATE SKIP LOCKED` for safe concurrent task claiming.
* Redis locks are an additional coordination mechanism, not the sole correctness mechanism.
* Kafka consumers must tolerate duplicate message delivery.
* Use the transactional outbox pattern when database changes require Kafka events.
* Use bounded worker pools; never create unlimited goroutines.
* Every long-running operation must support cancellation and timeouts through `context.Context`.
* Persist retry schedules instead of blocking worker goroutines with long `time.Sleep` calls.

## Testing Instructions

Add tests for meaningful behavioral changes.

Important areas to test:

* DAG validation and cycle detection
* Task state transitions
* Priority scheduling
* Retry and backoff calculations
* Timeout and cancellation behavior
* Concurrent task claiming
* Idempotency
* Duplicate event delivery
* Worker crashes and partial failures

For concurrency-related changes, always run:

```bash
go test -race ./...
```

Avoid tests that depend on arbitrary `time.Sleep` calls when deterministic synchronization is possible.

## Security Considerations

* Never commit secrets, credentials, API keys, tokens, private keys, or production connection strings.
* Keep local secrets in environment variables and provide safe examples through `.env.example`.
* Do not expose raw database errors, stack traces, or internal infrastructure details through public APIs.
* Do not log credentials, tokens, or sensitive task payloads.
* Validate all externally supplied workflow definitions and task payloads.
* Do not execute arbitrary user-provided code or shell commands without explicit sandboxing.

## Commit Guidelines

Use concise, imperative commit messages:

```text
feat: add atomic task claiming
fix: prevent duplicate task execution
test: add concurrent worker tests
refactor: extract state transition validation
docs: update local setup instructions
```

Keep each commit focused on one logical change.

## Instructions for Coding Agents

Before making changes:

1. Read the relevant existing code before implementing.
2. Keep changes focused on the requested task.
3. Do not rewrite unrelated code or introduce unnecessary abstractions.
4. Consider concurrency, retries, duplicate execution, and crash recovery.
5. Add or update tests for behavioral changes.
6. Update documentation when APIs, configuration, or architecture change.

For distributed-system changes, always consider:

* What happens if two workers execute this concurrently?
* What happens if the operation executes twice?
* What happens if the process crashes before or after persistence?
* Is the operation idempotent?
* Is a database transaction required?
* Does cancellation propagate correctly?
* Could this leak a goroutine or create an unbounded queue?

## Definition of Done

A change is complete when:

* The code compiles.
* `gofmt` has been applied.
* `go vet ./...` passes.
* Relevant tests pass.
* Race detection passes for concurrency-sensitive changes.
* Errors are handled explicitly.
* Concurrency and duplicate execution have been considered.
* Documentation is updated when necessary.
