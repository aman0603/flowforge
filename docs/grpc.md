# gRPC Service Reference

FlowForge uses gRPC for **synchronous internal** communication. Contracts are
defined in [`proto/flowforge/`](../proto/flowforge/) and generated into
`internal/proto/`. All RPCs are **unary** (no streaming).

## Services & Deployment

| Service | Hosted by | Called by |
|---|---|---|
| `SchedulerService` | `cmd/scheduler` (gRPC `:9090`) | Workers |
| `RecoveryService` | `cmd/recovery` (gRPC `:9090`) | Workers |
| `HealthService` | every gRPC server (incl. API's embedded server) | Health checkers / probes |

Workers use **local** in-process clients by default and switch to **remote gRPC**
when `SCHEDULER_ADDR` / `RECOVERY_ADDR` are set (Docker Compose uses remote).

## Transport, TLS & Middleware

- **TLS/mTLS:** configured via `GRPC_TLS_ENABLED`, `GRPC_TLS_CERT_FILE`,
  `GRPC_TLS_KEY_FILE`, `GRPC_TLS_CA_FILE` (presence of CA enables mTLS).
  Insecure by default. Helpers: `grpcutil.NewServerTLS`, `grpcutil.DialTLS`.
- **Message size:** send/recv capped at 16 MiB.
- **Interceptors** (`internal/telemetry/grpcmw`): both client and server inject/
  extract W3C trace context and `x-request-id` correlation ID, open spans, and
  record `flowforge_grpc_requests_total` + `flowforge_grpc_request_duration_seconds`
  labeled by `method` and `code`.
- **Retries (client):** `grpcutil.Call` retries retryable codes (e.g.
  `Unavailable`) with exponential backoff (base `GRPC_RETRY_BASE_DELAY`, max
  attempts `GRPC_RETRY_MAX_ATTEMPTS`, backoff capped at 2s). Per-call deadline
  `GRPC_REQUEST_TIMEOUT`. Dial uses `WithBlock()` with a 5s timeout.

---

## Shared Types — `flowforge.common` (`common.proto`)

```proto
enum ServiceStatus {
  SERVICE_STATUS_UNSPECIFIED = 0;
  STARTING = 1; HEALTHY = 2; DEGRADED = 3; UNHEALTHY = 4; STOPPING = 5;
}

enum ErrorCode {
  ERROR_CODE_UNSPECIFIED = 0;
  RETRYABLE = 1; PERMANENT = 2; VALIDATION = 3; INTERNAL = 4; UNAVAILABLE = 5;
}

message ErrorDetail {
  ErrorCode code = 1;
  string    message = 2;
}
```

`ErrorDetail` is returned in-band in scheduler/recovery responses (in addition
to gRPC status codes) to distinguish retryable vs permanent conditions.

---

## `HealthService` — `health.proto`

Full method: `/flowforge.health.HealthService/Check`

| RPC | Request | Response |
|---|---|---|
| `Check` | `HealthRequest` | `HealthResponse` |

```proto
message HealthRequest  { bool readiness = 1; }   // true = readiness, false = liveness
message HealthResponse {
  flowforge.common.ServiceStatus status = 1;
  string detail = 2;                              // e.g. "database unreachable"
}
```

Backed by `grpcutil.HealthServer`, which delegates to a `HealthChecker`
(`NewDBHealthChecker` pings PostgreSQL with a 2s timeout).

**Example (grpcurl):**

```bash
grpcurl -plaintext -d '{"readiness": true}' \
  localhost:9090 flowforge.health.HealthService/Check
# → { "status": "HEALTHY", "detail": "" }
```

---

## `SchedulerService` — `scheduler.proto`

Claiming and retry promotion. **Does not execute tasks.**

| RPC | Request | Response |
|---|---|---|
| `ClaimTasks` | `ClaimTasksRequest` | `ClaimTasksResponse` |
| `PromoteRetries` | `PromoteRetriesRequest` | `PromoteRetriesResponse` |

```proto
message ClaimedTask {
  string task_run_id = 1;
  string workflow_run_id = 2;
  string task_definition_id = 3;
  string name = 4;
  string task_type = 5;
  int64  timeout_ms = 6;
  int64  fencing_token = 7;
  bytes  input = 8;
}

message ClaimTasksRequest  { string worker_id = 1; int32 capacity = 2; }
message ClaimTasksResponse {
  repeated ClaimedTask tasks = 1;
  flowforge.common.ErrorDetail error = 2;
}

message PromoteRetriesRequest  {}
message PromoteRetriesResponse {
  int64 promoted = 1;
  flowforge.common.ErrorDetail error = 2;
}
```

- `ClaimTasks` → `repository.ClaimReadyTasksBatch` (`FOR UPDATE SKIP LOCKED`,
  ordered `priority DESC, created_at ASC`, `LIMIT capacity`). Atomically
  transitions `READY → CLAIMED`, sets `worker_id`/`claimed_at`, increments
  `fencing_token`.
- `PromoteRetries` → `repository.PromoteDueRetries` (`RETRY_WAIT → READY` where
  `next_retry_at <= NOW()`); returns count promoted.

**Errors:** transport failures surface as gRPC status codes (client retries
retryable ones); domain problems populate `error` (`ErrorDetail`).

**Example:**

```bash
grpcurl -plaintext -d '{"worker_id":"w1","capacity":8}' \
  localhost:9090 flowforge.scheduler.SchedulerService/ClaimTasks
```

---

## `RecoveryService` — `recovery.proto`

Lease-aware, fencing-token-guarded reclamation of stale tasks. **Does not
execute tasks.**

| RPC | Request | Response |
|---|---|---|
| `RecoverTask` | `RecoverTaskRequest` | `RecoverTaskResponse` |
| `RecoverStaleTasks` | `RecoverStaleTasksRequest` | `RecoverStaleTasksResponse` |

```proto
message RecoverTaskRequest {
  string task_run_id = 1;
  int64  fencing_token = 2;
  string task_status = 3;    // "CLAIMED" or "RUNNING"
}
message RecoverTaskResponse {
  bool reclaimed = 1;
  flowforge.common.ErrorDetail error = 2;
}

message RecoverStaleTasksRequest {
  int64 claimed_timeout_ms = 1;
  int64 running_timeout_ms = 2;
}
message RecoverStaleTasksResponse {
  int64 claimed_recovered = 1;
  int64 running_recovered = 2;
  flowforge.common.ErrorDetail error = 3;
}
```

- `RecoverTask` dispatches on `task_status`: `CLAIMED → RecoverClaimedTask`,
  `RUNNING → RecoverRunningTask`. Both are guarded by
  `status = $status AND fencing_token = $token`, reset the task to `READY`, null
  worker/claim/start columns, mark the in-flight attempt `ORPHANED`
  (failure type `WORKER_LOST`), and emit `TaskRecovered`. Increments
  `flowforge_tasks_recovered_total` on success.
- `RecoverStaleTasks` bulk-reclaims all stale `CLAIMED`/`RUNNING` tasks older
  than the provided timeouts.

**Example:**

```bash
grpcurl -plaintext \
  -d '{"task_run_id":"t1","fencing_token":3,"task_status":"RUNNING"}' \
  localhost:9090 flowforge.recovery.RecoveryService/RecoverTask
```

---

## Versioning

- Contracts are versioned by **proto package** (`flowforge.<service>`) and the
  Kafka topic version (`flowforge.workflow-events.v1`).
- gRPC messages evolve additively (append fields with new tags); never reuse or
  renumber field tags.
- Regenerate stubs after edits: `./scripts/gen-proto.sh` (requires `protoc`,
  `protoc-gen-go`, `protoc-gen-go-grpc`).

## Error Handling Summary

| Layer | Mechanism |
|---|---|
| Transport | gRPC status codes; `Unavailable`/`DeadlineExceeded` retried by client |
| Domain | `ErrorDetail{code,message}` in responses (`RETRYABLE`/`PERMANENT`/…) |
| Deadlines | Per-call `GRPC_REQUEST_TIMEOUT` |
| TLS | `GRPC_TLS_*`; handshake failures fail the dial |
