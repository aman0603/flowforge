# FlowForge

FlowForge is a distributed, high-performance workflow execution engine written in Go. It enables clients to register Directed Acyclic Graph (DAG) workflows and execute eligible tasks concurrently across multiple distributed worker nodes.

PostgreSQL is the durable source of truth for execution states, while Redis is utilized for ephemeral liveness coordination, heartbeats, and renewable task execution leases.

---

## Architecture Diagram (Current Phase 9 Status)

```mermaid
graph TD
    Client[Client / User] -->|HTTP/REST| API[API Server]
    API -->|Validate DAG| DAG[DAG Validator]
    API -->|Transactional Write| DB[(PostgreSQL)]
    
    DB -->|Atomic Batch Claim FOR UPDATE SKIP LOCKED| W1[Worker 1]
    DB -->|Atomic Batch Claim FOR UPDATE SKIP LOCKED| W2[Worker 2]

    W1 <-->|Heartbeats & Leases| Redis[(Redis)]
    W2 <-->|Heartbeats & Leases| Redis

    subgraph Worker Pool Architecture
        direction TB
        W1 -->|Buffered Channel| WP1[Worker Goroutines 1..N]
        W2 -->|Buffered Channel| WP2[Worker Goroutines 1..N]
    end
```

---

## Directory Layout

* **`cmd/flowforge/`**: Entry point of the application containing [main.go](file:///home/amanpaswan/aman/flowforge/cmd/flowforge/main.go), bootstrapping the database and the HTTP server.
* **`cmd/worker/`**: Entry point of the worker process containing [main.go](file:///home/amanpaswan/aman/flowforge/cmd/worker/main.go).
* **`internal/api/`**: The web service layers containing [server.go](file:///home/amanpaswan/aman/flowforge/internal/api/server.go), implementing routes using Go's native HTTP muxer and handling requests/responses.
* **`internal/config/`**: Configuration loading in [config.go](file:///home/amanpaswan/aman/flowforge/internal/config/config.go) using environment variables.
* **`internal/dag/`**: Core graph validation logic in [dag.go](file:///home/amanpaswan/aman/flowforge/internal/dag/dag.go) to detect circular dependencies before persisting workflows.
* **`internal/model/`**: Shared Go structs, constants, and API structures in [model.go](file:///home/amanpaswan/aman/flowforge/internal/model/model.go).
* **`internal/repository/`**: PostgreSQL database connector and transaction boundaries implemented in [postgres.go](file:///home/amanpaswan/aman/flowforge/internal/repository/postgres.go).
* **`internal/worker/`**: Worker process logic, execution loop, lease coordinator, and executors.
* **`schema.sql`**: Relational database schema layout [schema.sql](file:///home/amanpaswan/aman/flowforge/schema.sql).

---

## REST API Reference

All requests and responses use JSON format.

### 1. Health Check
* **Endpoint:** `GET /health`
* **Response Status:** `200 OK`
* **Response Body:**
  ```json
  {
    "status": "ok"
  }
  ```

### 2. Register Workflow Definition
Registers a new workflow template and validates that its tasks form a valid Directed Acyclic Graph (DAG) with no cycles.
* **Endpoint:** `POST /api/v1/workflows`
* **Request Body:**
  ```json
  {
    "name": "etl-pipeline",
    "description": "Simple ETL Workflow",
    "tasks": [
      {
        "name": "fetch-data",
        "task_type": "HTTP",
        "config": {"url": "https://api.example.com/data"},
        "max_retries": 3,
        "retry_backoff_ms": 1000,
        "timeout_ms": 5000,
        "dependencies": []
      },
      {
        "name": "process-data",
        "task_type": "SCRIPT",
        "config": {"script": "process.py"},
        "max_retries": 2,
        "retry_backoff_ms": 2000,
        "timeout_ms": 10000,
        "dependencies": ["fetch-data"]
      }
    ]
  }
  ```
* **Response Status:** `201 Created`
* **Response Body:**
  ```json
  {
    "id": "e0b0db73-c603-4ab6-8809-72b1574044ee",
    "name": "etl-pipeline",
    "description": "Simple ETL Workflow",
    "created_at": "2026-07-12T15:32:00Z"
  }
  ```

### 3. Trigger Workflow Run
Instantiates a new execution tracking run for a registered workflow definition, pre-populating task runs.
* **Endpoint:** `POST /runs`
* **Request Body:**
  ```json
  {
    "workflow_definition_id": "e0b0db73-c603-4ab6-8809-72b1574044ee",
    "input": {
      "batch_id": "1234"
    }
  }
  ```
* **Response Status:** `201 Created`
* **Response Body:**
  ```json
  {
    "id": "7809930f-b258-45a8-9d29-a1b7ad4f71a0",
    "workflow_definition_id": "e0b0db73-c603-4ab6-8809-72b1574044ee",
    "status": "PENDING",
    "input": {
      "batch_id": "1234"
    },
    "output": {}
  }
  ```

### 4. Fetch Workflow Run Progress
Queries the execution progress and state of all task runs for a given workflow run.
* **Endpoint:** `GET /runs/{id}`
* **Response Status:** `200 OK`
* **Response Body:**
  ```json
  {
    "run": {
      "id": "7809930f-b258-45a8-9d29-a1b7ad4f71a0",
      "workflow_definition_id": "e0b0db73-c603-4ab6-8809-72b1574044ee",
      "status": "PENDING",
      "input": {"batch_id": "1234"},
      "output": {},
      "created_at": "2026-07-12T15:32:05Z"
    },
    "tasks": [
      {
        "id": "f5d0d1b3-4632-475f-9fe3-c6722d3b25bb",
        "workflow_run_id": "7809930f-b258-45a8-9d29-a1b7ad4f71a0",
        "task_definition_id": "908bd2f1-6780-4965-b1a9-3d12f293cf3d",
        "status": "PENDING",
        "attempts": 0,
        "input": {},
        "output": {},
        "created_at": "2026-07-12T15:32:05Z"
      }
    ]
  }
  ```

---

## How to Run & Build

### Using Docker (Recommended)
We use Docker Compose to manage PostgreSQL, Redis, the API server, and 3 workers.

```bash
# Start all containers in the foreground with 3 concurrent workers
docker compose up --build --scale worker=3

# Shutdown and clean volumes
docker compose down -v
```

### Running Locally (Without Docker)
Make sure you have running PostgreSQL and Redis instances, and specify configuration via environment variables:

```bash
# Set configuration variables
export DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable"
export REDIS_ADDR="localhost:6379"
export PORT="8080"
export SCHEMA_PATH="schema.sql"

# Kafka & Outbox configurations
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="flowforge.workflow-events.v1"
export KAFKA_CLIENT_ID="flowforge-publisher"
export OUTBOX_POLL_INTERVAL="500ms"
export OUTBOX_BATCH_SIZE="100"
export OUTBOX_CLAIM_TIMEOUT="30s"
export OUTBOX_MAX_RETRIES="5"
export OUTBOX_RETRY_BASE_DELAY="1s"
export OUTBOX_RETENTION="24h"

# Run the server
go run cmd/flowforge/main.go

# Start a worker process locally
export WORKER_ID="local-worker-1"
go run cmd/worker/main.go
```

---

## Testing

Execute the test suites using the following commands:

```bash
# Run unit tests
go test -v ./...

# Run unit tests with Go's race detector enabled
go test -race ./...

# Run integration tests against a running PostgreSQL test database
TEST_DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable" go test -tags=integration -v ./internal/repository

# Run integration tests with Go's race detector enabled
TEST_DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable" go test -tags=integration -race -v ./internal/repository
```

---

## Concurrency & Execution Semantics

### 1. Multi-Worker Task Distribution
FlowForge utilizes PostgreSQL `FOR UPDATE SKIP LOCKED` inside a transaction within `ClaimNextReadyTask`. This allows multiple workers to claim ready tasks concurrently without conflicts or double-claiming. 

### 2. Workflow-Level Lock Serialization
To ensure DAG progression correctness (such as Diamond DAG sibling completion race conditions), all transitions affecting workflow progression (`MarkTaskRunCompleted` and `MarkTaskRunFailed`) exclusively lock the parent `workflow_runs` row:
```sql
SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE
```
This forces all completion/failure transactions for the same workflow to serialize, keeping sibling completions completely safe and deterministic.

### 3. Worker Ownership Fencing & Monotonic Fencing Tokens
Workers acquire tasks and stamp their unique `worker_id` (generated via container hostname and UUID). Additionally, each task claim atomically increments a monotonic `fencing_token` on the task run in PostgreSQL. All subsequent authoritative task mutations (`StartTaskRun`, `MarkTaskRunCompleted`, `MarkTaskRunFailed`) are guarded by checking:
```sql
WHERE worker_id = $workerID AND fencing_token = $fencingToken AND status = $expectedStatus
```
This prevents hijacked execution, split-brain transitions, or late-arriving writes from stale workers.

### 4. Ephemeral Redis Coordination & Renewable Leases
While PostgreSQL owns the durable workflow correctness, Redis acts as the ephemeral coordination layer.
* **Worker Liveness:** Workers register their presence via a TTL-backed heartbeat key (`flowforge:worker:{worker_id}:heartbeat`) in Redis. A background loop refreshes this heartbeat periodically. If a worker process crashes, its heartbeat key naturally expires.
* **Task Leases:** Upon claiming a task, the worker acquires a lease key (`flowforge:task:{task_run_id}:lease`) in Redis. The worker periodically renews this lease during active execution.
* **Context Interruption:** If a worker fails to renew its lease (due to network partition or Redis failure), it immediately cancels the executing task's context and aborts writing terminal results to PostgreSQL.

### 5. Lease-Aware Stale Task Crash Recovery
Workers run a background context-aware stale-task recovery loop.
* **Stale CLAIMED Reset:** Tasks stuck in `CLAIMED` status longer than `CLAIMED_STALE_TIMEOUT` (default `30s`) are reset back to `READY` if no active lease exists or the lease owner has died.
* **Stale RUNNING Reset:** Tasks stuck in `RUNNING` status longer than `RUNNING_STALE_TIMEOUT` (default `5m`) are reset back to `READY` (clearing `worker_id`, `claimed_at`, `started_at`, and resetting execution-result fields) if no active lease exists or the lease owner has died.
* **Attempt Orphaning:** Stale running resets transition the corresponding active attempt record in `task_attempts` to `ORPHANED`.

### 6. Automatic Retries and Exponential Backoff
When task execution fails or times out, and its execution `attempts <= max_retries`, FlowForge schedules a retry:
* The task status transitions to `RETRY_WAIT`.
* The parent workflow run remains `RUNNING`, and all downstream child tasks remain blocked in `PENDING`.
* Next execution time is computed using exponential backoff: `delay = base_backoff * 2^(attempts-1)`, capped at 1 hour and bounded to prevent numeric overflow.
* Once the backoff delay has elapsed, the background recovery routine promotes the task back to `READY`.

### 7. Priority-Based Scheduling Preference
Task execution selection prioritizes tasks with higher priority values:
* Claiming queries select `READY` tasks ordered by `priority DESC` and then `created_at ASC` (FIFO tie-breaking).
* Priority is a scheduling preference; already CLAIMED or RUNNING tasks are not preempted.
* Priority does not bypass DAG dependencies (downstream dependent tasks remain blocked until all parents succeed).

### 8. Task Execution Timeouts
Workers enforce execution-level context timeouts based on the task's `timeout_ms` definition:
* A timeout triggers context cancellation (`context.DeadlineExceeded`) on the executing task.
* On timeout, the task consumes a retry attempt and transitions to `RETRY_WAIT` (if budget remains) or to terminal `TIMED_OUT` (if retry budget is exhausted).
* Worker process graceful shutdown cancellations (`context.Canceled`) are distinguished from task execution timeouts, leaving the task in `RUNNING` for normal stale task recovery without consuming attempts.

### 9. Durable Attempt History
FlowForge records details of every individual execution attempt of a task in the `task_attempts` table.
* **Attempt Semantics:** Each `StartTaskRun` transaction atomically increments the task run's `attempts` count and inserts a corresponding `task_attempts` record with an incremented `attempt_number` and `RUNNING` status.
* **Attempt Statuses:**
  * `RUNNING`: The attempt is currently executing.
  * `COMPLETED`: The attempt completed successfully.
  * `FAILED`: The attempt failed with a normal execution error.
  * `TIMED_OUT`: The attempt exceeded its execution timeout limit.
  * `ORPHANED`: The worker process crashed or became slow, causing stale recovery to reclaim the task.
* **Failure Classification:**
  * `EXECUTION_ERROR`: Set on normal task execution failures.
  * `TIMEOUT`: Set when the execution timeout is exceeded.
  * `WORKER_LOST`: Set on `ORPHANED` attempts when the worker disappears.

### 10. Dead-Letter Queue (DLQ)
When a task run reaches an unrecoverable terminal execution state due to retry exhaustion (normal failures or timeouts), FlowForge writes a durable record to `dead_letter_tasks` in the same transaction as the task run terminal status update.
* **Dead-letter conditions:** Tasks are only dead-lettered when they transition to terminal `FAILED` or `TIMED_OUT` states (i.e. retry budget exhausted).
* **Uniqueness:** A unique index on `task_run_id` prevents duplicate DLQ records.
* **Replay Support:** Replay is not automatically supported in Phase 7; it is deferred to future design.

### 11. HTTP History & Observability APIs
FlowForge provides REST endpoints to inspect execution history and terminal failures:
* `GET /api/v1/runs/{run_id}/history`: Returns the complete history of a workflow run, including all its tasks and attempts.
* `GET /api/v1/tasks/{task_run_id}/attempts`: Returns attempts for a task run ordered by `attempt_number ASC`.
* `GET /api/v1/dead-letter`: Returns the paginated list of dead-lettered tasks.
  * **Pagination:** Query parameters `limit` (default 50, max 100) and `offset` are validated and enforced.

### 12. Delivery Semantics & Crash Windows
> [!IMPORTANT]
> **FlowForge provides at-least-once execution semantics.**
> Recovering a stale `RUNNING` task resets it back to `READY` to allow re-claiming and re-execution, and marks the failed attempt as `ORPHANED`. Fencing tokens prevent stale workers from committing authoritative FlowForge task results, but cannot undo an external side effect that occurred before lease loss, process failure, or stale-write rejection. Therefore, task executors that perform external side effects must be designed to be idempotent.

* **Crash Windows and Duplicate Execution:**
  * *Window A (Duplicate Side Effects):* Executor completes external side effects -> Worker crashes before `COMPLETED` is persisted -> Stale recovery reclaims task and marks attempt `ORPHANED` -> Task runs again. (Idempotency required).
  * *Window B (Stale Running Reclamation):* Worker starts execution -> Worker crashes -> Stale recovery transitions task to `READY` and attempt to `ORPHANED` -> Re-execution creates attempt 2.
  * *Window C (Claimed Reclamation):* Worker crashes while task is `CLAIMED` before `StartTaskRun` -> Stale recovery transitions task to `READY` -> No attempt record is created (attempts count remains unchanged).
  * *Window D (Terminal Transaction Crash):* Worker crashes mid-transaction when persisting terminal failure -> Database transaction rolls back completely (no partial terminal state or orphaned DLQ).

### 13. Payload and Payload Size Limitations
* **Large Payloads:** Task output (`output` JSONB) and error messages (`error_message` text) are stored in the database. Very large payloads can result in high memory consumption and latency. Object storage integration is deferred to future phases.
* **Unbounded History:** Attempt history and DLQ logs grow indefinitely without an automatic retention service.

### 14. API Security
* APIs are currently unauthenticated. Production deployment requires external authentication proxies or gateways.

### 15. Supported Executor Types
* **`SLEEP`**: Basic executor that sleeps for the duration configured in `duration_ms`.

### 16. Known Limitations
* Object storage integration for large payloads is deferred to future phases.

### 17. High-Performance Worker Pools, Concurrency Bounds, & Graceful Shutdown
Phase 9 introduces high-performance concurrent worker pools, bounded capacity-aware task claiming, and resilient graceful shutdown:
* **Worker Pool Execution:** A worker process spawns a fixed set of concurrent execution goroutines bounded by `WORKER_POOL_SIZE`. These routines consume tasks from a shared, in-memory buffered queue channel (`WORKER_QUEUE_CAPACITY`).
* **Capacity-Aware Backpressure:** The worker scheduler loop dynamically monitors slot availability:
  `available_slots = WORKER_POOL_SIZE - active_executions - queued_tasks`
  If `available_slots > 0`, it claims a batch of tasks up to the minimum of `available_slots` and `WORKER_CLAIM_BATCH_SIZE`. If capacity is fully saturated, claiming is paused, preventing memory overload.
* **Atomic Batch Claiming:** Ready tasks are claimed in batches using a single database transaction query utilizing a Common Table Expression (CTE). This ensures `FOR UPDATE SKIP LOCKED`, priority ranking, and FIFO tie-breaking are executed atomically.
* **Panic Isolation:** Executor runtime panics are intercepted using Go's `recover()`, recorded as execution failures with a stack trace in the task attempt details, and isolated to prevent cascading crashes.
* **Graceful Shutdown Drainage:** On context termination, the scheduler loop halts task claiming. The worker drains all queued tasks, transactionally returning them to `READY` status in PostgreSQL, while active executions are given up to `WORKER_SHUTDOWN_GRACE_PERIOD_MS` to finish execution. Context deadline cancellation terminates any tasks exceeding this grace period.

