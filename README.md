# FlowForge

FlowForge is a distributed, high-performance workflow execution engine written in Go. It enables clients to register Directed Acyclic Graph (DAG) workflows and execute eligible tasks concurrently across multiple distributed worker nodes.

PostgreSQL is the durable source of truth for execution states, while future phases will incorporate Redis for distributed locking and Kafka for asynchronous event delivery.

---

## Architecture Diagram (Current Phase 5 Status)

```mermaid
graph TD
    Client[Client / User] -->|HTTP/REST| API[API Server]
    API -->|Validate DAG| DAG[DAG Validator]
    API -->|Transactional Write| DB[(PostgreSQL)]
    
    DB -->|Claim READY task FOR UPDATE SKIP LOCKED| W1[Worker 1]
    DB -->|Claim READY task FOR UPDATE SKIP LOCKED| W2[Worker 2]
    DB -->|Claim READY task FOR UPDATE SKIP LOCKED| W3[Worker 3]

    subgraph Workers
        W1
        W2
        W3
    end
```

---

## Directory Layout

* **`cmd/flowforge/`**: Entry point of the application containing [main.go](file:///home/amanpaswan/aman/flowforge/cmd/flowforge/main.go), bootstrapping the database and the HTTP server.
* **`internal/api/`**: The web service layers containing [server.go](file:///home/amanpaswan/aman/flowforge/internal/api/server.go), implementing routes using Go's native HTTP muxer and handling requests/responses.
* **`internal/config/`**: Configuration loading in [config.go](file:///home/amanpaswan/aman/flowforge/internal/config/config.go) using environment variables.
* **`internal/dag/`**: Core graph validation logic in [dag.go](file:///home/amanpaswan/aman/flowforge/internal/dag/dag.go) to detect circular dependencies before persisting workflows.
* **`internal/model/`**: Shared Go structs, constants, and API structures in [model.go](file:///home/amanpaswan/aman/flowforge/internal/model/model.go).
* **`internal/repository/`**: PostgreSQL database connector and transaction boundaries implemented in [postgres.go](file:///home/amanpaswan/aman/flowforge/internal/repository/postgres.go).
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
We use Docker Compose to manage PostgreSQL, the API server, and 3 workers.

```bash
# Start all containers in the foreground with 3 concurrent workers
docker compose up --build --scale worker=3

# Shutdown and clean volumes
docker compose down -v
```

### Running Locally (Without Docker)
Make sure you have a running PostgreSQL database and specify its URL via environment variables:

```bash
# Set configuration variables
export DB_URL="postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable"
export PORT="8080"
export SCHEMA_PATH="schema.sql"

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

### 3. Worker Ownership Fencing
Workers acquire tasks and stamp their unique `worker_id` (generated via container hostname and UUID). All subsequent task mutations (`StartTaskRun`, `MarkTaskRunCompleted`, `MarkTaskRunFailed`) are guarded by checking:
```sql
WHERE worker_id = $workerID AND status = $expectedStatus
```
This prevents hijacked execution or split-brain transitions.

### 4. Stale Task Crash Recovery
Workers run a background context-aware stale-task recovery loop.
* **Stale CLAIMED Reset:** Tasks stuck in `CLAIMED` status longer than `CLAIMED_STALE_TIMEOUT` (default `30s`) are reset back to `READY` (clearing `worker_id` and `claimed_at`).
* **Stale RUNNING Reset:** Tasks stuck in `RUNNING` status longer than `RUNNING_STALE_TIMEOUT` (default `5m`) are reset back to `READY` (clearing `worker_id`, `claimed_at`, `started_at`, and resetting execution-result fields to default).

### 5. Automatic Retries and Exponential Backoff
When task execution fails or times out, and its execution `attempts <= max_retries`, FlowForge schedules a retry:
* The task status transitions to `RETRY_WAIT`.
* The parent workflow run remains `RUNNING`, and all downstream child tasks remain blocked in `PENDING`.
* Next execution time is computed using exponential backoff: `delay = base_backoff * 2^(attempts-1)`, capped at 1 hour and bounded to prevent numeric overflow.
* Once the backoff delay has elapsed, the background recovery routine promotes the task back to `READY`.

### 6. Priority-Based Scheduling Preference
Task execution selection prioritizes tasks with higher priority values:
* Claiming queries select `READY` tasks ordered by `priority DESC` and then `created_at ASC` (FIFO tie-breaking).
* Priority is a scheduling preference; already CLAIMED or RUNNING tasks are not preempted.
* Priority does not bypass DAG dependencies (downstream dependent tasks remain blocked until all parents succeed).

### 7. Task Execution Timeouts
Workers enforce execution-level context timeouts based on the task's `timeout_ms` definition:
* A timeout triggers context cancellation (`context.DeadlineExceeded`) on the executing task.
* On timeout, the task consumes a retry attempt and transitions to `RETRY_WAIT` (if budget remains) or to terminal `TIMED_OUT` (if retry budget is exhausted).
* Worker process graceful shutdown cancellations (`context.Canceled`) are distinguished from task execution timeouts, leaving the task in `RUNNING` for normal stale task recovery without consuming attempts.

### 8. Delivery Semantics
> [!IMPORTANT]
> **FlowForge provides at-least-once execution semantics.**
> Recovering a stale `RUNNING` task resets it back to `READY` to allow re-claiming and re-execution. If a worker crashed during execution or after completing side effects but before persisting the state, the task will execute again. Therefore, task executors that perform external side effects must be designed to be idempotent.

### 9. Supported Executor Types
* **`SLEEP`**: Basic executor that sleeps for the duration configured in `duration_ms`.

### 10. Known Limitations
* Crash recovery relies on static timeouts and periodic scans; a worker that is slow or temporarily network-partitioned may have its tasks reclaimed (resulting in concurrent duplicate executions). Leases and heartbeats are scheduled for future phases.

