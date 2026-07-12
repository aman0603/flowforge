# FlowForge

FlowForge is a distributed, high-performance workflow execution engine written in Go. It enables clients to register Directed Acyclic Graph (DAG) workflows and execute eligible tasks concurrently across multiple distributed worker nodes.

PostgreSQL is the durable source of truth for execution states, while future phases will incorporate Redis for distributed locking and Kafka for asynchronous event delivery.

---

## Architecture Diagram (Current Phase 1 Status)

```mermaid
graph TD
    Client[Client / User] -->|HTTP/REST| API[API Server]
    API -->|Validate DAG| DAG[DAG Validator]
    API -->|Transactional Write| DB[(PostgreSQL)]
    
    subgraph Local Workspace
        API
        DAG
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
* **Endpoint:** `POST /definitions`
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
We use Docker Compose to manage PostgreSQL and the API server. The database schema is automatically executed and updated on startup.

```bash
# Start all containers in the foreground
docker compose up --build

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
```

---

## Testing

Execute the test suites using the following commands:

```bash
# Run unit tests
go test -v ./...

# Run unit tests with Go's race detector enabled
go test -race ./...
```
