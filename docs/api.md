# REST API Reference

The REST API is FlowForge's external control plane, served by the `flowforge`
binary on `PORT` (default `:8080`). All routes are defined in
`internal/api/server.go`.

## Conventions

- **Content type:** `application/json` for requests and responses.
- **Error envelope:** `{ "error": "<message>" }`. When `ENV=development`, an
  additional `"details"` field carries the underlying error string.
- **Body limits:** write endpoints wrap the body in `http.MaxBytesReader` capped
  at `MAX_REQUEST_BODY_BYTES` (default 1 MiB). Header size is capped at 1 MiB.
- **Rate limiting:** when `RATE_LIMIT_RPS > 0`, a global token bucket returns
  `429 Too Many Requests` with `Retry-After: 1`. Disabled by default.
- **Timeouts:** `ReadTimeout 10s`, `ReadHeaderTimeout 5s`, `WriteTimeout 10s`,
  `IdleTimeout 60s`.
- **Authentication:** none built in — deploy behind an auth proxy/gateway.

> **Path convention note:** `POST /runs` and `GET /runs/{id}` do **not** carry
> the `/api/v1/` prefix used by newer endpoints. Both styles coexist in the
> current implementation.

## Endpoint Summary

| # | Method + Path | Purpose |
|---|---|---|
| 1 | `POST /api/v1/workflows` | Create a workflow definition |
| 2 | `POST /runs` | Start a workflow run |
| 3 | `GET /runs/{id}` | Run details + tasks |
| 4 | `GET /api/v1/runs/{run_id}/history` | Full run + attempt history |
| 5 | `GET /api/v1/tasks/{task_run_id}/attempts` | Attempts for a task |
| 6 | `GET /api/v1/dead-letter` | Dead-letter queue (paginated) |
| 7 | `GET /health` | Basic health |
| 8 | `GET /healthz` | Liveness probe |
| 9 | `GET /readyz` | Readiness probe (DB ping) |
| 10 | `GET /metrics` | Prometheus metrics |

---

## 1. `POST /api/v1/workflows`

Create a workflow definition (a DAG of task definitions).

**Request body** (`CreateDefinitionRequest`):

```json
{
  "name": "ci-pipeline",
  "description": "build then deploy",
  "tasks": [
    {
      "name": "build",
      "task_type": "SLEEP",
      "config": { "duration_ms": 500 },
      "max_retries": 3,
      "retry_backoff_ms": 1000,
      "timeout_ms": 60000,
      "priority": 0,
      "dependencies": []
    },
    {
      "name": "deploy",
      "task_type": "SLEEP",
      "config": { "duration_ms": 200 },
      "dependencies": ["build"]
    }
  ]
}
```

**Task fields:** `name` (required, unique), `task_type` (executor key, e.g.
`SLEEP`), `config` (raw JSON passed to the executor), `max_retries`,
`retry_backoff_ms`, `timeout_ms`, `priority`, `dependencies` (parent task
**names**).

**Validation** (`dag.Validate`):
- at least one task; no empty/duplicate names;
- no self-dependency; all dependencies must reference existing tasks;
- graph must be acyclic (DFS cycle detection).

**Responses:**
- `201 Created` — `WorkflowDefinition` `{ id, name, description, created_at }`
- `400 Bad Request` — invalid JSON body
- `422 Unprocessable Entity` — DAG validation failure
- `500 Internal Server Error` — persistence failure

---

## 2. `POST /runs`

Instantiate a workflow definition as a run.

**Request body** (`CreateRunRequest`):

```json
{ "workflow_definition_id": "<definition-id>", "input": { } }
```

**Validation:** `workflow_definition_id` is required.

**Responses:**
- `201 Created` — `WorkflowRun`:
  ```json
  {
    "id": "<run-id>",
    "workflow_definition_id": "<definition-id>",
    "status": "PENDING",
    "input": {},
    "output": null,
    "created_at": "2026-01-01T00:00:00Z"
  }
  ```
- `400 Bad Request` — invalid JSON or missing `workflow_definition_id`
- `500 Internal Server Error` — failed to start run

On creation, root tasks (no dependencies) become `READY`, others `PENDING`, and
a `WorkflowStarted` event is written to the outbox in the same transaction.

---

## 3. `GET /runs/{id}`

Fetch a run and its tasks.

**Path param:** `id` — workflow run ID.

**Responses:**
- `200 OK` — `WorkflowRunDetails` `{ run: WorkflowRun, tasks: [TaskRun] }`
- `400 Bad Request` — missing id
- `500 Internal Server Error` — fetch failure

> **Audit finding:** this endpoint returns `500` (not `404`) when the run does
> not exist — `sql.ErrNoRows` is not mapped here (a known TODO in the code).
> Endpoints 4 and 5 correctly return `404`.

`TaskRun` fields: `id`, `workflow_run_id`, `task_definition_id`, `status`,
`attempts`, `input`, `output`, `error_message`, `next_retry_at`, `worker_id`,
`claimed_at`, `started_at`, `completed_at`, `fencing_token`, `created_at`.

---

## 4. `GET /api/v1/runs/{run_id}/history`

Full execution history for a run: each task with all of its attempts.

**Path param:** `run_id`.

**Responses:**
- `200 OK` — `WorkflowHistoryResponse`:
  ```json
  {
    "workflow_run_id": "<run-id>",
    "workflow_status": "COMPLETED",
    "tasks": [
      {
        "task_run_id": "<task-run-id>",
        "task_name": "build",
        "status": "COMPLETED",
        "attempts": [
          {
            "id": "<attempt-id>",
            "attempt_number": 1,
            "worker_id": "<worker>",
            "status": "COMPLETED",
            "started_at": "…",
            "completed_at": "…",
            "duration_ms": 512,
            "fencing_token": 1
          }
        ]
      }
    ]
  }
  ```
- `400 Bad Request` — missing id
- `404 Not Found` — run not found
- `500 Internal Server Error` — other failures

---

## 5. `GET /api/v1/tasks/{task_run_id}/attempts`

List all attempts for a single task run.

**Path param:** `task_run_id`.

**Responses:**
- `200 OK` — `[TaskAttempt]` (fields: `id`, `task_run_id`, `workflow_run_id`,
  `attempt_number`, `worker_id`, `status`, `claimed_at`, `started_at`,
  `completed_at`, `duration_ms`, `output`, `error_message`, `failure_type`,
  `fencing_token`, `created_at`)
- `400 Bad Request` — missing id
- `404 Not Found` — task run not found
- `500 Internal Server Error` — other failures

Attempt `status` values: `RUNNING`, `COMPLETED`, `FAILED`, `TIMED_OUT`,
`ORPHANED`.

---

## 6. `GET /api/v1/dead-letter`

Paginated list of dead-lettered (permanently failed) tasks.

**Query params:**
- `limit` — default `50`, clamped to max `100`, must be ≥ 0
- `offset` — default `0`, must be ≥ 0

**Responses:**
- `200 OK` — `[DeadLetterTask]` (empty `[]` if none). Fields: `id`,
  `task_run_id`, `workflow_run_id`, `task_definition_id`, `terminal_status`,
  `failure_type`, `reason`, `final_attempt`, `worker_id`, `dead_lettered_at`,
  `created_at`
- `400 Bad Request` — invalid `limit`/`offset`
- `500 Internal Server Error` — fetch failure

---

## 7–9. Health & Probes

| Endpoint | Body (200) | Behavior |
|---|---|---|
| `GET /health` | `{"status":"ok"}` | Always 200 |
| `GET /healthz` | `{"status":"alive"}` | Liveness — always 200 while serving; no dependency check |
| `GET /readyz` | `{"status":"ready"}` | Readiness — pings DB (2s timeout); `503 {"status":"unready",…}` if DB unreachable or no readiness dependency configured |

Wire orchestrator liveness probes to `/healthz` and readiness probes to
`/readyz`.

## 10. `GET /metrics`

Prometheus exposition of `flowforge_*` metrics (registered only when the metrics
registry is initialized). A dedicated metrics server also serves `/metrics` on
`METRICS_ADDR`.

## Authentication Readiness

The API ships without authentication or authorization. For production:

- Terminate TLS and authenticate at an ingress/API gateway.
- Restrict network access to the API and to internal gRPC/metrics ports.
- Apply `RATE_LIMIT_RPS`/`RATE_LIMIT_BURST` and `MAX_REQUEST_BODY_BYTES`.
