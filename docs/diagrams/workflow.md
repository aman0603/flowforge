# Workflow Sequence

The end-to-end lifecycle of a workflow, from creation to completion. This is a
focused view; the complete set of sequence diagrams lives in
[sequences.md](sequences.md).

```mermaid
sequenceDiagram
  participant C as Client
  participant API
  participant DB as PostgreSQL
  participant W as Worker
  participant S as Scheduler
  participant Pub as Publisher
  participant K as Kafka

  C->>API: POST /api/v1/workflows (DAG)
  API->>DB: CreateWorkflowDefinition
  C->>API: POST /runs
  API->>DB: CreateWorkflowRun (root->READY; WorkflowStarted outbox)

  loop until all tasks terminal
    W->>S: ClaimTasks
    S->>DB: ClaimReadyTasksBatch (SKIP LOCKED, fencing++)
    W->>DB: StartTaskRun -> execute -> MarkTaskRunCompleted
    note over DB: unlock children READY; emit Task* events
  end
  note over DB: all tasks COMPLETED -> workflow COMPLETED (WorkflowCompleted)

  Pub->>DB: poll outbox
  Pub->>K: publish events (keyed by workflow_run_id)
  C->>API: GET /api/v1/runs/{id}/history
```

Related detailed flows:

- [Task Scheduling](sequences.md#2-task-scheduling)
- [Task Execution](sequences.md#3-task-execution)
- [Transactional Outbox](sequences.md#8-transactional-outbox)
