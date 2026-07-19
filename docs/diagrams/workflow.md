# Workflow Execution

The end-to-end lifecycle of a workflow, from creation to completion.

## End-to-end lifecycle

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

## Task scheduling (claiming)

Workers pull work; the scheduler never pushes. Claiming is a single SQL
statement using `FOR UPDATE SKIP LOCKED`, so many workers can claim
concurrently without a distributed lock in the hot path.

```mermaid
sequenceDiagram
  participant W as Worker
  participant S as Scheduler (local or gRPC)
  participant DB as PostgreSQL

  loop poll (WORKER_POLL_INTERVAL)
    W->>W: available = pool - active - queued
    alt available > 0
      W->>S: ClaimTasks(worker_id, capacity)
      S->>DB: ClaimReadyTasksBatch (FOR UPDATE SKIP LOCKED)
      note over DB: READY -> CLAIMED; set worker_id, claimed_at;<br/>fencing_token++
      DB-->>S: claimed tasks
      S-->>W: []ClaimedTask
      W->>W: enqueue to bounded task queue
    else saturated
      W->>W: pause claiming (backpressure)
    end
  end
```

## Task execution

Every state-changing write is guarded by `worker_id` + `fencing_token`, so a
slow or resurrected worker cannot mutate a task that has been reclaimed.

```mermaid
sequenceDiagram
  participant P as Worker pool goroutine
  participant R as Redis
  participant DB as PostgreSQL
  participant E as Executor

  P->>R: AcquireTaskLease(task_id, ttl)
  P->>DB: StartTaskRun (CLAIMED->RUNNING, guarded by worker_id+fencing_token)
  note over DB: insert task_attempt (RUNNING); emit TaskStarted
  P->>E: Execute(ctx, task)
  alt success
    E-->>P: output
    P->>DB: MarkTaskRunCompleted (RUNNING->COMPLETED)
    note over DB: unlock children PENDING->READY;<br/>if all done: workflow COMPLETED; emit TaskCompleted
  else failure / panic (recovered) / timeout
    E-->>P: error
    P->>DB: MarkTaskRunFailed
  end
  P->>R: ReleaseTaskLease(task_id)
```

See also: [retry.md](retry.md), [recovery.md](recovery.md),
[event-flow.md](event-flow.md).
