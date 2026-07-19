# Retry & Dead-Letter Flow

How failed tasks are retried with backoff, and what happens when retries run out.

## Retry with backoff

A failed task is not retried inline. It transitions to `RETRY_WAIT` with a
future `next_retry_at`, and the scheduler promotes it back to `READY` once the
backoff has elapsed. This keeps workers free instead of blocking on `sleep`.

```mermaid
sequenceDiagram
  participant W as Worker
  participant DB as PostgreSQL
  participant S as Scheduler

  W->>DB: MarkTaskRunFailed
  alt attempts < max_retries
    note over DB: RETRY_WAIT; next_retry_at = now + exp backoff (cap 1h);<br/>emit RetryScheduled
    S->>DB: PromoteDueRetries (RETRY_WAIT -> READY where next_retry_at <= now)
    note over DB: emit RetryPromoted
  else attempts == max_retries
    note over DB: insert dead_letter_tasks row; task terminal FAILED;<br/>workflow FAILED; emit RetryExhausted + DLQCreated
  end
```

## Dead-letter queue

When a task exhausts its retries it is written to `dead_letter_tasks` and the
workflow is marked `FAILED`. The DLQ is queryable through the API for
inspection.

```mermaid
sequenceDiagram
  participant P as Worker
  participant DB as PostgreSQL
  participant API
  participant C as Operator

  P->>DB: MarkTaskRunFailed (attempts == max_retries)
  note over DB: insert dead_letter_tasks row;<br/>task terminal FAILED; workflow FAILED;<br/>emit RetryExhausted + DLQCreated
  C->>API: GET /api/v1/dead-letter?limit&offset
  API->>DB: GetDeadLetterTasks(limit, offset)
  DB-->>API: []DeadLetterTask
  API-->>C: 200 OK
```

See also: [recovery.md](recovery.md) for crash recovery (a different failure
mode — the task never *failed*, its worker disappeared).
