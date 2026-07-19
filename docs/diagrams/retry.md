# Retry & Recovery Sequences

Focused view of retry, dead-lettering, and crash recovery. Full set in
[sequences.md](sequences.md).

## Retry with backoff

```mermaid
sequenceDiagram
  participant W as Worker
  participant DB as PostgreSQL
  participant S as Scheduler

  W->>DB: MarkTaskRunFailed
  alt attempts < max_retries
    note over DB: RETRY_WAIT; next_retry_at = now + exp backoff (cap 1h);<br/>emit RetryScheduled
    S->>DB: PromoteDueRetries -> READY (emit RetryPromoted)
  else attempts == max_retries
    note over DB: DLQ row; workflow FAILED;<br/>emit RetryExhausted + DLQCreated
  end
```

## Crash recovery

```mermaid
sequenceDiagram
  participant RL as Recovery loop
  participant R as Redis
  participant Rec as RecoveryService
  participant DB as PostgreSQL

  RL->>DB: GetActiveTaskRuns
  RL->>R: lease owner alive?
  alt owner dead / no lease
    RL->>Rec: RecoverTask(fencing_token, status)
    Rec->>DB: reset READY; attempt ORPHANED (WORKER_LOST); TaskRecovered
  end
```

See also: [Retry](sequences.md#4-retry),
[Worker Crash Recovery](sequences.md#6-worker-crash-recovery),
[DLQ Flow](sequences.md#7-dlq-flow).
