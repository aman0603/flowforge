package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/repository"
)

// Repository defines the subset of repository methods required by the worker.
type Repository interface {
	ClaimNextReadyTask(ctx context.Context, workerID string) (*model.ClaimedTask, error)
	StartTaskRun(ctx context.Context, taskRunID string, workerID string, fencingToken ...int64) error
	MarkTaskRunCompleted(ctx context.Context, taskRunID string, workerID string, output json.RawMessage, fencingToken ...int64) error
	MarkTaskRunFailed(ctx context.Context, taskRunID string, workerID string, errMsg string, isTimeout bool, fencingToken ...int64) error
	GetActiveTaskRuns(ctx context.Context) ([]*model.TaskRun, error)
	RecoverRunningTask(ctx context.Context, taskRunID string, fencingToken int64) (bool, error)
	RecoverClaimedTask(ctx context.Context, taskRunID string, fencingToken int64) (bool, error)
	PromoteDueRetries(ctx context.Context) (int64, error)
}

// Worker polls, claims, executes, and transitions task runs.
type Worker struct {
	id                  string
	repo                Repository
	executors           map[string]Executor
	pollInterval        time.Duration
	claimedStaleTimeout time.Duration
	runningStaleTimeout time.Duration
	recoveryInterval    time.Duration
	coord               Coordinator
	hbInterval          time.Duration
	hbTTL               time.Duration
	leaseTTL            time.Duration
	leaseRenewInterval  time.Duration
}

// New creates a new Worker instance.
func New(
	id string,
	repo Repository,
	executors map[string]Executor,
	pollInterval time.Duration,
	claimedStaleTimeout time.Duration,
	runningStaleTimeout time.Duration,
	recoveryInterval time.Duration,
	coord Coordinator,
	hbInterval time.Duration,
	hbTTL time.Duration,
	leaseTTL time.Duration,
	leaseRenewInterval time.Duration,
) *Worker {
	if pollInterval <= 0 {
		pollInterval = 1 * time.Second
	}
	if claimedStaleTimeout <= 0 {
		claimedStaleTimeout = 30 * time.Second
	}
	if runningStaleTimeout <= 0 {
		runningStaleTimeout = 5 * time.Minute
	}
	if recoveryInterval <= 0 {
		recoveryInterval = 30 * time.Second
	}
	if hbInterval <= 0 {
		hbInterval = 1 * time.Second
	}
	if hbTTL <= 0 {
		hbTTL = 3 * time.Second
	}
	if leaseTTL <= 0 {
		leaseTTL = 5 * time.Second
	}
	if leaseRenewInterval <= 0 {
		leaseRenewInterval = 1500 * time.Millisecond
	}
	return &Worker{
		id:                  id,
		repo:                repo,
		executors:           executors,
		pollInterval:        pollInterval,
		claimedStaleTimeout: claimedStaleTimeout,
		runningStaleTimeout: runningStaleTimeout,
		recoveryInterval:    recoveryInterval,
		coord:               coord,
		hbInterval:          hbInterval,
		hbTTL:               hbTTL,
		leaseTTL:            leaseTTL,
		leaseRenewInterval:  leaseRenewInterval,
	}
}

// Run starts the polling loop, blocking until the context is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	log.Printf("[worker-%s] Starting worker loop (pollInterval=%v)...", w.id, w.pollInterval)

	// 1. Register worker heartbeat
	if err := w.coord.RegisterWorker(ctx, w.id, w.hbTTL); err != nil {
		return fmt.Errorf("failed to register worker heartbeat at startup: %w", err)
	}

	// 2. Start background heartbeat loop
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer func() {
		hbCancel()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = w.coord.DeregisterWorker(cleanupCtx, w.id)
		cancel()
	}()

	go func() {
		ticker := time.NewTicker(w.hbInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(hbCtx, 2*time.Second)
				if err := w.coord.HeartbeatWorker(renewCtx, w.id, w.hbTTL); err != nil {
					log.Printf("[worker-%s] Heartbeat failed: %v", w.id, err)
				}
				cancel()
			}
		}
	}()

	// 3. Start background recovery loop
	go w.startRecoveryLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker-%s] Shutdown signal received, exiting gracefully...", w.id)
			return nil
		default:
		}

		processed, err := w.runIteration(ctx)
		if err != nil {
			log.Printf("[worker-%s] Worker loop stopped due to error: %v", w.id, err)
			return err
		}

		// If we processed a task, poll again immediately.
		// Otherwise, wait pollInterval before querying again.
		if !processed {
			timer := time.NewTimer(w.pollInterval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				log.Printf("[worker-%s] Shutdown signal received during poll sleep, exiting gracefully...", w.id)
				return nil
			}
		}
	}
}

func (w *Worker) startRecoveryLoop(ctx context.Context) {
	log.Printf("[worker-%s] Starting periodic stale task recovery loop (interval=%v, claimedTimeout=%v, runningTimeout=%v)...",
		w.id, w.recoveryInterval, w.claimedStaleTimeout, w.runningStaleTimeout)

	ticker := time.NewTicker(w.recoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker-%s] Stale task recovery loop stopped.", w.id)
			return
		case <-ticker.C:
			w.runRecoveryIteration(ctx)
		}
	}
}

func (w *Worker) runRecoveryIteration(ctx context.Context) {
	active, err := w.repo.GetActiveTaskRuns(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.Printf("[worker-%s] ERROR fetching active task runs for recovery: %v", w.id, err)
		}
		return
	}

	var claimedRecovered, runningRecovered int64
	now := time.Now()

	for _, tr := range active {
		if tr.Status == "CLAIMED" {
			if tr.ClaimedAt != nil && now.Sub(*tr.ClaimedAt) >= w.claimedStaleTimeout {
				// Lease check
				hasLease := false
				leaseWorker, _, err := w.coord.GetTaskLease(ctx, tr.ID)
				if err == nil && leaseWorker != "" {
					alive, err := w.coord.IsWorkerAlive(ctx, leaseWorker)
					if err == nil && alive {
						hasLease = true
					}
				}
				if !hasLease {
					recovered, err := w.repo.RecoverClaimedTask(ctx, tr.ID, tr.FencingToken)
					if err != nil {
						log.Printf("[worker-%s] ERROR recovering CLAIMED task %s: %v", w.id, tr.ID, err)
					} else if recovered {
						claimedRecovered++
					}
				}
			}
		} else if tr.Status == "RUNNING" {
			if tr.StartedAt != nil && now.Sub(*tr.StartedAt) >= w.runningStaleTimeout {
				// Lease check
				hasLease := false
				leaseWorker, leaseToken, err := w.coord.GetTaskLease(ctx, tr.ID)
				if err == nil && leaseWorker != "" && leaseToken == tr.FencingToken {
					alive, err := w.coord.IsWorkerAlive(ctx, leaseWorker)
					if err == nil && alive {
						hasLease = true
					}
				}
				if !hasLease {
					recovered, err := w.repo.RecoverRunningTask(ctx, tr.ID, tr.FencingToken)
					if err != nil {
						log.Printf("[worker-%s] ERROR recovering RUNNING task %s: %v", w.id, tr.ID, err)
					} else if recovered {
						runningRecovered++
					}
				}
			}
		}
	}

	if claimedRecovered > 0 || runningRecovered > 0 {
		log.Printf("[worker-%s] Recovery run complete. Recovered %d stale CLAIMED tasks, %d stale RUNNING tasks.",
			w.id, claimedRecovered, runningRecovered)
	}

	promoted, err := w.repo.PromoteDueRetries(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.Printf("[worker-%s] ERROR promoting due retries: %v", w.id, err)
		}
	} else if promoted > 0 {
		log.Printf("[worker-%s] Retry sweep complete. Promoted %d due task(s) from RETRY_WAIT to READY.", w.id, promoted)
	}
}

// runIteration executes a single worker iteration.
// Returns true if a task was evaluated, false if no tasks were available.
func (w *Worker) runIteration(ctx context.Context) (bool, error) {
	// 1. Claim a READY task
	task, err := w.repo.ClaimNextReadyTask(ctx, w.id)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false, nil
		}
		log.Printf("[worker-%s] Claim database error: %v", w.id, err)
		return false, fmt.Errorf("claim unexpected database error: %w", err)
	}

	if task == nil {
		return false, nil
	}

	log.Printf("[worker-%s] Task claimed: worker_id=%s, workflow_run_id=%s, task_run_id=%s, name=%s, task_type=%s, fencing_token=%d",
		w.id, w.id, task.WorkflowRunID, task.TaskRunID, task.Name, task.TaskType, task.FencingToken)

	// 1.5 Acquire Redis Task Lease
	acquired, err := w.coord.AcquireTaskLease(ctx, task.TaskRunID, w.id, task.FencingToken, w.leaseTTL)
	if err != nil {
		log.Printf("[worker-%s] Redis lease acquisition error: %v, task_run_id=%s", w.id, err, task.TaskRunID)
		return true, nil
	}
	if !acquired {
		log.Printf("[worker-%s] Failed to acquire Redis lease (contended), task_run_id=%s", w.id, task.TaskRunID)
		return true, nil
	}

	// 2. Start the CLAIMED task (transition CLAIMED -> RUNNING)
	err = w.repo.StartTaskRun(ctx, task.TaskRunID, w.id, task.FencingToken)
	if err != nil {
		// Clean up lease in Redis
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = w.coord.ReleaseTaskLease(cleanupCtx, task.TaskRunID, w.id, task.FencingToken)
		cancel()

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false, nil
		}
		if errors.Is(err, repository.ErrInvalidTaskTransition) {
			log.Printf("[worker-%s] StartTaskRun returned ErrInvalidTaskTransition. Task may have been hijacked or cancelled. worker_id=%s, task_run_id=%s",
				w.id, w.id, task.TaskRunID)
			return true, nil
		}
		log.Printf("[worker-%s] CRITICAL: StartTaskRun database error: %v, worker_id=%s, task_run_id=%s",
			w.id, err, w.id, task.TaskRunID)
		return false, fmt.Errorf("start task run unexpected database error: %w", err)
	}

	log.Printf("[worker-%s] Task started: worker_id=%s, workflow_run_id=%s, task_run_id=%s",
		w.id, w.id, task.WorkflowRunID, task.TaskRunID)

	// 2.5 Start lease renewal loop
	leaseCtx, leaseCancel := context.WithCancel(ctx)
	defer leaseCancel()

	go func() {
		ticker := time.NewTicker(w.leaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(leaseCtx, 2*time.Second)
				renewed, err := w.coord.RenewTaskLease(renewCtx, task.TaskRunID, w.id, task.FencingToken, w.leaseTTL)
				cancel()
				if err != nil || !renewed {
					log.Printf("[worker-%s] Lost lease ownership for task %s, cancelling execution...", w.id, task.TaskRunID)
					leaseCancel()
					return
				}
			}
		}
	}()

	// 3. Executor Dispatch
	exec, exists := w.executors[task.TaskType]
	if !exists {
		log.Printf("[worker-%s] Unsupported task type: %s, worker_id=%s, task_run_id=%s",
			w.id, task.TaskType, w.id, task.TaskRunID)

		if leaseCtx.Err() != nil {
			log.Printf("[worker-%s] Lease lost before persisting unsupported task type failure. Discarding state. task_run_id=%s", w.id, task.TaskRunID)
			return true, nil
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		errMsg := fmt.Sprintf("unsupported task type: %s", task.TaskType)
		if err := w.repo.MarkTaskRunFailed(cleanupCtx, task.TaskRunID, w.id, errMsg, false, task.FencingToken); err != nil {
			if errors.Is(err, repository.ErrInvalidTaskTransition) {
				log.Printf("[worker-%s] MarkTaskRunFailed returned ErrInvalidTaskTransition: %v", w.id, err)
				return false, fmt.Errorf("fail transition state mismatch: %w", err)
			}
			log.Printf("[worker-%s] MarkTaskRunFailed database error: %v", w.id, err)
			return false, fmt.Errorf("fail transition unexpected database error: %w", err)
		}
		return true, nil
	}

	// 4. Execute the task logic
	timeout := time.Duration(task.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	execCtx, execCancel := context.WithTimeout(leaseCtx, timeout)
	output, execErr := exec.Execute(execCtx, task)
	execCancel()

	if execErr != nil {
		// Distinguish worker shutdown context cancellation from task execution timeout
		if ctx.Err() != nil {
			log.Printf("[worker-%s] Executor interrupted by worker context cancellation. Leaving task RUNNING. worker_id=%s, task_run_id=%s",
				w.id, w.id, task.TaskRunID)
			return false, nil
		}

		// Distinguish lease loss from normal execution failures/timeouts
		if leaseCtx.Err() != nil {
			log.Printf("[worker-%s] Executor cancelled due to lease loss. Discarding state. worker_id=%s, task_run_id=%s",
				w.id, w.id, task.TaskRunID)
			return true, nil
		}

		isTimeout := errors.Is(execErr, context.DeadlineExceeded) || (execCtx.Err() == context.DeadlineExceeded)
		var errMsg string
		if isTimeout {
			errMsg = "execution timeout: " + execErr.Error()
			log.Printf("[worker-%s] Task execution timed out: worker_id=%s, task_run_id=%s",
				w.id, w.id, task.TaskRunID)
		} else {
			errMsg = execErr.Error()
			log.Printf("[worker-%s] Task execution failed: %v, worker_id=%s, task_run_id=%s",
				w.id, execErr, w.id, task.TaskRunID)
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := w.repo.MarkTaskRunFailed(cleanupCtx, task.TaskRunID, w.id, errMsg, isTimeout, task.FencingToken); err != nil {
			if errors.Is(err, repository.ErrInvalidTaskTransition) {
				log.Printf("[worker-%s] MarkTaskRunFailed returned ErrInvalidTaskTransition: %v", w.id, err)
				return false, fmt.Errorf("fail transition state mismatch: %w", err)
			}
			log.Printf("[worker-%s] MarkTaskRunFailed database error: %v", w.id, err)
			return false, fmt.Errorf("fail transition unexpected database error: %w", err)
		}

		log.Printf("[worker-%s] Task failure processed: worker_id=%s, task_run_id=%s, isTimeout=%t",
			w.id, w.id, task.TaskRunID, isTimeout)
		return true, nil
	}

	// 5. Persist COMPLETED status
	if leaseCtx.Err() != nil {
		log.Printf("[worker-%s] Lease lost before persisting completion. Discarding state. task_run_id=%s", w.id, task.TaskRunID)
		return true, nil
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.repo.MarkTaskRunCompleted(cleanupCtx, task.TaskRunID, w.id, output, task.FencingToken); err != nil {
		if errors.Is(err, repository.ErrInvalidTaskTransition) {
			log.Printf("[worker-%s] MarkTaskRunCompleted returned ErrInvalidTaskTransition: %v", w.id, err)
			return false, fmt.Errorf("complete transition state mismatch: %w", err)
		}
		log.Printf("[worker-%s] MarkTaskRunCompleted database error: %v", w.id, err)
		return false, fmt.Errorf("complete transition unexpected database error: %w", err)
	}

	// Release lease in Redis
	_ = w.coord.ReleaseTaskLease(cleanupCtx, task.TaskRunID, w.id, task.FencingToken)

	log.Printf("[worker-%s] Task completed: worker_id=%s, workflow_run_id=%s, task_run_id=%s",
		w.id, w.id, task.WorkflowRunID, task.TaskRunID)
	return true, nil
}
