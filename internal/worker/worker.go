package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/repository"
)

// Repository defines the subset of repository methods required by the worker.
type Repository interface {
	ClaimNextReadyTask(ctx context.Context, workerID string) (*model.ClaimedTask, error)
	ClaimReadyTasksBatch(ctx context.Context, workerID string, limit int) ([]*model.ClaimedTask, error)
	StartTaskRun(ctx context.Context, taskRunID string, workerID string, fencingToken ...int64) error
	MarkTaskRunCompleted(ctx context.Context, taskRunID string, workerID string, output json.RawMessage, fencingToken ...int64) error
	MarkTaskRunFailed(ctx context.Context, taskRunID string, workerID string, errMsg string, isTimeout bool, fencingToken ...int64) error
	GetActiveTaskRuns(ctx context.Context) ([]*model.TaskRun, error)
	RecoverRunningTask(ctx context.Context, taskRunID string, fencingToken int64) (bool, error)
	RecoverClaimedTask(ctx context.Context, taskRunID string, fencingToken int64) (bool, error)
	PromoteDueRetries(ctx context.Context) (int64, error)
}

// SchedulerCounters tracks scheduler activities and executions.
type SchedulerCounters struct {
	ActiveExecutions int64 `json:"active_executions"`
	QueuedTasks      int64 `json:"queued_tasks"`
	TotalClaimed     int64 `json:"total_claimed"`
	TotalStarted     int64 `json:"total_started"`
	TotalCompleted   int64 `json:"total_completed"`
	TotalFailed      int64 `json:"total_failed"`
	TotalTimedOut    int64 `json:"total_timed_out"`
	TotalPanics      int64 `json:"total_panics"`
	TotalLeaseLosses int64 `json:"total_lease_losses"`
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

	// Worker Pool & Queue settings
	poolSize      int
	queueCapacity int
	batchSize     int
	shutdownGrace time.Duration

	taskQueue    chan *model.ClaimedTask
	shutdownChan chan struct{}
	activeWG     sync.WaitGroup
	errChan      chan error

	// Parent context for all task executions to coordinate graceful timeout cancellations
	execParentCtx context.Context
	execCancel    context.CancelFunc

	counters *SchedulerCounters
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
	poolSize int,
	queueCapacity int,
	batchSize int,
	shutdownGrace time.Duration,
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

	if poolSize <= 0 {
		poolSize = 16
	}
	if queueCapacity < poolSize {
		queueCapacity = poolSize * 2
	}
	if batchSize <= 0 {
		batchSize = 8
	}
	if shutdownGrace <= 0 {
		shutdownGrace = 10 * time.Second
	}

	execParentCtx, execCancel := context.WithCancel(context.Background())

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

		poolSize:      poolSize,
		queueCapacity: queueCapacity,
		batchSize:     batchSize,
		shutdownGrace: shutdownGrace,

		taskQueue:    make(chan *model.ClaimedTask, queueCapacity),
		shutdownChan: make(chan struct{}),

		execParentCtx: execParentCtx,
		execCancel:    execCancel,

		counters: &SchedulerCounters{},
	}
}

// GetCounters returns a thread-safe snapshot of the scheduler/execution counters.
func (w *Worker) GetCounters() SchedulerCounters {
	return SchedulerCounters{
		ActiveExecutions: atomic.LoadInt64(&w.counters.ActiveExecutions),
		QueuedTasks:      atomic.LoadInt64(&w.counters.QueuedTasks),
		TotalClaimed:     atomic.LoadInt64(&w.counters.TotalClaimed),
		TotalStarted:     atomic.LoadInt64(&w.counters.TotalStarted),
		TotalCompleted:   atomic.LoadInt64(&w.counters.TotalCompleted),
		TotalFailed:      atomic.LoadInt64(&w.counters.TotalFailed),
		TotalTimedOut:    atomic.LoadInt64(&w.counters.TotalTimedOut),
		TotalPanics:      atomic.LoadInt64(&w.counters.TotalPanics),
		TotalLeaseLosses: atomic.LoadInt64(&w.counters.TotalLeaseLosses),
	}
}

// Run starts the worker process and its worker pool.
func (w *Worker) Run(ctx context.Context) error {
	log.Printf("[worker-%s] Starting worker loop (poolSize=%d, queueCapacity=%d, batchSize=%d)...", 
		w.id, w.poolSize, w.queueCapacity, w.batchSize)

	// Initialize the error channel
	w.errChan = make(chan error, w.poolSize+2)

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

	// 4. Start the execution goroutine worker pool
	for i := 0; i < w.poolSize; i++ {
		go w.workerLoop(ctx)
	}

	// 5. Start the scheduler/claiming loop synchronously in the main thread
	// It blocks until context is cancelled or a database claim/execution error is encountered.
	runErr := w.schedulerLoop(ctx)

	// 6. Graceful shutdown drainage
	log.Printf("[worker-%s] Scheduler stopping. Draining queue and waiting for active tasks...", w.id)

	// Signal workers to reject any unstarted tasks
	close(w.shutdownChan)

	// Close task queue so worker goroutines stop accepting new tasks once the queue is empty
	close(w.taskQueue)

	// Wait for active tasks or grace period expiration
	graceTimer := time.NewTimer(w.shutdownGrace)
	defer graceTimer.Stop()

	doneChan := make(chan struct{})
	go func() {
		w.activeWG.Wait()
		close(doneChan)
	}()

	select {
	case <-doneChan:
		log.Printf("[worker-%s] All active tasks completed successfully during grace period.", w.id)
	case <-graceTimer.C:
		log.Printf("[worker-%s] Grace period expired. Cancelling remaining active task contexts...", w.id)
		w.execCancel() // cancels all active tasks
		<-doneChan
		log.Printf("[worker-%s] Remaining tasks cancelled and cleaned up.", w.id)
	}

	return runErr
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

func (w *Worker) schedulerLoop(ctx context.Context) error {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		// First check if an execution error has aborted us
		select {
		case err := <-w.errChan:
			return err
		default:
		}

		// Calculate available capacity
		active := atomic.LoadInt64(&w.counters.ActiveExecutions)
		queued := atomic.LoadInt64(&w.counters.QueuedTasks)
		capacity := int64(w.poolSize) - (active + queued)

		if capacity > 0 {
			limit := int(capacity)
			if limit > w.batchSize {
				limit = w.batchSize
			}

			claimed, err := w.repo.ClaimReadyTasksBatch(ctx, w.id, limit)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					log.Printf("[worker-%s] Scheduler batch claim database error: %v", w.id, err)
					return err
				}
			} else if len(claimed) > 0 {
				log.Printf("[worker-%s] Scheduler claimed batch of %d task(s)", w.id, len(claimed))
				for _, task := range claimed {
					// Increment QueuedTasks counter before pushing to avoid race condition
					atomic.AddInt64(&w.counters.QueuedTasks, 1)
					atomic.AddInt64(&w.counters.TotalClaimed, 1)

					select {
					case w.taskQueue <- task:
						// Dispatched successfully
					case <-ctx.Done():
						// Scheduler cancelled. Return claimed task to READY.
						w.returnClaimedTaskToReady(task)
					case err := <-w.errChan:
						// Execution error during dispatch unblocks scheduler
						w.returnClaimedTaskToReady(task)
						return err
					}
				}
				// Poll again immediately if we successfully claimed tasks
				continue
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case err := <-w.errChan:
			return err
		case <-ticker.C:
		}
	}
}

func (w *Worker) workerLoop(ctx context.Context) {
	for task := range w.taskQueue {
		// Decrement queued task count
		atomic.AddInt64(&w.counters.QueuedTasks, -1)

		// Check if we should execute or return the task
		select {
		case <-w.shutdownChan:
			w.returnClaimedTaskToReady(task)
			continue
		default:
		}

		// Execute task
		w.activeWG.Add(1)
		atomic.AddInt64(&w.counters.ActiveExecutions, 1)

		w.executeTask(ctx, task)

		atomic.AddInt64(&w.counters.ActiveExecutions, -1)
		w.activeWG.Done()
	}
}

func (w *Worker) executeTask(ctx context.Context, task *model.ClaimedTask) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[worker-%s] PANIC recovered in executor for task %s: %v", w.id, task.TaskRunID, r)
			atomic.AddInt64(&w.counters.TotalPanics, 1)

			// Treat panic as execution failure
			panicErr := fmt.Errorf("executor panic: %v", r)
			w.handleTaskFailure(task, panicErr, false)
		}
	}()

	w.runTask(ctx, task)
}

func (w *Worker) runTask(ctx context.Context, task *model.ClaimedTask) {
	// 1. Acquire Redis Task Lease
	acquired, err := w.coord.AcquireTaskLease(w.execParentCtx, task.TaskRunID, w.id, task.FencingToken, w.leaseTTL)
	if err != nil {
		log.Printf("[worker-%s] Redis lease acquisition error: %v, task_run_id=%s", w.id, err, task.TaskRunID)
		w.returnClaimedTaskToReady(task)
		return
	}
	if !acquired {
		log.Printf("[worker-%s] Failed to acquire Redis lease (contended), task_run_id=%s", w.id, task.TaskRunID)
		w.returnClaimedTaskToReady(task)
		return
	}

	// 2. Start the CLAIMED task (transition CLAIMED -> RUNNING)
	err = w.repo.StartTaskRun(w.execParentCtx, task.TaskRunID, w.id, task.FencingToken)
	if err != nil {
		// Clean up lease in Redis
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = w.coord.ReleaseTaskLease(cleanupCtx, task.TaskRunID, w.id, task.FencingToken)
		cancel()

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if errors.Is(err, repository.ErrInvalidTaskTransition) {
			log.Printf("[worker-%s] StartTaskRun returned ErrInvalidTaskTransition. Task may have been hijacked or cancelled. worker_id=%s, task_run_id=%s",
				w.id, w.id, task.TaskRunID)
			return
		}
		log.Printf("[worker-%s] CRITICAL: StartTaskRun database error: %v, worker_id=%s, task_run_id=%s",
			w.id, err, w.id, task.TaskRunID)
		return
	}

	log.Printf("[worker-%s] Task started: worker_id=%s, workflow_run_id=%s, task_run_id=%s",
		w.id, w.id, task.WorkflowRunID, task.TaskRunID)
	atomic.AddInt64(&w.counters.TotalStarted, 1)

	// 3. Start lease renewal loop
	leaseCtx, leaseCancel := context.WithCancel(w.execParentCtx)
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
					atomic.AddInt64(&w.counters.TotalLeaseLosses, 1)
					leaseCancel()
					return
				}
			}
		}
	}()

	// 4. Executor Dispatch
	exec, exists := w.executors[task.TaskType]
	if !exists {
		log.Printf("[worker-%s] Unsupported task type: %s, worker_id=%s, task_run_id=%s",
			w.id, task.TaskType, w.id, task.TaskRunID)

		if leaseCtx.Err() != nil {
			log.Printf("[worker-%s] Lease lost before persisting unsupported task type failure. Discarding state. task_run_id=%s", w.id, task.TaskRunID)
			return
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		errMsg := fmt.Sprintf("unsupported task type: %s", task.TaskType)
		w.saveTaskFailure(cleanupCtx, task, errMsg, false)
		return
	}

	// 5. Execute the task logic
	timeout := time.Duration(task.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	execCtx, execCancel := context.WithTimeout(leaseCtx, timeout)
	output, execErr := exec.Execute(execCtx, task)
	execCancel()

	if execErr != nil {
		// Distinguish worker shutdown context cancellation from task execution timeout
		if ctx.Err() != nil || w.execParentCtx.Err() != nil {
			log.Printf("[worker-%s] Executor interrupted by worker context cancellation. Leaving task RUNNING. worker_id=%s, task_run_id=%s",
				w.id, w.id, task.TaskRunID)
			return
		}

		// Distinguish lease loss from normal execution failures/timeouts
		if leaseCtx.Err() != nil {
			log.Printf("[worker-%s] Executor cancelled due to lease loss. Discarding state. worker_id=%s, task_run_id=%s",
				w.id, w.id, task.TaskRunID)
			return
		}

		isTimeout := errors.Is(execErr, context.DeadlineExceeded) || (execCtx.Err() == context.DeadlineExceeded)
		var errMsg string
		if isTimeout {
			errMsg = "execution timeout: " + execErr.Error()
			atomic.AddInt64(&w.counters.TotalTimedOut, 1)
			log.Printf("[worker-%s] Task execution timed out: worker_id=%s, task_run_id=%s",
				w.id, w.id, task.TaskRunID)
		} else {
			errMsg = execErr.Error()
			log.Printf("[worker-%s] Task execution failed: %v, worker_id=%s, task_run_id=%s",
				w.id, execErr, w.id, task.TaskRunID)
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		w.saveTaskFailure(cleanupCtx, task, errMsg, isTimeout)
		return
	}

	// 6. Persist COMPLETED status
	if leaseCtx.Err() != nil {
		log.Printf("[worker-%s] Lease lost before persisting completion. Discarding state. task_run_id=%s", w.id, task.TaskRunID)
		return
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.repo.MarkTaskRunCompleted(cleanupCtx, task.TaskRunID, w.id, output, task.FencingToken); err != nil {
		if errors.Is(err, repository.ErrInvalidTaskTransition) {
			log.Printf("[worker-%s] MarkTaskRunCompleted returned ErrInvalidTaskTransition: %v", w.id, err)
			return
		}
		log.Printf("[worker-%s] MarkTaskRunCompleted database error: %v", w.id, err)
		w.sendErr(err)
		return
	}

	// Release lease in Redis
	_ = w.coord.ReleaseTaskLease(cleanupCtx, task.TaskRunID, w.id, task.FencingToken)

	atomic.AddInt64(&w.counters.TotalCompleted, 1)
	log.Printf("[worker-%s] Task completed: worker_id=%s, workflow_run_id=%s, task_run_id=%s",
		w.id, w.id, task.WorkflowRunID, task.TaskRunID)
}

func (w *Worker) handleTaskFailure(task *model.ClaimedTask, err error, isTimeout bool) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.saveTaskFailure(cleanupCtx, task, err.Error(), isTimeout)
}

func (w *Worker) saveTaskFailure(ctx context.Context, task *model.ClaimedTask, errMsg string, isTimeout bool) {
	atomic.AddInt64(&w.counters.TotalFailed, 1)

	if err := w.repo.MarkTaskRunFailed(ctx, task.TaskRunID, w.id, errMsg, isTimeout, task.FencingToken); err != nil {
		if errors.Is(err, repository.ErrInvalidTaskTransition) {
			log.Printf("[worker-%s] MarkTaskRunFailed returned ErrInvalidTaskTransition: %v", w.id, err)
			return
		}
		log.Printf("[worker-%s] MarkTaskRunFailed database error: %v", w.id, err)
		w.sendErr(err)
		return
	}

	// Release lease in Redis
	_ = w.coord.ReleaseTaskLease(ctx, task.TaskRunID, w.id, task.FencingToken)

	log.Printf("[worker-%s] Task failure processed: worker_id=%s, task_run_id=%s, isTimeout=%t",
		w.id, w.id, task.TaskRunID, isTimeout)
}

func (w *Worker) returnClaimedTaskToReady(task *model.ClaimedTask) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := w.repo.RecoverClaimedTask(cleanupCtx, task.TaskRunID, task.FencingToken)
	if err != nil {
		log.Printf("[worker-%s] ERROR returning claimed task %s to READY: %v", w.id, task.TaskRunID, err)
	} else {
		log.Printf("[worker-%s] Returned claimed task %s to READY.", w.id, task.TaskRunID)
	}
}

func (w *Worker) sendErr(err error) {
	select {
	case w.errChan <- err:
	default:
	}
}
