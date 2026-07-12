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
	StartTaskRun(ctx context.Context, taskRunID string, workerID string) error
	MarkTaskRunCompleted(ctx context.Context, taskRunID string, workerID string, output json.RawMessage) error
	MarkTaskRunFailed(ctx context.Context, taskRunID string, workerID string, errMsg string) error
}

// Worker polls, claims, executes, and transitions task runs.
type Worker struct {
	id           string
	repo         Repository
	executors    map[string]Executor
	pollInterval time.Duration
}

// New creates a new Worker instance.
func New(id string, repo Repository, executors map[string]Executor, pollInterval time.Duration) *Worker {
	if pollInterval <= 0 {
		pollInterval = 1 * time.Second
	}
	return &Worker{
		id:           id,
		repo:         repo,
		executors:    executors,
		pollInterval: pollInterval,
	}
}

// Run starts the polling loop, blocking until the context is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	log.Printf("[worker-%s] Starting worker loop (pollInterval=%v)...", w.id, w.pollInterval)

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

		// If we processed a task (or resolved an state mismatch), poll again immediately.
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

	log.Printf("[worker-%s] Task claimed: worker_id=%s, workflow_run_id=%s, task_run_id=%s, name=%s, task_type=%s",
		w.id, w.id, task.WorkflowRunID, task.TaskRunID, task.Name, task.TaskType)

	// 2. Start the CLAIMED task (transition CLAIMED -> RUNNING)
	err = w.repo.StartTaskRun(ctx, task.TaskRunID, w.id)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false, nil
		}
		if errors.Is(err, repository.ErrInvalidTaskTransition) {
			log.Printf("[worker-%s] Warning: StartTaskRun returned ErrInvalidTaskTransition. Task may have been hijacked or cancelled. worker_id=%s, workflow_run_id=%s, task_run_id=%s",
				w.id, w.id, task.WorkflowRunID, task.TaskRunID)
			return true, nil
		}
		log.Printf("[worker-%s] CRITICAL: StartTaskRun database error: %v, worker_id=%s, workflow_run_id=%s, task_run_id=%s",
			w.id, err, w.id, task.WorkflowRunID, task.TaskRunID)
		return false, fmt.Errorf("start task run unexpected database error: %w", err)
	}

	log.Printf("[worker-%s] Task started: worker_id=%s, workflow_run_id=%s, task_run_id=%s",
		w.id, w.id, task.WorkflowRunID, task.TaskRunID)

	// 3. Executor Dispatch
	exec, exists := w.executors[task.TaskType]
	if !exists {
		log.Printf("[worker-%s] Unsupported task type: %s, worker_id=%s, workflow_run_id=%s, task_run_id=%s",
			w.id, task.TaskType, w.id, task.WorkflowRunID, task.TaskRunID)

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		errMsg := fmt.Sprintf("unsupported task type: %s", task.TaskType)
		if err := w.repo.MarkTaskRunFailed(cleanupCtx, task.TaskRunID, w.id, errMsg); err != nil {
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
	output, execErr := exec.Execute(ctx, task)
	if execErr != nil {
		if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
			log.Printf("[worker-%s] Executor interrupted by worker context cancellation. Leaving task RUNNING. worker_id=%s, workflow_run_id=%s, task_run_id=%s",
				w.id, w.id, task.WorkflowRunID, task.TaskRunID)
			return false, nil
		}

		log.Printf("[worker-%s] Task execution failed: %v, worker_id=%s, workflow_run_id=%s, task_run_id=%s",
			w.id, execErr, w.id, task.WorkflowRunID, task.TaskRunID)

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := w.repo.MarkTaskRunFailed(cleanupCtx, task.TaskRunID, w.id, execErr.Error()); err != nil {
			if errors.Is(err, repository.ErrInvalidTaskTransition) {
				log.Printf("[worker-%s] MarkTaskRunFailed returned ErrInvalidTaskTransition: %v", w.id, err)
				return false, fmt.Errorf("fail transition state mismatch: %w", err)
			}
			log.Printf("[worker-%s] MarkTaskRunFailed database error: %v", w.id, err)
			return false, fmt.Errorf("fail transition unexpected database error: %w", err)
		}

		log.Printf("[worker-%s] Task marked FAILED: worker_id=%s, workflow_run_id=%s, task_run_id=%s",
			w.id, w.id, task.WorkflowRunID, task.TaskRunID)
		return true, nil
	}

	// 5. Persist COMPLETED status
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.repo.MarkTaskRunCompleted(cleanupCtx, task.TaskRunID, w.id, output); err != nil {
		if errors.Is(err, repository.ErrInvalidTaskTransition) {
			log.Printf("[worker-%s] MarkTaskRunCompleted returned ErrInvalidTaskTransition: %v", w.id, err)
			return false, fmt.Errorf("complete transition state mismatch: %w", err)
		}
		log.Printf("[worker-%s] MarkTaskRunCompleted database error: %v", w.id, err)
		return false, fmt.Errorf("complete transition unexpected database error: %w", err)
	}

	log.Printf("[worker-%s] Task completed: worker_id=%s, workflow_run_id=%s, task_run_id=%s",
		w.id, w.id, task.WorkflowRunID, task.TaskRunID)
	return true, nil
}
