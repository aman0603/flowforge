package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/repository"
)

type fakeRepository struct {
	mu             sync.Mutex
	claimFunc      func(ctx context.Context, workerID string) (*model.ClaimedTask, error)
	startFunc      func(ctx context.Context, taskRunID string, workerID string) error
	completeFunc   func(ctx context.Context, taskRunID string, workerID string, output json.RawMessage) error
	failFunc       func(ctx context.Context, taskRunID string, workerID string, errMsg string) error
	startedCalls   int
	completedCalls int
	failedCalls    int
}

func (f *fakeRepository) ClaimNextReadyTask(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimFunc != nil {
		return f.claimFunc(ctx, workerID)
	}
	return nil, nil
}

func (f *fakeRepository) StartTaskRun(ctx context.Context, taskRunID string, workerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startedCalls++
	if f.startFunc != nil {
		return f.startFunc(ctx, taskRunID, workerID)
	}
	return nil
}

func (f *fakeRepository) MarkTaskRunCompleted(ctx context.Context, taskRunID string, workerID string, output json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completedCalls++
	if f.completeFunc != nil {
		return f.completeFunc(ctx, taskRunID, workerID, output)
	}
	return nil
}

func (f *fakeRepository) MarkTaskRunFailed(ctx context.Context, taskRunID string, workerID string, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failedCalls++
	if f.failFunc != nil {
		return f.failFunc(ctx, taskRunID, workerID, errMsg)
	}
	return nil
}

type fakeExecutor struct {
	executeFunc func(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error)
}

func (f *fakeExecutor) Execute(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
	if f.executeFunc != nil {
		return f.executeFunc(ctx, task)
	}
	return json.RawMessage(`{"status":"completed"}`), nil
}

func TestWorkerOrchestration(t *testing.T) {
	t.Run("no task available, then context cancellation", func(t *testing.T) {
		repo := &fakeRepository{}
		executors := map[string]Executor{
			"SLEEP": &fakeExecutor{},
		}
		w := New("test-worker", repo, executors, 1*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()

		err := w.Run(ctx)
		if err != nil {
			t.Fatalf("expected nil error on clean shutdown, got: %v", err)
		}
	})

	t.Run("successful task lifecycle: claim -> start -> execute -> complete", func(t *testing.T) {
		var claimedTask = &model.ClaimedTask{
			TaskRunID:        "run-1",
			WorkflowRunID:    "wf-1",
			TaskDefinitionID: "def-1",
			Name:             "TaskA",
			TaskType:         "SLEEP",
			Config:           json.RawMessage(`{"duration_ms": 5}`),
		}

		calls := 0
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				calls++
				if calls == 1 {
					return claimedTask, nil
				}
				// Return nil on second call to let the loop poll empty next time
				return nil, nil
			},
		}

		exec := &fakeExecutor{
			executeFunc: func(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
				return json.RawMessage(`{"status":"completed"}`), nil
			},
		}

		w := New("test-worker", repo, map[string]Executor{"SLEEP": exec}, 1*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel context in a background routine after processing starts
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		err := w.Run(ctx)
		if err != nil {
			t.Fatalf("expected nil error on clean exit, got: %v", err)
		}

		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.startedCalls != 1 {
			t.Errorf("expected StartTaskRun to be called 1 time, got %d", repo.startedCalls)
		}
		if repo.completedCalls != 1 {
			t.Errorf("expected MarkTaskRunCompleted to be called 1 time, got %d", repo.completedCalls)
		}
		if repo.failedCalls != 0 {
			t.Errorf("expected MarkTaskRunFailed to not be called, got %d", repo.failedCalls)
		}
	})

	t.Run("executor error: claim -> start -> execute error -> fail", func(t *testing.T) {
		var claimedTask = &model.ClaimedTask{
			TaskRunID: "run-1",
			TaskType:  "SLEEP",
		}
		calls := 0
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				calls++
				if calls == 1 {
					return claimedTask, nil
				}
				return nil, nil
			},
		}

		exec := &fakeExecutor{
			executeFunc: func(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
				return nil, errors.New("execution error")
			},
		}

		w := New("test-worker", repo, map[string]Executor{"SLEEP": exec}, 1*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		err := w.Run(ctx)
		if err != nil {
			t.Fatalf("expected nil error, got: %v", err)
		}

		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.completedCalls != 0 {
			t.Errorf("expected completed to not be called, got %d", repo.completedCalls)
		}
		if repo.failedCalls != 1 {
			t.Errorf("expected fail to be called 1 time, got %d", repo.failedCalls)
		}
	})

	t.Run("unsupported task type -> fail", func(t *testing.T) {
		var claimedTask = &model.ClaimedTask{
			TaskRunID: "run-1",
			TaskType:  "HTTP", // No HTTP executor registered
		}
		calls := 0
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				calls++
				if calls == 1 {
					return claimedTask, nil
				}
				return nil, nil
			},
		}

		w := New("test-worker", repo, map[string]Executor{}, 1*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		err := w.Run(ctx)
		if err != nil {
			t.Fatalf("expected nil error, got: %v", err)
		}

		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.failedCalls != 1 {
			t.Errorf("expected fail to be called for unsupported type, got %d", repo.failedCalls)
		}
	})

	t.Run("StartTaskRun returns ErrInvalidTaskTransition -> continue polling", func(t *testing.T) {
		var claimedTask = &model.ClaimedTask{
			TaskRunID: "run-1",
			TaskType:  "SLEEP",
		}
		calls := 0
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				calls++
				if calls == 1 {
					return claimedTask, nil
				}
				return nil, nil
			},
			startFunc: func(ctx context.Context, taskRunID string, workerID string) error {
				return repository.ErrInvalidTaskTransition
			},
		}

		w := New("test-worker", repo, map[string]Executor{"SLEEP": &fakeExecutor{}}, 1*time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		err := w.Run(ctx)
		if err != nil {
			t.Fatalf("expected nil error, got: %v", err)
		}

		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.startedCalls != 1 {
			t.Errorf("expected StartTaskRun to be called once, got %d", repo.startedCalls)
		}
		if repo.completedCalls != 0 {
			t.Errorf("expected complete to not be called, got %d", repo.completedCalls)
		}
	})

	t.Run("executor cancelled by worker context -> leave RUNNING and exit", func(t *testing.T) {
		var claimedTask = &model.ClaimedTask{
			TaskRunID: "run-1",
			TaskType:  "SLEEP",
		}
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				return claimedTask, nil
			},
		}

		exec := &fakeExecutor{
			executeFunc: func(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
				return nil, context.Canceled // Interrupted by shutdown context
			},
		}

		w := New("test-worker", repo, map[string]Executor{"SLEEP": exec}, 1*time.Millisecond)
		err := w.Run(context.Background()) // Note: context.Canceled returned by executor acts as cancellation check
		if err != nil {
			t.Fatalf("expected nil error on shutdown, got: %v", err)
		}

		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.completedCalls != 0 {
			t.Errorf("expected complete to not be called, got %d", repo.completedCalls)
		}
		if repo.failedCalls != 0 {
			t.Errorf("expected fail to not be called, got %d", repo.failedCalls)
		}
	})

	t.Run("claim database error -> worker stops", func(t *testing.T) {
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				return nil, errors.New("infrastructure database failure")
			},
		}
		w := New("test-worker", repo, map[string]Executor{"SLEEP": &fakeExecutor{}}, 1*time.Millisecond)
		err := w.Run(context.Background())
		if err == nil {
			t.Fatalf("expected error on claim DB error, got nil")
		}
	})

	t.Run("completion persistence error -> worker stops", func(t *testing.T) {
		var claimedTask = &model.ClaimedTask{
			TaskRunID: "run-1",
			TaskType:  "SLEEP",
		}
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				return claimedTask, nil
			},
			completeFunc: func(ctx context.Context, taskRunID string, workerID string, output json.RawMessage) error {
				return errors.New("completion persistence failure")
			},
		}
		w := New("test-worker", repo, map[string]Executor{"SLEEP": &fakeExecutor{}}, 1*time.Millisecond)
		err := w.Run(context.Background())
		if err == nil {
			t.Fatalf("expected error on completion DB failure, got nil")
		}
	})

	t.Run("failure persistence error -> worker stops", func(t *testing.T) {
		var claimedTask = &model.ClaimedTask{
			TaskRunID: "run-1",
			TaskType:  "SLEEP",
		}
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				return claimedTask, nil
			},
			failFunc: func(ctx context.Context, taskRunID string, workerID string, errMsg string) error {
				return errors.New("failure persistence failure")
			},
		}
		exec := &fakeExecutor{
			executeFunc: func(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
				return nil, errors.New("execution error")
			},
		}
		w := New("test-worker", repo, map[string]Executor{"SLEEP": exec}, 1*time.Millisecond)
		err := w.Run(context.Background())
		if err == nil {
			t.Fatalf("expected error on failure DB failure, got nil")
		}
	})
}
