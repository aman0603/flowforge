package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/repository"
)

type fakeRepository struct {
	mu             sync.Mutex
	claimFunc      func(ctx context.Context, workerID string) (*model.ClaimedTask, error)
	startFunc      func(ctx context.Context, taskRunID string, workerID string, fencingToken ...int64) error
	completeFunc   func(ctx context.Context, taskRunID string, workerID string, output json.RawMessage, fencingToken ...int64) error
	failFunc       func(ctx context.Context, taskRunID string, workerID string, errMsg string, isTimeout bool, fencingToken ...int64) error
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

func (f *fakeRepository) ClaimReadyTasksBatch(ctx context.Context, workerID string, limit int) ([]*model.ClaimedTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		return nil, nil
	}
	if f.claimFunc != nil {
		task, err := f.claimFunc(ctx, workerID)
		if err != nil {
			return nil, err
		}
		if task == nil {
			return nil, nil
		}
		return []*model.ClaimedTask{task}, nil
	}
	return nil, nil
}

func (f *fakeRepository) StartTaskRun(ctx context.Context, taskRunID string, workerID string, fencingToken ...int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startedCalls++
	if f.startFunc != nil {
		return f.startFunc(ctx, taskRunID, workerID, fencingToken...)
	}
	return nil
}

func (f *fakeRepository) MarkTaskRunCompleted(ctx context.Context, taskRunID string, workerID string, output json.RawMessage, fencingToken ...int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completedCalls++
	if f.completeFunc != nil {
		return f.completeFunc(ctx, taskRunID, workerID, output, fencingToken...)
	}
	return nil
}

func (f *fakeRepository) MarkTaskRunFailed(ctx context.Context, taskRunID string, workerID string, errMsg string, isTimeout bool, fencingToken ...int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failedCalls++
	if f.failFunc != nil {
		return f.failFunc(ctx, taskRunID, workerID, errMsg, isTimeout, fencingToken...)
	}
	return nil
}

func (f *fakeRepository) GetActiveTaskRuns(ctx context.Context) ([]*model.TaskRun, error) {
	return nil, nil
}

func (f *fakeRepository) RecoverRunningTask(ctx context.Context, taskRunID string, fencingToken int64) (bool, error) {
	return true, nil
}

func (f *fakeRepository) RecoverClaimedTask(ctx context.Context, taskRunID string, fencingToken int64) (bool, error) {
	return true, nil
}

func (f *fakeRepository) PromoteDueRetries(ctx context.Context) (int64, error) {
	return 0, nil
}

type fakeLease struct {
	workerID     string
	fencingToken int64
}

type fakeCoordinator struct {
	registerFunc     func(ctx context.Context, workerID string, ttl time.Duration) error
	deregisterFunc   func(ctx context.Context, workerID string) error
	heartbeatFunc    func(ctx context.Context, workerID string, ttl time.Duration) error
	isAliveFunc      func(ctx context.Context, workerID string) (bool, error)
	acquireLeaseFunc func(ctx context.Context, taskRunID string, workerID string, fencingToken int64, ttl time.Duration) (bool, error)
	renewLeaseFunc   func(ctx context.Context, taskRunID string, workerID string, fencingToken int64, ttl time.Duration) (bool, error)
	releaseLeaseFunc func(ctx context.Context, taskRunID string, workerID string, fencingToken int64) error
	getLeaseFunc     func(ctx context.Context, taskRunID string) (string, int64, error)

	mu           sync.Mutex
	leases       map[string]fakeLease
	aliveWorkers map[string]bool
}

func (f *fakeCoordinator) RegisterWorker(ctx context.Context, workerID string, ttl time.Duration) error {
	if f.registerFunc != nil {
		return f.registerFunc(ctx, workerID, ttl)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.aliveWorkers == nil {
		f.aliveWorkers = make(map[string]bool)
	}
	f.aliveWorkers[workerID] = true
	return nil
}

func (f *fakeCoordinator) DeregisterWorker(ctx context.Context, workerID string) error {
	if f.deregisterFunc != nil {
		return f.deregisterFunc(ctx, workerID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.aliveWorkers != nil {
		delete(f.aliveWorkers, workerID)
	}
	return nil
}

func (f *fakeCoordinator) HeartbeatWorker(ctx context.Context, workerID string, ttl time.Duration) error {
	if f.heartbeatFunc != nil {
		return f.heartbeatFunc(ctx, workerID, ttl)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.aliveWorkers == nil {
		f.aliveWorkers = make(map[string]bool)
	}
	f.aliveWorkers[workerID] = true
	return nil
}

func (f *fakeCoordinator) IsWorkerAlive(ctx context.Context, workerID string) (bool, error) {
	if f.isAliveFunc != nil {
		return f.isAliveFunc(ctx, workerID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.aliveWorkers == nil {
		return true, nil
	}
	alive, exists := f.aliveWorkers[workerID]
	if !exists {
		return true, nil // default to true if never registered to prevent false positives in simple unit tests
	}
	return alive, nil
}

func (f *fakeCoordinator) AcquireTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64, ttl time.Duration) (bool, error) {
	if f.acquireLeaseFunc != nil {
		return f.acquireLeaseFunc(ctx, taskRunID, workerID, fencingToken, ttl)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases == nil {
		f.leases = make(map[string]fakeLease)
	}
	f.leases[taskRunID] = fakeLease{workerID: workerID, fencingToken: fencingToken}
	return true, nil
}

func (f *fakeCoordinator) RenewTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64, ttl time.Duration) (bool, error) {
	if f.renewLeaseFunc != nil {
		return f.renewLeaseFunc(ctx, taskRunID, workerID, fencingToken, ttl)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases == nil {
		return false, nil
	}
	lease, exists := f.leases[taskRunID]
	if !exists || lease.workerID != workerID || lease.fencingToken != fencingToken {
		return false, nil
	}
	return true, nil
}

func (f *fakeCoordinator) ReleaseTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64) error {
	if f.releaseLeaseFunc != nil {
		return f.releaseLeaseFunc(ctx, taskRunID, workerID, fencingToken)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases != nil {
		lease, exists := f.leases[taskRunID]
		if exists && lease.workerID == workerID && lease.fencingToken == fencingToken {
			delete(f.leases, taskRunID)
		}
	}
	return nil
}

func (f *fakeCoordinator) GetTaskLease(ctx context.Context, taskRunID string) (string, int64, error) {
	if f.getLeaseFunc != nil {
		return f.getLeaseFunc(ctx, taskRunID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases == nil {
		return "", 0, nil
	}
	lease, exists := f.leases[taskRunID]
	if !exists {
		return "", 0, nil
	}
	return lease.workerID, lease.fencingToken, nil
}

func (f *fakeCoordinator) Close() error {
	return nil
}

func newTestWorker(id string, repo Repository, executors map[string]Executor, pollInterval time.Duration) *Worker {
	return New(id, repo, executors, pollInterval, 0, 0, 0, &fakeCoordinator{}, 0, 0, 0, 0, 1, 2, 1, 500*time.Millisecond)
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
		w := newTestWorker("test-worker", repo, executors, 1*time.Millisecond)

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

		w := newTestWorker("test-worker", repo, map[string]Executor{"SLEEP": exec}, 1*time.Millisecond)
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

		w := newTestWorker("test-worker", repo, map[string]Executor{"SLEEP": exec}, 1*time.Millisecond)
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

		w := newTestWorker("test-worker", repo, map[string]Executor{}, 1*time.Millisecond)
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
			startFunc: func(ctx context.Context, taskRunID string, workerID string, fencingToken ...int64) error {
				return repository.ErrInvalidTaskTransition
			},
		}

		w := newTestWorker("test-worker", repo, map[string]Executor{"SLEEP": &fakeExecutor{}}, 1*time.Millisecond)
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

		ctx, cancel := context.WithCancel(context.Background())
		exec := &fakeExecutor{
			executeFunc: func(c context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
				cancel() // Trigger cancel during execution simulation
				return nil, context.Canceled
			},
		}

		w := newTestWorker("test-worker", repo, map[string]Executor{"SLEEP": exec}, 1*time.Millisecond)
		err := w.Run(ctx)
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
		w := newTestWorker("test-worker", repo, map[string]Executor{"SLEEP": &fakeExecutor{}}, 1*time.Millisecond)
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
			completeFunc: func(ctx context.Context, taskRunID string, workerID string, output json.RawMessage, fencingToken ...int64) error {
				return errors.New("completion persistence failure")
			},
		}
		w := newTestWorker("test-worker", repo, map[string]Executor{"SLEEP": &fakeExecutor{}}, 1*time.Millisecond)
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
			failFunc: func(ctx context.Context, taskRunID string, workerID string, errMsg string, isTimeout bool, fencingToken ...int64) error {
				return errors.New("failure persistence failure")
			},
		}
		exec := &fakeExecutor{
			executeFunc: func(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
				return nil, errors.New("execution error")
			},
		}
		w := newTestWorker("test-worker", repo, map[string]Executor{"SLEEP": exec}, 1*time.Millisecond)
		err := w.Run(context.Background())
		if err == nil {
			t.Fatalf("expected error on failure DB failure, got nil")
		}
	})
}

func TestWorkerPoolConcurrencyAndPanics(t *testing.T) {
	var cancel context.CancelFunc
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("executor panic is isolated and converted to failure", func(t *testing.T) {
		var claimedTask = &model.ClaimedTask{
			TaskRunID: "panic-run-1",
			TaskType:  "PANIC_TYPE",
		}
		var claimedCount int64
		repo := &fakeRepository{
			claimFunc: func(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
				if atomic.AddInt64(&claimedCount, 1) == 1 {
					return claimedTask, nil
				}
				return nil, nil
			},
			failFunc: func(ctx context.Context, taskRunID string, workerID string, errMsg string, isTimeout bool, fencingToken ...int64) error {
				cancel()
				return nil
			},
		}

		panicExecutor := &fakeExecutor{
			executeFunc: func(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
				panic("something went horribly wrong inside the task")
			},
		}

		w := New(
			"panic-worker",
			repo,
			map[string]Executor{"PANIC_TYPE": panicExecutor},
			1*time.Millisecond,
			0, 0, 0,
			&fakeCoordinator{},
			0, 0, 0, 0,
			2, 4, 2, // poolSize=2, queueCapacity=4, batchSize=2
			500*time.Millisecond,
		)

		err := w.Run(ctx)
		if err != nil {
			t.Fatalf("expected nil error on panic isolation run, got %v", err)
		}

		counters := w.GetCounters()
		if counters.TotalPanics != 1 {
			t.Errorf("expected 1 panic, got %d", counters.TotalPanics)
		}
		if counters.TotalFailed != 1 {
			t.Errorf("expected 1 task failure, got %d", counters.TotalFailed)
		}

		repo.mu.Lock()
		defer repo.mu.Unlock()
		if repo.failedCalls != 1 {
			t.Errorf("expected MarkTaskRunFailed to be called exactly once, got %d", repo.failedCalls)
		}
	})
}
