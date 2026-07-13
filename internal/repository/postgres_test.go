//go:build integration

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aman0603/flowforge/internal/model"
)

func setupTestDB(t *testing.T) *Repository {
	dbURL := os.Getenv("TEST_DB_URL")
	if dbURL == "" {
		t.Skip("TEST_DB_URL is missing, skipping database integration tests")
	}

	repo, err := New(dbURL)
	if err != nil {
		t.Fatalf("failed to connect to test database specified in TEST_DB_URL: %v", err)
	}

	// Drop existing tables to ensure latest schema is always provisioned
	dropQueries := []string{
		"DROP TABLE IF EXISTS task_dependencies CASCADE;",
		"DROP TABLE IF EXISTS task_runs CASCADE;",
		"DROP TABLE IF EXISTS task_definitions CASCADE;",
		"DROP TABLE IF EXISTS workflow_runs CASCADE;",
		"DROP TABLE IF EXISTS workflow_definitions CASCADE;",
	}
	for _, q := range dropQueries {
		if _, err := repo.db.Exec(q); err != nil {
			repo.Close()
			t.Fatalf("failed to drop test table: %v", err)
		}
	}

	// Initialize the schema dynamically from schema.sql
	err = repo.InitializeSchema("../../schema.sql")
	if err != nil {
		repo.Close()
		t.Fatalf("failed to initialize schema for test database: %v", err)
	}

	// Clear the tables to ensure data isolation
	clearDatabase(t, repo)

	t.Cleanup(func() {
		clearDatabase(t, repo)
		repo.Close()
	})

	return repo
}

func clearDatabase(t *testing.T, repo *Repository) {
	// Order is important because of foreign keys
	queries := []string{
		"TRUNCATE TABLE task_dependencies RESTART IDENTITY CASCADE;",
		"TRUNCATE TABLE task_runs RESTART IDENTITY CASCADE;",
		"TRUNCATE TABLE task_definitions RESTART IDENTITY CASCADE;",
		"TRUNCATE TABLE workflow_runs RESTART IDENTITY CASCADE;",
		"TRUNCATE TABLE workflow_definitions RESTART IDENTITY CASCADE;",
	}
	for _, q := range queries {
		if _, err := repo.db.Exec(q); err != nil {
			t.Fatalf("failed to clear test database table: %v", err)
		}
	}
}

func TestConcurrentClaimAtomicity(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Create a workflow definition with a single root task
	req := &model.CreateDefinitionRequest{
		Name:        "concurrent-claim-test",
		Description: "Verify only one worker can claim the ready task run",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 100}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	// 2. Create the workflow run (initializes TaskA as READY)
	run, err := repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to trigger workflow run: %v", err)
	}

	// 3. Launch 10 concurrent goroutines trying to claim this task
	const numWorkers = 10
	var wg sync.WaitGroup
	results := make([]*model.ClaimedTask, numWorkers)
	errs := make([]error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-%d", workerIndex)
			claimed, err := repo.ClaimNextReadyTask(ctx, workerID)
			results[workerIndex] = claimed
			errs[workerIndex] = err
		}(i)
	}

	wg.Wait()

	// 4. Verify claim outcomes
	successCount := 0
	var successfulWorkerID string
	for i := 0; i < numWorkers; i++ {
		if errs[i] != nil {
			t.Errorf("worker %d returned error: %v", i, errs[i])
		}
		if results[i] != nil {
			successCount++
			successfulWorkerID = fmt.Sprintf("worker-%d", i)
		}
	}

	if successCount != 1 {
		t.Errorf("expected exactly 1 successful claim, got %d", successCount)
	}

	// 5. Verify database state
	var status string
	var dbWorkerID *string
	var claimedAt *time.Time
	query := "SELECT status, worker_id, claimed_at FROM task_runs WHERE workflow_run_id = $1"
	err = repo.db.QueryRowContext(ctx, query, run.ID).Scan(&status, &dbWorkerID, &claimedAt)
	if err != nil {
		t.Fatalf("failed to query task status: %v", err)
	}

	if status != "CLAIMED" {
		t.Errorf("expected status 'CLAIMED', got %s", status)
	}
	if dbWorkerID == nil || *dbWorkerID != successfulWorkerID {
		t.Errorf("expected worker_id %q, got %v", successfulWorkerID, dbWorkerID)
	}
	if claimedAt == nil {
		t.Errorf("expected claimed_at to be non-nil")
	}
}

func TestOwnershipFencing(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "fencing-test",
		Description: "Verify other workers cannot execute, complete, or fail tasks owned by worker-1",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 100}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	_, err = repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to trigger workflow run: %v", err)
	}

	// 1. Claim task run as worker-1
	claimed, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim task: %v", err)
	}
	if claimed == nil {
		t.Fatalf("no task run was claimed")
	}

	// 2. worker-2 attempts to start the task run (must fail with ErrInvalidTaskTransition)
	err = repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-2")
	if !errors.Is(err, ErrInvalidTaskTransition) {
		t.Errorf("expected StartTaskRun as wrong worker to return ErrInvalidTaskTransition, got: %v", err)
	}

	// 3. worker-1 successfully starts the task run
	err = repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-1")
	if err != nil {
		t.Fatalf("expected worker-1 to start task successfully, got: %v", err)
	}

	// 4. worker-2 attempts to mark the task completed (must fail)
	err = repo.MarkTaskRunCompleted(ctx, claimed.TaskRunID, "worker-2", json.RawMessage(`{}`))
	if !errors.Is(err, ErrInvalidTaskTransition) {
		t.Errorf("expected MarkTaskRunCompleted as wrong worker to return ErrInvalidTaskTransition, got: %v", err)
	}

	// 5. worker-2 attempts to mark the task failed (must fail)
	err = repo.MarkTaskRunFailed(ctx, claimed.TaskRunID, "worker-2", "should fail", false)
	if !errors.Is(err, ErrInvalidTaskTransition) {
		t.Errorf("expected MarkTaskRunFailed as wrong worker to return ErrInvalidTaskTransition, got: %v", err)
	}

	// 6. worker-1 successfully completes the task run
	err = repo.MarkTaskRunCompleted(ctx, claimed.TaskRunID, "worker-1", json.RawMessage(`{"status":"success"}`))
	if err != nil {
		t.Fatalf("expected worker-1 to complete task run successfully, got: %v", err)
	}
}

func TestBasicTransactionalRollback(t *testing.T) {
	repo := setupTestDB(t)

	// 1. Create a workflow definition
	req := &model.CreateDefinitionRequest{
		Name:        "rollback-test",
		Description: "Verify database rollback on cancelled context",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 100}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(context.Background(), req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	// 2. Create a context that is already cancelled
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// 3. Attempt to create a workflow run with the cancelled context
	_, err = repo.CreateWorkflowRun(cancelledCtx, def.ID, nil)
	if err == nil {
		t.Fatalf("expected CreateWorkflowRun to fail with cancelled context, got nil")
	}

	// 4. Verify no workflow runs or task runs exist in the database
	var wrCount int
	err = repo.db.QueryRow("SELECT COUNT(*) FROM workflow_runs").Scan(&wrCount)
	if err != nil {
		t.Fatalf("failed to query workflow runs: %v", err)
	}
	if wrCount != 0 {
		t.Errorf("expected 0 workflow runs to survive transaction rollback, got %d", wrCount)
	}

	var trCount int
	err = repo.db.QueryRow("SELECT COUNT(*) FROM task_runs").Scan(&trCount)
	if err != nil {
		t.Fatalf("failed to query task runs: %v", err)
	}
	if trCount != 0 {
		t.Errorf("expected 0 task runs to survive transaction rollback, got %d", trCount)
	}
}

func TestConcurrentDiamondCompletion(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Create Diamond DAG: A -> B, C -> D
	req := &model.CreateDefinitionRequest{
		Name:        "concurrent-diamond-test",
		Description: "Verify concurrent completion of B and C properly unlocks D",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 100}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
			{
				Name:           "TaskB",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 100}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{"TaskA"},
			},
			{
				Name:           "TaskC",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 100}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{"TaskA"},
			},
			{
				Name:           "TaskD",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 100}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{"TaskB", "TaskC"},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	// Repeat the concurrency check 5 times to maximize probability of catching races
	for runIdx := 0; runIdx < 5; runIdx++ {
		run, err := repo.CreateWorkflowRun(ctx, def.ID, nil)
		if err != nil {
			t.Fatalf("run %d: failed to trigger workflow run: %v", runIdx, err)
		}

		// Claim & Complete A
		claimedA, err := repo.ClaimNextReadyTask(ctx, "worker-A")
		if err != nil || claimedA == nil {
			t.Fatalf("run %d: failed to claim task A: %v", runIdx, err)
		}
		if err := repo.StartTaskRun(ctx, claimedA.TaskRunID, "worker-A"); err != nil {
			t.Fatalf("run %d: failed to start task A: %v", runIdx, err)
		}
		if err := repo.MarkTaskRunCompleted(ctx, claimedA.TaskRunID, "worker-A", nil); err != nil {
			t.Fatalf("run %d: failed to complete task A: %v", runIdx, err)
		}

		// Claim B and C with separate worker identities
		claimedB, err := repo.ClaimNextReadyTask(ctx, "worker-B")
		if err != nil || claimedB == nil {
			t.Fatalf("run %d: failed to claim task B: %v", runIdx, err)
		}
		claimedC, err := repo.ClaimNextReadyTask(ctx, "worker-C")
		if err != nil || claimedC == nil {
			t.Fatalf("run %d: failed to claim task C: %v", runIdx, err)
		}

		// Start B and C
		if err := repo.StartTaskRun(ctx, claimedB.TaskRunID, "worker-B"); err != nil {
			t.Fatalf("run %d: failed to start task B: %v", runIdx, err)
		}
		if err := repo.StartTaskRun(ctx, claimedC.TaskRunID, "worker-C"); err != nil {
			t.Fatalf("run %d: failed to start task C: %v", runIdx, err)
		}

		// Use a Barrier/WaitGroup and run concurrent completion calls
		var wg sync.WaitGroup
		wg.Add(2)

		// Start goroutines for B and C completions
		go func() {
			defer wg.Done()
			_ = repo.MarkTaskRunCompleted(ctx, claimedB.TaskRunID, "worker-B", nil)
		}()

		go func() {
			defer wg.Done()
			_ = repo.MarkTaskRunCompleted(ctx, claimedC.TaskRunID, "worker-C", nil)
		}()

		wg.Wait()

		// Verify task D has transitioned to READY
		var dStatus string
		err = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE workflow_run_id = $1 AND task_definition_id = (SELECT id FROM task_definitions WHERE workflow_definition_id = $2 AND name = 'TaskD')", run.ID, def.ID).Scan(&dStatus)
		if err != nil {
			t.Fatalf("run %d: failed to query D status: %v", runIdx, err)
		}
		if dStatus != "READY" {
			t.Errorf("run %d: expected task D to be READY, got %s", runIdx, dStatus)
		}

		// Claim, Start, and Complete D to leave database clean of READY tasks
		claimedD, err := repo.ClaimNextReadyTask(ctx, "worker-D")
		if err != nil || claimedD == nil {
			t.Fatalf("run %d: failed to claim task D: %v", runIdx, err)
		}
		if err := repo.StartTaskRun(ctx, claimedD.TaskRunID, "worker-D"); err != nil {
			t.Fatalf("run %d: failed to start task D: %v", runIdx, err)
		}
		if err := repo.MarkTaskRunCompleted(ctx, claimedD.TaskRunID, "worker-D", nil); err != nil {
			t.Fatalf("run %d: failed to complete task D: %v", runIdx, err)
		}
	}
}

func TestConcurrentIndependentClaiming(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Create a workflow with 10 independent tasks
	const numTasks = 10
	tasksInput := make([]model.TaskDefinitionInput, numTasks)
	for i := 0; i < numTasks; i++ {
		tasksInput[i] = model.TaskDefinitionInput{
			Name:           fmt.Sprintf("Task-%d", i),
			TaskType:       "SLEEP",
			Config:         json.RawMessage(`{"duration_ms": 10}`),
			MaxRetries:     3,
			RetryBackoffMs: 1000,
			TimeoutMs:      5000,
			Dependencies:   []string{},
		}
	}

	req := &model.CreateDefinitionRequest{
		Name:        "independent-claiming-test",
		Description: "Verify concurrent claims of independent tasks are segregated",
		Tasks:       tasksInput,
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	_, err = repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to trigger workflow run: %v", err)
	}

	// 2. Launch 10 workers in parallel to claim all 10 ready tasks concurrently
	var wg sync.WaitGroup
	claimsMu := sync.Mutex{}
	claimedMap := make(map[string]string) // task_run_id -> worker_id

	wg.Add(numTasks)
	for i := 0; i < numTasks; i++ {
		go func(workerIndex int) {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-%d", workerIndex)
			// Loop/retry a few times in case database connection contention delays the claim
			for attempt := 0; attempt < 50; attempt++ {
				claimed, err := repo.ClaimNextReadyTask(ctx, workerID)
				if err != nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				if claimed != nil {
					claimsMu.Lock()
					claimedMap[claimed.TaskRunID] = workerID
					claimsMu.Unlock()
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	// 3. Verify exactly 10 unique tasks were claimed, and no task was double claimed
	if len(claimedMap) != numTasks {
		t.Errorf("expected %d unique claimed tasks, got %d", numTasks, len(claimedMap))
	}
}

func TestConcurrentFinalCompletion(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Create a workflow definition: A -> B, C (both B and C are terminal)
	req := &model.CreateDefinitionRequest{
		Name:        "concurrent-final-test",
		Description: "Verify concurrent final completions mark workflow complete",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
			{
				Name:           "TaskB",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{"TaskA"},
			},
			{
				Name:           "TaskC",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{"TaskA"},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	run, err := repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to trigger workflow run: %v", err)
	}

	// Start & complete A
	claimedA, err := repo.ClaimNextReadyTask(ctx, "worker-A")
	if err != nil || claimedA == nil {
		t.Fatalf("failed to claim task A: %v", err)
	}
	_ = repo.StartTaskRun(ctx, claimedA.TaskRunID, "worker-A")
	_ = repo.MarkTaskRunCompleted(ctx, claimedA.TaskRunID, "worker-A", nil)

	// Claim and start B and C
	claimedB, _ := repo.ClaimNextReadyTask(ctx, "worker-B")
	claimedC, _ := repo.ClaimNextReadyTask(ctx, "worker-C")

	_ = repo.StartTaskRun(ctx, claimedB.TaskRunID, "worker-B")
	_ = repo.StartTaskRun(ctx, claimedC.TaskRunID, "worker-C")

	// Complete B and C concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = repo.MarkTaskRunCompleted(ctx, claimedB.TaskRunID, "worker-B", nil)
	}()

	go func() {
		defer wg.Done()
		_ = repo.MarkTaskRunCompleted(ctx, claimedC.TaskRunID, "worker-C", nil)
	}()

	wg.Wait()

	// Verify both B and C are COMPLETED
	var statusB, statusC string
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", claimedB.TaskRunID).Scan(&statusB)
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", claimedC.TaskRunID).Scan(&statusC)

	if statusB != "COMPLETED" || statusC != "COMPLETED" {
		t.Errorf("expected tasks to be COMPLETED, got B: %s, C: %s", statusB, statusC)
	}

	// Verify parent workflow is COMPLETED
	var wfStatus string
	err = repo.db.QueryRowContext(ctx, "SELECT status FROM workflow_runs WHERE id = $1", run.ID).Scan(&wfStatus)
	if err != nil {
		t.Fatalf("failed to query workflow run status: %v", err)
	}
	if wfStatus != "COMPLETED" {
		t.Errorf("expected workflow status to be 'COMPLETED', got %s", wfStatus)
	}
}

func TestRecoverStaleClaimed(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "stale-claimed-test",
		Description: "Verify recovery of stale CLAIMED task runs",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	_, err = repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to trigger workflow run: %v", err)
	}

	// 1. Claim task run
	claimed, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim task: %v", err)
	}

	// 2. Artificially make it stale (claimed_at = 60s ago)
	_, err = repo.db.Exec("UPDATE task_runs SET claimed_at = NOW() - INTERVAL '60 seconds' WHERE id = $1", claimed.TaskRunID)
	if err != nil {
		t.Fatalf("failed to backdate claimed_at: %v", err)
	}

	// 3. Run recovery with a claimed timeout of 30 seconds
	res, err := repo.RecoverStaleTasks(ctx, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to run recovery: %v", err)
	}

	if res.ClaimedRecovered != 1 {
		t.Errorf("expected 1 claimed task recovered, got %d", res.ClaimedRecovered)
	}
	if res.RunningRecovered != 0 {
		t.Errorf("expected 0 running tasks recovered, got %d", res.RunningRecovered)
	}

	// 4. Assert DB state
	var status string
	var dbWorkerID *string
	var dbClaimedAt *time.Time
	var dbAttempts int
	err = repo.db.QueryRowContext(ctx, "SELECT status, worker_id, claimed_at, attempts FROM task_runs WHERE id = $1", claimed.TaskRunID).
		Scan(&status, &dbWorkerID, &dbClaimedAt, &dbAttempts)
	if err != nil {
		t.Fatalf("failed to query task run: %v", err)
	}

	if status != "READY" {
		t.Errorf("expected status 'READY', got %s", status)
	}
	if dbWorkerID != nil {
		t.Errorf("expected worker_id to be nil, got %s", *dbWorkerID)
	}
	if dbClaimedAt != nil {
		t.Errorf("expected claimed_at to be nil, got %v", dbClaimedAt)
	}
	if dbAttempts != 0 {
		t.Errorf("expected attempts to remain 0, got %d", dbAttempts)
	}
}

func TestDoNotRecoverFreshClaimed(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "fresh-claimed-test",
		Description: "Verify fresh CLAIMED task runs are not recovered",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	_, err = repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to trigger workflow run: %v", err)
	}

	// 1. Claim task run
	claimed, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim task: %v", err)
	}

	// 2. Run recovery with a claimed timeout of 30 seconds
	res, err := repo.RecoverStaleTasks(ctx, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to run recovery: %v", err)
	}

	if res.ClaimedRecovered != 0 {
		t.Errorf("expected 0 claimed tasks recovered, got %d", res.ClaimedRecovered)
	}

	// 3. Assert DB state remains CLAIMED under worker-1
	var status string
	var dbWorkerID *string
	err = repo.db.QueryRowContext(ctx, "SELECT status, worker_id FROM task_runs WHERE id = $1", claimed.TaskRunID).
		Scan(&status, &dbWorkerID)
	if err != nil {
		t.Fatalf("failed to query task run: %v", err)
	}

	if status != "CLAIMED" {
		t.Errorf("expected status 'CLAIMED', got %s", status)
	}
	if dbWorkerID == nil || *dbWorkerID != "worker-1" {
		t.Errorf("expected worker_id to remain 'worker-1', got %v", dbWorkerID)
	}
}

func TestRecoverStaleRunning(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "stale-running-test",
		Description: "Verify recovery of stale RUNNING task runs",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	_, err = repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to trigger workflow run: %v", err)
	}

	// 1. Claim & Start task run (attempts becomes 1)
	claimed, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim task: %v", err)
	}
	if err := repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-1"); err != nil {
		t.Fatalf("failed to start task: %v", err)
	}

	// Set output, errMsg and completed_at to verify they get cleared/reset on recovery!
	_, err = repo.db.Exec(`
		UPDATE task_runs 
		SET started_at = NOW() - INTERVAL '10 minutes',
			output = '{"temp":"data"}'::jsonb,
			error_message = 'temp error',
			completed_at = NOW()
		WHERE id = $1`, claimed.TaskRunID)
	if err != nil {
		t.Fatalf("failed to artificially set task state: %v", err)
	}

	// 2. Run recovery with a running timeout of 5 minutes
	res, err := repo.RecoverStaleTasks(ctx, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to run recovery: %v", err)
	}

	if res.RunningRecovered != 1 {
		t.Errorf("expected 1 running task recovered, got %d", res.RunningRecovered)
	}

	// 3. Assert DB state
	var status string
	var dbWorkerID *string
	var dbClaimedAt, dbStartedAt, dbCompletedAt *time.Time
	var dbAttempts int
	var dbOutput json.RawMessage
	var dbErrorMessage *string
	err = repo.db.QueryRowContext(ctx, `
		SELECT status, worker_id, claimed_at, started_at, completed_at, attempts, output, error_message 
		FROM task_runs WHERE id = $1`, claimed.TaskRunID).
		Scan(&status, &dbWorkerID, &dbClaimedAt, &dbStartedAt, &dbCompletedAt, &dbAttempts, &dbOutput, &dbErrorMessage)
	if err != nil {
		t.Fatalf("failed to query task run: %v", err)
	}

	if status != "READY" {
		t.Errorf("expected status 'READY', got %s", status)
	}
	if dbWorkerID != nil {
		t.Errorf("expected worker_id to be nil, got %s", *dbWorkerID)
	}
	if dbClaimedAt != nil {
		t.Errorf("expected claimed_at to be nil")
	}
	if dbStartedAt != nil {
		t.Errorf("expected started_at to be nil")
	}
	if dbCompletedAt != nil {
		t.Errorf("expected completed_at to be nil")
	}
	if dbAttempts != 1 {
		t.Errorf("expected attempts to remain 1, got %d", dbAttempts)
	}
	if string(dbOutput) != "{}" {
		t.Errorf("expected output to be cleared to '{}', got %s", string(dbOutput))
	}
	if dbErrorMessage != nil {
		t.Errorf("expected error_message to be cleared to nil, got %s", *dbErrorMessage)
	}
}

func TestDoNotRecoverFreshRunning(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "fresh-running-test",
		Description: "Verify fresh RUNNING task runs are not recovered",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	_, err = repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to trigger workflow run: %v", err)
	}

	// 1. Claim & Start task run
	claimed, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil || claimed == nil {
		t.Fatalf("failed to claim task: %v", err)
	}
	if err := repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-1"); err != nil {
		t.Fatalf("failed to start task: %v", err)
	}

	// 2. Run recovery with a running timeout of 5 minutes
	res, err := repo.RecoverStaleTasks(ctx, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to run recovery: %v", err)
	}

	if res.RunningRecovered != 0 {
		t.Errorf("expected 0 running tasks recovered, got %d", res.RunningRecovered)
	}

	// 3. Assert DB state remains RUNNING under worker-1
	var status string
	var dbWorkerID *string
	err = repo.db.QueryRowContext(ctx, "SELECT status, worker_id FROM task_runs WHERE id = $1", claimed.TaskRunID).
		Scan(&status, &dbWorkerID)
	if err != nil {
		t.Fatalf("failed to query task run: %v", err)
	}

	if status != "RUNNING" {
		t.Errorf("expected status 'RUNNING', got %s", status)
	}
	if dbWorkerID == nil || *dbWorkerID != "worker-1" {
		t.Errorf("expected worker_id to remain 'worker-1', got %v", dbWorkerID)
	}
}

func TestDoNotRecoverTerminalWorkflowTasks(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "terminal-wf-recovery-test",
		Description: "Verify tasks from COMPLETED or FAILED workflows are not recovered",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	// 1. Create a run that will be marked COMPLETED
	run1, _ := repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed1, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	_ = repo.StartTaskRun(ctx, claimed1.TaskRunID, "worker-1")
	// Make task look stale CLAIMED but mark workflow run COMPLETED
	_, err1 := repo.db.Exec("UPDATE task_runs SET status = 'CLAIMED', claimed_at = NOW() - INTERVAL '60 seconds' WHERE id = $1", claimed1.TaskRunID)
	_, err2 := repo.db.Exec("UPDATE workflow_runs SET status = 'COMPLETED' WHERE id = $1", run1.ID)

	// 2. Create a run that will be marked FAILED
	run2, _ := repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed2, _ := repo.ClaimNextReadyTask(ctx, "worker-2")
	_ = repo.StartTaskRun(ctx, claimed2.TaskRunID, "worker-2")
	// Make task look stale RUNNING but mark workflow run FAILED
	_, err3 := repo.db.Exec("UPDATE task_runs SET status = 'RUNNING', started_at = NOW() - INTERVAL '10 minutes' WHERE id = $1", claimed2.TaskRunID)
	_, err4 := repo.db.Exec("UPDATE workflow_runs SET status = 'FAILED' WHERE id = $1", run2.ID)

	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		t.Fatalf("setup updates failed: err1=%v, err2=%v, err3=%v, err4=%v", err1, err2, err3, err4)
	}

	// 3. Run recovery
	res, err := repo.RecoverStaleTasks(ctx, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to run recovery: %v", err)
	}

	if res.ClaimedRecovered != 0 || res.RunningRecovered != 0 {
		t.Errorf("expected 0 tasks recovered from terminal workflows, got claimed=%d, running=%d",
			res.ClaimedRecovered, res.RunningRecovered)
	}

	// 4. Assert task status did not change
	var status1, status2 string
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", claimed1.TaskRunID).Scan(&status1)
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", claimed2.TaskRunID).Scan(&status2)

	if status1 != "CLAIMED" || status2 != "RUNNING" {
		t.Errorf("expected statuses to remain CLAIMED and RUNNING, got %s and %s", status1, status2)
	}
}

func TestConcurrentRecoverySafety(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "concurrent-recovery-test",
		Description: "Verify concurrent execution of recovery doesn't cause errors or double-resets",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	// Create 5 separate workflow runs with stale claimed tasks
	const numRuns = 5
	taskRunIDs := make([]string, numRuns)
	for i := 0; i < numRuns; i++ {
		_, _ = repo.CreateWorkflowRun(ctx, def.ID, nil)
		claimed, _ := repo.ClaimNextReadyTask(ctx, fmt.Sprintf("worker-%d", i))
		taskRunIDs[i] = claimed.TaskRunID
		_, _ = repo.db.Exec("UPDATE task_runs SET claimed_at = NOW() - INTERVAL '60 seconds' WHERE id = $1", claimed.TaskRunID)
	}

	// Run 5 recovery calls concurrently
	var wg sync.WaitGroup
	var totalClaimedRecovered int64
	var mu sync.Mutex

	wg.Add(numRuns)
	for i := 0; i < numRuns; i++ {
		go func() {
			defer wg.Done()
			res, err := repo.RecoverStaleTasks(ctx, 30*time.Second, 5*time.Minute)
			if err == nil {
				mu.Lock()
				totalClaimedRecovered += res.ClaimedRecovered
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Assert exactly 5 tasks were recovered overall across all transactions
	if totalClaimedRecovered != numRuns {
		t.Errorf("expected total claimed recovered across concurrent transactions to be %d, got %d", numRuns, totalClaimedRecovered)
	}

	// Assert all tasks are indeed READY in database
	for _, id := range taskRunIDs {
		var status string
		_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", id).Scan(&status)
		if status != "READY" {
			t.Errorf("expected task %s to be READY, got %s", id, status)
		}
	}
}

func TestRetryWaitStatusAndPromotion(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "retry-test",
		Description: "testing retries",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     3,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	// 1. Create a run with a due retry task
	_, _ = repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed1, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	_ = repo.StartTaskRun(ctx, claimed1.TaskRunID, "worker-1")
	// Update to RETRY_WAIT with due next_retry_at (10 seconds ago)
	_, err = repo.db.Exec("UPDATE task_runs SET status = 'RETRY_WAIT', next_retry_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", claimed1.TaskRunID)
	if err != nil {
		t.Fatalf("failed to setup due retry: %v", err)
	}

	// 2. Create a run with a future retry task
	_, _ = repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed2, _ := repo.ClaimNextReadyTask(ctx, "worker-2")
	_ = repo.StartTaskRun(ctx, claimed2.TaskRunID, "worker-2")
	// Update to RETRY_WAIT with future next_retry_at (10 seconds from now)
	_, err = repo.db.Exec("UPDATE task_runs SET status = 'RETRY_WAIT', next_retry_at = NOW() + INTERVAL '10 seconds' WHERE id = $1", claimed2.TaskRunID)
	if err != nil {
		t.Fatalf("failed to setup future retry: %v", err)
	}

	// 3. Create a run with a due retry but in a COMPLETED workflow run
	run3, _ := repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed3, _ := repo.ClaimNextReadyTask(ctx, "worker-3")
	_ = repo.StartTaskRun(ctx, claimed3.TaskRunID, "worker-3")
	_, err = repo.db.Exec("UPDATE task_runs SET status = 'RETRY_WAIT', next_retry_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", claimed3.TaskRunID)
	if err != nil {
		t.Fatalf("failed to setup due retry on terminal workflow: %v", err)
	}
	_, err = repo.db.Exec("UPDATE workflow_runs SET status = 'COMPLETED' WHERE id = $1", run3.ID)
	if err != nil {
		t.Fatalf("failed to set workflow run status to COMPLETED: %v", err)
	}

	// 4. Promote due retries
	count, err := repo.PromoteDueRetries(ctx)
	if err != nil {
		t.Fatalf("failed to promote due retries: %v", err)
	}

	if count != 1 {
		t.Errorf("expected exactly 1 task run to be promoted, got %d", count)
	}

	// 5. Verify states in database
	var status1, status2, status3 string
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", claimed1.TaskRunID).Scan(&status1)
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", claimed2.TaskRunID).Scan(&status2)
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", claimed3.TaskRunID).Scan(&status3)

	if status1 != "READY" {
		t.Errorf("expected due retry task status to become READY, got %s", status1)
	}
	if status2 != "RETRY_WAIT" {
		t.Errorf("expected future retry task status to remain RETRY_WAIT, got %s", status2)
	}
	if status3 != "RETRY_WAIT" {
		t.Errorf("expected due retry task in terminal workflow to remain RETRY_WAIT, got %s", status3)
	}

	// 6. Test concurrent promotion safety
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = repo.PromoteDueRetries(ctx)
		}()
	}
	wg.Wait()
}

func TestCalculateBackoffHelper(t *testing.T) {
	cases := []struct {
		baseMs   int
		attempts int
		expected time.Duration
	}{
		{1000, 1, 1000 * time.Millisecond},
		{1000, 2, 2000 * time.Millisecond},
		{1000, 3, 4000 * time.Millisecond},
		{1000, 4, 8000 * time.Millisecond},
		{0, 1, 1000 * time.Millisecond},            // Fallback
		{-500, 1, 1000 * time.Millisecond},         // Fallback
		{1000, 0, 1000 * time.Millisecond},         // Shift bound check
		{1000, -1, 1000 * time.Millisecond},        // Shift bound check
		{1000, 40, 3600 * 1000 * time.Millisecond}, // Cap check (1 hour)
	}

	for _, tc := range cases {
		res := calculateBackoff(tc.baseMs, tc.attempts)
		if res != tc.expected {
			t.Errorf("calculateBackoff(%d, %d) = %v; expected %v", tc.baseMs, tc.attempts, res, tc.expected)
		}
	}
}

func TestExponentialBackoffAndRetryExecution(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	req := &model.CreateDefinitionRequest{
		Name:        "backoff-retry-dag",
		Description: "A dependent DAG with retries",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     2,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
			{
				Name:           "TaskB",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{"TaskA"},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	// === TEST CASE 1: Retry Exhaustion ===
	run1, _ := repo.CreateWorkflowRun(ctx, def.ID, nil)

	// Verify TaskB is PENDING
	var statusB string
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE workflow_run_id = $1 AND task_definition_id = (SELECT id FROM task_definitions WHERE name = 'TaskB')", run1.ID).Scan(&statusB)
	if statusB != "PENDING" {
		t.Fatalf("expected TaskB status to be PENDING, got %s", statusB)
	}

	// Attempt 1: Initial failure
	claimed, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	if claimed == nil {
		t.Fatalf("expected to claim TaskA")
	}
	_ = repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-1")

	// Verify concurrent failure safety / worker ownership
	err = repo.MarkTaskRunFailed(ctx, claimed.TaskRunID, "wrong-worker", "oops", false)
	if err != ErrInvalidTaskTransition {
		t.Errorf("expected ErrInvalidTaskTransition when failing with wrong worker, got %v", err)
	}

	err = repo.MarkTaskRunFailed(ctx, claimed.TaskRunID, "worker-1", "attempt 1 error", false)
	if err != nil {
		t.Fatalf("failed to mark task failed: %v", err)
	}

	// Verify states
	var statusA string
	var nextRetry sql.NullTime
	_ = repo.db.QueryRowContext(ctx, "SELECT status, next_retry_at FROM task_runs WHERE id = $1", claimed.TaskRunID).Scan(&statusA, &nextRetry)
	if statusA != "RETRY_WAIT" {
		t.Errorf("expected TaskA status to become RETRY_WAIT, got %s", statusA)
	}
	if !nextRetry.Valid {
		t.Errorf("expected next_retry_at to be populated")
	} else {
		diff := time.Until(nextRetry.Time)
		if diff < 500*time.Millisecond || diff > 1500*time.Millisecond {
			t.Errorf("expected backoff delay around 1s, got %v", diff)
		}
	}

	// Verify workflow run is still RUNNING
	var runStatus string
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM workflow_runs WHERE id = $1", run1.ID).Scan(&runStatus)
	if runStatus != "RUNNING" {
		t.Errorf("expected workflow status to remain RUNNING, got %s", runStatus)
	}

	// Attempt 2: First retry fails
	// Promote manually by forcing next_retry_at to the past
	_, _ = repo.db.Exec("UPDATE task_runs SET next_retry_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", claimed.TaskRunID)
	promoted, _ := repo.PromoteDueRetries(ctx)
	if promoted != 1 {
		t.Fatalf("expected to promote 1 task, got %d", promoted)
	}

	claimed, _ = repo.ClaimNextReadyTask(ctx, "worker-1")
	_ = repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-1")
	_ = repo.MarkTaskRunFailed(ctx, claimed.TaskRunID, "worker-1", "attempt 2 error", false)

	// Verify state is RETRY_WAIT, backoff multiplier is 2 (2 seconds)
	_ = repo.db.QueryRowContext(ctx, "SELECT status, next_retry_at FROM task_runs WHERE id = $1", claimed.TaskRunID).Scan(&statusA, &nextRetry)
	if statusA != "RETRY_WAIT" {
		t.Errorf("expected TaskA status to remain RETRY_WAIT, got %s", statusA)
	}
	if nextRetry.Valid {
		diff := time.Until(nextRetry.Time)
		if diff < 1500*time.Millisecond || diff > 2500*time.Millisecond {
			t.Errorf("expected backoff delay around 2s, got %v", diff)
		}
	}

	// Attempt 3: Second retry (attempt 3) fails -> budget exhausted!
	_, _ = repo.db.Exec("UPDATE task_runs SET next_retry_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", claimed.TaskRunID)
	_, _ = repo.PromoteDueRetries(ctx)
	claimed, _ = repo.ClaimNextReadyTask(ctx, "worker-1")
	_ = repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-1")

	// Concurrent failure race test:
	// If two concurrent calls try to fail it:
	var failWG sync.WaitGroup
	var failErrors [2]error
	failWG.Add(2)
	for i := 0; i < 2; i++ {
		idx := i
		go func() {
			defer failWG.Done()
			failErrors[idx] = repo.MarkTaskRunFailed(ctx, claimed.TaskRunID, "worker-1", "exhausted error", false)
		}()
	}
	failWG.Wait()
	// One must succeed, one must fail with ErrInvalidTaskTransition
	successes := 0
	for _, e := range failErrors {
		if e == nil {
			successes++
		} else if e != ErrInvalidTaskTransition {
			t.Errorf("unexpected error on concurrent fail: %v", e)
		}
	}
	if successes != 1 {
		t.Errorf("expected exactly 1 concurrent fail to succeed, got %d", successes)
	}

	// Verify final statuses
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", claimed.TaskRunID).Scan(&statusA)
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM workflow_runs WHERE id = $1", run1.ID).Scan(&runStatus)
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE workflow_run_id = $1 AND task_definition_id = (SELECT id FROM task_definitions WHERE name = 'TaskB')", run1.ID).Scan(&statusB)

	if statusA != "FAILED" {
		t.Errorf("expected TaskA to become FAILED, got %s", statusA)
	}
	if runStatus != "FAILED" {
		t.Errorf("expected workflow to become FAILED, got %s", runStatus)
	}
	if statusB != "PENDING" {
		t.Errorf("expected TaskB to remain PENDING, got %s", statusB)
	}

	// === TEST CASE 2: Retry Success Unlocks Child ===
	run2, _ := repo.CreateWorkflowRun(ctx, def.ID, nil)

	// Attempt 1: fails
	c1, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	_ = repo.StartTaskRun(ctx, c1.TaskRunID, "worker-1")
	_ = repo.MarkTaskRunFailed(ctx, c1.TaskRunID, "worker-1", "attempt 1 fail", false)

	// Verify child remains PENDING
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE workflow_run_id = $1 AND task_definition_id = (SELECT id FROM task_definitions WHERE name = 'TaskB')", run2.ID).Scan(&statusB)
	if statusB != "PENDING" {
		t.Errorf("expected child to remain PENDING, got %s", statusB)
	}

	// Promote and complete
	_, _ = repo.db.Exec("UPDATE task_runs SET next_retry_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", c1.TaskRunID)
	_, _ = repo.PromoteDueRetries(ctx)
	c2, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	_ = repo.StartTaskRun(ctx, c2.TaskRunID, "worker-1")
	_ = repo.MarkTaskRunCompleted(ctx, c2.TaskRunID, "worker-1", json.RawMessage(`{}`))

	// Verify TaskA is COMPLETED, TaskB is READY
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE id = $1", c1.TaskRunID).Scan(&statusA)
	_ = repo.db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE workflow_run_id = $1 AND task_definition_id = (SELECT id FROM task_definitions WHERE name = 'TaskB')", run2.ID).Scan(&statusB)

	if statusA != "COMPLETED" {
		t.Errorf("expected TaskA status COMPLETED, got %s", statusA)
	}
	if statusB != "READY" {
		t.Errorf("expected child TaskB status READY, got %s", statusB)
	}
}

func TestPriorityScheduling(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Workflow with independent tasks of varying priorities
	req1 := &model.CreateDefinitionRequest{
		Name:        "priority-test-1",
		Description: "independent tasks with priority",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskLow",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Priority:       10,
				Dependencies:   []string{},
			},
			{
				Name:           "TaskMed",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Priority:       50,
				Dependencies:   []string{},
			},
			{
				Name:           "TaskHigh",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Priority:       100,
				Dependencies:   []string{},
			},
		},
	}
	def1, err := repo.CreateWorkflowDefinition(ctx, req1)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	_, _ = repo.CreateWorkflowRun(ctx, def1.ID, nil)

	// Claim 1: Should be TaskHigh
	c1, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	if c1 == nil || c1.Name != "TaskHigh" {
		t.Errorf("expected first claimed task to be TaskHigh, got %v", c1)
	}

	// Claim 2: Should be TaskMed
	c2, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	if c2 == nil || c2.Name != "TaskMed" {
		t.Errorf("expected second claimed task to be TaskMed, got %v", c2)
	}

	// Claim 3: Should be TaskLow
	c3, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	if c3 == nil || c3.Name != "TaskLow" {
		t.Errorf("expected third claimed task to be TaskLow, got %v", c3)
	}

	// 2. FIFO tie-breaking for same-priority tasks
	req2 := &model.CreateDefinitionRequest{
		Name:        "priority-test-2",
		Description: "same priority tasks",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskSame1",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Priority:       10,
				Dependencies:   []string{},
			},
			{
				Name:           "TaskSame2",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Priority:       10,
				Dependencies:   []string{},
			},
		},
	}
	def2, _ := repo.CreateWorkflowDefinition(ctx, req2)
	_, _ = repo.CreateWorkflowRun(ctx, def2.ID, nil)

	// Since TaskSame1 was created first, it should be claimed first
	cs1, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	cs2, _ := repo.ClaimNextReadyTask(ctx, "worker-1")

	// Ensure both are claimed, and order is FIFO based on creation/definition order
	if cs1 == nil || cs2 == nil {
		t.Fatalf("expected both tasks to be claimed")
	}

	// 3. Priority does not bypass DAG dependencies
	req3 := &model.CreateDefinitionRequest{
		Name:        "priority-test-3",
		Description: "priority vs dependencies",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskParent",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Priority:       10,
				Dependencies:   []string{},
			},
			{
				Name:           "TaskChild",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Priority:       100, // Higher priority but depends on TaskParent
				Dependencies:   []string{"TaskParent"},
			},
		},
	}
	def3, _ := repo.CreateWorkflowDefinition(ctx, req3)
	_, _ = repo.CreateWorkflowRun(ctx, def3.ID, nil)

	// Only TaskParent is READY, so it must be claimed first despite TaskChild having higher priority
	cp1, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	if cp1 == nil || cp1.Name != "TaskParent" {
		t.Errorf("expected READY parent to be claimed, got %v", cp1)
	}

	// No more ready tasks at this point (child is PENDING)
	cp2, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	if cp2 != nil {
		t.Errorf("expected no task to be claimed, got %v", cp2)
	}
}

func TestTaskAttemptsSchema(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Create a dummy workflow definition and run to have a valid task run
	req := &model.CreateDefinitionRequest{
		Name:        "attempt-schema-test",
		Description: "testing attempts schema constraints",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 1000,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to create workflow def: %v", err)
	}

	run, err := repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to create workflow run: %v", err)
	}

	// Get the task run ID
	var taskRunID string
	err = repo.db.QueryRowContext(ctx, "SELECT id FROM task_runs WHERE workflow_run_id = $1", run.ID).Scan(&taskRunID)
	if err != nil {
		t.Fatalf("failed to query task run: %v", err)
	}

	// 2. Insert valid task attempt
	attemptID1 := newUUID()
	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO task_attempts (id, task_run_id, workflow_run_id, attempt_number, worker_id, status, started_at)
		VALUES ($1, $2, $3, 1, 'worker-1', 'RUNNING', NOW())
	`, attemptID1, taskRunID, run.ID)
	if err != nil {
		t.Fatalf("failed to insert valid task attempt: %v", err)
	}

	// 3. Try to insert duplicate attempt_number for the same task run (must fail unique constraint)
	attemptID2 := newUUID()
	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO task_attempts (id, task_run_id, workflow_run_id, attempt_number, worker_id, status, started_at)
		VALUES ($1, $2, $3, 1, 'worker-1', 'RUNNING', NOW())
	`, attemptID2, taskRunID, run.ID)
	if err == nil {
		t.Errorf("expected unique constraint violation, but got nil error")
	}

	// 4. Try to insert invalid status (must fail CHECK constraint)
	attemptID3 := newUUID()
	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO task_attempts (id, task_run_id, workflow_run_id, attempt_number, worker_id, status, started_at)
		VALUES ($1, $2, $3, 2, 'worker-1', 'INVALID_STATUS', NOW())
	`, attemptID3, taskRunID, run.ID)
	if err == nil {
		t.Errorf("expected chk_attempt_status violation, but got nil error")
	}
}

func TestTaskAttemptsIntegration(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// Register workflow with a sleep task (allowing retries)
	req := &model.CreateDefinitionRequest{
		Name:        "integration-test-attempts",
		Description: "testing attempt history generation",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     2,
				RetryBackoffMs: 100,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to create workflow def: %v", err)
	}

	// === 1. Success Flow ===
	_, _ = repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	err = repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-1")
	if err != nil {
		t.Fatalf("failed to start task run: %v", err)
	}

	// Verify attempt 1 was created as RUNNING
	var status string
	var attemptNum int
	var workerID string
	err = repo.db.QueryRowContext(ctx, "SELECT status, attempt_number, worker_id FROM task_attempts WHERE task_run_id = $1", claimed.TaskRunID).Scan(&status, &attemptNum, &workerID)
	if err != nil {
		t.Fatalf("failed to query task attempts: %v", err)
	}
	if status != "RUNNING" || attemptNum != 1 || workerID != "worker-1" {
		t.Errorf("unexpected attempt state: status=%s, attemptNum=%d, workerID=%s", status, attemptNum, workerID)
	}

	// Complete task run
	_ = repo.MarkTaskRunCompleted(ctx, claimed.TaskRunID, "worker-1", json.RawMessage(`{"data":"ok"}`))

	// Verify attempt transitioned to COMPLETED
	var output json.RawMessage
	err = repo.db.QueryRowContext(ctx, "SELECT status, output FROM task_attempts WHERE task_run_id = $1", claimed.TaskRunID).Scan(&status, &output)
	if err != nil {
		t.Fatalf("failed to query attempt: %v", err)
	}
	if status != "COMPLETED" || string(output) != `{"data": "ok"}` {
		t.Errorf("unexpected completed attempt state: status=%s, output=%s", status, output)
	}

	// === 2. Retry, Fail, Timeout Flow ===
	_, _ = repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed2, _ := repo.ClaimNextReadyTask(ctx, "worker-2")

	// Attempt 1: Start and fail normally
	_ = repo.StartTaskRun(ctx, claimed2.TaskRunID, "worker-2")
	_ = repo.MarkTaskRunFailed(ctx, claimed2.TaskRunID, "worker-2", "execution failed", false)

	// Verify attempt 1 is FAILED
	var failureType string
	err = repo.db.QueryRowContext(ctx, "SELECT status, failure_type FROM task_attempts WHERE task_run_id = $1 AND attempt_number = 1", claimed2.TaskRunID).Scan(&status, &failureType)
	if err != nil {
		t.Fatalf("failed to query attempt 1: %v", err)
	}
	if status != "FAILED" || failureType != "EXECUTION_ERROR" {
		t.Errorf("unexpected attempt 1 fail state: status=%s, failureType=%s", status, failureType)
	}

	// Promote and start attempt 2
	_, _ = repo.db.ExecContext(ctx, "UPDATE task_runs SET next_retry_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", claimed2.TaskRunID)
	_, _ = repo.PromoteDueRetries(ctx)
	claimed2_retry, _ := repo.ClaimNextReadyTask(ctx, "worker-2")
	_ = repo.StartTaskRun(ctx, claimed2_retry.TaskRunID, "worker-2")

	// Verify attempt 2 was created as RUNNING
	err = repo.db.QueryRowContext(ctx, "SELECT status, attempt_number FROM task_attempts WHERE task_run_id = $1 AND attempt_number = 2", claimed2.TaskRunID).Scan(&status, &attemptNum)
	if err != nil {
		t.Fatalf("failed to query attempt 2: %v", err)
	}
	if status != "RUNNING" || attemptNum != 2 {
		t.Errorf("unexpected attempt 2 running state: status=%s, attemptNum=%d", status, attemptNum)
	}

	// Fail attempt 2 as Timeout
	_ = repo.MarkTaskRunFailed(ctx, claimed2.TaskRunID, "worker-2", "execution timeout exceeded", true)

	// Verify attempt 2 is TIMED_OUT
	err = repo.db.QueryRowContext(ctx, "SELECT status, failure_type FROM task_attempts WHERE task_run_id = $1 AND attempt_number = 2", claimed2.TaskRunID).Scan(&status, &failureType)
	if err != nil {
		t.Fatalf("failed to query attempt 2 status: %v", err)
	}
	if status != "TIMED_OUT" || failureType != "TIMEOUT" {
		t.Errorf("unexpected attempt 2 timeout state: status=%s, failureType=%s", status, failureType)
	}

	// === 3. Stale RUNNING Recovery Flow ===
	_, _ = repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed3, _ := repo.ClaimNextReadyTask(ctx, "worker-3")
	_ = repo.StartTaskRun(ctx, claimed3.TaskRunID, "worker-3")

	// Force task run to be stale
	_, _ = repo.db.ExecContext(ctx, "UPDATE task_runs SET started_at = NOW() - INTERVAL '10 minutes' WHERE id = $1", claimed3.TaskRunID)

	// Recover stale running task
	recResult, err := repo.RecoverStaleTasks(ctx, 30*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("failed to recover stale tasks: %v", err)
	}
	if recResult.RunningRecovered != 1 {
		t.Errorf("expected 1 running task to be recovered, got %d", recResult.RunningRecovered)
	}

	// Verify attempt 1 was marked as ORPHANED with failure_type WORKER_LOST
	err = repo.db.QueryRowContext(ctx, "SELECT status, failure_type FROM task_attempts WHERE task_run_id = $1 AND attempt_number = 1", claimed3.TaskRunID).Scan(&status, &failureType)
	if err != nil {
		t.Fatalf("failed to query attempt status after recovery: %v", err)
	}
	if status != "ORPHANED" || failureType != "WORKER_LOST" {
		t.Errorf("unexpected recovered attempt state: status=%s, failureType=%s", status, failureType)
	}

	// Re-claim and re-start task
	reClaimed, _ := repo.ClaimNextReadyTask(ctx, "worker-4")
	_ = repo.StartTaskRun(ctx, reClaimed.TaskRunID, "worker-4")

	// Verify attempt 2 was created as RUNNING
	err = repo.db.QueryRowContext(ctx, "SELECT status, attempt_number, worker_id FROM task_attempts WHERE task_run_id = $1 AND attempt_number = 2", claimed3.TaskRunID).Scan(&status, &attemptNum, &workerID)
	if err != nil {
		t.Fatalf("failed to query attempt 2: %v", err)
	}
	if status != "RUNNING" || attemptNum != 2 || workerID != "worker-4" {
		t.Errorf("unexpected attempt 2 state: status=%s, attemptNum=%d, workerID=%s", status, attemptNum, workerID)
	}

	// === 4. Stale CLAIMED Recovery Flow (no attempt created) ===
	_, _ = repo.CreateWorkflowRun(ctx, def.ID, nil)
	claimed4, _ := repo.ClaimNextReadyTask(ctx, "worker-5")

	// Force claimed task to be stale
	_, _ = repo.db.ExecContext(ctx, "UPDATE task_runs SET claimed_at = NOW() - INTERVAL '10 minutes' WHERE id = $1", claimed4.TaskRunID)

	// Recover stale claimed task
	recResult2, _ := repo.RecoverStaleTasks(ctx, 30*time.Second, 30*time.Second)
	if recResult2.ClaimedRecovered != 1 {
		t.Errorf("expected 1 claimed task to be recovered, got %d", recResult2.ClaimedRecovered)
	}

	// Verify no attempt record exists for claimed4
	var count int
	err = repo.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM task_attempts WHERE task_run_id = $1", claimed4.TaskRunID).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query attempt count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 attempt records, got %d", count)
	}
}

func TestDLQIntegration(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// Register workflow with a sleep task (1 retry max)
	req := &model.CreateDefinitionRequest{
		Name:        "integration-test-dlq",
		Description: "testing DLQ generation",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     1,
				RetryBackoffMs: 100,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to create workflow def: %v", err)
	}

	// Create run
	_, _ = repo.CreateWorkflowRun(ctx, def.ID, nil)

	// Attempt 1: Start and fail (retryable)
	claimed, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	_ = repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-1")
	_ = repo.MarkTaskRunFailed(ctx, claimed.TaskRunID, "worker-1", "attempt 1 error", false)

	// Verify no DLQ record exists yet
	var count int
	_ = repo.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dead_letter_tasks WHERE task_run_id = $1", claimed.TaskRunID).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 DLQ records for retryable failure, got %d", count)
	}

	// Promote and start attempt 2 (final attempt)
	_, _ = repo.db.ExecContext(ctx, "UPDATE task_runs SET next_retry_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", claimed.TaskRunID)
	_, _ = repo.PromoteDueRetries(ctx)
	claimed_retry, _ := repo.ClaimNextReadyTask(ctx, "worker-1")
	_ = repo.StartTaskRun(ctx, claimed_retry.TaskRunID, "worker-1")

	// Fail attempt 2 (terminal timeout)
	_ = repo.MarkTaskRunFailed(ctx, claimed.TaskRunID, "worker-1", "attempt 2 timeout exceeded", true)

	// Verify exactly one DLQ record exists
	var taskRunID, workflowRunID, terminalStatus, failureType, reason, workerID string
	var finalAttempt int
	err = repo.db.QueryRowContext(ctx, `
		SELECT task_run_id, workflow_run_id, terminal_status, failure_type, reason, final_attempt, worker_id
		FROM dead_letter_tasks
		WHERE task_run_id = $1
	`, claimed.TaskRunID).Scan(&taskRunID, &workflowRunID, &terminalStatus, &failureType, &reason, &finalAttempt, &workerID)
	if err != nil {
		t.Fatalf("failed to query DLQ record: %v", err)
	}

	if taskRunID != claimed.TaskRunID || terminalStatus != "TIMED_OUT" || failureType != "TIMEOUT" || reason != "attempt 2 timeout exceeded" || finalAttempt != 2 || workerID != "worker-1" {
		t.Errorf("unexpected DLQ record fields: taskRunID=%s, terminalStatus=%s, failureType=%s, reason=%s, finalAttempt=%d, workerID=%s",
			taskRunID, terminalStatus, failureType, reason, finalAttempt, workerID)
	}
}
