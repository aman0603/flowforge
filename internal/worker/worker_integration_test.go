//go:build integration

package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/repository"
	_ "github.com/lib/pq"
)

func setupIntegrationTestDB(t *testing.T) *repository.Repository {
	dbURL := os.Getenv("TEST_DB_URL")
	if dbURL == "" {
		t.Skip("TEST_DB_URL is missing, skipping worker database integration tests")
	}

	// Open direct connection to clean up schema
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Fatalf("failed to open direct connection for test setup: %v", err)
	}
	defer db.Close()

	dropQueries := []string{
		"DROP TABLE IF EXISTS task_dependencies CASCADE;",
		"DROP TABLE IF EXISTS task_runs CASCADE;",
		"DROP TABLE IF EXISTS task_definitions CASCADE;",
		"DROP TABLE IF EXISTS workflow_runs CASCADE;",
		"DROP TABLE IF EXISTS workflow_definitions CASCADE;",
	}
	for _, q := range dropQueries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("failed to drop test table: %v", err)
		}
	}

	repo, err := repository.New(dbURL)
	if err != nil {
		t.Fatalf("failed to connect to integration test DB: %v", err)
	}

	err = repo.InitializeSchema("../../schema.sql")
	if err != nil {
		repo.Close()
		t.Fatalf("failed to initialize schema: %v", err)
	}

	t.Cleanup(func() {
		repo.Close()
	})

	return repo
}

func TestWorkerTimeoutIntegration(t *testing.T) {
	repo := setupIntegrationTestDB(t)
	dbURL := os.Getenv("TEST_DB_URL")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Fatalf("failed to open direct connection for assertions: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &model.CreateDefinitionRequest{
		Name:        "timeout-workflow",
		Description: "testing task timeout",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 500}`),
				MaxRetries:     1,
				RetryBackoffMs: 100,
				TimeoutMs:      50, // 50ms timeout < 500ms duration
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	run, _ := repo.CreateWorkflowRun(ctx, def.ID, nil)

	// Create and start worker
	w := New(
		"timeout-worker",
		repo,
		map[string]Executor{"SLEEP": NewSleepExecutor()},
		1*time.Millisecond,
		50*time.Millisecond,
		50*time.Millisecond,
		50*time.Millisecond,
	)

	// Start recovery/retry sweep loop in background
	go w.Run(ctx)

	// Wait for the task to be executed, timed out, and rescheduled (RETRY_WAIT)
	var status1 string
	var attempts1 int
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		_ = db.QueryRowContext(ctx, "SELECT status, attempts FROM task_runs WHERE workflow_run_id = $1", run.ID).Scan(&status1, &attempts1)
		if status1 == "RETRY_WAIT" {
			break
		}
	}

	if status1 != "RETRY_WAIT" {
		t.Errorf("expected task run status to become RETRY_WAIT, got %s (attempts=%d)", status1, attempts1)
	}

	// Now wait for the second attempt.
	var status2 string
	var runStatus string
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		_ = db.QueryRowContext(ctx, "SELECT status FROM task_runs WHERE workflow_run_id = $1", run.ID).Scan(&status2)
		_ = db.QueryRowContext(ctx, "SELECT status FROM workflow_runs WHERE id = $1", run.ID).Scan(&runStatus)
		if status2 == "TIMED_OUT" && runStatus == "FAILED" {
			break
		}
	}

	if status2 != "TIMED_OUT" {
		t.Errorf("expected task run status to become TIMED_OUT, got %s", status2)
	}
	if runStatus != "FAILED" {
		t.Errorf("expected workflow run status to become FAILED, got %s", runStatus)
	}
}

func TestWorkerShutdownDoesNotConsumeAttempts(t *testing.T) {
	repo := setupIntegrationTestDB(t)
	dbURL := os.Getenv("TEST_DB_URL")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Fatalf("failed to open direct connection for assertions: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	req := &model.CreateDefinitionRequest{
		Name:        "shutdown-workflow",
		Description: "testing worker shutdown context cancellation",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 2000}`), // Sleep for 2 seconds
				MaxRetries:     3,
				RetryBackoffMs: 100,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, req)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	run, _ := repo.CreateWorkflowRun(ctx, def.ID, nil)

	// Create and start worker
	w := New(
		"shutdown-worker",
		repo,
		map[string]Executor{"SLEEP": NewSleepExecutor()},
		1*time.Millisecond,
		50*time.Millisecond,
		50*time.Millisecond,
		50*time.Millisecond,
	)

	// Start worker loop in background
	go w.Run(ctx)

	// Wait until task is RUNNING
	var status string
	var attempts int
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		_ = db.QueryRowContext(ctx, "SELECT status, attempts FROM task_runs WHERE workflow_run_id = $1", run.ID).Scan(&status, &attempts)
		if status == "RUNNING" {
			break
		}
	}

	if status != "RUNNING" {
		t.Fatalf("expected task to enter RUNNING, got %s", status)
	}

	// Trigger worker shutdown by canceling context
	cancel()

	// Wait 200ms and verify task status remains RUNNING and attempts is still 1
	time.Sleep(200 * time.Millisecond)

	var finalStatus string
	var finalAttempts int
	_ = db.QueryRowContext(context.Background(), "SELECT status, attempts FROM task_runs WHERE workflow_run_id = $1", run.ID).Scan(&finalStatus, &finalAttempts)

	if finalStatus != "RUNNING" {
		t.Errorf("expected task to remain RUNNING after worker shutdown, got %s", finalStatus)
	}
	if finalAttempts != 1 {
		t.Errorf("expected attempts to remain 1, got %d", finalAttempts)
	}
}
