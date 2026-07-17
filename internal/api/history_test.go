package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/repository"
)

func setupAPITestDB(t *testing.T) *repository.Repository {
	dbURL := os.Getenv("TEST_DB_URL")
	if dbURL == "" {
		t.Skip("TEST_DB_URL is missing, skipping API integration tests")
	}

	repo, err := repository.New(dbURL)
	if err != nil {
		t.Fatalf("failed to connect to test database specified in TEST_DB_URL: %v", err)
	}

	// Drop existing tables to ensure latest schema is always provisioned
	dropQueries := []string{
		"DROP TABLE IF EXISTS outbox_events CASCADE;",
		"DROP TABLE IF EXISTS dead_letter_tasks CASCADE;",
		"DROP TABLE IF EXISTS task_attempts CASCADE;",
		"DROP TABLE IF EXISTS task_dependencies CASCADE;",
		"DROP TABLE IF EXISTS task_runs CASCADE;",
		"DROP TABLE IF EXISTS task_definitions CASCADE;",
		"DROP TABLE IF EXISTS workflow_runs CASCADE;",
		"DROP TABLE IF EXISTS workflow_definitions CASCADE;",
	}
	for _, q := range dropQueries {
		if _, err := repo.DB().Exec(q); err != nil {
			repo.Close()
			t.Fatalf("failed to drop test table: %v", err)
		}
	}

	// Initialize the schema dynamically from schema.sql
	schemaPath := "../../schema.sql"
	if err := repo.InitializeSchema(schemaPath); err != nil {
		repo.Close()
		t.Fatalf("failed to initialize schema: %v", err)
	}

	return repo
}

func TestAPIHistoryAndDLQEndpoints(t *testing.T) {
	repo := setupAPITestDB(t)
	cfg := &config.Config{Port: "8080", Env: "test"}
	server := NewServer(cfg, repo)

	ctx := context.Background()

	// 1. Register a workflow definition
	reqBody := &model.CreateDefinitionRequest{
		Name:        "api-history-test",
		Description: "testing api history and dlq endpoints",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "SLEEP",
				Config:         json.RawMessage(`{"duration_ms": 10}`),
				MaxRetries:     0,
				RetryBackoffMs: 100,
				TimeoutMs:      5000,
				Dependencies:   []string{},
			},
		},
	}

	def, err := repo.CreateWorkflowDefinition(ctx, reqBody)
	if err != nil {
		t.Fatalf("failed to create definition: %v", err)
	}

	// 2. Instantiate a workflow run
	run, err := repo.CreateWorkflowRun(ctx, def.ID, nil)
	if err != nil {
		t.Fatalf("failed to create workflow run: %v", err)
	}

	// Claim and start the task
	claimed, err := repo.ClaimNextReadyTask(ctx, "worker-api-1")
	if err != nil {
		t.Fatalf("failed to claim task: %v", err)
	}
	err = repo.StartTaskRun(ctx, claimed.TaskRunID, "worker-api-1")
	if err != nil {
		t.Fatalf("failed to start task: %v", err)
	}

	// Verify GET /api/v1/tasks/{task_run_id}/attempts returns RUNNING attempt
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+claimed.TaskRunID+"/attempts", nil)
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	var attempts []model.TaskAttempt
	if err := json.NewDecoder(w.Body).Decode(&attempts); err != nil {
		t.Fatalf("failed to decode attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Status != "RUNNING" {
		t.Errorf("unexpected attempts response: %+v", attempts)
	}

	// Fail the task (it has 0 max retries, so it will exhaust retries and go to DLQ)
	err = repo.MarkTaskRunFailed(ctx, claimed.TaskRunID, "worker-api-1", "api error", false)
	if err != nil {
		t.Fatalf("failed to mark task failed: %v", err)
	}

	// Verify GET /api/v1/runs/{run_id}/history
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+run.ID+"/history", nil)
	server.router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w2.Code)
	}
	var history model.WorkflowHistoryResponse
	if err := json.NewDecoder(w2.Body).Decode(&history); err != nil {
		t.Fatalf("failed to decode history: %v", err)
	}
	if history.WorkflowStatus != "FAILED" || len(history.Tasks) != 1 || len(history.Tasks[0].Attempts) != 1 {
		t.Errorf("unexpected history response: %+v", history)
	}

	// Verify GET /api/v1/dead-letter
	w3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/api/v1/dead-letter?limit=10&offset=0", nil)
	server.router.ServeHTTP(w3, req3)

	if w3.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w3.Code)
	}
	var dlqs []model.DeadLetterTask
	if err := json.NewDecoder(w3.Body).Decode(&dlqs); err != nil {
		t.Fatalf("failed to decode DLQ: %v", err)
	}
	if len(dlqs) != 1 || dlqs[0].TerminalStatus != "FAILED" || *dlqs[0].Reason != "api error" {
		t.Errorf("unexpected DLQ response: %+v", dlqs)
	}
}
