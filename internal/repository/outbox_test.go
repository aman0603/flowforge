//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aman0603/flowforge/internal/model"
)

func TestOutboxSchemaAndSerialization(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Create a dummy workflow definition and run to satisfy foreign key constraints
	defReq := &model.CreateDefinitionRequest{
		Name:        "outbox-test-workflow",
		Description: "test outbox schema constraints",
		Tasks:       []model.TaskDefinitionInput{},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, defReq)
	if err != nil {
		t.Fatalf("failed to create workflow definition: %v", err)
	}

	run, err := repo.CreateWorkflowRun(ctx, def.ID, json.RawMessage(`{"key": "value"}`))
	if err != nil {
		t.Fatalf("failed to create workflow run: %v", err)
	}

	// 2. Test case: verify we can insert and retrieve an outbox event
	eventID := newUUID()
	eventType := model.EventWorkflowStarted
	eventVersion := 1
	aggregateType := "workflow_run"
	aggregateID := run.ID
	sequence := int64(2)

	payload := model.WorkflowStartedPayload{
		WorkflowDefinitionID: def.ID,
		Input:                json.RawMessage(`{"key": "value"}`),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	insertQuery := `
		INSERT INTO outbox_events (
			id, event_type, event_version, aggregate_type, aggregate_id, 
			workflow_run_id, sequence, payload, created_at, available_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	occurredAt := time.Now().UTC()
	_, err = repo.db.ExecContext(ctx, insertQuery,
		eventID, eventType, eventVersion, aggregateType, aggregateID,
		run.ID, sequence, payloadBytes, occurredAt, occurredAt,
	)
	if err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}

	// 3. Retrieve the event and verify JSON preservation and serialization
	var retrievedID, retrievedType, retrievedAggType, retrievedWorkflowRunID string
	var retrievedAggID string
	var retrievedVersion int
	var retrievedSequence int64
	var retrievedPayload json.RawMessage
	var retrievedCreatedAt, retrievedAvailableAt time.Time

	selectQuery := `
		SELECT id, event_type, event_version, aggregate_type, aggregate_id, 
		       workflow_run_id, sequence, payload, created_at, available_at
		FROM outbox_events
		WHERE id = $1
	`
	err = repo.db.QueryRowContext(ctx, selectQuery, eventID).Scan(
		&retrievedID, &retrievedType, &retrievedVersion, &retrievedAggType, &retrievedAggID,
		&retrievedWorkflowRunID, &retrievedSequence, &retrievedPayload, &retrievedCreatedAt, &retrievedAvailableAt,
	)
	if err != nil {
		t.Fatalf("failed to query outbox event: %v", err)
	}

	if retrievedID != eventID {
		t.Errorf("expected ID %s, got %s", eventID, retrievedID)
	}
	if retrievedType != eventType {
		t.Errorf("expected Type %s, got %s", eventType, retrievedType)
	}
	if retrievedVersion != eventVersion {
		t.Errorf("expected Version %d, got %d", eventVersion, retrievedVersion)
	}
	if retrievedAggType != aggregateType {
		t.Errorf("expected AggregateType %s, got %s", aggregateType, retrievedAggType)
	}
	if retrievedAggID != aggregateID {
		t.Errorf("expected AggregateID %s, got %s", aggregateID, retrievedAggID)
	}
	if retrievedWorkflowRunID != run.ID {
		t.Errorf("expected WorkflowRunID %s, got %s", run.ID, retrievedWorkflowRunID)
	}
	if retrievedSequence != sequence {
		t.Errorf("expected Sequence %d, got %d", sequence, retrievedSequence)
	}

	// Check payload preservation
	var retrievedPayloadStruct model.WorkflowStartedPayload
	if err := json.Unmarshal(retrievedPayload, &retrievedPayloadStruct); err != nil {
		t.Fatalf("failed to unmarshal retrieved payload: %v", err)
	}
	if retrievedPayloadStruct.WorkflowDefinitionID != def.ID {
		t.Errorf("expected payload definition ID %s, got %s", def.ID, retrievedPayloadStruct.WorkflowDefinitionID)
	}

	// 4. Verify sequence uniqueness constraint (workflow_run_id, sequence)
	duplicateEventID := newUUID()
	_, err = repo.db.ExecContext(ctx, insertQuery,
		duplicateEventID, eventType, eventVersion, aggregateType, aggregateID,
		run.ID, sequence, payloadBytes, occurredAt, occurredAt,
	)
	if err == nil {
		t.Error("expected duplicate sequence insert to fail due to unique constraint, but it succeeded")
	}
}

func TestAtomicRepositoryEventInsertion(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Create a definition with tasks
	defReq := &model.CreateDefinitionRequest{
		Name:        "atomic-outbox-workflow",
		Description: "testing atomic event insertion",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "TEST",
				Config:         json.RawMessage(`{}`),
				MaxRetries:     1,
				RetryBackoffMs: 10,
				TimeoutMs:      100,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, defReq)
	if err != nil {
		t.Fatalf("failed to create workflow definition: %v", err)
	}

	// Create run - should emit WorkflowStarted (sequence 1)
	run, err := repo.CreateWorkflowRun(ctx, def.ID, json.RawMessage(`{"input": "data"}`))
	if err != nil {
		t.Fatalf("failed to create workflow run: %v", err)
	}

	// Verify WorkflowStarted outbox event
	events := getOutboxEvents(t, repo, run.ID)
	if len(events) != 1 {
		t.Fatalf("expected 1 outbox event, got %d", len(events))
	}
	if events[0].EventType != model.EventWorkflowStarted {
		t.Errorf("expected EventWorkflowStarted, got %s", events[0].EventType)
	}
	if events[0].Sequence != 1 {
		t.Errorf("expected sequence 1, got %d", events[0].Sequence)
	}

	// Get tasks to start
	details, err := repo.GetWorkflowRunDetails(ctx, run.ID)
	if err != nil {
		t.Fatalf("failed to get run details: %v", err)
	}
	if len(details.Tasks) != 1 {
		t.Fatalf("expected 1 task run, got %d", len(details.Tasks))
	}
	taskRun := details.Tasks[0]

	// Try claiming and starting the task
	// First, let's claim it to put it in CLAIMED state
	claimed, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim task: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected to claim ready task")
	}

	// Verify claiming task does not emit events (only transitions to CLAIMED)
	events = getOutboxEvents(t, repo, run.ID)
	if len(events) != 1 {
		t.Errorf("expected events count to remain 1 after claim, got %d", len(events))
	}

	// Try starting the task run with stale fencing token (should fail)
	err = repo.StartTaskRun(ctx, taskRun.ID, "worker-1", 99999) // stale fencing token
	if err == nil {
		t.Error("expected start task to fail with stale fencing token")
	}
	// Verify no new event was inserted
	events = getOutboxEvents(t, repo, run.ID)
	if len(events) != 1 {
		t.Errorf("expected events count to remain 1 after failed start, got %d", len(events))
	}

	// Start task correctly (should transition task to RUNNING and emit TaskStarted sequence 2)
	err = repo.StartTaskRun(ctx, taskRun.ID, "worker-1", claimed.FencingToken)
	if err != nil {
		t.Fatalf("failed to start task: %v", err)
	}

	events = getOutboxEvents(t, repo, run.ID)
	if len(events) != 2 {
		t.Fatalf("expected 2 outbox events after task start, got %d", len(events))
	}
	if events[1].EventType != model.EventTaskStarted {
		t.Errorf("expected EventTaskStarted, got %s", events[1].EventType)
	}
	if events[1].Sequence != 2 {
		t.Errorf("expected sequence 2, got %d", events[1].Sequence)
	}

	// Test case: Task Failure with Retry remaining (transitions to RETRY_WAIT, emits TaskFailed + RetryScheduled)
	err = repo.MarkTaskRunFailed(ctx, taskRun.ID, "worker-1", "test failure", false, claimed.FencingToken)
	if err != nil {
		t.Fatalf("failed to mark task failed: %v", err)
	}

	events = getOutboxEvents(t, repo, run.ID)
	if len(events) != 4 {
		t.Fatalf("expected 4 outbox events after task retry scheduling, got %d", len(events))
	}
	if events[2].EventType != model.EventTaskFailed {
		t.Errorf("expected EventTaskFailed, got %s", events[2].EventType)
	}
	if events[2].Sequence != 3 {
		t.Errorf("expected sequence 3, got %d", events[2].Sequence)
	}
	if events[3].EventType != model.EventRetryScheduled {
		t.Errorf("expected EventRetryScheduled, got %s", events[3].EventType)
	}
	if events[3].Sequence != 4 {
		t.Errorf("expected sequence 4, got %d", events[3].Sequence)
	}

	// Test case: Retry Promotion (RETRY_WAIT -> READY, emits RetryPromoted)
	// We force-promote due retries by waiting or setting the next_retry_at to past
	_, err = repo.db.ExecContext(ctx, "UPDATE task_runs SET next_retry_at = NOW() - INTERVAL '1 second' WHERE id = $1", taskRun.ID)
	if err != nil {
		t.Fatalf("failed to set next_retry_at: %v", err)
	}

	promotedCount, err := repo.PromoteDueRetries(ctx)
	if err != nil {
		t.Fatalf("failed to promote due retries: %v", err)
	}
	if promotedCount != 1 {
		t.Fatalf("expected 1 task promoted, got %d", promotedCount)
	}

	events = getOutboxEvents(t, repo, run.ID)
	if len(events) != 5 {
		t.Fatalf("expected 5 outbox events after retry promotion, got %d", len(events))
	}
	if events[4].EventType != model.EventRetryPromoted {
		t.Errorf("expected EventRetryPromoted, got %s", events[4].EventType)
	}
	if events[4].Sequence != 5 {
		t.Errorf("expected sequence 5, got %d", events[4].Sequence)
	}

	// Test case: Claim, Start, and Terminally fail task (exhaust retries, emits TaskFailed, RetryExhausted, DLQCreated, WorkflowFailed)
	claimed2, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim task 2nd time: %v", err)
	}
	if claimed2 == nil {
		t.Fatal("expected to claim task 2nd time")
	}

	err = repo.StartTaskRun(ctx, taskRun.ID, "worker-1", claimed2.FencingToken)
	if err != nil {
		t.Fatalf("failed to start task 2nd time: %v", err)
	}

	// Emits TaskStarted (sequence 6)
	events = getOutboxEvents(t, repo, run.ID)
	if len(events) != 6 {
		t.Fatalf("expected 6 outbox events, got %d", len(events))
	}
	if events[5].EventType != model.EventTaskStarted {
		t.Errorf("expected EventTaskStarted, got %s", events[5].EventType)
	}
	if events[5].Sequence != 6 {
		t.Errorf("expected sequence 6, got %d", events[5].Sequence)
	}

	// Terminally fail task run
	err = repo.MarkTaskRunFailed(ctx, taskRun.ID, "worker-1", "terminal failure", false, claimed2.FencingToken)
	if err != nil {
		t.Fatalf("failed to fail task terminally: %v", err)
	}

	// Emits:
	// - TaskFailed (sequence 7)
	// - RetryExhausted (sequence 8)
	// - DLQCreated (sequence 9)
	// - WorkflowFailed (sequence 10)
	events = getOutboxEvents(t, repo, run.ID)
	if len(events) != 10 {
		t.Fatalf("expected 10 outbox events, got %d", len(events))
	}
	expectedEvents := []string{
		model.EventWorkflowStarted,
		model.EventTaskStarted,
		model.EventTaskFailed,
		model.EventRetryScheduled,
		model.EventRetryPromoted,
		model.EventTaskStarted,
		model.EventTaskFailed,
		model.EventRetryExhausted,
		model.EventDLQCreated,
		model.EventWorkflowFailed,
	}
	for i, expected := range expectedEvents {
		if events[i].EventType != expected {
			t.Errorf("event at index %d: expected %s, got %s", i, expected, events[i].EventType)
		}
		if events[i].Sequence != int64(i+1) {
			t.Errorf("event at index %d: expected sequence %d, got %d", i, i+1, events[i].Sequence)
		}
	}
}

func TestOutboxRecoveryEvents(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()

	// 1. Create a dummy workflow
	defReq := &model.CreateDefinitionRequest{
		Name:        "recovery-events-workflow",
		Description: "testing recovery events",
		Tasks: []model.TaskDefinitionInput{
			{
				Name:           "TaskA",
				TaskType:       "TEST",
				Config:         json.RawMessage(`{}`),
				MaxRetries:     0,
				RetryBackoffMs: 10,
				TimeoutMs:      100,
				Dependencies:   []string{},
			},
		},
	}
	def, err := repo.CreateWorkflowDefinition(ctx, defReq)
	if err != nil {
		t.Fatalf("failed to create workflow definition: %v", err)
	}

	run, err := repo.CreateWorkflowRun(ctx, def.ID, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("failed to create workflow run: %v", err)
	}

	claimed, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim task: %v", err)
	}

	// Test case: RecoverClaimedTask (emits TaskRecovered)
	recovered, err := repo.RecoverClaimedTask(ctx, claimed.TaskRunID, claimed.FencingToken)
	if err != nil {
		t.Fatalf("failed to recover claimed task: %v", err)
	}
	if !recovered {
		t.Fatal("expected task to be recovered")
	}

	events := getOutboxEvents(t, repo, run.ID)
	if len(events) != 2 {
		t.Fatalf("expected 2 outbox events (WorkflowStarted, TaskRecovered), got %d", len(events))
	}
	if events[1].EventType != model.EventTaskRecovered {
		t.Errorf("expected EventTaskRecovered, got %s", events[1].EventType)
	}
	if events[1].Sequence != 2 {
		t.Errorf("expected sequence 2, got %d", events[1].Sequence)
	}

	// Start task
	claimed2, err := repo.ClaimNextReadyTask(ctx, "worker-1")
	if err != nil {
		t.Fatalf("failed to claim task again: %v", err)
	}
	err = repo.StartTaskRun(ctx, claimed2.TaskRunID, "worker-1", claimed2.FencingToken)
	if err != nil {
		t.Fatalf("failed to start task: %v", err)
	}

	// Test case: RecoverRunningTask (emits TaskRecovered)
	recovered, err = repo.RecoverRunningTask(ctx, claimed2.TaskRunID, claimed2.FencingToken)
	if err != nil {
		t.Fatalf("failed to recover running task: %v", err)
	}
	if !recovered {
		t.Fatal("expected running task to be recovered")
	}

	events = getOutboxEvents(t, repo, run.ID)
	// Events so far: WorkflowStarted (1), TaskRecovered (2), TaskStarted (3), TaskRecovered (4)
	if len(events) != 4 {
		t.Fatalf("expected 4 outbox events, got %d", len(events))
	}
	if events[3].EventType != model.EventTaskRecovered {
		t.Errorf("expected EventTaskRecovered, got %s", events[3].EventType)
	}
	if events[3].Sequence != 4 {
		t.Errorf("expected sequence 4, got %d", events[3].Sequence)
	}
}

// Helper to query and sort outbox events from the DB
func getOutboxEvents(t *testing.T, repo *Repository, workflowRunID string) []model.OutboxEvent {
	rows, err := repo.db.Query(`
		SELECT id, event_type, event_version, aggregate_type, aggregate_id, workflow_run_id, sequence, payload, created_at
		FROM outbox_events
		WHERE workflow_run_id = $1
		ORDER BY sequence ASC
	`, workflowRunID)
	if err != nil {
		t.Fatalf("failed to query outbox events: %v", err)
	}
	defer rows.Close()

	var events []model.OutboxEvent
	for rows.Next() {
		var e model.OutboxEvent
		err := rows.Scan(&e.ID, &e.EventType, &e.EventVersion, &e.AggregateType, &e.AggregateID, &e.WorkflowRunID, &e.Sequence, &e.Payload, &e.CreatedAt)
		if err != nil {
			t.Fatalf("failed to scan outbox event: %v", err)
		}
		events = append(events, e)
	}
	return events
}
