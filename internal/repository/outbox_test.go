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
	sequence := int64(1)

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
