package model

import (
	"encoding/json"
	"time"
)

// Workflow states
const (
	WorkflowPending   = "PENDING"
	WorkflowRunning   = "RUNNING"
	WorkflowCompleted = "COMPLETED"
	WorkflowFailed    = "FAILED"
	WorkflowCancelled = "CANCELLED"
)

// Task states
const (
	TaskPending   = "PENDING"
	TaskReady     = "READY"
	TaskClaimed   = "CLAIMED"
	TaskRunning   = "RUNNING"
	TaskCompleted = "COMPLETED"
	TaskFailed    = "FAILED"
	TaskSkipped   = "SKIPPED"
	TaskTimedOut  = "TIMED_OUT"
)

// WorkflowDefinition represents a workflow template.
type WorkflowDefinition struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}

// TaskDefinition represents a task template within a workflow.
type TaskDefinition struct {
	ID                   string          `json:"id"`
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Name                 string          `json:"name"`
	TaskType             string          `json:"task_type"`
	Config               json.RawMessage `json:"config"`
	MaxRetries           int             `json:"max_retries"`
	RetryBackoffMs       int             `json:"retry_backoff_ms"`
	TimeoutMs            int             `json:"timeout_ms"`
	CreatedAt            time.Time       `json:"created_at"`
}

// TaskDependency represents a DAG edge between task definitions.
type TaskDependency struct {
	WorkflowDefinitionID      string `json:"workflow_definition_id"`
	TaskDefinitionID          string `json:"task_definition_id"`
	DependsOnTaskDefinitionID string `json:"depends_on_task_definition_id"`
}

// WorkflowRun represents an execution instance of a workflow.
type WorkflowRun struct {
	ID                   string          `json:"id"`
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Status               string          `json:"status"`
	Input                json.RawMessage `json:"input"`
	Output               json.RawMessage `json:"output"`
	ErrorMessage         *string         `json:"error_message,omitempty"`
	StartedAt            *time.Time      `json:"started_at,omitempty"`
	CompletedAt          *time.Time      `json:"completed_at,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
}

// TaskRun represents an execution instance of a task.
type TaskRun struct {
	ID               string          `json:"id"`
	WorkflowRunID    string          `json:"workflow_run_id"`
	TaskDefinitionID string          `json:"task_definition_id"`
	Status           string          `json:"status"`
	Attempts         int             `json:"attempts"`
	Input            json.RawMessage `json:"input"`
	Output           json.RawMessage `json:"output"`
	ErrorMessage     *string         `json:"error_message,omitempty"`
	NextRetryAt      *time.Time      `json:"next_retry_at,omitempty"`
	WorkerID         *string         `json:"worker_id,omitempty"`
	ClaimedAt        *time.Time      `json:"claimed_at,omitempty"`
	StartedAt        *time.Time      `json:"started_at,omitempty"`
	CompletedAt      *time.Time      `json:"completed_at,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
}

// TaskDefinitionInput represents input properties for a task during workflow registration.
type TaskDefinitionInput struct {
	Name           string          `json:"name"`
	TaskType       string          `json:"task_type"`
	Config         json.RawMessage `json:"config"`
	MaxRetries     int             `json:"max_retries"`
	RetryBackoffMs int             `json:"retry_backoff_ms"`
	TimeoutMs      int             `json:"timeout_ms"`
	Dependencies   []string        `json:"dependencies"` // List of parent task names
}

// CreateDefinitionRequest represents the registration payload.
type CreateDefinitionRequest struct {
	Name        string                `json:"name"`
	Description string                `json:"description"`
	Tasks       []TaskDefinitionInput `json:"tasks"`
}

// CreateRunRequest represents the run request payload.
type CreateRunRequest struct {
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Input                json.RawMessage `json:"input"`
}

// WorkflowRunDetails response payload providing complete run status and tasks status.
type WorkflowRunDetails struct {
	Run   *WorkflowRun `json:"run"`
	Tasks []*TaskRun   `json:"tasks"`
}

// ClaimedTask represents an execution-ready task run combined with its definition configurations.
type ClaimedTask struct {
	TaskRunID        string          `json:"task_run_id"`
	WorkflowRunID    string          `json:"workflow_run_id"`
	TaskDefinitionID string          `json:"task_definition_id"`
	Name             string          `json:"name"`
	TaskType         string          `json:"task_type"`
	Config           json.RawMessage `json:"config"`
	Input            json.RawMessage `json:"input"`
	TimeoutMs        int             `json:"timeout_ms"`
}
