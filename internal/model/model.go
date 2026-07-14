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
	TaskRetryWait = "RETRY_WAIT"
)

// Task attempt states
const (
	AttemptRunning   = "RUNNING"
	AttemptCompleted = "COMPLETED"
	AttemptFailed    = "FAILED"
	AttemptTimedOut  = "TIMED_OUT"
	AttemptOrphaned  = "ORPHANED"
)

// Failure classification types
const (
	FailureExecutionError = "EXECUTION_ERROR"
	FailureTimeout        = "TIMEOUT"
	FailureWorkerLost     = "WORKER_LOST"
	FailureRetryExhausted = "RETRY_EXHAUSTED"
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
	Priority             int             `json:"priority"`
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
	FencingToken     int64           `json:"fencing_token"`
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
	Priority       int             `json:"priority"`
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
	FencingToken     int64           `json:"fencing_token"`
}

// RecoveryResult represents the outcome of a stale task recovery operation.
type RecoveryResult struct {
	ClaimedRecovered int64 `json:"claimed_recovered"`
	RunningRecovered int64 `json:"running_recovered"`
}

// TaskAttempt represents a single execution attempt of a task.
type TaskAttempt struct {
	ID            string          `json:"id"`
	TaskRunID     string          `json:"task_run_id"`
	WorkflowRunID string          `json:"workflow_run_id"`
	AttemptNumber int             `json:"attempt_number"`
	WorkerID      string          `json:"worker_id"`
	Status        string          `json:"status"`
	ClaimedAt     *time.Time      `json:"claimed_at,omitempty"`
	StartedAt     time.Time       `json:"started_at"`
	CompletedAt   *time.Time      `json:"completed_at,omitempty"`
	DurationMs    *int64          `json:"duration_ms,omitempty"`
	Output        json.RawMessage `json:"output"`
	ErrorMessage  *string         `json:"error_message,omitempty"`
	FailureType   *string         `json:"failure_type,omitempty"`
	FencingToken  int64           `json:"fencing_token"`
	CreatedAt     time.Time       `json:"created_at"`
}

// DeadLetterTask represents a terminally failed task run stored in the DLQ.
type DeadLetterTask struct {
	ID               string    `json:"id"`
	TaskRunID        string    `json:"task_run_id"`
	WorkflowRunID    string    `json:"workflow_run_id"`
	TaskDefinitionID string    `json:"task_definition_id"`
	TerminalStatus   string    `json:"terminal_status"`
	FailureType      string    `json:"failure_type"`
	Reason           *string   `json:"reason,omitempty"`
	FinalAttempt     int       `json:"final_attempt"`
	WorkerID         *string   `json:"worker_id,omitempty"`
	DeadLetteredAt   time.Time `json:"dead_lettered_at"`
	CreatedAt        time.Time `json:"created_at"`
}

// WorkflowHistoryResponse represents the execution history of a workflow run.
type WorkflowHistoryResponse struct {
	WorkflowRunID  string                `json:"workflow_run_id"`
	WorkflowStatus string                `json:"workflow_status"`
	Tasks          []TaskHistoryResponse `json:"tasks"`
}

// TaskHistoryResponse represents a task run and its execution attempts.
type TaskHistoryResponse struct {
	TaskRunID string        `json:"task_run_id"`
	TaskName  string        `json:"task_name"`
	Status    string        `json:"status"`
	Attempts  []TaskAttempt `json:"attempts"`
}
