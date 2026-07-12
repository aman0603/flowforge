package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aman0603/flowforge/internal/model"
	_ "github.com/lib/pq" // Postgres driver
)

// Repository manages database operations for FlowForge.
type Repository struct {
	db *sql.DB
}

// New initializes a new Postgres Repository and connects to the database.
func New(dbURL string) (*Repository, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Ping database to verify connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Repository{db: db}, nil
}

// Close closes the database connection.
func (r *Repository) Close() error {
	return r.db.Close()
}

// InitializeSchema reads and runs the SQL schema file to provision the tables.
func (r *Repository) InitializeSchema(schemaPath string) error {
	file, err := os.Open(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to open schema file: %w", err)
	}
	defer file.Close()

	schemaSQL, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	// Execute the schema SQL
	if _, err := r.db.Exec(string(schemaSQL)); err != nil {
		return fmt.Errorf("failed to execute schema: %w", err)
	}

	return nil
}

// CreateWorkflowDefinition registers a new workflow and its tasks inside a transaction.
func (r *Repository) CreateWorkflowDefinition(ctx context.Context, req *model.CreateDefinitionRequest) (*model.WorkflowDefinition, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	workflowID := newUUID()
	now := time.Now()

	// 1. Insert Workflow Definition
	const insertWorkflow = `
		INSERT INTO workflow_definitions (id, name, description, created_at)
		VALUES ($1, $2, $3, $4)
	`
	_, err = tx.ExecContext(ctx, insertWorkflow, workflowID, req.Name, req.Description, now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert workflow definition: %w", err)
	}

	// 2. Insert Task Definitions & map names to IDs
	taskNameToID := make(map[string]string)
	type taskWithDeps struct {
		id           string
		name         string
		dependencies []string
	}
	tasksToProcess := make([]taskWithDeps, len(req.Tasks))

	const insertTask = `
		INSERT INTO task_definitions (id, workflow_definition_id, name, task_type, config, max_retries, retry_backoff_ms, timeout_ms, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	for i, t := range req.Tasks {
		taskID := newUUID()
		taskNameToID[t.Name] = taskID
		tasksToProcess[i] = taskWithDeps{
			id:           taskID,
			name:         t.Name,
			dependencies: t.Dependencies,
		}

		configJSON, err := json.Marshal(t.Config)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal config for task %s: %w", t.Name, err)
		}

		_, err = tx.ExecContext(ctx, insertTask,
			taskID,
			workflowID,
			t.Name,
			t.TaskType,
			configJSON,
			t.MaxRetries,
			t.RetryBackoffMs,
			t.TimeoutMs,
			now,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to insert task definition %s: %w", t.Name, err)
		}
	}

	// 3. Insert Task Dependencies
	const insertDependency = `
		INSERT INTO task_dependencies (workflow_definition_id, task_definition_id, depends_on_task_definition_id)
		VALUES ($1, $2, $3)
	`
	for _, t := range tasksToProcess {
		for _, depName := range t.dependencies {
			parentID, exists := taskNameToID[depName]
			if !exists {
				return nil, fmt.Errorf("dependency %s for task %s does not exist", depName, t.name)
			}

			_, err = tx.ExecContext(ctx, insertDependency, workflowID, t.id, parentID)
			if err != nil {
				return nil, fmt.Errorf("failed to insert dependency for task %s on %s: %w", t.name, depName, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &model.WorkflowDefinition{
		ID:          workflowID,
		Name:        req.Name,
		Description: req.Description,
		CreatedAt:   now,
	}, nil
}

// CreateWorkflowRun instantiates a run of a workflow definition inside a transaction.
func (r *Repository) CreateWorkflowRun(ctx context.Context, definitionID string, input json.RawMessage) (*model.WorkflowRun, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Verify workflow definition exists
	var dummy string
	err = tx.QueryRowContext(ctx, "SELECT id FROM workflow_definitions WHERE id = $1", definitionID).Scan(&dummy)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("workflow definition not found: %s", definitionID)
	} else if err != nil {
		return nil, fmt.Errorf("failed to verify workflow definition: %w", err)
	}

	// 2. Fetch all task definitions associated with this workflow and count their parent dependencies
	const getTasks = `
		SELECT td.id, td.name, COUNT(dep.depends_on_task_definition_id) AS parent_count
		FROM task_definitions td
		LEFT JOIN task_dependencies dep ON td.id = dep.task_definition_id
		WHERE td.workflow_definition_id = $1
		GROUP BY td.id, td.name
	`
	rows, err := tx.QueryContext(ctx, getTasks, definitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve task definitions: %w", err)
	}
	defer rows.Close()

	type taskInfo struct {
		id          string
		name        string
		parentCount int
	}
	var tasks []taskInfo
	for rows.Next() {
		var t taskInfo
		if err := rows.Scan(&t.id, &t.name, &t.parentCount); err != nil {
			return nil, fmt.Errorf("failed to scan task definition: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading task definitions: %w", err)
	}

	// 3. Create Workflow Run
	runID := newUUID()
	now := time.Now()
	const insertRun = `
		INSERT INTO workflow_runs (id, workflow_definition_id, status, input, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	if len(input) == 0 {
		input = json.RawMessage("{}")
	}

	_, err = tx.ExecContext(ctx, insertRun, runID, definitionID, model.WorkflowPending, input, now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert workflow run: %w", err)
	}

	// 4. Create Task Runs (root tasks start in READY state, others in PENDING state)
	const insertTaskRun = `
		INSERT INTO task_runs (id, workflow_run_id, task_definition_id, status, attempts, input, output, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	for _, t := range tasks {
		taskRunID := newUUID()
		status := model.TaskPending
		if t.parentCount == 0 {
			status = model.TaskReady
		}
		_, err = tx.ExecContext(ctx, insertTaskRun,
			taskRunID,
			runID,
			t.id,
			status,
			0,
			json.RawMessage("{}"),
			json.RawMessage("{}"),
			now,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create task run for task %s: %w", t.name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &model.WorkflowRun{
		ID:                   runID,
		WorkflowDefinitionID: definitionID,
		Status:               model.WorkflowPending,
		Input:                input,
		Output:               json.RawMessage("{}"),
		CreatedAt:            now,
	}, nil
}

// GetWorkflowRunDetails retrieves the details of a workflow run and all its tasks.
func (r *Repository) GetWorkflowRunDetails(ctx context.Context, runID string) (*model.WorkflowRunDetails, error) {
	// 1. Fetch Workflow Run
	var run model.WorkflowRun
	const selectRun = `
		SELECT id, workflow_definition_id, status, input, output, error_message, started_at, completed_at, created_at
		FROM workflow_runs
		WHERE id = $1
	`
	err := r.db.QueryRowContext(ctx, selectRun, runID).Scan(
		&run.ID,
		&run.WorkflowDefinitionID,
		&run.Status,
		&run.Input,
		&run.Output,
		&run.ErrorMessage,
		&run.StartedAt,
		&run.CompletedAt,
		&run.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("workflow run not found: %s", runID)
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch workflow run: %w", err)
	}

	// 2. Fetch Task Runs
	const selectTaskRuns = `
		SELECT id, workflow_run_id, task_definition_id, status, attempts, input, output, error_message, next_retry_at, started_at, completed_at, created_at
		FROM task_runs
		WHERE workflow_run_id = $1
	`
	rows, err := r.db.QueryContext(ctx, selectTaskRuns, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to query task runs: %w", err)
	}
	defer rows.Close()

	var tasks []*model.TaskRun
	for rows.Next() {
		var tr model.TaskRun
		err := rows.Scan(
			&tr.ID,
			&tr.WorkflowRunID,
			&tr.TaskDefinitionID,
			&tr.Status,
			&tr.Attempts,
			&tr.Input,
			&tr.Output,
			&tr.ErrorMessage,
			&tr.NextRetryAt,
			&tr.StartedAt,
			&tr.CompletedAt,
			&tr.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task run: %w", err)
		}
		tasks = append(tasks, &tr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading task runs: %w", err)
	}

	return &model.WorkflowRunDetails{
		Run:   &run,
		Tasks: tasks,
	}, nil
}

// newUUID generates a basic RFC 4122 v4 UUID in pure Go.
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
