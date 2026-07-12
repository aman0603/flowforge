package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aman0603/flowforge/internal/model"
	_ "github.com/lib/pq" // Postgres driver
)

// ErrInvalidTaskTransition is returned when a guarded update affects 0 rows.
var ErrInvalidTaskTransition = errors.New("invalid task state transition or ownership mismatch")

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

// ClaimNextReadyTask atomically claims the oldest READY task and updates it to CLAIMED,
// returning the combined execution details. Returns (nil, nil) if no READY tasks are found.
func (r *Repository) ClaimNextReadyTask(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start claim transaction: %w", err)
	}
	defer tx.Rollback()

	const claimQuery = `
		WITH next_task AS (
			SELECT id 
			FROM task_runs 
			WHERE status = 'READY'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		),
		updated_task AS (
			UPDATE task_runs
			SET status = 'CLAIMED', worker_id = $1, claimed_at = NOW()
			FROM next_task
			WHERE task_runs.id = next_task.id
			RETURNING 
				task_runs.id, 
				task_runs.workflow_run_id, 
				task_runs.task_definition_id, 
				task_runs.input
		)
		SELECT 
			ut.id, 
			ut.workflow_run_id, 
			ut.task_definition_id, 
			td.name, 
			td.task_type, 
			td.config, 
			ut.input, 
			td.timeout_ms
		FROM updated_task ut
		JOIN task_definitions td ON ut.task_definition_id = td.id
	`

	var ct model.ClaimedTask
	err = tx.QueryRowContext(ctx, claimQuery, workerID).Scan(
		&ct.TaskRunID,
		&ct.WorkflowRunID,
		&ct.TaskDefinitionID,
		&ct.Name,
		&ct.TaskType,
		&ct.Config,
		&ct.Input,
		&ct.TimeoutMs,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to claim task run: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit claim transaction: %w", err)
	}

	return &ct, nil
}

// StartTaskRun transitions a task from CLAIMED to RUNNING inside a transaction.
// Guards by ID, status = CLAIMED, and worker_id. It also validates that the parent
// workflow status is not terminal (is PENDING or RUNNING) in the same query.
// Transitions workflow status from PENDING to RUNNING if applicable.
func (r *Repository) StartTaskRun(ctx context.Context, taskRunID string, workerID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start StartTaskRun transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Transition task CLAIMED -> RUNNING (guarded by task status, worker_id, and workflow status)
	const updateTaskQuery = `
		UPDATE task_runs tr
		SET status = 'RUNNING', started_at = NOW(), attempts = attempts + 1
		WHERE tr.id = $1
		  AND tr.status = 'CLAIMED'
		  AND tr.worker_id = $2
		  AND EXISTS (
			  SELECT 1
			  FROM workflow_runs wr
			  WHERE wr.id = tr.workflow_run_id
				AND wr.status IN ('PENDING', 'RUNNING')
		  )
	`
	res, err := tx.ExecContext(ctx, updateTaskQuery, taskRunID, workerID)
	if err != nil {
		return fmt.Errorf("failed to update task run to RUNNING: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check task rows affected: %w", err)
	}
	if rows == 0 {
		return ErrInvalidTaskTransition
	}

	// 2. Transition workflow PENDING -> RUNNING
	const updateWorkflowQuery = `
		UPDATE workflow_runs
		SET status = 'RUNNING', started_at = NOW()
		WHERE id = (SELECT workflow_run_id FROM task_runs WHERE id = $1)
		  AND status = 'PENDING'
	`
	_, err = tx.ExecContext(ctx, updateWorkflowQuery, taskRunID)
	if err != nil {
		return fmt.Errorf("failed to update workflow run to RUNNING: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit StartTaskRun transaction: %w", err)
	}

	return nil
}

// MarkTaskRunCompleted transitions a task from RUNNING to COMPLETED inside a transaction.
// It also unlocks eligible child task runs and checks if the parent workflow run has finished.
func (r *Repository) MarkTaskRunCompleted(ctx context.Context, taskRunID string, workerID string, output json.RawMessage) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start MarkTaskRunCompleted transaction: %w", err)
	}
	defer tx.Rollback()

	if len(output) == 0 {
		output = json.RawMessage("{}")
	}

	// 1. Guarded task completion & retrieve run and definition IDs
	const completeTaskQuery = `
		UPDATE task_runs
		SET status = 'COMPLETED', output = $1, error_message = NULL, completed_at = NOW()
		WHERE id = $2 AND status = 'RUNNING' AND worker_id = $3
		RETURNING workflow_run_id, task_definition_id
	`
	var workflowRunID, taskDefID string
	err = tx.QueryRowContext(ctx, completeTaskQuery, output, taskRunID, workerID).Scan(&workflowRunID, &taskDefID)
	if err == sql.ErrNoRows {
		return ErrInvalidTaskTransition
	} else if err != nil {
		return fmt.Errorf("failed to update task run status: %w", err)
	}

	// 2. Unlock eligible direct child tasks (PENDING -> READY)
	const unlockQuery = `
		UPDATE task_runs
		SET status = 'READY'
		WHERE workflow_run_id = $1
		  AND status = 'PENDING'
		  AND task_definition_id IN (
			  SELECT dep.task_definition_id
			  FROM task_dependencies dep
			  WHERE dep.depends_on_task_definition_id = $2
		  )
		  AND NOT EXISTS (
			  SELECT 1
			  FROM task_dependencies parent_dep
			  JOIN task_runs parent_tr ON parent_tr.workflow_run_id = $1 
				   AND parent_tr.task_definition_id = parent_dep.depends_on_task_definition_id
			  WHERE parent_dep.task_definition_id = task_runs.task_definition_id
				AND parent_tr.status != 'COMPLETED'
		  )
	`
	_, err = tx.ExecContext(ctx, unlockQuery, workflowRunID, taskDefID)
	if err != nil {
		return fmt.Errorf("failed to unlock child tasks: %w", err)
	}

	// 3. Attempt workflow completion
	const completeWorkflowQuery = `
		UPDATE workflow_runs
		SET status = 'COMPLETED', completed_at = NOW()
		WHERE id = $1
		  AND status = 'RUNNING'
		  AND NOT EXISTS (
			  SELECT 1
			  FROM task_runs
			  WHERE workflow_run_id = $1
				AND status != 'COMPLETED'
		  )
	`
	_, err = tx.ExecContext(ctx, completeWorkflowQuery, workflowRunID)
	if err != nil {
		return fmt.Errorf("failed to complete workflow run: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit MarkTaskRunCompleted transaction: %w", err)
	}

	return nil
}

// MarkTaskRunFailed transitions a task from RUNNING to FAILED inside a transaction.
// Atomically marks the parent workflow run as FAILED if it is in PENDING or RUNNING status.
func (r *Repository) MarkTaskRunFailed(ctx context.Context, taskRunID string, workerID string, errMsg string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start MarkTaskRunFailed transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Transition task status and retrieve workflow run ID
	const failTaskQuery = `
		UPDATE task_runs
		SET status = 'FAILED', error_message = $1, completed_at = NOW()
		WHERE id = $2 AND status = 'RUNNING' AND worker_id = $3
		RETURNING workflow_run_id
	`
	var workflowRunID string
	err = tx.QueryRowContext(ctx, failTaskQuery, errMsg, taskRunID, workerID).Scan(&workflowRunID)
	if err == sql.ErrNoRows {
		return ErrInvalidTaskTransition
	} else if err != nil {
		return fmt.Errorf("failed to fail task run: %w", err)
	}

	// 2. Transition workflow status to FAILED only if current status is PENDING or RUNNING
	const failWorkflowQuery = `
		UPDATE workflow_runs
		SET status = 'FAILED', completed_at = NOW(), error_message = $1
		WHERE id = $2 AND status IN ('PENDING', 'RUNNING')
	`
	_, err = tx.ExecContext(ctx, failWorkflowQuery, "Task failed: "+errMsg, workflowRunID)
	if err != nil {
		return fmt.Errorf("failed to update workflow status to FAILED: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit MarkTaskRunFailed transaction: %w", err)
	}

	return nil
}

// newUUID generates a basic RFC 4122 v4 UUID in pure Go.
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
