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

// DB returns the underlying sql.DB connection (mainly for testing).
func (r *Repository) DB() *sql.DB {
	return r.db
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

// Ping verifies the database connection is alive.
func (r *Repository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
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
		INSERT INTO task_definitions (id, workflow_definition_id, name, task_type, config, max_retries, retry_backoff_ms, timeout_ms, priority, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
			t.Priority,
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

	// Insert WorkflowStarted event
	payload := model.WorkflowStartedPayload{
		WorkflowDefinitionID: definitionID,
		Input:                input,
	}
	err = r.insertOutboxEventTx(ctx, tx, runID, model.EventWorkflowStarted, 1, "workflow_run", runID, nil, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to insert WorkflowStarted outbox event: %w", err)
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
		SELECT id, workflow_run_id, task_definition_id, status, attempts, input, output, error_message, next_retry_at, started_at, completed_at, created_at, worker_id, claimed_at
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
			&tr.WorkerID,
			&tr.ClaimedAt,
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

// ClaimReadyTasksBatch atomically claims up to limit READY tasks and updates them to CLAIMED,
// returning the combined execution details. Returns nil or empty list if no READY tasks are found.
func (r *Repository) ClaimReadyTasksBatch(ctx context.Context, workerID string, limit int) ([]*model.ClaimedTask, error) {
	if limit <= 0 {
		return nil, nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start batch claim transaction: %w", err)
	}
	defer tx.Rollback()

	const claimBatchQuery = `
		WITH next_tasks AS (
			SELECT tr.id 
			FROM task_runs tr
			JOIN task_definitions td ON tr.task_definition_id = td.id
			WHERE tr.status = 'READY'
			ORDER BY td.priority DESC, tr.created_at ASC
			LIMIT $2
			FOR UPDATE OF tr SKIP LOCKED
		),
		updated_tasks AS (
			UPDATE task_runs
			SET status = 'CLAIMED', worker_id = $1, claimed_at = NOW(), fencing_token = fencing_token + 1
			FROM next_tasks
			WHERE task_runs.id = next_tasks.id
			RETURNING 
				task_runs.id, 
				task_runs.workflow_run_id, 
				task_runs.task_definition_id, 
				task_runs.input,
				task_runs.fencing_token
		)
		SELECT 
			ut.id, 
			ut.workflow_run_id, 
			ut.task_definition_id, 
			td.name, 
			td.task_type, 
			td.config, 
			ut.input, 
			td.timeout_ms,
			ut.fencing_token
		FROM updated_tasks ut
		JOIN task_definitions td ON ut.task_definition_id = td.id
		ORDER BY td.priority DESC, ut.id ASC
	`

	rows, err := tx.QueryContext(ctx, claimBatchQuery, workerID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query batch claim: %w", err)
	}
	defer rows.Close()

	var claimedTasks []*model.ClaimedTask
	for rows.Next() {
		var ct model.ClaimedTask
		err = rows.Scan(
			&ct.TaskRunID,
			&ct.WorkflowRunID,
			&ct.TaskDefinitionID,
			&ct.Name,
			&ct.TaskType,
			&ct.Config,
			&ct.Input,
			&ct.TimeoutMs,
			&ct.FencingToken,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan batch claimed task: %w", err)
		}
		claimedTasks = append(claimedTasks, &ct)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during batch claim rows iteration: %w", err)
	}

	if len(claimedTasks) == 0 {
		return nil, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit batch claim transaction: %w", err)
	}

	return claimedTasks, nil
}

// ClaimNextReadyTask atomically claims the oldest READY task and updates it to CLAIMED,
// returning the combined execution details. Returns (nil, nil) if no READY tasks are found.
func (r *Repository) ClaimNextReadyTask(ctx context.Context, workerID string) (*model.ClaimedTask, error) {
	tasks, err := r.ClaimReadyTasksBatch(ctx, workerID, 1)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	return tasks[0], nil
}

// StartTaskRun transitions a task from CLAIMED to RUNNING inside a transaction.
// Guards by ID, status = CLAIMED, worker_id, and fencing_token if supplied. It also validates that the parent
// workflow status is not terminal (is PENDING or RUNNING) in the same query.
// Transitions workflow status from PENDING to RUNNING if applicable.
func (r *Repository) StartTaskRun(ctx context.Context, taskRunID string, workerID string, fencingToken ...int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start StartTaskRun transaction: %w", err)
	}
	defer tx.Rollback()

	var tokenCheck string
	if len(fencingToken) > 0 {
		tokenCheck = fmt.Sprintf("AND tr.fencing_token = %d", fencingToken[0])
	}

	updateTaskQuery := fmt.Sprintf(`
		UPDATE task_runs tr
		SET status = 'RUNNING', started_at = NOW(), attempts = attempts + 1
		FROM task_definitions td
		WHERE tr.task_definition_id = td.id
		  AND tr.id = $1
		  AND tr.status = 'CLAIMED'
		  AND tr.worker_id = $2
		  %s
		  AND EXISTS (
			  SELECT 1
			  FROM workflow_runs wr
			  WHERE wr.id = tr.workflow_run_id
				AND wr.status IN ('PENDING', 'RUNNING')
		  )
		RETURNING tr.workflow_run_id, tr.attempts, tr.claimed_at, tr.task_definition_id, td.name, td.task_type, tr.input, tr.fencing_token
	`, tokenCheck)

	var workflowRunID string
	var newAttempts int
	var claimedAt sql.NullTime
	var taskDefinitionID string
	var taskName string
	var taskType string
	var taskInput json.RawMessage
	var fencingTokenVal int64

	err = tx.QueryRowContext(ctx, updateTaskQuery, taskRunID, workerID).Scan(
		&workflowRunID, &newAttempts, &claimedAt, &taskDefinitionID, &taskName, &taskType, &taskInput, &fencingTokenVal,
	)
	if err == sql.ErrNoRows {
		return ErrInvalidTaskTransition
	} else if err != nil {
		return fmt.Errorf("failed to update task run to RUNNING: %w", err)
	}

	// 1.5 Create task_attempt record
	attemptID := newUUID()
	var actualToken int64
	if len(fencingToken) > 0 {
		actualToken = fencingToken[0]
	}
	const insertAttemptQuery = `
		INSERT INTO task_attempts (id, task_run_id, workflow_run_id, attempt_number, worker_id, status, claimed_at, started_at, fencing_token)
		VALUES ($1, $2, $3, $4, $5, 'RUNNING', $6, NOW(), $7)
	`
	_, err = tx.ExecContext(ctx, insertAttemptQuery, attemptID, taskRunID, workflowRunID, newAttempts, workerID, claimedAt, actualToken)
	if err != nil {
		return fmt.Errorf("failed to insert task attempt: %w", err)
	}

	// Insert TaskStarted event
	taskStartedPayload := model.TaskStartedPayload{
		TaskDefinitionID: taskDefinitionID,
		Name:             taskName,
		TaskType:         taskType,
		Input:            taskInput,
		WorkerID:         workerID,
		FencingToken:     fencingTokenVal,
	}
	err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventTaskStarted, 1, "task_run", taskRunID, &taskRunID, taskStartedPayload)
	if err != nil {
		return fmt.Errorf("failed to insert TaskStarted outbox event: %w", err)
	}

	// 2. Transition workflow PENDING -> RUNNING
	const updateWorkflowQuery = `
		UPDATE workflow_runs
		SET status = 'RUNNING', started_at = NOW()
		WHERE id = $1
		  AND status = 'PENDING'
	`
	_, err = tx.ExecContext(ctx, updateWorkflowQuery, workflowRunID)
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
func (r *Repository) MarkTaskRunCompleted(ctx context.Context, taskRunID string, workerID string, output json.RawMessage, fencingToken ...int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start MarkTaskRunCompleted transaction: %w", err)
	}
	defer tx.Rollback()

	if len(output) == 0 {
		output = json.RawMessage("{}")
	}

	// 1. Read workflow_run_id without acquiring a row lock
	var workflowRunID string
	err = tx.QueryRowContext(ctx, "SELECT workflow_run_id FROM task_runs WHERE id = $1", taskRunID).Scan(&workflowRunID)
	if err == sql.ErrNoRows {
		return ErrInvalidTaskTransition
	} else if err != nil {
		return fmt.Errorf("failed to read workflow_run_id for task completion: %w", err)
	}

	// 2. Acquire the parent workflow-run row lock (blocks concurrent updates for the same workflow)
	var wfLockedID string
	err = tx.QueryRowContext(ctx, "SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE", workflowRunID).Scan(&wfLockedID)
	if err != nil {
		return fmt.Errorf("failed to acquire workflow run lock: %w", err)
	}

	// 3. Guarded task completion & retrieve definition ID and attempts
	var tokenCheck string
	var args []any
	if len(fencingToken) > 0 {
		tokenCheck = "AND fencing_token = $4"
		args = []any{output, taskRunID, workerID, fencingToken[0]}
	} else {
		tokenCheck = ""
		args = []any{output, taskRunID, workerID}
	}

	completeTaskQuery := fmt.Sprintf(`
		UPDATE task_runs
		SET status = 'COMPLETED', output = $1, error_message = NULL, completed_at = NOW()
		WHERE id = $2 AND status = 'RUNNING' AND worker_id = $3 %s
		RETURNING task_definition_id, attempts
	`, tokenCheck)

	var taskDefID string
	var attempts int
	err = tx.QueryRowContext(ctx, completeTaskQuery, args...).Scan(&taskDefID, &attempts)
	if err == sql.ErrNoRows {
		return ErrInvalidTaskTransition
	} else if err != nil {
		return fmt.Errorf("failed to update task run status: %w", err)
	}

	// 3.5 Update corresponding task attempt
	const completeAttemptQuery = `
		UPDATE task_attempts
		SET status = 'COMPLETED', completed_at = NOW(), output = $1
		WHERE task_run_id = $2 AND attempt_number = $3
	`
	_, err = tx.ExecContext(ctx, completeAttemptQuery, output, taskRunID, attempts)
	if err != nil {
		return fmt.Errorf("failed to update task attempt status: %w", err)
	}

	// Insert TaskCompleted event
	taskCompletedPayload := model.TaskCompletedPayload{
		Output: output,
	}
	err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventTaskCompleted, 1, "task_run", taskRunID, &taskRunID, taskCompletedPayload)
	if err != nil {
		return fmt.Errorf("failed to insert TaskCompleted outbox event: %w", err)
	}

	// 4. Unlock eligible direct child tasks (PENDING -> READY)
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

	// 5. Attempt workflow completion
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
	resWf, err := tx.ExecContext(ctx, completeWorkflowQuery, workflowRunID)
	if err != nil {
		return fmt.Errorf("failed to complete workflow run: %w", err)
	}
	wfAffected, err := resWf.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get workflow completion rows affected: %w", err)
	}

	if wfAffected > 0 {
		var wfOutput json.RawMessage
		err = tx.QueryRowContext(ctx, "SELECT output FROM workflow_runs WHERE id = $1", workflowRunID).Scan(&wfOutput)
		if err != nil {
			return fmt.Errorf("failed to read workflow output: %w", err)
		}
		wfCompletedPayload := model.WorkflowCompletedPayload{
			Output: wfOutput,
		}
		err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventWorkflowCompleted, 1, "workflow_run", workflowRunID, nil, wfCompletedPayload)
		if err != nil {
			return fmt.Errorf("failed to insert WorkflowCompleted outbox event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit MarkTaskRunCompleted transaction: %w", err)
	}

	return nil
}

func calculateBackoff(baseBackoffMs int, attempts int) time.Duration {
	if baseBackoffMs <= 0 {
		baseBackoffMs = 1000 // default fallback
	}
	shift := attempts - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 30 {
		shift = 30
	}

	multiplier := int64(1) << shift
	delayMs := int64(baseBackoffMs) * multiplier

	// Cap max backoff to 1 hour
	maxBackoffMs := int64(3600 * 1000)
	if delayMs > maxBackoffMs || delayMs < 0 {
		delayMs = maxBackoffMs
	}

	return time.Duration(delayMs) * time.Millisecond
}

// MarkTaskRunFailed transitions a task from RUNNING to FAILED (or RETRY_WAIT, or TIMED_OUT) inside a transaction.
// Atomically marks the parent workflow run as FAILED if retry budget is exhausted.
func (r *Repository) MarkTaskRunFailed(ctx context.Context, taskRunID string, workerID string, errMsg string, isTimeout bool, fencingToken ...int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start MarkTaskRunFailed transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Read task details and definition limits without locking
	var workflowRunID string
	var attempts, maxRetries, retryBackoff, timeoutMs int
	var currentStatus string
	var currentWorkerID sql.NullString
	var currentFencingToken int64
	const selectTaskInfo = `
		SELECT tr.workflow_run_id, tr.attempts, td.max_retries, td.retry_backoff_ms, td.timeout_ms, tr.status, tr.worker_id, tr.fencing_token
		FROM task_runs tr
		JOIN task_definitions td ON tr.task_definition_id = td.id
		WHERE tr.id = $1
	`
	err = tx.QueryRowContext(ctx, selectTaskInfo, taskRunID).Scan(&workflowRunID, &attempts, &maxRetries, &retryBackoff, &timeoutMs, &currentStatus, &currentWorkerID, &currentFencingToken)
	if err == sql.ErrNoRows {
		return ErrInvalidTaskTransition
	} else if err != nil {
		return fmt.Errorf("failed to read task info for failure: %w", err)
	}

	// Ownership and state validation including fencing token
	if currentStatus != "RUNNING" || !currentWorkerID.Valid || currentWorkerID.String != workerID {
		return ErrInvalidTaskTransition
	}

	if len(fencingToken) > 0 && currentFencingToken != fencingToken[0] {
		return ErrInvalidTaskTransition
	}

	// 2. Acquire the parent workflow-run row lock to serialize workflow state updates
	var wfLockedID string
	err = tx.QueryRowContext(ctx, "SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE", workflowRunID).Scan(&wfLockedID)
	if err != nil {
		return fmt.Errorf("failed to acquire workflow run lock: %w", err)
	}

	// 2.5 Update corresponding task attempt status
	var attemptStatus string
	var failureType string
	if isTimeout {
		attemptStatus = "TIMED_OUT"
		failureType = "TIMEOUT"
	} else {
		attemptStatus = "FAILED"
		failureType = "EXECUTION_ERROR"
	}

	const updateAttemptQuery = `
		UPDATE task_attempts
		SET status = $1, completed_at = NOW(), error_message = $2, failure_type = $3
		WHERE task_run_id = $4 AND attempt_number = $5
	`
	_, err = tx.ExecContext(ctx, updateAttemptQuery, attemptStatus, errMsg, failureType, taskRunID, attempts)
	if err != nil {
		return fmt.Errorf("failed to update task attempt status: %w", err)
	}

	// 3. Determine if retry budget remains
	if attempts <= maxRetries {
		// Retry budget remains! Transition to RETRY_WAIT
		backoff := calculateBackoff(retryBackoff, attempts)
		nextRetryAt := time.Now().Add(backoff)

		const retryTaskQuery = `
			UPDATE task_runs
			SET status = 'RETRY_WAIT',
				worker_id = NULL,
				claimed_at = NULL,
				started_at = NULL,
				next_retry_at = $1,
				error_message = $2
			WHERE id = $3
		`
		_, err = tx.ExecContext(ctx, retryTaskQuery, nextRetryAt, errMsg, taskRunID)
		if err != nil {
			return fmt.Errorf("failed to schedule task retry: %w", err)
		}

		// Emit TaskFailed or TaskTimedOut, and RetryScheduled outbox events
		if isTimeout {
			taskTimedOutPayload := model.TaskTimedOutPayload{
				TimeoutMs: timeoutMs,
				Attempt:   attempts,
			}
			err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventTaskTimedOut, 1, "task_run", taskRunID, &taskRunID, taskTimedOutPayload)
		} else {
			taskFailedPayload := model.TaskFailedPayload{
				ErrorMessage: errMsg,
				Attempt:      attempts,
			}
			err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventTaskFailed, 1, "task_run", taskRunID, &taskRunID, taskFailedPayload)
		}
		if err != nil {
			return fmt.Errorf("failed to insert task failure outbox event: %w", err)
		}

		retryScheduledPayload := model.RetryScheduledPayload{
			NextRetryAt: nextRetryAt,
			Attempt:     attempts,
		}
		err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventRetryScheduled, 1, "task_run", taskRunID, &taskRunID, retryScheduledPayload)
		if err != nil {
			return fmt.Errorf("failed to insert RetryScheduled outbox event: %w", err)
		}
	} else {
		// Retry budget exhausted! Permanent task and workflow failure.
		var terminalStatus string
		if isTimeout {
			terminalStatus = "TIMED_OUT"
		} else {
			terminalStatus = "FAILED"
		}

		const failTaskQuery = `
			UPDATE task_runs
			SET status = $1, error_message = $2, completed_at = NOW()
			WHERE id = $3
			RETURNING task_definition_id
		`
		var taskDefID string
		err = tx.QueryRowContext(ctx, failTaskQuery, terminalStatus, errMsg, taskRunID).Scan(&taskDefID)
		if err != nil {
			return fmt.Errorf("failed to fail task run: %w", err)
		}

		// Insert Dead-Letter tasks record atomically
		dlqID := newUUID()
		var failureType string
		if isTimeout {
			failureType = "TIMEOUT"
		} else {
			failureType = "EXECUTION_ERROR"
		}

		const insertDLQQuery = `
			INSERT INTO dead_letter_tasks (id, task_run_id, workflow_run_id, task_definition_id, terminal_status, failure_type, reason, final_attempt, worker_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`
		_, err = tx.ExecContext(ctx, insertDLQQuery, dlqID, taskRunID, workflowRunID, taskDefID, terminalStatus, failureType, errMsg, attempts, workerID)
		if err != nil {
			return fmt.Errorf("failed to insert dead letter task: %w", err)
		}

		const failWorkflowQuery = `
			UPDATE workflow_runs
			SET status = 'FAILED', completed_at = NOW(), error_message = $1
			WHERE id = $2 AND status IN ('PENDING', 'RUNNING')
		`
		_, err = tx.ExecContext(ctx, failWorkflowQuery, "Task failed: "+errMsg, workflowRunID)
		if err != nil {
			return fmt.Errorf("failed to update workflow status to FAILED: %w", err)
		}

		// Emit TaskFailed or TaskTimedOut, RetryExhausted, DLQCreated, and WorkflowFailed events
		if isTimeout {
			taskTimedOutPayload := model.TaskTimedOutPayload{
				TimeoutMs: timeoutMs,
				Attempt:   attempts,
			}
			err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventTaskTimedOut, 1, "task_run", taskRunID, &taskRunID, taskTimedOutPayload)
		} else {
			taskFailedPayload := model.TaskFailedPayload{
				ErrorMessage: errMsg,
				Attempt:      attempts,
			}
			err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventTaskFailed, 1, "task_run", taskRunID, &taskRunID, taskFailedPayload)
		}
		if err != nil {
			return fmt.Errorf("failed to insert task failure outbox event: %w", err)
		}

		retryExhaustedPayload := model.RetryExhaustedPayload{
			ErrorMessage: errMsg,
			Attempts:     attempts,
		}
		err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventRetryExhausted, 1, "task_run", taskRunID, &taskRunID, retryExhaustedPayload)
		if err != nil {
			return fmt.Errorf("failed to insert RetryExhausted outbox event: %w", err)
		}

		dlqCreatedPayload := model.DLQCreatedPayload{
			TerminalStatus: terminalStatus,
			FailureType:    failureType,
			Reason:         &errMsg,
			FinalAttempt:   attempts,
		}
		err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventDLQCreated, 1, "task_run", taskRunID, &taskRunID, dlqCreatedPayload)
		if err != nil {
			return fmt.Errorf("failed to insert DLQCreated outbox event: %w", err)
		}

		wfFailedPayload := model.WorkflowFailedPayload{
			ErrorMessage: "Task failed: " + errMsg,
		}
		err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventWorkflowFailed, 1, "workflow_run", workflowRunID, nil, wfFailedPayload)
		if err != nil {
			return fmt.Errorf("failed to insert WorkflowFailed outbox event: %w", err)
		}
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

// RecoverStaleTasks scans the database for stale CLAIMED and RUNNING tasks and resets them.
func (r *Repository) RecoverStaleTasks(
	ctx context.Context,
	claimedTimeout time.Duration,
	runningTimeout time.Duration,
) (model.RecoveryResult, error) {
	var res model.RecoveryResult

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("failed to start stale task recovery transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Recover stale CLAIMED tasks
	const recoverClaimedQuery = `
		UPDATE task_runs tr
		SET status = 'READY',
			worker_id = NULL,
			claimed_at = NULL
		FROM workflow_runs wr
		WHERE tr.workflow_run_id = wr.id
		  AND tr.status = 'CLAIMED'
		  AND tr.claimed_at < NOW() - ($1 * INTERVAL '1 second')
		  AND wr.status IN ('PENDING', 'RUNNING')
		RETURNING tr.id, tr.workflow_run_id, tr.worker_id, tr.fencing_token
	`
	claimedSecs := claimedTimeout.Seconds()
	rowsClaimed, err := tx.QueryContext(ctx, recoverClaimedQuery, claimedSecs)
	if err != nil {
		return res, fmt.Errorf("failed to recover stale CLAIMED tasks: %w", err)
	}
	defer rowsClaimed.Close()

	type recoveredTaskInfo struct {
		id            string
		workflowRunID string
		workerID      sql.NullString
		fencingToken  int64
	}
	var recoveredClaimed []recoveredTaskInfo
	for rowsClaimed.Next() {
		var rt recoveredTaskInfo
		if err := rowsClaimed.Scan(&rt.id, &rt.workflowRunID, &rt.workerID, &rt.fencingToken); err != nil {
			return res, fmt.Errorf("failed to scan recovered claimed task: %w", err)
		}
		recoveredClaimed = append(recoveredClaimed, rt)
	}
	if err := rowsClaimed.Err(); err != nil {
		return res, fmt.Errorf("error reading recovered claimed tasks: %w", err)
	}
	res.ClaimedRecovered = int64(len(recoveredClaimed))

	// 2. Recover stale RUNNING tasks
	const recoverRunningQuery = `
		UPDATE task_runs tr
		SET status = 'READY',
			worker_id = NULL,
			claimed_at = NULL,
			started_at = NULL,
			output = '{}'::jsonb,
			error_message = NULL,
			completed_at = NULL
		FROM workflow_runs wr
		WHERE tr.workflow_run_id = wr.id
		  AND tr.status = 'RUNNING'
		  AND tr.started_at < NOW() - ($1 * INTERVAL '1 second')
		  AND wr.status IN ('PENDING', 'RUNNING')
		RETURNING tr.id, tr.workflow_run_id, tr.worker_id, tr.fencing_token, tr.attempts
	`
	runningSecs := runningTimeout.Seconds()
	rowsRunning, err := tx.QueryContext(ctx, recoverRunningQuery, runningSecs)
	if err != nil {
		return res, fmt.Errorf("failed to recover stale RUNNING tasks: %w", err)
	}
	defer rowsRunning.Close()

	type recoveredRunningInfo struct {
		id            string
		workflowRunID string
		workerID      sql.NullString
		fencingToken  int64
		attempts      int
	}
	var recoveredRunning []recoveredRunningInfo
	for rowsRunning.Next() {
		var rt recoveredRunningInfo
		if err := rowsRunning.Scan(&rt.id, &rt.workflowRunID, &rt.workerID, &rt.fencingToken, &rt.attempts); err != nil {
			return res, fmt.Errorf("failed to scan recovered running task: %w", err)
		}
		recoveredRunning = append(recoveredRunning, rt)
	}
	if err := rowsRunning.Err(); err != nil {
		return res, fmt.Errorf("error reading recovered running tasks: %w", err)
	}
	res.RunningRecovered = int64(len(recoveredRunning))

	// Update corresponding task attempts to ORPHANED
	const orphanAttemptsQuery = `
		UPDATE task_attempts
		SET status = 'ORPHANED', completed_at = NOW(), failure_type = 'WORKER_LOST', error_message = 'worker execution became stale'
		WHERE task_run_id = $1 AND attempt_number = $2 AND status = 'RUNNING'
	`
	for _, rt := range recoveredRunning {
		_, err = tx.ExecContext(ctx, orphanAttemptsQuery, rt.id, rt.attempts)
		if err != nil {
			return res, fmt.Errorf("failed to orphan task attempt for task %s, attempt %d: %w", rt.id, rt.attempts, err)
		}
	}

	// Emit TaskRecovered events for all recovered tasks
	for _, rt := range recoveredClaimed {
		var workerIDStr string
		if rt.workerID.Valid {
			workerIDStr = rt.workerID.String
		}
		taskRecoveredPayload := model.TaskRecoveredPayload{
			PreviousWorkerID: workerIDStr,
			FencingToken:     rt.fencingToken,
		}
		err = r.insertOutboxEventTx(ctx, tx, rt.workflowRunID, model.EventTaskRecovered, 1, "task_run", rt.id, &rt.id, taskRecoveredPayload)
		if err != nil {
			return res, fmt.Errorf("failed to insert TaskRecovered outbox event: %w", err)
		}
	}

	for _, rt := range recoveredRunning {
		var workerIDStr string
		if rt.workerID.Valid {
			workerIDStr = rt.workerID.String
		}
		taskRecoveredPayload := model.TaskRecoveredPayload{
			PreviousWorkerID: workerIDStr,
			FencingToken:     rt.fencingToken,
		}
		err = r.insertOutboxEventTx(ctx, tx, rt.workflowRunID, model.EventTaskRecovered, 1, "task_run", rt.id, &rt.id, taskRecoveredPayload)
		if err != nil {
			return res, fmt.Errorf("failed to insert TaskRecovered outbox event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("failed to commit stale task recovery transaction: %w", err)
	}

	return res, nil
}

// PromoteDueRetries transitions due tasks from RETRY_WAIT to READY.
func (r *Repository) PromoteDueRetries(ctx context.Context) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to start PromoteDueRetries transaction: %w", err)
	}
	defer tx.Rollback()

	const promoteQuery = `
		UPDATE task_runs tr
		SET status = 'READY',
			next_retry_at = NULL
		FROM workflow_runs wr
		WHERE tr.workflow_run_id = wr.id
		  AND tr.status = 'RETRY_WAIT'
		  AND tr.next_retry_at <= NOW()
		  AND wr.status IN ('PENDING', 'RUNNING')
		RETURNING tr.id, tr.workflow_run_id, tr.fencing_token
	`
	rows, err := tx.QueryContext(ctx, promoteQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to promote due retries: %w", err)
	}
	defer rows.Close()

	type promotedTask struct {
		id            string
		workflowRunID string
		fencingToken  int64
	}
	var promoted []promotedTask
	for rows.Next() {
		var pt promotedTask
		if err := rows.Scan(&pt.id, &pt.workflowRunID, &pt.fencingToken); err != nil {
			return 0, fmt.Errorf("failed to scan promoted task: %w", err)
		}
		promoted = append(promoted, pt)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error reading promoted tasks: %w", err)
	}

	for _, pt := range promoted {
		payload := model.RetryPromotedPayload{
			FencingToken: pt.fencingToken,
		}
		err = r.insertOutboxEventTx(ctx, tx, pt.workflowRunID, model.EventRetryPromoted, 1, "task_run", pt.id, &pt.id, payload)
		if err != nil {
			return 0, fmt.Errorf("failed to insert RetryPromoted outbox event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit PromoteDueRetries transaction: %w", err)
	}

	return int64(len(promoted)), nil
}

// GetWorkflowRunHistory retrieves the history of a workflow run and all its attempts.
func (r *Repository) GetWorkflowRunHistory(ctx context.Context, runID string) (*model.WorkflowHistoryResponse, error) {
	// Query the workflow run details
	var wfStatus string
	err := r.db.QueryRowContext(ctx, "SELECT status FROM workflow_runs WHERE id = $1", runID).Scan(&wfStatus)
	if err == sql.ErrNoRows {
		return nil, sql.ErrNoRows
	} else if err != nil {
		return nil, fmt.Errorf("failed to query workflow run for history: %w", err)
	}

	// Query task runs
	const queryTasks = `
		SELECT tr.id, td.name, tr.status
		FROM task_runs tr
		JOIN task_definitions td ON tr.task_definition_id = td.id
		WHERE tr.workflow_run_id = $1
		ORDER BY tr.created_at ASC
	`
	rows, err := r.db.QueryContext(ctx, queryTasks, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to query task runs for history: %w", err)
	}
	defer rows.Close()

	var tasks []model.TaskHistoryResponse
	for rows.Next() {
		var t model.TaskHistoryResponse
		if err := rows.Scan(&t.TaskRunID, &t.TaskName, &t.Status); err != nil {
			return nil, fmt.Errorf("failed to scan task run: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating task runs: %w", err)
	}

	// Query attempts for all task runs of this workflow
	const queryAttempts = `
		SELECT id, task_run_id, workflow_run_id, attempt_number, worker_id, status, claimed_at, started_at, completed_at, output, error_message, failure_type, created_at
		FROM task_attempts
		WHERE workflow_run_id = $1
		ORDER BY attempt_number ASC
	`
	attRows, err := r.db.QueryContext(ctx, queryAttempts, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to query task attempts for history: %w", err)
	}
	defer attRows.Close()

	attemptsMap := make(map[string][]model.TaskAttempt)
	for attRows.Next() {
		var att model.TaskAttempt
		var claimedAt, completedAt sql.NullTime
		var errMsg, failureType sql.NullString
		err := attRows.Scan(
			&att.ID, &att.TaskRunID, &att.WorkflowRunID, &att.AttemptNumber,
			&att.WorkerID, &att.Status, &claimedAt, &att.StartedAt, &completedAt,
			&att.Output, &errMsg, &failureType, &att.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task attempt: %w", err)
		}
		if claimedAt.Valid {
			att.ClaimedAt = &claimedAt.Time
		}
		if completedAt.Valid {
			att.CompletedAt = &completedAt.Time
			dur := completedAt.Time.Sub(att.StartedAt).Milliseconds()
			att.DurationMs = &dur
		}
		if errMsg.Valid {
			att.ErrorMessage = &errMsg.String
		}
		if failureType.Valid {
			att.FailureType = &failureType.String
		}
		attemptsMap[att.TaskRunID] = append(attemptsMap[att.TaskRunID], att)
	}
	if err := attRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating task attempts: %w", err)
	}

	// Associate attempts with tasks
	for i := range tasks {
		atts, ok := attemptsMap[tasks[i].TaskRunID]
		if !ok {
			tasks[i].Attempts = []model.TaskAttempt{}
		} else {
			tasks[i].Attempts = atts
		}
	}

	return &model.WorkflowHistoryResponse{
		WorkflowRunID:  runID,
		WorkflowStatus: wfStatus,
		Tasks:          tasks,
	}, nil
}

// GetTaskAttempts retrieves all attempts of a task run.
func (r *Repository) GetTaskAttempts(ctx context.Context, taskRunID string) ([]model.TaskAttempt, error) {
	// Verify task run exists
	var dummy string
	err := r.db.QueryRowContext(ctx, "SELECT id FROM task_runs WHERE id = $1", taskRunID).Scan(&dummy)
	if err == sql.ErrNoRows {
		return nil, sql.ErrNoRows
	} else if err != nil {
		return nil, fmt.Errorf("failed to query task run: %w", err)
	}

	const queryAttempts = `
		SELECT id, task_run_id, workflow_run_id, attempt_number, worker_id, status, claimed_at, started_at, completed_at, output, error_message, failure_type, created_at
		FROM task_attempts
		WHERE task_run_id = $1
		ORDER BY attempt_number ASC
	`
	rows, err := r.db.QueryContext(ctx, queryAttempts, taskRunID)
	if err != nil {
		return nil, fmt.Errorf("failed to query task attempts: %w", err)
	}
	defer rows.Close()

	var attempts []model.TaskAttempt
	for rows.Next() {
		var att model.TaskAttempt
		var claimedAt, completedAt sql.NullTime
		var errMsg, failureType sql.NullString
		err := rows.Scan(
			&att.ID, &att.TaskRunID, &att.WorkflowRunID, &att.AttemptNumber,
			&att.WorkerID, &att.Status, &claimedAt, &att.StartedAt, &completedAt,
			&att.Output, &errMsg, &failureType, &att.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task attempt: %w", err)
		}
		if claimedAt.Valid {
			att.ClaimedAt = &claimedAt.Time
		}
		if completedAt.Valid {
			att.CompletedAt = &completedAt.Time
			dur := completedAt.Time.Sub(att.StartedAt).Milliseconds()
			att.DurationMs = &dur
		}
		if errMsg.Valid {
			att.ErrorMessage = &errMsg.String
		}
		if failureType.Valid {
			att.FailureType = &failureType.String
		}
		attempts = append(attempts, att)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating task attempts: %w", err)
	}

	return attempts, nil
}

// GetDeadLetterTasks lists tasks that failed terminally and are stored in the DLQ.
func (r *Repository) GetDeadLetterTasks(ctx context.Context, limit, offset int) ([]model.DeadLetterTask, error) {
	const queryDLQ = `
		SELECT id, task_run_id, workflow_run_id, task_definition_id, terminal_status, failure_type, reason, final_attempt, worker_id, dead_lettered_at, created_at
		FROM dead_letter_tasks
		ORDER BY dead_lettered_at DESC
		LIMIT $1 OFFSET $2
	`
	rows, err := r.db.QueryContext(ctx, queryDLQ, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query DLQ: %w", err)
	}
	defer rows.Close()

	var dlqs []model.DeadLetterTask
	for rows.Next() {
		var dlq model.DeadLetterTask
		var reason, workerID sql.NullString
		err := rows.Scan(
			&dlq.ID, &dlq.TaskRunID, &dlq.WorkflowRunID, &dlq.TaskDefinitionID,
			&dlq.TerminalStatus, &dlq.FailureType, &reason, &dlq.FinalAttempt, &workerID,
			&dlq.DeadLetteredAt, &dlq.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan DLQ task: %w", err)
		}
		if reason.Valid {
			dlq.Reason = &reason.String
		}
		if workerID.Valid {
			dlq.WorkerID = &workerID.String
		}
		dlqs = append(dlqs, dlq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating DLQ tasks: %w", err)
	}

	return dlqs, nil
}

// GetActiveTaskRuns returns all task runs in CLAIMED or RUNNING status that belong to active workflows.
func (r *Repository) GetActiveTaskRuns(ctx context.Context) ([]*model.TaskRun, error) {
	const query = `
		SELECT tr.id, tr.workflow_run_id, tr.task_definition_id, tr.status, tr.attempts, tr.worker_id, tr.claimed_at, tr.started_at, tr.fencing_token
		FROM task_runs tr
		JOIN workflow_runs wr ON tr.workflow_run_id = wr.id
		WHERE tr.status IN ('CLAIMED', 'RUNNING')
		  AND wr.status IN ('PENDING', 'RUNNING')
	`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active task runs: %w", err)
	}
	defer rows.Close()

	var list []*model.TaskRun
	for rows.Next() {
		var tr model.TaskRun
		var workerID sql.NullString
		var claimedAt, startedAt sql.NullTime
		err := rows.Scan(
			&tr.ID, &tr.WorkflowRunID, &tr.TaskDefinitionID, &tr.Status,
			&tr.Attempts, &workerID, &claimedAt, &startedAt, &tr.FencingToken,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan active task run: %w", err)
		}
		if workerID.Valid {
			tr.WorkerID = &workerID.String
		}
		if claimedAt.Valid {
			tr.ClaimedAt = &claimedAt.Time
		}
		if startedAt.Valid {
			tr.StartedAt = &startedAt.Time
		}
		list = append(list, &tr)
	}
	return list, nil
}

// RecoverRunningTask resets a RUNNING task run back to READY, increments attempts, and marks attempt as ORPHANED.
func (r *Repository) RecoverRunningTask(ctx context.Context, taskRunID string, fencingToken int64) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to start RecoverRunningTask transaction: %w", err)
	}
	defer tx.Rollback()

	// Query workflow_run_id and worker_id first
	var workflowRunID string
	var previousWorkerID sql.NullString
	err = tx.QueryRowContext(ctx, "SELECT workflow_run_id, worker_id FROM task_runs WHERE id = $1", taskRunID).Scan(&workflowRunID, &previousWorkerID)
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to select task run info for recovery: %w", err)
	}

	// Guard by status = 'RUNNING' and fencing_token
	const updateQuery = `
		UPDATE task_runs
		SET status = 'READY',
			worker_id = NULL,
			claimed_at = NULL,
			started_at = NULL,
			output = '{}'::jsonb,
			error_message = NULL,
			completed_at = NULL
		WHERE id = $1 AND status = 'RUNNING' AND fencing_token = $2
	`
	res, err := tx.ExecContext(ctx, updateQuery, taskRunID, fencingToken)
	if err != nil {
		return false, fmt.Errorf("failed to update running task to READY: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return false, err
	}

	const orphanQuery = `
		UPDATE task_attempts
		SET status = 'ORPHANED', completed_at = NOW(), failure_type = 'WORKER_LOST', error_message = 'worker execution became stale'
		WHERE task_run_id = $1 AND status = 'RUNNING'
	`
	_, err = tx.ExecContext(ctx, orphanQuery, taskRunID)
	if err != nil {
		return false, fmt.Errorf("failed to mark attempt as ORPHANED: %w", err)
	}

	// Insert TaskRecovered event
	var workerIDStr string
	if previousWorkerID.Valid {
		workerIDStr = previousWorkerID.String
	}
	taskRecoveredPayload := model.TaskRecoveredPayload{
		PreviousWorkerID: workerIDStr,
		FencingToken:     fencingToken,
	}
	err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventTaskRecovered, 1, "task_run", taskRunID, &taskRunID, taskRecoveredPayload)
	if err != nil {
		return false, fmt.Errorf("failed to insert TaskRecovered outbox event: %w", err)
	}

	return true, tx.Commit()
}

// RecoverClaimedTask resets a CLAIMED task run back to READY.
func (r *Repository) RecoverClaimedTask(ctx context.Context, taskRunID string, fencingToken int64) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to start RecoverClaimedTask transaction: %w", err)
	}
	defer tx.Rollback()

	// Query workflow_run_id and worker_id first
	var workflowRunID string
	var previousWorkerID sql.NullString
	err = tx.QueryRowContext(ctx, "SELECT workflow_run_id, worker_id FROM task_runs WHERE id = $1", taskRunID).Scan(&workflowRunID, &previousWorkerID)
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to select task run info for recovery: %w", err)
	}

	const updateQuery = `
		UPDATE task_runs
		SET status = 'READY',
			worker_id = NULL,
			claimed_at = NULL
		WHERE id = $1 AND status = 'CLAIMED' AND fencing_token = $2
	`
	res, err := tx.ExecContext(ctx, updateQuery, taskRunID, fencingToken)
	if err != nil {
		return false, fmt.Errorf("failed to update claimed task to READY: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return false, err
	}

	// Insert TaskRecovered event
	var workerIDStr string
	if previousWorkerID.Valid {
		workerIDStr = previousWorkerID.String
	}
	taskRecoveredPayload := model.TaskRecoveredPayload{
		PreviousWorkerID: workerIDStr,
		FencingToken:     fencingToken,
	}
	err = r.insertOutboxEventTx(ctx, tx, workflowRunID, model.EventTaskRecovered, 1, "task_run", taskRunID, &taskRunID, taskRecoveredPayload)
	if err != nil {
		return false, fmt.Errorf("failed to insert TaskRecovered outbox event: %w", err)
	}

	err = tx.Commit()
	return true, err
}

// insertOutboxEventTx inserts a new outbox event atomically.
// It locks the workflow run, allocates the next sequence number, and inserts the event.
func (r *Repository) insertOutboxEventTx(
	ctx context.Context,
	tx *sql.Tx,
	workflowRunID string,
	eventType string,
	eventVersion int,
	aggregateType string,
	aggregateID string,
	taskRunID *string,
	payload interface{},
) error {
	// 1. Get and increment the sequence number on the workflow run.
	var sequence int64
	const incrementSeqQuery = `
		UPDATE workflow_runs
		SET event_sequence = event_sequence + 1
		WHERE id = $1
		RETURNING event_sequence
	`
	err := tx.QueryRowContext(ctx, incrementSeqQuery, workflowRunID).Scan(&sequence)
	if err != nil {
		return fmt.Errorf("failed to increment workflow run event sequence: %w", err)
	}

	// 2. Marshal the payload to json
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}

	// 3. Insert the outbox event
	eventID := newUUID()
	now := time.Now().UTC()
	const insertQuery = `
		INSERT INTO outbox_events (
			id, event_type, event_version, aggregate_type, aggregate_id,
			workflow_run_id, task_run_id, sequence, payload, created_at, available_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err = tx.ExecContext(ctx, insertQuery,
		eventID, eventType, eventVersion, aggregateType, aggregateID,
		workflowRunID, taskRunID, sequence, payloadBytes, now, now,
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	return nil
}

// ClaimPendingOutboxEvents selects up to batchSize pending/unclaimed outbox
// events using FOR UPDATE SKIP LOCKED, then marks them claimed under the given
// publisherID with a lease expiring at lockedUntil. Events whose previous claim
// has expired are reclaimed. Events scheduled for a future available_at are
// skipped. Each returned event carries the claim token (publisherID) required
// to later mark it published.
func (r *Repository) ClaimPendingOutboxEvents(
	ctx context.Context,
	publisherID string,
	batchSize int,
	claimTimeout time.Duration,
	now time.Time,
) ([]model.OutboxEvent, error) {
	if batchSize <= 0 {
		batchSize = 1
	}
	lockedUntil := now.Add(claimTimeout).UTC()

	const claimQuery = `
		UPDATE outbox_events o
		SET locked_by = $1,
		    locked_until = $2,
		    attempts = o.attempts + 1
		FROM (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NULL
			  AND available_at <= $3
			  AND (locked_until IS NULL OR locked_until <= $3)
			ORDER BY created_at ASC
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		) sub
		WHERE o.id = sub.id
		RETURNING o.id, o.event_type, o.event_version, o.aggregate_type,
		          o.aggregate_id, o.workflow_run_id, o.task_run_id, o.sequence,
		          o.payload, o.created_at, o.available_at, o.attempts,
		          o.last_error, o.locked_by, o.locked_until, o.published_at
	`

	rows, err := r.db.QueryContext(ctx, claimQuery, publisherID, lockedUntil, now.UTC(), batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to claim pending outbox events: %w", err)
	}
	defer rows.Close()

	events, err := scanOutboxEvents(rows)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// MarkOutboxPublished marks a previously claimed event as published, but only
// if it is still claimed by the given publisherID. This prevents a restarted
// publisher from marking an event that was already reclaimed by another
// publisher. A crash between Kafka acknowledgement and this call intentionally
// produces a duplicate publication, which consumers must tolerate.
func (r *Repository) MarkOutboxPublished(ctx context.Context, eventID, publisherID string, now time.Time) error {
	const query = `
		UPDATE outbox_events
		SET published_at = $3,
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1
		  AND locked_by = $2
		  AND published_at IS NULL
	`
	res, err := r.db.ExecContext(ctx, query, eventID, publisherID, now.UTC())
	if err != nil {
		return fmt.Errorf("failed to mark outbox event published: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read affected rows: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("outbox event %s was not claimed by %s or already published", eventID, publisherID)
	}
	return nil
}

// RecordOutboxError records a publication failure for a claimed event and
// schedules its next attempt using exponential backoff capped at the given
// maxRetries. Once maxRetries is exceeded the event is released (unlocked) with
// an extended available_at so it is not retried in the immediate loop; the
// retention/cleanup policy and operator alerts handle permanent failures.
func (r *Repository) RecordOutboxError(
	ctx context.Context,
	eventID, publisherID, lastError string,
	attempts int,
	baseDelay time.Duration,
	maxRetries int,
	now time.Time,
) error {
	availableAt := now.UTC().Add(backoffDelay(attempts, baseDelay))
	if attempts > maxRetries {
		availableAt = now.UTC().Add(backoffDelay(maxRetries, baseDelay) * 10)
	}
	const query = `
		UPDATE outbox_events
		SET last_error = $3,
		    available_at = $4,
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1
		  AND locked_by = $2
	`
	_, err := r.db.ExecContext(ctx, query, eventID, publisherID, lastError, availableAt)
	if err != nil {
		return fmt.Errorf("failed to record outbox error: %w", err)
	}
	return nil
}

// CleanupPublishedOutboxEvents removes published events older than the
// retention window. Unpublished events are never removed.
func (r *Repository) CleanupPublishedOutboxEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	const query = `
		DELETE FROM outbox_events
		WHERE published_at IS NOT NULL
		  AND published_at < $1
	`
	res, err := r.db.ExecContext(ctx, query, olderThan.UTC())
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup published outbox events: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read affected rows: %w", err)
	}
	return affected, nil
}

// backoffDelay computes an exponential backoff duration for the given attempt
// number (1-based). It doubles the base delay each attempt with a sensible cap.
func backoffDelay(attempts int, base time.Duration) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := base
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
			break
		}
	}
	return delay
}

// scanOutboxEvents reads a result set of outbox_events rows into OutboxEvent values.
func scanOutboxEvents(rows *sql.Rows) ([]model.OutboxEvent, error) {
	var events []model.OutboxEvent
	for rows.Next() {
		var e model.OutboxEvent
		var taskRunID, lockedBy, lastError sql.NullString
		var lockedUntil, publishedAt sql.NullTime
		if err := rows.Scan(
			&e.ID, &e.EventType, &e.EventVersion, &e.AggregateType, &e.AggregateID,
			&e.WorkflowRunID, &taskRunID, &e.Sequence, &e.Payload, &e.CreatedAt, &e.AvailableAt,
			&e.Attempts, &lastError, &lockedBy, &lockedUntil, &publishedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		if taskRunID.Valid {
			v := taskRunID.String
			e.TaskRunID = &v
		}
		if lockedBy.Valid {
			v := lockedBy.String
			e.LockedBy = &v
		}
		if lastError.Valid {
			v := lastError.String
			e.LastError = &v
		}
		if lockedUntil.Valid {
			v := lockedUntil.Time
			e.LockedUntil = &v
		}
		if publishedAt.Valid {
			v := publishedAt.Time
			e.PublishedAt = &v
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating outbox events: %w", err)
	}
	return events, nil
}
