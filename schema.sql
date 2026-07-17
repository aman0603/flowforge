-- Enable UUID generation support in PostgreSQL if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Workflow Definitions
CREATE TABLE IF NOT EXISTS workflow_definitions (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Task Definitions
CREATE TABLE IF NOT EXISTS task_definitions (
    id UUID PRIMARY KEY,
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    task_type VARCHAR(100) NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    max_retries INT NOT NULL DEFAULT 3,
    retry_backoff_ms INT NOT NULL DEFAULT 1000,
    timeout_ms INT NOT NULL DEFAULT 60000,
    priority INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_workflow_task_name UNIQUE (workflow_definition_id, name),
    CONSTRAINT chk_max_retries CHECK (max_retries >= 0),
    CONSTRAINT chk_retry_backoff CHECK (retry_backoff_ms >= 0),
    CONSTRAINT chk_timeout CHECK (timeout_ms > 0)
);

CREATE INDEX IF NOT EXISTS idx_task_definitions_workflow_def ON task_definitions(workflow_definition_id);

-- 3. Task Dependencies
CREATE TABLE IF NOT EXISTS task_dependencies (
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id) ON DELETE CASCADE,
    task_definition_id UUID NOT NULL REFERENCES task_definitions(id) ON DELETE CASCADE,
    depends_on_task_definition_id UUID NOT NULL REFERENCES task_definitions(id) ON DELETE CASCADE,
    PRIMARY KEY (task_definition_id, depends_on_task_definition_id)
);

CREATE INDEX IF NOT EXISTS idx_task_deps_depends_on ON task_dependencies(depends_on_task_definition_id);
CREATE INDEX IF NOT EXISTS idx_task_deps_task_def ON task_dependencies(task_definition_id);

-- 4. Workflow Runs
CREATE TABLE IF NOT EXISTS workflow_runs (
    id UUID PRIMARY KEY,
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id) ON DELETE RESTRICT,
    status VARCHAR(50) NOT NULL,
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    output JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    event_sequence BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_workflow_run_status CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'))
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_status_created ON workflow_runs(status, created_at);

-- 5. Task Runs
CREATE TABLE IF NOT EXISTS task_runs (
    id UUID PRIMARY KEY,
    workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    task_definition_id UUID NOT NULL REFERENCES task_definitions(id) ON DELETE RESTRICT,
    status VARCHAR(50) NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    output JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    worker_id VARCHAR(255),
    claimed_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_run_task_def UNIQUE (workflow_run_id, task_definition_id),
    CONSTRAINT chk_task_run_status CHECK (status IN ('PENDING', 'READY', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED', 'TIMED_OUT', 'RETRY_WAIT')),
    CONSTRAINT chk_task_run_attempts CHECK (attempts >= 0)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_status_retry ON task_runs(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_task_runs_workflow_run ON task_runs(workflow_run_id);

-- 6. Task Attempts
CREATE TABLE IF NOT EXISTS task_attempts (
    id UUID PRIMARY KEY,
    task_run_id UUID NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
    workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    worker_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    claimed_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    output JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    failure_type VARCHAR(100),
    fencing_token BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_task_run_attempt UNIQUE (task_run_id, attempt_number),
    CONSTRAINT chk_attempt_status CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'ORPHANED')),
    CONSTRAINT chk_attempt_number CHECK (attempt_number > 0)
);

CREATE INDEX IF NOT EXISTS idx_task_attempts_workflow_run ON task_attempts(workflow_run_id);

-- 7. Dead Letter Tasks
CREATE TABLE IF NOT EXISTS dead_letter_tasks (
    id UUID PRIMARY KEY,
    task_run_id UUID NOT NULL UNIQUE REFERENCES task_runs(id) ON DELETE CASCADE,
    workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    task_definition_id UUID NOT NULL REFERENCES task_definitions(id) ON DELETE RESTRICT,
    terminal_status VARCHAR(50) NOT NULL,
    failure_type VARCHAR(100) NOT NULL,
    reason TEXT,
    final_attempt INT NOT NULL,
    worker_id VARCHAR(255),
    dead_lettered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_tasks_workflow_run ON dead_letter_tasks(workflow_run_id);

-- 8. Outbox Events
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    event_version INT NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    workflow_run_id UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    task_run_id UUID REFERENCES task_runs(id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    available_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    locked_by VARCHAR(255),
    locked_until TIMESTAMP WITH TIME ZONE,
    published_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT unique_workflow_run_event_sequence UNIQUE (workflow_run_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending ON outbox_events(published_at, available_at, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_events_locked_until ON outbox_events(locked_until);
