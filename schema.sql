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
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_run_task_def UNIQUE (workflow_run_id, task_definition_id),
    CONSTRAINT chk_task_run_status CHECK (status IN ('PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED', 'TIMED_OUT')),
    CONSTRAINT chk_task_run_attempts CHECK (attempts >= 0)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_status_retry ON task_runs(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_task_runs_workflow_run ON task_runs(workflow_run_id);
