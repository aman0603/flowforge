package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aman0603/flowforge/internal/model"
)

// Executor defines the contract for task execution handlers.
type Executor interface {
	Execute(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error)
}

// SleepConfig defines the expected config format for the SleepExecutor.
type SleepConfig struct {
	DurationMs int `json:"duration_ms"`
}

// SleepExecutor delays execution for a specified duration_ms configured in JSON.
type SleepExecutor struct{}

// NewSleepExecutor instantiates a new SleepExecutor instance.
func NewSleepExecutor() *SleepExecutor {
	return &SleepExecutor{}
}

// Execute parses config, validates duration_ms, and sleeps respecting context cancellation.
func (e *SleepExecutor) Execute(ctx context.Context, task *model.ClaimedTask) (json.RawMessage, error) {
	if task.TaskType != "SLEEP" {
		return nil, fmt.Errorf("unsupported task type for sleep executor: %s", task.TaskType)
	}

	// 1. Parse Config
	var cfg SleepConfig
	if err := json.Unmarshal(task.Config, &cfg); err != nil {
		return nil, fmt.Errorf("malformed JSON config: %w", err)
	}

	// 2. Validate Config
	// Note: Omitted or missing duration_ms defaults to 0 in unmarshal
	if cfg.DurationMs <= 0 {
		return nil, fmt.Errorf("duration_ms must be positive and non-zero, got %d", cfg.DurationMs)
	}

	// 3. Execute Sleep with Context sensitivity
	timer := time.NewTimer(time.Duration(cfg.DurationMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
		// Return successful JSON output
		return json.RawMessage(`{"status":"completed"}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
