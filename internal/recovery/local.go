package recovery

import (
	"context"
	"fmt"

	"github.com/aman0603/flowforge/internal/repository"
)

// Client abstracts the recovery operations the Worker depends on. Two
// implementations exist: a local one backed directly by the repository (the
// modular monolith default) and a gRPC one calling a standalone Recovery
// service. This seam lets Phase 11 extract recovery without changing the
// worker's lease-aware reclaim logic.
type Client interface {
	// RecoverTask reclaims a single stale task, dispatching on its observed
	// status ("CLAIMED" or "RUNNING"), using the provided fencing token. It
	// returns true if the task was actually reclaimed.
	RecoverTask(ctx context.Context, taskRunID string, status string, fencingToken int64) (bool, error)
}

// LocalClient implements Client using the repository directly, preserving
// pre-Phase-11 in-process behavior when no gRPC recovery service is configured.
type LocalClient struct {
	repo *repository.Repository
}

// NewLocalClient constructs a LocalClient.
func NewLocalClient(repo *repository.Repository) *LocalClient {
	return &LocalClient{repo: repo}
}

// RecoverTask routes to the correct guarded repository transition.
func (c *LocalClient) RecoverTask(ctx context.Context, taskRunID string, status string, fencingToken int64) (bool, error) {
	switch status {
	case "CLAIMED":
		return c.repo.RecoverClaimedTask(ctx, taskRunID, fencingToken)
	case "RUNNING":
		return c.repo.RecoverRunningTask(ctx, taskRunID, fencingToken)
	default:
		return false, fmt.Errorf("recovery: unsupported task status %q", status)
	}
}
