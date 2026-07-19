package recovery

import (
	"context"
	"fmt"

	"github.com/aman0603/flowforge/internal/repository"
	"github.com/aman0603/flowforge/internal/telemetry"
)

// Client abstracts the recovery operations the Worker depends on. Two
// implementations exist: a local one backed directly by the repository (the
// modular monolith default) and a gRPC one calling a standalone Recovery
// service. This seam lets recovery run either in-process or as a standalone
// service without changing the worker's lease-aware reclaim logic.
type Client interface {
	// RecoverTask reclaims a single stale task, dispatching on its observed
	// status ("CLAIMED" or "RUNNING"), using the provided fencing token. It
	// returns true if the task was actually reclaimed.
	RecoverTask(ctx context.Context, taskRunID string, status string, fencingToken int64) (bool, error)
}

// LocalClient implements Client using the repository directly, providing
// in-process behavior when no gRPC recovery service is configured.
type LocalClient struct {
	repo *repository.Repository
}

// NewLocalClient constructs a LocalClient.
func NewLocalClient(repo *repository.Repository) *LocalClient {
	return &LocalClient{repo: repo}
}

// RecoverTask routes to the correct guarded repository transition.
func (c *LocalClient) RecoverTask(ctx context.Context, taskRunID string, status string, fencingToken int64) (bool, error) {
	var (
		reclaimed bool
		err       error
	)
	switch status {
	case "CLAIMED":
		reclaimed, err = c.repo.RecoverClaimedTask(ctx, taskRunID, fencingToken)
	case "RUNNING":
		reclaimed, err = c.repo.RecoverRunningTask(ctx, taskRunID, fencingToken)
	default:
		return false, fmt.Errorf("recovery: unsupported task status %q", status)
	}
	if reclaimed && err == nil {
		if m := telemetry.GetMetrics(); m != nil {
			m.TasksRecovered.Add(ctx, 1)
		}
	}
	return reclaimed, err
}
