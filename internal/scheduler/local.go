package scheduler

import (
	"context"

	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/repository"
)

// Client abstracts the scheduling operations the Worker depends on. It has two
// implementations: a local one backed directly by the repository (default, for
// the modular monolith), and a gRPC one that calls a standalone Scheduler
// service. This seam lets the scheduler run either in-process or as a
// standalone service without changing worker execution logic.
type Client interface {
	// ClaimTasks claims up to capacity READY tasks for the worker.
	ClaimTasks(ctx context.Context, workerID string, capacity int) ([]*model.ClaimedTask, error)
	// PromoteRetries promotes due RETRY_WAIT tasks to READY.
	PromoteRetries(ctx context.Context) (int64, error)
}

// LocalClient implements Client using the repository directly. It provides
// in-process behavior when no gRPC scheduler is configured.
type LocalClient struct {
	repo *repository.Repository
}

// NewLocalClient constructs a LocalClient.
func NewLocalClient(repo *repository.Repository) *LocalClient {
	return &LocalClient{repo: repo}
}

// ClaimTasks delegates to the repository batch claim.
func (c *LocalClient) ClaimTasks(ctx context.Context, workerID string, capacity int) ([]*model.ClaimedTask, error) {
	return c.repo.ClaimReadyTasksBatch(ctx, workerID, capacity)
}

// PromoteRetries delegates to the repository retry promotion.
func (c *LocalClient) PromoteRetries(ctx context.Context) (int64, error) {
	return c.repo.PromoteDueRetries(ctx)
}
