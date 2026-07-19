package scheduler

import (
	"context"

	"github.com/aman0603/flowforge/internal/proto/common"
	pbsched "github.com/aman0603/flowforge/internal/proto/scheduler"
	"github.com/aman0603/flowforge/internal/repository"
)

// GRPCServer implements the generated SchedulerService interface using the
// repository. It owns only claiming and retry promotion; it never executes
// tasks. A standalone process (cmd/scheduler) hosts it in Loop 4.
type GRPCServer struct {
	pbsched.UnimplementedSchedulerServiceServer
	repo *repository.Repository
}

// NewGRPCServer constructs a Scheduler gRPC server backed by repo.
func NewGRPCServer(repo *repository.Repository) *GRPCServer {
	return &GRPCServer{repo: repo}
}

// ClaimTasks claims READY tasks for the requesting worker.
func (s *GRPCServer) ClaimTasks(ctx context.Context, req *pbsched.ClaimTasksRequest) (*pbsched.ClaimTasksResponse, error) {
	tasks, err := s.repo.ClaimReadyTasksBatch(ctx, req.GetWorkerId(), int(req.GetCapacity()))
	if err != nil {
		return &pbsched.ClaimTasksResponse{
			Error: &common.ErrorDetail{
				Code:    common.ErrorCode_ERROR_CODE_INTERNAL,
				Message: err.Error(),
			},
		}, nil
	}

	out := make([]*pbsched.ClaimedTask, 0, len(tasks))
	for _, t := range tasks {
		var input []byte
		if len(t.Input) > 0 {
			input = t.Input
		}
		out = append(out, &pbsched.ClaimedTask{
			TaskRunId:        t.TaskRunID,
			WorkflowRunId:    t.WorkflowRunID,
			TaskDefinitionId: t.TaskDefinitionID,
			Name:             t.Name,
			TaskType:         t.TaskType,
			TimeoutMs:        int64(t.TimeoutMs),
			FencingToken:     t.FencingToken,
			Input:            input,
		})
	}
	return &pbsched.ClaimTasksResponse{Tasks: out}, nil
}

// PromoteRetries promotes due RETRY_WAIT tasks to READY.
func (s *GRPCServer) PromoteRetries(ctx context.Context, _ *pbsched.PromoteRetriesRequest) (*pbsched.PromoteRetriesResponse, error) {
	promoted, err := s.repo.PromoteDueRetries(ctx)
	if err != nil {
		return &pbsched.PromoteRetriesResponse{
			Error: &common.ErrorDetail{
				Code:    common.ErrorCode_ERROR_CODE_INTERNAL,
				Message: err.Error(),
			},
		}, nil
	}
	return &pbsched.PromoteRetriesResponse{Promoted: promoted}, nil
}
