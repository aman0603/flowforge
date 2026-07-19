package recovery

import (
	"context"

	"github.com/aman0603/flowforge/internal/proto/common"
	pbrecov "github.com/aman0603/flowforge/internal/proto/recovery"
	"github.com/aman0603/flowforge/internal/repository"
)

// GRPCServer implements the generated RecoveryService interface using the
// repository. It owns only guarded reclaim transitions; it never executes
// tasks. A standalone process (cmd/recovery) hosts it in Loop 4.
type GRPCServer struct {
	pbrecov.UnimplementedRecoveryServiceServer
	repo *repository.Repository
}

// NewGRPCServer constructs a Recovery gRPC server backed by repo.
func NewGRPCServer(repo *repository.Repository) *GRPCServer {
	return &GRPCServer{repo: repo}
}

// RecoverTask performs the guarded, fencing-token-checked reclaim transition.
func (s *GRPCServer) RecoverTask(ctx context.Context, req *pbrecov.RecoverTaskRequest) (*pbrecov.RecoverTaskResponse, error) {
	var reclaimed bool
	var err error
	switch req.GetTaskStatus() {
	case "CLAIMED":
		reclaimed, err = s.repo.RecoverClaimedTask(ctx, req.GetTaskRunId(), req.GetFencingToken())
	case "RUNNING":
		reclaimed, err = s.repo.RecoverRunningTask(ctx, req.GetTaskRunId(), req.GetFencingToken())
	default:
		return &pbrecov.RecoverTaskResponse{
			Error: &common.ErrorDetail{
				Code:    common.ErrorCode_ERROR_CODE_VALIDATION,
				Message: "unsupported task status: " + req.GetTaskStatus(),
			},
		}, nil
	}
	if err != nil {
		return &pbrecov.RecoverTaskResponse{
			Error: &common.ErrorDetail{
				Code:    common.ErrorCode_ERROR_CODE_INTERNAL,
				Message: err.Error(),
			},
		}, nil
	}
	return &pbrecov.RecoverTaskResponse{Reclaimed: reclaimed}, nil
}
