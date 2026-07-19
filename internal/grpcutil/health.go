package grpcutil

import (
	"context"

	"github.com/aman0603/flowforge/internal/proto/common"
	"github.com/aman0603/flowforge/internal/proto/health"
)

// HealthChecker abstracts the liveness/readiness signal a service exposes.
type HealthChecker interface {
	// Status returns the current service status and an optional detail string.
	Status(ctx context.Context, readiness bool) (common.ServiceStatus, string)
}

// HealthServer implements the generated HealthService interface.
type HealthServer struct {
	health.UnimplementedHealthServiceServer
	checker HealthChecker
}

// NewHealthServer constructs a gRPC HealthService server backed by checker.
func NewHealthServer(checker HealthChecker) *HealthServer {
	return &HealthServer{checker: checker}
}

// Check returns the probed service health.
func (s *HealthServer) Check(ctx context.Context, req *health.HealthRequest) (*health.HealthResponse, error) {
	status, detail := s.checker.Status(ctx, req.GetReadiness())
	return &health.HealthResponse{
		Status: status,
		Detail: detail,
	}, nil
}
