package grpcutil

import (
	"context"
	"time"

	"github.com/aman0603/flowforge/internal/proto/common"
	"github.com/aman0603/flowforge/internal/proto/health"
	"github.com/aman0603/flowforge/internal/repository"
)

// DBHealthChecker reports a service's health based on database connectivity.
// It implements HealthChecker and is reused by every service that owns a repo.
type DBHealthChecker struct {
	repo *repository.Repository
}

// NewDBHealthChecker constructs a DBHealthChecker.
func NewDBHealthChecker(repo *repository.Repository) *DBHealthChecker {
	return &DBHealthChecker{repo: repo}
}

// Status returns HEALTHY if the database can be pinged, otherwise UNHEALTHY.
func (c *DBHealthChecker) Status(ctx context.Context, _ bool) (common.ServiceStatus, string) {
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.repo.Ping(pingCtx); err != nil {
		return common.ServiceStatus_SERVICE_STATUS_UNHEALTHY, "database unreachable: " + err.Error()
	}
	return common.ServiceStatus_SERVICE_STATUS_HEALTHY, ""
}

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
