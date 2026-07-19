package grpcmw

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aman0603/flowforge/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryServerInterceptorRecordsMetrics(t *testing.T) {
	tel, err := telemetry.Init(telemetry.Config{ServiceName: "grpcmw-test", OTelDisabled: true, MetricsAddr: ":0"})
	if err != nil {
		t.Fatalf("telemetry init: %v", err)
	}
	defer tel.Shutdown(context.Background())
	if _, err := telemetry.InitMetrics(); err != nil {
		t.Fatalf("init metrics: %v", err)
	}

	interceptor := UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/flowforge.SchedulerService/ClaimTasks"}

	// Success call.
	if _, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Error call.
	_, _ = interceptor(context.Background(), nil, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, status.Error(codes.Unavailable, "down")
	})

	mrec := httptest.NewRecorder()
	promhttp.HandlerFor(telemetry.GetMetricsRegistry(), promhttp.HandlerOpts{}).
		ServeHTTP(mrec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := mrec.Body.String()
	if !strings.Contains(body, "flowforge_grpc_requests_total") {
		t.Fatalf("expected grpc request counter, got:\n%s", body)
	}
}
