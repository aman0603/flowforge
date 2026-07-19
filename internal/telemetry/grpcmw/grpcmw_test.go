package grpcmw

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aman0603/flowforge/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

func TestClientInterceptorPropagatesTraceAndCorrelation(t *testing.T) {
	tel, err := telemetry.Init(telemetry.Config{ServiceName: "grpcmw-prop-test", OTelDisabled: true, MetricsAddr: ":0"})
	if err != nil {
		t.Fatalf("telemetry init: %v", err)
	}
	defer tel.Shutdown(context.Background())

	client := UnaryClientInterceptor()
	server := UnaryServerInterceptor()

	// Start a client span + correlation ID.
	ctx := telemetry.WithCorrelationID(context.Background(), "corr-123")
	ctx, span := telemetry.StartSpan(ctx, "caller")
	defer span.End()
	wantTrace := trace.SpanContextFromContext(ctx).TraceID()

	var gotCorrelation string
	var gotTrace trace.TraceID

	// The client interceptor's invoker simulates the wire: it hands the
	// outgoing metadata to the server interceptor as incoming metadata.
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		serverCtx := metadata.NewIncomingContext(context.Background(), md)
		_, err := server(serverCtx, nil, &grpc.UnaryServerInfo{FullMethod: method},
			func(ctx context.Context, req interface{}) (interface{}, error) {
				gotCorrelation = telemetry.CorrelationID(ctx)
				gotTrace = trace.SpanContextFromContext(ctx).TraceID()
				return nil, nil
			})
		return err
	}

	if err := client(ctx, "/svc/Method", nil, nil, nil, invoker); err != nil {
		t.Fatalf("client interceptor error: %v", err)
	}
	if gotCorrelation != "corr-123" {
		t.Fatalf("correlation id = %q, want corr-123", gotCorrelation)
	}
	if gotTrace != wantTrace {
		t.Fatalf("trace id = %s, want %s", gotTrace, wantTrace)
	}
}
