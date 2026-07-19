package grpcutil

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/aman0603/flowforge/internal/proto/common"
	health "github.com/aman0603/flowforge/internal/proto/health"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type fakeChecker struct {
	status common.ServiceStatus
	detail string
}

func (f fakeChecker) Status(_ context.Context, _ bool) (common.ServiceStatus, string) {
	return f.status, f.detail
}

func startTestServer(t *testing.T, checker HealthChecker) (*grpc.ClientConn, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	health.RegisterHealthServiceServer(srv, NewHealthServer(checker))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}

	return conn, func() {
		conn.Close()
		srv.Stop()
	}
}

func TestHealthServiceCheck(t *testing.T) {
	conn, cleanup := startTestServer(t, fakeChecker{status: common.ServiceStatus_SERVICE_STATUS_HEALTHY, detail: "ok"})
	defer cleanup()

	client := health.NewHealthServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &health.HealthRequest{Readiness: true})
	if err != nil {
		t.Fatalf("Check RPC failed: %v", err)
	}
	if resp.GetStatus() != common.ServiceStatus_SERVICE_STATUS_HEALTHY {
		t.Fatalf("expected HEALTHY, got %v", resp.GetStatus())
	}
	if resp.GetDetail() != "ok" {
		t.Fatalf("expected detail 'ok', got %q", resp.GetDetail())
	}
}

func TestHealthServiceUnhealthy(t *testing.T) {
	conn, cleanup := startTestServer(t, fakeChecker{status: common.ServiceStatus_SERVICE_STATUS_UNHEALTHY, detail: "db down"})
	defer cleanup()

	client := health.NewHealthServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &health.HealthRequest{Readiness: false})
	if err != nil {
		t.Fatalf("Check RPC failed: %v", err)
	}
	if resp.GetStatus() != common.ServiceStatus_SERVICE_STATUS_UNHEALTHY {
		t.Fatalf("expected UNHEALTHY, got %v", resp.GetStatus())
	}
}
