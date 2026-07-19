//go:build integration

package scheduler

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	pbsched "github.com/aman0603/flowforge/internal/proto/scheduler"
	"github.com/aman0603/flowforge/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newIntegrationRepo(t *testing.T) *repository.Repository {
	dbURL := os.Getenv("TEST_DB_URL")
	if dbURL == "" {
		t.Skip("TEST_DB_URL missing, skipping scheduler integration test")
	}
	repo, err := repository.New(dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { repo.Close() })
	return repo
}

func TestSchedulerGRPCServerIntegration(t *testing.T) {
	repo := newIntegrationRepo(t)

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pbsched.RegisterSchedulerServiceServer(srv, NewGRPCServer(repo))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := &GRPCClient{conn: conn, client: pbsched.NewSchedulerServiceClient(conn)}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Claim against an (likely empty) database must not error.
	if _, err := client.ClaimTasks(ctx, "integration-worker", 10); err != nil {
		t.Fatalf("ClaimTasks integration error: %v", err)
	}

	// PromoteRetries must return a count without error.
	if _, err := client.PromoteRetries(ctx); err != nil {
		t.Fatalf("PromoteRetries integration error: %v", err)
	}
}
