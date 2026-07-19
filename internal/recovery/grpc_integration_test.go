//go:build integration

package recovery

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	pbrecov "github.com/aman0603/flowforge/internal/proto/recovery"
	"github.com/aman0603/flowforge/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newIntegrationRepo(t *testing.T) *repository.Repository {
	dbURL := os.Getenv("TEST_DB_URL")
	if dbURL == "" {
		t.Skip("TEST_DB_URL missing, skipping recovery integration test")
	}
	repo, err := repository.New(dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { repo.Close() })
	return repo
}

func TestRecoveryGRPCServerIntegration(t *testing.T) {
	repo := newIntegrationRepo(t)

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pbrecov.RegisterRecoveryServiceServer(srv, NewGRPCServer(repo))
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

	client := &GRPCClient{conn: conn, client: pbrecov.NewRecoveryServiceClient(conn)}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Recovering an unknown (but well-formed) task must not error and must
	// report not reclaimed.
	reclaimed, err := client.RecoverTask(ctx, "00000000-0000-0000-0000-000000000000", "CLAIMED", 1)
	if err != nil {
		t.Fatalf("RecoverTask integration error: %v", err)
	}
	if reclaimed {
		t.Fatal("expected unknown task to not be reclaimed")
	}
}
