package recovery

import (
	"context"
	"net"
	"testing"

	"github.com/aman0603/flowforge/internal/proto/common"
	pbrecov "github.com/aman0603/flowforge/internal/proto/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// stubRecovery implements pbrecov.RecoveryServiceServer with canned data.
type stubRecovery struct {
	pbrecov.UnimplementedRecoveryServiceServer
	claimedReclaimed bool
	runningReclaimed bool
	failClaimed      bool
	badStatus        bool
}

func (s *stubRecovery) RecoverTask(_ context.Context, req *pbrecov.RecoverTaskRequest) (*pbrecov.RecoverTaskResponse, error) {
	switch req.GetTaskStatus() {
	case "CLAIMED":
		if s.failClaimed {
			return &pbrecov.RecoverTaskResponse{Error: &common.ErrorDetail{Code: common.ErrorCode_ERROR_CODE_INTERNAL, Message: "boom"}}, nil
		}
		return &pbrecov.RecoverTaskResponse{Reclaimed: s.claimedReclaimed}, nil
	case "RUNNING":
		return &pbrecov.RecoverTaskResponse{Reclaimed: s.runningReclaimed}, nil
	default:
		return &pbrecov.RecoverTaskResponse{Error: &common.ErrorDetail{Code: common.ErrorCode_ERROR_CODE_VALIDATION, Message: "bad status"}}, nil
	}
}

func startStubServer(t *testing.T, srv pbrecov.RecoveryServiceServer) *GRPCClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pbrecov.RegisterRecoveryServiceServer(s, srv)
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := &GRPCClient{conn: conn, client: pbrecov.NewRecoveryServiceClient(conn)}
	t.Cleanup(func() { conn.Close(); s.Stop() })
	return client
}

func TestGRPCClientRecoverClaimed(t *testing.T) {
	stub := &stubRecovery{claimedReclaimed: true}
	client := startStubServer(t, stub)

	reclaimed, err := client.RecoverTask(context.Background(), "task-1", "CLAIMED", 4)
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if !reclaimed {
		t.Fatal("expected claimed task to be reclaimed")
	}
}

func TestGRPCClientRecoverRunning(t *testing.T) {
	stub := &stubRecovery{runningReclaimed: true}
	client := startStubServer(t, stub)

	reclaimed, err := client.RecoverTask(context.Background(), "task-2", "RUNNING", 9)
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if !reclaimed {
		t.Fatal("expected running task to be reclaimed")
	}
}

func TestGRPCClientRecoverServerError(t *testing.T) {
	stub := &stubRecovery{failClaimed: true}
	client := startStubServer(t, stub)

	_, err := client.RecoverTask(context.Background(), "task-3", "CLAIMED", 1)
	if err == nil {
		t.Fatal("expected error on server ErrorDetail")
	}
}

func TestGRPCClientRecoverBadStatus(t *testing.T) {
	stub := &stubRecovery{badStatus: true}
	client := startStubServer(t, stub)

	_, err := client.RecoverTask(context.Background(), "task-4", "BOGUS", 1)
	if err == nil {
		t.Fatal("expected error on unsupported status")
	}
}

// Compile-time check that both implementations satisfy Client.
var _ Client = (*LocalClient)(nil)
var _ Client = (*GRPCClient)(nil)
