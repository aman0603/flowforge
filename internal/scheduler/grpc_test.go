package scheduler

import (
	"context"
	"net"
	"testing"

	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/proto/common"
	pbsched "github.com/aman0603/flowforge/internal/proto/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// stubScheduler implements pbsched.SchedulerServiceServer with canned data so we
// can validate the gRPC client mapping without a database.
type stubScheduler struct {
	pbsched.UnimplementedSchedulerServiceServer
	tasks     []*pbsched.ClaimedTask
	promoted  int64
	failClaim bool
}

func (s *stubScheduler) ClaimTasks(_ context.Context, req *pbsched.ClaimTasksRequest) (*pbsched.ClaimTasksResponse, error) {
	if s.failClaim {
		return &pbsched.ClaimTasksResponse{
			Error: &common.ErrorDetail{
				Code:    common.ErrorCode_ERROR_CODE_INTERNAL,
				Message: "claim failed",
			},
		}, nil
	}
	if req.GetCapacity() <= 0 {
		return &pbsched.ClaimTasksResponse{}, nil
	}
	return &pbsched.ClaimTasksResponse{Tasks: s.tasks}, nil
}

func (s *stubScheduler) PromoteRetries(_ context.Context, _ *pbsched.PromoteRetriesRequest) (*pbsched.PromoteRetriesResponse, error) {
	return &pbsched.PromoteRetriesResponse{Promoted: s.promoted}, nil
}

func startStubServer(t *testing.T, srv pbsched.SchedulerServiceServer) *GRPCClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pbsched.RegisterSchedulerServiceServer(s, srv)
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

	client := &GRPCClient{conn: conn, client: pbsched.NewSchedulerServiceClient(conn)}
	t.Cleanup(func() { conn.Close(); s.Stop() })
	return client
}

func TestGRPCClientClaimTasksMapsCorrectly(t *testing.T) {
	stub := &stubScheduler{tasks: []*pbsched.ClaimedTask{
		{
			TaskRunId:        "t1",
			WorkflowRunId:    "w1",
			TaskDefinitionId: "d1",
			Name:             "first",
			TaskType:         "SLEEP",
			TimeoutMs:        5000,
			FencingToken:     3,
			Input:            []byte(`{"a":1}`),
		},
	}}
	client := startStubServer(t, stub)

	tasks, err := client.ClaimTasks(context.Background(), "worker-1", 10)
	if err != nil {
		t.Fatalf("ClaimTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	got := tasks[0]
	if got.TaskRunID != "t1" || got.WorkflowRunID != "w1" || got.TaskType != "SLEEP" {
		t.Fatalf("incorrect mapping: %+v", got)
	}
	if got.FencingToken != 3 || got.TimeoutMs != 5000 {
		t.Fatalf("incorrect scalar mapping: %+v", got)
	}
	if string(got.Input) != `{"a":1}` {
		t.Fatalf("incorrect input mapping: %s", got.Input)
	}
}

func TestGRPCClientClaimTasksServerError(t *testing.T) {
	stub := &stubScheduler{failClaim: true}
	client := startStubServer(t, stub)

	_, err := client.ClaimTasks(context.Background(), "worker-1", 10)
	if err == nil {
		t.Fatal("expected error when server returns ErrorDetail")
	}
}

func TestGRPCClientPromoteRetries(t *testing.T) {
	stub := &stubScheduler{promoted: 7}
	client := startStubServer(t, stub)

	n, err := client.PromoteRetries(context.Background())
	if err != nil {
		t.Fatalf("PromoteRetries: %v", err)
	}
	if n != 7 {
		t.Fatalf("expected 7 promoted, got %d", n)
	}
}

// Compile-time check that both implementations satisfy the Client interface.
var _ Client = (*LocalClient)(nil)
var _ Client = (*GRPCClient)(nil)

// Ensure the model import is used (ClaimedTask is the domain type the client
// returns); referenced here to keep the test package self-documenting.
var _ = model.ClaimedTask{}
