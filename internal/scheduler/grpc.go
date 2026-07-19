package scheduler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aman0603/flowforge/internal/grpcutil"
	"github.com/aman0603/flowforge/internal/model"
	pbsched "github.com/aman0603/flowforge/internal/proto/scheduler"
	"google.golang.org/grpc"
)

// GRPCClient implements Client by calling a standalone Scheduler gRPC service.
type GRPCClient struct {
	conn   *grpc.ClientConn
	client pbsched.SchedulerServiceClient
	opts   grpcutil.CallOptions
}

// NewGRPCClient dials the Scheduler service at addr and returns a Client. The
// caller must Close it when done. opts is optional; nil selects defaults.
func NewGRPCClient(ctx context.Context, addr string, opts ...*grpcutil.CallOptions) (*GRPCClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("scheduler gRPC address is empty")
	}
	callOpts := grpcutil.DefaultCallOptions()
	if len(opts) > 0 && opts[0] != nil {
		callOpts = *opts[0]
	}
	conn, err := grpcutil.DialTLS(ctx, addr, grpcutil.TLSConfigFromEnv())
	if err != nil {
		return nil, fmt.Errorf("failed to dial scheduler at %s: %w", addr, err)
	}
	return &GRPCClient{
		conn:   conn,
		client: pbsched.NewSchedulerServiceClient(conn),
		opts:   callOpts,
	}, nil
}

// Close releases the underlying connection.
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

// ClaimTasks calls the SchedulerService over gRPC and maps the response back to
// domain models. A structured error from the server is returned as a Go error.
// The call is wrapped with deadline + retry/backoff resilience.
func (c *GRPCClient) ClaimTasks(ctx context.Context, workerID string, capacity int) ([]*model.ClaimedTask, error) {
	var resp *pbsched.ClaimTasksResponse
	err := grpcutil.Call(ctx, c.opts, func(ctx context.Context) error {
		r, callErr := c.client.ClaimTasks(ctx, &pbsched.ClaimTasksRequest{
			WorkerId: workerID,
			Capacity: int32(capacity),
		})
		if callErr != nil {
			return callErr
		}
		resp = r
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scheduler ClaimTasks RPC failed: %w", err)
	}
	if resp.GetError() != nil {
		return nil, fmt.Errorf("scheduler ClaimTasks error: %s", resp.GetError().GetMessage())
	}

	tasks := make([]*model.ClaimedTask, 0, len(resp.GetTasks()))
	for _, t := range resp.GetTasks() {
		var input json.RawMessage
		if len(t.GetInput()) > 0 {
			input = json.RawMessage(t.GetInput())
		}
		tasks = append(tasks, &model.ClaimedTask{
			TaskRunID:        t.GetTaskRunId(),
			WorkflowRunID:    t.GetWorkflowRunId(),
			TaskDefinitionID: t.GetTaskDefinitionId(),
			Name:             t.GetName(),
			TaskType:         t.GetTaskType(),
			TimeoutMs:        int(t.GetTimeoutMs()),
			FencingToken:     t.GetFencingToken(),
			Input:            input,
		})
	}
	return tasks, nil
}

// PromoteRetries calls the SchedulerService over gRPC with resilience.
func (c *GRPCClient) PromoteRetries(ctx context.Context) (int64, error) {
	var resp *pbsched.PromoteRetriesResponse
	err := grpcutil.Call(ctx, c.opts, func(ctx context.Context) error {
		r, callErr := c.client.PromoteRetries(ctx, &pbsched.PromoteRetriesRequest{})
		if callErr != nil {
			return callErr
		}
		resp = r
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("scheduler PromoteRetries RPC failed: %w", err)
	}
	if resp.GetError() != nil {
		return 0, fmt.Errorf("scheduler PromoteRetries error: %s", resp.GetError().GetMessage())
	}
	return resp.GetPromoted(), nil
}
