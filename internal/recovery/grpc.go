package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/aman0603/flowforge/internal/grpcutil"
	pbrecov "github.com/aman0603/flowforge/internal/proto/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCClient implements Client by calling a standalone Recovery gRPC service.
type GRPCClient struct {
	conn   *grpc.ClientConn
	client pbrecov.RecoveryServiceClient
	opts   grpcutil.CallOptions
}

// NewGRPCClient dials the Recovery service at addr and returns a Client. The
// caller must Close it when done. opts is optional; nil selects defaults.
func NewGRPCClient(ctx context.Context, addr string, opts ...*grpcutil.CallOptions) (*GRPCClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("recovery gRPC address is empty")
	}
	callOpts := grpcutil.DefaultCallOptions()
	if len(opts) > 0 && opts[0] != nil {
		callOpts = *opts[0]
	}
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial recovery at %s: %w", addr, err)
	}
	return &GRPCClient{
		conn:   conn,
		client: pbrecov.NewRecoveryServiceClient(conn),
		opts:   callOpts,
	}, nil
}

// Close releases the underlying connection.
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

// RecoverTask calls the RecoveryService over gRPC with resilience.
func (c *GRPCClient) RecoverTask(ctx context.Context, taskRunID string, status string, fencingToken int64) (bool, error) {
	var resp *pbrecov.RecoverTaskResponse
	err := grpcutil.Call(ctx, c.opts, func(ctx context.Context) error {
		r, callErr := c.client.RecoverTask(ctx, &pbrecov.RecoverTaskRequest{
			TaskRunId:    taskRunID,
			FencingToken: fencingToken,
			TaskStatus:   status,
		})
		if callErr != nil {
			return callErr
		}
		resp = r
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("recovery RecoverTask RPC failed: %w", err)
	}
	if resp.GetError() != nil {
		return false, fmt.Errorf("recovery RecoverTask error: %s", resp.GetError().GetMessage())
	}
	return resp.GetReclaimed(), nil
}
