package recovery

import (
	"context"
	"fmt"
	"time"

	pbrecov "github.com/aman0603/flowforge/internal/proto/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCClient implements Client by calling a standalone Recovery gRPC service.
type GRPCClient struct {
	conn   *grpc.ClientConn
	client pbrecov.RecoveryServiceClient
}

// NewGRPCClient dials the Recovery service at addr and returns a Client. The
// caller must Close it when done.
func NewGRPCClient(ctx context.Context, addr string) (*GRPCClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("recovery gRPC address is empty")
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
	}, nil
}

// Close releases the underlying connection.
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

// RecoverTask calls the RecoveryService over gRPC.
func (c *GRPCClient) RecoverTask(ctx context.Context, taskRunID string, status string, fencingToken int64) (bool, error) {
	resp, err := c.client.RecoverTask(ctx, &pbrecov.RecoverTaskRequest{
		TaskRunId:    taskRunID,
		FencingToken: fencingToken,
		TaskStatus:   status,
	})
	if err != nil {
		return false, fmt.Errorf("recovery RecoverTask RPC failed: %w", err)
	}
	if resp.GetError() != nil {
		return false, fmt.Errorf("recovery RecoverTask error: %s", resp.GetError().GetMessage())
	}
	return resp.GetReclaimed(), nil
}
