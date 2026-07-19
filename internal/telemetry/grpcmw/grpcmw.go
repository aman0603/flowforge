// Package grpcmw provides gRPC server interceptors for FlowForge services:
// metrics and (in later loops) tracing and correlation propagation.
package grpcmw

import (
	"context"
	"time"

	"github.com/aman0603/flowforge/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor records request count and latency for each unary RPC.
// It is safe to install even if telemetry metrics are not initialized.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		m := telemetry.GetMetrics()
		if m == nil {
			return resp, err
		}
		code := status.Code(err).String()
		attrs := metric.WithAttributes(
			attribute.String("method", info.FullMethod),
			attribute.String("code", code),
		)
		m.GRPCRequests.Add(ctx, 1, attrs)
		m.GRPCDuration.Record(ctx, time.Since(start).Seconds(), attrs)
		return resp, err
	}
}
