// Package grpcmw provides gRPC server interceptors for FlowForge services:
// metrics and (in later loops) tracing and correlation propagation.
package grpcmw

import (
	"context"
	"time"

	"github.com/aman0603/flowforge/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// metadataCarrier adapts gRPC metadata to a TextMapCarrier for trace context
// propagation.
type metadataCarrier metadata.MD

func (c metadataCarrier) Get(key string) string {
	v := metadata.MD(c).Get(key)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (c metadataCarrier) Set(key, value string) { metadata.MD(c).Set(key, value) }

func (c metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// correlationMetaKey carries the correlation ID across gRPC boundaries.
const correlationMetaKey = "x-request-id"

// UnaryServerInterceptor records request count and latency for each unary RPC
// and creates a server span linked to any inbound trace context. It is safe to
// install even if telemetry is not initialized (spans become no-ops).
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		// Extract inbound trace context + correlation ID from metadata.
		md, _ := metadata.FromIncomingContext(ctx)
		if md == nil {
			md = metadata.MD{}
		}
		ctx = telemetry.ExtractContext(ctx, metadataCarrier(md))
		if vals := md.Get(correlationMetaKey); len(vals) > 0 && vals[0] != "" {
			ctx = telemetry.WithCorrelationID(ctx, vals[0])
		}

		ctx, span := telemetry.StartSpan(ctx, info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("rpc.method", info.FullMethod)),
		)
		resp, err := handler(ctx, req)
		telemetry.EndSpan(span, err)

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

// UnaryClientInterceptor injects the current trace context and correlation ID
// into outbound gRPC metadata and wraps the call in a client span.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, span := telemetry.StartSpan(ctx, method,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("rpc.method", method)),
		)
		defer span.End()

		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.MD{}
		} else {
			md = md.Copy()
		}
		telemetry.InjectContext(ctx, metadataCarrier(md))
		if cid := telemetry.CorrelationID(ctx); cid != "" {
			md.Set(correlationMetaKey, cid)
		}
		ctx = metadata.NewOutgoingContext(ctx, md)

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			span.RecordError(err)
		}
		return err
	}
}

var _ propagation.TextMapCarrier = metadataCarrier{}
