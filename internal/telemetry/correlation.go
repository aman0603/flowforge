package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// correlationKey is the context key for the request/correlation ID.
type correlationKey struct{}

// WithCorrelationID stores a correlation ID in the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID extracts the correlation ID from the context, if present.
func CorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey{}).(string); ok {
		return v
	}
	return ""
}

// LoggerWithContext augments the global logger with trace/span/correlation
// fields derived from ctx so every log line is traceable. If no logger is
// initialized it returns a no-op logger.
func LoggerWithContext(ctx context.Context) *zap.Logger {
	l := Logger()
	sc := trace.SpanContextFromContext(ctx)
	fields := []zapcore.Field{}
	if sc.HasTraceID() {
		fields = append(fields, zap.String("trace_id", sc.TraceID().String()))
	}
	if sc.HasSpanID() {
		fields = append(fields, zap.String("span_id", sc.SpanID().String()))
	}
	if cid := CorrelationID(ctx); cid != "" {
		fields = append(fields, zap.String("request_id", cid))
	}
	if len(fields) == 0 {
		return l
	}
	return l.With(fields...)
}

// NewCorrelationID returns a random correlation ID. It is used when an incoming
// request lacks one.
func NewCorrelationID() string {
	return newUUID()
}
