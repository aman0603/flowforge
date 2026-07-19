package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope used for spans created via the
// package-level helpers.
const tracerName = "github.com/aman0603/flowforge"

// StartSpan starts a span using the global tracer. When tracing is disabled the
// returned span is a no-op and ctx is unchanged in observable behavior. Callers
// must End the span (typically via defer).
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer(tracerName).Start(ctx, name, opts...)
}

// EndSpan ends span, recording err as the span status when non-nil. It is a
// convenience for the common defer pattern.
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// Propagator returns the globally configured TextMap propagator (W3C
// TraceContext + Baggage). Init installs it; before Init it is the OTel default.
func Propagator() propagation.TextMapPropagator {
	return otel.GetTextMapPropagator()
}

// InjectContext writes trace context from ctx into the supplied carrier so it
// can travel across a process boundary (Kafka headers, custom transports).
func InjectContext(ctx context.Context, carrier propagation.TextMapCarrier) {
	Propagator().Inject(ctx, carrier)
}

// ExtractContext reads trace context from carrier and returns a context that
// carries it, so a downstream span becomes a child of the upstream trace.
func ExtractContext(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return Propagator().Extract(ctx, carrier)
}

// SpanAttrString is a small helper for adding a string attribute to the current
// span in ctx without importing the attribute package at call sites.
func SpanAttrString(ctx context.Context, key, value string) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String(key, value))
}
