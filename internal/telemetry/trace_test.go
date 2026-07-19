package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectExtractRoundTrip(t *testing.T) {
	tel, err := Init(Config{ServiceName: "trace-test", OTelDisabled: true, MetricsAddr: ":0"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	defer tel.Shutdown(context.Background())

	// Start a span so there is an active trace context to propagate.
	ctx, span := StartSpan(context.Background(), "producer")
	defer span.End()

	carrier := propagation.MapCarrier{}
	InjectContext(ctx, carrier)
	if carrier["traceparent"] == "" {
		t.Fatal("expected traceparent to be injected into carrier")
	}

	// Extract into a fresh context and confirm the trace ID matches.
	extracted := ExtractContext(context.Background(), carrier)
	got := trace.SpanContextFromContext(extracted)
	want := trace.SpanContextFromContext(ctx)
	if got.TraceID() != want.TraceID() {
		t.Fatalf("trace id mismatch: got %s want %s", got.TraceID(), want.TraceID())
	}
}

func TestStartSpanNoopWhenUninitialized(t *testing.T) {
	// Reset global so Tracer falls back to the no-op provider.
	mu.Lock()
	prev := globalTL
	globalTL = nil
	mu.Unlock()
	defer func() {
		mu.Lock()
		globalTL = prev
		mu.Unlock()
	}()

	_, span := StartSpan(context.Background(), "noop")
	if span.SpanContext().IsSampled() {
		t.Fatal("expected non-sampled span when telemetry is uninitialized")
	}
	span.End()
}
