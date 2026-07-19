package outbox

import (
	"context"
	"testing"

	"github.com/aman0603/flowforge/internal/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestKafkaHeaderCarrierSetGetKeys(t *testing.T) {
	var headers []kafka.Header
	c := NewKafkaHeaderCarrier(&headers)

	c.Set("a", "1")
	c.Set("b", "2")
	c.Set("a", "3") // overwrite

	if got := c.Get("a"); got != "3" {
		t.Fatalf("Get(a) = %q, want 3", got)
	}
	if got := c.Get("missing"); got != "" {
		t.Fatalf("Get(missing) = %q, want empty", got)
	}
	if len(c.Keys()) != 2 {
		t.Fatalf("Keys() = %v, want 2 keys", c.Keys())
	}
	if len(headers) != 2 {
		t.Fatalf("backing slice len = %d, want 2", len(headers))
	}
}

func TestKafkaTraceContextRoundTrip(t *testing.T) {
	tel, err := telemetry.Init(telemetry.Config{ServiceName: "kafka-trace-test", OTelDisabled: true, MetricsAddr: ":0"})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	defer tel.Shutdown(context.Background())

	ctx, span := telemetry.StartSpan(context.Background(), "produce")
	defer span.End()

	var headers []kafka.Header
	telemetry.InjectContext(ctx, NewKafkaHeaderCarrier(&headers))

	// Simulate consumer extracting from received headers.
	consumerCtx := telemetry.ExtractContext(context.Background(), NewKafkaHeaderCarrier(&headers))
	if trace.SpanContextFromContext(consumerCtx).TraceID() != trace.SpanContextFromContext(ctx).TraceID() {
		t.Fatal("trace id did not propagate through kafka headers")
	}
}

var _ propagation.TextMapCarrier = KafkaHeaderCarrier{}
