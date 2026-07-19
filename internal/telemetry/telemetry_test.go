package telemetry

import (
	"context"
	"testing"
)

func TestInitAndShutdown(t *testing.T) {
	tel, err := Init(Config{
		ServiceName:  "flowforge-test",
		OTelDisabled: true,
		MetricsAddr:  ":0",
		LogLevel:     "info",
	})
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	if tel.ServiceName() != "flowforge-test" {
		t.Fatalf("unexpected service name %q", tel.ServiceName())
	}
	if tel.Logger() == nil {
		t.Fatal("expected non-nil logger")
	}
	if tel.Tracer("t") == nil {
		t.Fatal("expected non-nil tracer")
	}
	if tel.Meter("m") == nil {
		t.Fatal("expected non-nil meter")
	}
	if tel.PrometheusRegistry() == nil {
		t.Fatal("expected non-nil registry")
	}

	// Shutdown must be idempotent.
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown returned error: %v", err)
	}
}

func TestPackageHelpersFallbackToNoop(t *testing.T) {
	// Even without Init, package helpers must not panic.
	if Logger() == nil {
		t.Fatal("Logger fallback nil")
	}
	if Tracer("x") == nil {
		t.Fatal("Tracer fallback nil")
	}
	if Meter("x") == nil {
		t.Fatal("Meter fallback nil")
	}
}

func TestCorrelationRoundTrip(t *testing.T) {
	ctx := WithCorrelationID(context.Background(), "abc-123")
	if got := CorrelationID(ctx); got != "abc-123" {
		t.Fatalf("CorrelationID = %q, want abc-123", got)
	}
	if CorrelationID(context.Background()) != "" {
		t.Fatal("expected empty correlation id on bare context")
	}
	if NewCorrelationID() == "" {
		t.Fatal("NewCorrelationID returned empty")
	}
}
