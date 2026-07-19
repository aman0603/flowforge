package httpmw

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aman0603/flowforge/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMiddlewareCorrelationAndMetrics(t *testing.T) {
	tel, err := telemetry.Init(telemetry.Config{ServiceName: "httpmw-test", OTelDisabled: true, MetricsAddr: ":0"})
	if err != nil {
		t.Fatalf("telemetry init: %v", err)
	}
	defer tel.Shutdown(context.Background())
	if _, err := telemetry.InitMetrics(); err != nil {
		t.Fatalf("init metrics: %v", err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if telemetry.CorrelationID(r.Context()) == "" {
			t.Error("expected correlation id in context")
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/runs/123", nil)
	rec := httptest.NewRecorder()
	Middleware(handler).ServeHTTP(rec, req)

	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID response header")
	}

	// Scrape the registry and confirm the HTTP counter is present.
	mrec := httptest.NewRecorder()
	promhttp.HandlerFor(telemetry.GetMetricsRegistry(), promhttp.HandlerOpts{}).
		ServeHTTP(mrec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := mrec.Body.String()
	if !strings.Contains(body, "flowforge_http_requests_total") {
		t.Fatalf("expected http request counter in /metrics, got:\n%s", body)
	}
}

func TestMiddlewarePropagatesIncomingCorrelationID(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := telemetry.CorrelationID(r.Context()); got != "given-id" {
			t.Errorf("correlation id = %q, want given-id", got)
		}
	})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Request-ID", "given-id")
	rec := httptest.NewRecorder()
	Middleware(handler).ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-ID") != "given-id" {
		t.Fatalf("expected echoed correlation id, got %q", rec.Header().Get("X-Request-ID"))
	}
}
