// Package httpmw provides HTTP middleware for FlowForge services: metrics,
// correlation IDs, and (in later loops) tracing and structured access logs.
package httpmw

import (
	"net/http"
	"strconv"
	"time"

	"github.com/aman0603/flowforge/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// correlationHeader is the HTTP header carrying the request/correlation ID.
const correlationHeader = "X-Request-ID"

// statusRecorder captures the response status code for metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Middleware wraps an http.Handler with correlation-ID propagation and request
// metrics. It is a no-op for metrics if telemetry metrics are not initialized.
func Middleware(next http.Handler) http.Handler {
	m := telemetry.GetMetrics()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Extract any inbound trace context (W3C traceparent) so this request
		// becomes a child of an upstream trace.
		ctx := telemetry.ExtractContext(r.Context(), propagation.HeaderCarrier(r.Header))

		// Correlation ID: reuse incoming header or mint a new one.
		cid := r.Header.Get(correlationHeader)
		if cid == "" {
			cid = telemetry.NewCorrelationID()
		}
		ctx = telemetry.WithCorrelationID(ctx, cid)
		w.Header().Set(correlationHeader, cid)

		// Server span for the request.
		route := routePattern(r)
		ctx, span := telemetry.StartSpan(ctx, "HTTP "+r.Method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.HTTPRoute(route),
				attribute.String("request_id", cid),
			),
		)
		defer span.End()

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		span.SetAttributes(semconv.HTTPResponseStatusCode(rec.status))

		if m == nil {
			return
		}
		attrs := metric.WithAttributes(
			attribute.String("method", r.Method),
			attribute.String("path", route),
			attribute.String("status", strconv.Itoa(rec.status)),
		)
		m.HTTPRequests.Add(ctx, 1, attrs)
		m.HTTPDuration.Record(ctx, time.Since(start).Seconds(), attrs)
	})
}

// routePattern returns a low-cardinality label for the request. Go 1.22+
// ServeMux exposes the matched pattern via r.Pattern.
func routePattern(r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	return r.URL.Path
}
