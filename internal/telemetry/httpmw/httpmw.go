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

		// Correlation ID: reuse incoming header or mint a new one.
		cid := r.Header.Get(correlationHeader)
		if cid == "" {
			cid = telemetry.NewCorrelationID()
		}
		ctx := telemetry.WithCorrelationID(r.Context(), cid)
		w.Header().Set(correlationHeader, cid)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		if m == nil {
			return
		}
		attrs := metric.WithAttributes(
			attribute.String("method", r.Method),
			attribute.String("path", routePattern(r)),
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
