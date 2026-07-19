package telemetry

import (
	"context"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServeMetrics starts an HTTP server exposing the Prometheus registry at
// /metrics on the configured address. It blocks until the context is cancelled,
// then shuts down gracefully. Call from a goroutine.
func (t *Telemetry) ServeMetrics(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(t.registry, promhttp.HandlerOpts{}))
	if t.pprofEnabled {
		registerPProf(mux)
	}
	srv := &http.Server{
		Addr:              t.MetricsAddr(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// registerPProf mounts the standard net/http/pprof handlers under /debug/pprof
// on the supplied mux. Kept separate so it can be unit-tested in isolation.
func registerPProf(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// MetricsAddr returns the configured metrics listen address.
func (t *Telemetry) MetricsAddr() string { return t.metricsAddr }

// SetMetricsAddr records the metrics listen address on the telemetry instance.
func (t *Telemetry) SetMetricsAddr(addr string) { t.metricsAddr = addr }
