// Package telemetry centralizes observability setup for FlowForge services.
//
// A single Init wires an OpenTelemetry TracerProvider (OTLP, opt-out via
// OTEL_DISABLED), a Prometheus-backed MeterProvider, and a structured JSON
// logger. Services call Shutdown on exit to flush. When OTEL_DISABLED is true
// (the default in development) tracing becomes a no-op and metrics still work,
// so enabling observability never changes business behavior.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Telemetry bundles the initialized providers and logger for a process.
type Telemetry struct {
	serviceName  string
	logger       *zap.Logger
	tracerProv   *sdktrace.TracerProvider
	meterProv    metric.MeterProvider
	registry     *promclient.Registry
	metricsAddr  string
	shutdownOnce sync.Once
	shutdownFns  []func(context.Context) error
}

var (
	mu       sync.RWMutex
	globalTL *Telemetry
)

const shutdownTimeout = 5 * time.Second

// Config is the minimal observability configuration sourced from app config.
type Config struct {
	ServiceName      string
	OTelDisabled     bool
	ExporterEndpoint string
	MetricsAddr      string
	LogLevel         string
}

// Init builds the providers and logger and stores them as the package-global
// telemetry instance. It is safe to call once at process start. The returned
// Telemetry is also accessible via the package helpers (Logger, Tracer, Meter).
func Init(cfg Config) (*Telemetry, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "flowforge"
	}

	logger, err := newLogger(cfg.ServiceName, cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("telemetry: build logger: %w", err)
	}

	t := &Telemetry{
		serviceName: cfg.ServiceName,
		logger:      logger,
	}

	// MeterProvider with Prometheus exporter (always on so /metrics works).
	res, err := resource.New(context.Background(),
		resource.WithAttributes(attribute.String("service.name", cfg.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}
	promReg := promclient.NewRegistry()
	promExporter, err := prometheus.New(prometheus.WithRegisterer(promReg))
	if err != nil {
		return nil, fmt.Errorf("telemetry: prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithResource(res),
	)
	t.meterProv = mp
	t.registry = promReg
	t.metricsAddr = cfg.MetricsAddr
	t.shutdownFns = append(t.shutdownFns, func(ctx context.Context) error {
		sCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
		defer cancel()
		return mp.Shutdown(sCtx)
	})

	// TracerProvider: OTLP unless disabled.
	if cfg.OTelDisabled {
		t.tracerProv = sdktrace.NewTracerProvider(sdktrace.WithResource(res))
		logger.Info("telemetry tracing disabled (OTEL_DISABLED=true)")
	} else {
		exp, err := otlptracegrpc.New(context.Background(),
			otlptracegrpc.WithEndpoint(cfg.ExporterEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
		}
		t.tracerProv = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
		)
		t.shutdownFns = append(t.shutdownFns, func(ctx context.Context) error {
			return t.tracerProv.Shutdown(ctx)
		})
		logger.Info("telemetry tracing enabled", zap.String("otlp_endpoint", cfg.ExporterEndpoint))
	}

	// Install global propagator so HTTP/gRPC/Kafka carry trace context.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	mu.Lock()
	globalTL = t
	mu.Unlock()

	return t, nil
}

// Shutdown flushes exporters. Safe to call multiple times.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var lastErr error
	t.shutdownOnce.Do(func() {
		for _, fn := range t.shutdownFns {
			if err := fn(ctx); err != nil {
				lastErr = err
			}
		}
		_ = t.logger.Sync()
	})
	return lastErr
}

// ServiceName returns the configured service name.
func (t *Telemetry) ServiceName() string { return t.serviceName }

// Logger returns the structured logger.
func (t *Telemetry) Logger() *zap.Logger { return t.logger }

// Tracer returns a named tracer.
func (t *Telemetry) Tracer(name string) trace.Tracer { return t.tracerProv.Tracer(name) }

// Meter returns a named meter.
func (t *Telemetry) Meter(name string) metric.Meter { return t.meterProv.Meter(name) }

// PrometheusRegistry returns the underlying registry served at /metrics.
func (t *Telemetry) PrometheusRegistry() *promclient.Registry { return t.registry }

func newLogger(service, level string) (*zap.Logger, error) {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "timestamp"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg),
		zapcore.Lock(os.Stdout),
		lvl,
	)
	return zap.New(core).With(zap.String("service", service)), nil
}

// package-level helpers -------------------------------------------------------

// Logger returns the global logger, falling back to a no-op-safe logger.
func Logger() *zap.Logger {
	mu.RLock()
	defer mu.RUnlock()
	if globalTL != nil {
		return globalTL.logger
	}
	return zap.NewNop()
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	mu.RLock()
	defer mu.RUnlock()
	if globalTL != nil {
		return globalTL.tracerProv.Tracer(name)
	}
	return trace.NewNoopTracerProvider().Tracer(name)
}

// Meter returns a named meter from the global provider.
func Meter(name string) metric.Meter {
	mu.RLock()
	defer mu.RUnlock()
	if globalTL != nil {
		return globalTL.meterProv.Meter(name)
	}
	return metricnoop.NewMeterProvider().Meter(name)
}

// Shutdown flushes the global telemetry instance.
func Shutdown(ctx context.Context) error {
	mu.RLock()
	t := globalTL
	mu.RUnlock()
	if t == nil {
		return nil
	}
	return t.Shutdown(ctx)
}
