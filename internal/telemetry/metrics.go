package telemetry

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds the shared OpenTelemetry instruments used across FlowForge
// services. Instruments are created lazily on first Init of the metrics set and
// are safe for concurrent use.
type Metrics struct {
	// HTTP server
	HTTPRequests metric.Int64Counter
	HTTPDuration metric.Float64Histogram

	// gRPC server
	GRPCRequests metric.Int64Counter
	GRPCDuration metric.Float64Histogram

	// Recovery (incremented directly by recovery paths)
	TasksRecovered metric.Int64Counter
}

var (
	metricsOnce sync.Once
	metricsInst *Metrics
	metricsErr  error
)

// Meter name used for all FlowForge instruments.
const meterName = "github.com/aman0603/flowforge"

// InitMetrics builds (once) and returns the shared instrument set. It uses the
// global MeterProvider, so telemetry.Init must be called first. Repeated calls
// return the same instance.
func InitMetrics() (*Metrics, error) {
	metricsOnce.Do(func() {
		m := Meter(meterName)
		inst := &Metrics{}
		var err error

		if inst.HTTPRequests, err = m.Int64Counter("flowforge_http_requests_total",
			metric.WithDescription("Total HTTP requests handled")); err != nil {
			metricsErr = err
			return
		}
		if inst.HTTPDuration, err = m.Float64Histogram("flowforge_http_request_duration_seconds",
			metric.WithDescription("HTTP request duration in seconds")); err != nil {
			metricsErr = err
			return
		}
		if inst.GRPCRequests, err = m.Int64Counter("flowforge_grpc_requests_total",
			metric.WithDescription("Total gRPC requests handled")); err != nil {
			metricsErr = err
			return
		}
		if inst.GRPCDuration, err = m.Float64Histogram("flowforge_grpc_request_duration_seconds",
			metric.WithDescription("gRPC request duration in seconds")); err != nil {
			metricsErr = err
			return
		}
		if inst.TasksRecovered, err = m.Int64Counter("flowforge_tasks_recovered_total",
			metric.WithDescription("Total stale tasks recovered")); err != nil {
			metricsErr = err
			return
		}
		metricsInst = inst
	})
	return metricsInst, metricsErr
}

// GetMetrics returns the shared instrument set if initialized, else nil.
func GetMetrics() *Metrics {
	return metricsInst
}

// WorkerCounters is the snapshot the worker exposes for metric bridging.
type WorkerCounters struct {
	ActiveExecutions int64
	QueuedTasks      int64
	TotalClaimed     int64
	TotalStarted     int64
	TotalCompleted   int64
	TotalFailed      int64
	TotalTimedOut    int64
	TotalPanics      int64
	TotalLeaseLosses int64
}

// RegisterWorkerMetrics wires observable instruments that read the worker's
// counters via read on each collection. It exposes a queue-depth gauge plus
// observable counters for claimed/started/completed/failed/timed-out tasks.
// This bridges the worker's existing atomic counters without restructuring the
// worker execution paths. Call once per process after InitMetrics.
func RegisterWorkerMetrics(read func() WorkerCounters) error {
	m := Meter(meterName)

	depth, err := m.Int64ObservableGauge("flowforge_worker_queue_depth",
		metric.WithDescription("Current worker queue depth and active executions"))
	if err != nil {
		return err
	}
	claimed, err := m.Int64ObservableCounter("flowforge_tasks_claimed_total",
		metric.WithDescription("Total tasks claimed"))
	if err != nil {
		return err
	}
	started, err := m.Int64ObservableCounter("flowforge_tasks_started_total",
		metric.WithDescription("Total tasks started"))
	if err != nil {
		return err
	}
	completed, err := m.Int64ObservableCounter("flowforge_tasks_completed_total",
		metric.WithDescription("Total tasks completed"))
	if err != nil {
		return err
	}
	failed, err := m.Int64ObservableCounter("flowforge_tasks_failed_total",
		metric.WithDescription("Total tasks failed"))
	if err != nil {
		return err
	}
	timedOut, err := m.Int64ObservableCounter("flowforge_tasks_timed_out_total",
		metric.WithDescription("Total tasks timed out"))
	if err != nil {
		return err
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		c := read()
		o.ObserveInt64(depth, c.QueuedTasks, metric.WithAttributes(attribute.String("state", "queued")))
		o.ObserveInt64(depth, c.ActiveExecutions, metric.WithAttributes(attribute.String("state", "active")))
		o.ObserveInt64(claimed, c.TotalClaimed)
		o.ObserveInt64(started, c.TotalStarted)
		o.ObserveInt64(completed, c.TotalCompleted)
		o.ObserveInt64(failed, c.TotalFailed)
		o.ObserveInt64(timedOut, c.TotalTimedOut)
		return nil
	}, depth, claimed, started, completed, failed, timedOut)
	return err
}

// RegisterOutboxMetrics wires observable counters that read the publisher's
// existing snapshot counters via read on each collection.
func RegisterOutboxMetrics(read func() (published, failed, retried, cleaned int64)) error {
	m := Meter(meterName)
	published, err := m.Int64ObservableCounter("flowforge_outbox_published_total",
		metric.WithDescription("Total outbox events published to Kafka"))
	if err != nil {
		return err
	}
	failed, err := m.Int64ObservableCounter("flowforge_outbox_failed_total",
		metric.WithDescription("Total outbox publish failures"))
	if err != nil {
		return err
	}
	retried, err := m.Int64ObservableCounter("flowforge_outbox_retried_total",
		metric.WithDescription("Total outbox events rescheduled for retry"))
	if err != nil {
		return err
	}
	cleaned, err := m.Int64ObservableCounter("flowforge_outbox_cleaned_total",
		metric.WithDescription("Total published outbox events pruned"))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		p, f, r, c := read()
		o.ObserveInt64(published, p)
		o.ObserveInt64(failed, f)
		o.ObserveInt64(retried, r)
		o.ObserveInt64(cleaned, c)
		return nil
	}, published, failed, retried, cleaned)
	return err
}
