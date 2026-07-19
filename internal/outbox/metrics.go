package outbox

import (
	"context"
	"sync/atomic"
	"time"
)

// Metrics holds bounded counters for publisher observability. They are safe for
// concurrent use. A real deployment would export these to Prometheus; here they
// are exposed via Snapshot for logging and future scraping.
type Metrics struct {
	Published    int64
	Failed       int64
	Retried      int64
	CleanedUp    int64
	PublishedAge atomic.Int64 // age in milliseconds of the oldest published event batch
}

// Snapshot returns a point-in-time copy of the counters.
func (m *Metrics) Snapshot() Metrics {
	return Metrics{
		Published: atomic.LoadInt64(&m.Published),
		Failed:    atomic.LoadInt64(&m.Failed),
		Retried:   atomic.LoadInt64(&m.Retried),
		CleanedUp: atomic.LoadInt64(&m.CleanedUp),
	}
}

// recordPublished increments the published counter.
func (m *Metrics) recordPublished(n int) {
	if m == nil {
		return
	}
	atomic.AddInt64(&m.Published, int64(n))
}

// recordFailure increments the failed (publish error) counter.
func (m *Metrics) recordFailure() {
	if m == nil {
		return
	}
	atomic.AddInt64(&m.Failed, 1)
}

// recordRetry increments the retried counter (event rescheduled after failure).
func (m *Metrics) recordRetry() {
	if m == nil {
		return
	}
	atomic.AddInt64(&m.Retried, 1)
}

// recordCleanedUp increments the cleaned-up counter.
func (m *Metrics) recordCleanedUp(n int64) {
	if m == nil {
		return
	}
	atomic.AddInt64(&m.CleanedUp, n)
}

// CleanupRepo is the optional repository surface for retention cleanup.
type CleanupRepo interface {
	CleanupPublishedOutboxEvents(ctx context.Context, olderThan time.Time) (int64, error)
}
