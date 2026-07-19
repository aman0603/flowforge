package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// correlationHeaderKey is the Kafka header carrying the correlation/request ID.
const correlationHeaderKey = "x-request-id"

// OutboxRepo is the repository surface the Publisher depends on. It is an
// interface so the publisher can be exercised with a fake in unit tests while
// *repository.Repository satisfies it in production.
type OutboxRepo interface {
	ClaimPendingOutboxEvents(ctx context.Context, publisherID string, batchSize int, claimTimeout time.Duration, now time.Time) ([]model.OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, eventID, publisherID string, now time.Time) error
	RecordOutboxError(ctx context.Context, eventID, publisherID, lastError string, attempts int, baseDelay time.Duration, maxRetries int, now time.Time) error
	CleanupPublishedOutboxEvents(ctx context.Context, olderThan time.Time) (int64, error)
}

// Publisher polls the transactional outbox and publishes pending events to
// Kafka. It claims events with a renewable lease, publishes them outside the
// database transaction, then marks them published using the claim token.
//
// Crash safety: a crash after Kafka acknowledgement but before marking the
// event published intentionally produces a duplicate, which Kafka consumers
// must tolerate. A crash before acknowledgement leaves the event claimed; the
// claim expires and another poll reclaims it.
type Publisher struct {
	repo     OutboxRepo
	producer Producer
	cfg      *config.Config
	pubID    string
	metrics  *Metrics

	mu      sync.Mutex
	stopped bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewPublisher constructs a Publisher. The metrics pointer may be nil if
// observability is not required.
func NewPublisher(repo OutboxRepo, producer Producer, cfg *config.Config) *Publisher {
	return NewPublisherWithMetrics(repo, producer, cfg, &Metrics{})
}

// NewPublisherWithMetrics constructs a Publisher with a shared metrics sink.
func NewPublisherWithMetrics(repo OutboxRepo, producer Producer, cfg *config.Config, metrics *Metrics) *Publisher {
	return &Publisher{
		repo:     repo,
		producer: producer,
		cfg:      cfg,
		pubID:    fmt.Sprintf("%s-%d", cfg.KafkaClientID, time.Now().UnixNano()),
		metrics:  metrics,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Run starts the publish loop and blocks until Stop is called or the context
// is cancelled. It performs a graceful shutdown: in-flight batches complete
// before Run returns.
func (p *Publisher) Run(ctx context.Context) {
	defer close(p.doneCh)

	pollTicker := time.NewTicker(p.cfg.OutboxPollInterval)
	defer pollTicker.Stop()

	cleanupTicker := time.NewTicker(p.cleanupInterval())
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-pollTicker.C:
			p.publishBatch(ctx)
		case <-cleanupTicker.C:
			p.runCleanup(ctx)
		}
	}
}

// cleanupInterval bounds how often published events are pruned. It is derived
// from the retention window so cleanup runs at most a few times per retention
// period.
func (p *Publisher) cleanupInterval() time.Duration {
	d := p.cfg.OutboxRetention / 12
	if d < time.Minute {
		d = time.Minute
	}
	return d
}

// runCleanup removes published events older than the retention window.
func (p *Publisher) runCleanup(ctx context.Context) {
	olderThan := time.Now().UTC().Add(-p.cfg.OutboxRetention)
	removed, err := p.repo.CleanupPublishedOutboxEvents(ctx, olderThan)
	if err != nil {
		return
	}
	if removed > 0 {
		p.metrics.recordCleanedUp(removed)
	}
}

// Metrics returns a snapshot of the publisher's observability counters.
func (p *Publisher) Metrics() Metrics {
	return p.metrics.Snapshot()
}

// Stop signals the loop to stop and waits for in-flight work to finish.
func (p *Publisher) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		<-p.doneCh
		return
	}
	p.stopped = true
	p.mu.Unlock()

	close(p.stopCh)
	<-p.doneCh
}

// publishBatch claims a batch of pending events and publishes each one
// independently so a single failure does not block sibling events.
func (p *Publisher) publishBatch(ctx context.Context) {
	now := time.Now().UTC()
	events, err := p.repo.ClaimPendingOutboxEvents(ctx, p.pubID, p.cfg.OutboxBatchSize, p.cfg.OutboxClaimTimeout, now)
	if err != nil {
		// Database transient error: back off by skipping this poll.
		return
	}
	if len(events) == 0 {
		return
	}

	for _, e := range events {
		select {
		case <-ctx.Done():
			return
		default:
		}
		p.publishOne(ctx, e)
	}
}

// publishOne serializes, publishes, and marks a single event. On any failure it
// records the error and schedules a retry; duplicate publication after a
// successful Kafka write but failed mark is acceptable.
func (p *Publisher) publishOne(ctx context.Context, e model.OutboxEvent) {
	value, err := json.Marshal(envelopeFromEvent(e))
	if err != nil {
		_ = p.repo.RecordOutboxError(ctx, e.ID, p.pubID, fmt.Sprintf("marshal: %v", err), e.Attempts, p.cfg.OutboxRetryBaseDelay, p.cfg.OutboxMaxRetries, time.Now().UTC())
		return
	}

	pubCtx, span := telemetry.StartSpan(ctx, "kafka.publish "+p.cfg.KafkaTopic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", p.cfg.KafkaTopic),
			attribute.String("event.id", e.ID),
			attribute.String("event.type", e.EventType),
		),
	)

	var headers []kafka.Header
	telemetry.InjectContext(pubCtx, NewKafkaHeaderCarrier(&headers))
	if cid := telemetry.CorrelationID(pubCtx); cid != "" {
		headers = append(headers, kafka.Header{Key: correlationHeaderKey, Value: []byte(cid)})
	}

	if err := p.producer.Publish(pubCtx, p.cfg.KafkaTopic, e.WorkflowRunID, value, headers...); err != nil {
		telemetry.EndSpan(span, err)
		_ = p.repo.RecordOutboxError(ctx, e.ID, p.pubID, err.Error(), e.Attempts, p.cfg.OutboxRetryBaseDelay, p.cfg.OutboxMaxRetries, time.Now().UTC())
		p.metrics.recordFailure()
		return
	}
	span.End()

	if err := p.repo.MarkOutboxPublished(ctx, e.ID, p.pubID, time.Now().UTC()); err != nil {
		// Already reclaimed or published: treat as acceptable duplicate.
		return
	}
	p.metrics.recordPublished(1)
}

// envelopeFromEvent builds the external Kafka envelope from a stored outbox row.
func envelopeFromEvent(e model.OutboxEvent) model.EventEnvelope {
	return model.EventEnvelope{
		EventID:       e.ID,
		EventType:     e.EventType,
		EventVersion:  e.EventVersion,
		OccurredAt:    e.CreatedAt,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		WorkflowRunID: e.WorkflowRunID,
		TaskRunID:     e.TaskRunID,
		Sequence:      e.Sequence,
		Payload:       e.Payload,
	}
}
