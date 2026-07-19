package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/model"
)

// OutboxRepo is the repository surface the Publisher depends on. It is an
// interface so the publisher can be exercised with a fake in unit tests while
// *repository.Repository satisfies it in production.
type OutboxRepo interface {
	ClaimPendingOutboxEvents(ctx context.Context, publisherID string, batchSize int, claimTimeout time.Duration, now time.Time) ([]model.OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, eventID, publisherID string, now time.Time) error
	RecordOutboxError(ctx context.Context, eventID, publisherID, lastError string, attempts int, baseDelay time.Duration, maxRetries int, now time.Time) error
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

	mu      sync.Mutex
	stopped bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewPublisher constructs a Publisher.
func NewPublisher(repo OutboxRepo, producer Producer, cfg *config.Config) *Publisher {
	return &Publisher{
		repo:     repo,
		producer: producer,
		cfg:      cfg,
		pubID:    fmt.Sprintf("%s-%d", cfg.KafkaClientID, time.Now().UnixNano()),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Run starts the publish loop and blocks until Stop is called or the context
// is cancelled. It performs a graceful shutdown: in-flight batches complete
// before Run returns.
func (p *Publisher) Run(ctx context.Context) {
	defer close(p.doneCh)

	ticker := time.NewTicker(p.cfg.OutboxPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.publishBatch(ctx)
		}
	}
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

	if err := p.producer.Publish(ctx, p.cfg.KafkaTopic, e.WorkflowRunID, value); err != nil {
		_ = p.repo.RecordOutboxError(ctx, e.ID, p.pubID, err.Error(), e.Attempts, p.cfg.OutboxRetryBaseDelay, p.cfg.OutboxMaxRetries, time.Now().UTC())
		return
	}

	if err := p.repo.MarkOutboxPublished(ctx, e.ID, p.pubID, time.Now().UTC()); err != nil {
		// Already reclaimed or published: treat as acceptable duplicate.
		return
	}
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
