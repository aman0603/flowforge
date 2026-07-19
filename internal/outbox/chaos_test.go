//go:build chaos

// Package outbox chaos tests validate publisher behavior under simulated
// failures. They are gated behind the `chaos` build tag so the default
// `go test ./...` run stays fast and infra-free:
//
//	go test -tags chaos ./internal/outbox/...
package outbox

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/model"
	"github.com/segmentio/kafka-go"
)

// crashProducer publishes successfully to Kafka but the test simulates a crash
// between the Kafka ack and the outbox MarkPublished, forcing a re-claim on the
// next poll — the duplicate-delivery scenario the design explicitly tolerates.
type crashProducer struct {
	mu        sync.Mutex
	published [][]byte
}

func (p *crashProducer) Publish(_ context.Context, _, _ string, value []byte, _ ...kafka.Header) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	p.published = append(p.published, cp)
	return nil
}
func (p *crashProducer) Close() error { return nil }
func (p *crashProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

// markFailOnceRepo fails the first MarkOutboxPublished (simulating a crash after
// Kafka ack) then behaves normally, so the event is re-claimed and re-published.
type markFailOnceRepo struct {
	*fakeRepo
	failMarkOnce map[string]bool
}

func newMarkFailOnceRepo(events ...model.OutboxEvent) *markFailOnceRepo {
	return &markFailOnceRepo{
		fakeRepo:     newFakeRepo(events...),
		failMarkOnce: make(map[string]bool),
	}
}

func (r *markFailOnceRepo) MarkOutboxPublished(ctx context.Context, eventID, publisherID string, now time.Time) error {
	r.mu.Lock()
	if !r.failMarkOnce[eventID] {
		r.failMarkOnce[eventID] = true
		// Release the claim so the event can be reclaimed on the next poll.
		r.claimed[eventID] = ""
		r.mu.Unlock()
		return fmt.Errorf("simulated crash after kafka ack, before mark")
	}
	r.mu.Unlock()
	return r.fakeRepo.MarkOutboxPublished(ctx, eventID, publisherID, now)
}

func chaosConfig() *config.Config {
	return &config.Config{
		KafkaClientID:        "chaos",
		KafkaTopic:           "flowforge.workflow-events.v1",
		OutboxBatchSize:      10,
		OutboxPollInterval:   time.Millisecond,
		OutboxClaimTimeout:   time.Second,
		OutboxMaxRetries:     5,
		OutboxRetryBaseDelay: time.Millisecond,
		OutboxRetention:      time.Hour,
	}
}

func sampleEvent(id string) model.OutboxEvent {
	return model.OutboxEvent{
		ID:            id,
		EventType:     "workflow.created",
		EventVersion:  1,
		AggregateType: "workflow_run",
		AggregateID:   "run-" + id,
		WorkflowRunID: "run-" + id,
		CreatedAt:     time.Now().UTC(),
	}
}

// TestChaosDuplicateDeliveryOnCrashAfterAck verifies that a crash between the
// Kafka acknowledgement and the outbox mark results in the event being
// re-published (at-least-once) and eventually marked — never lost.
func TestChaosDuplicateDeliveryOnCrashAfterAck(t *testing.T) {
	repo := newMarkFailOnceRepo(sampleEvent("e1"))
	producer := &crashProducer{}
	cfg := chaosConfig()

	p := NewPublisher(repo, producer, cfg)

	// First batch: publish succeeds, mark fails -> claim released.
	p.publishBatch(context.Background())
	if producer.count() != 1 {
		t.Fatalf("expected 1 publish after first batch, got %d", producer.count())
	}
	repo.mu.Lock()
	marked := repo.marked["e1"]
	repo.mu.Unlock()
	if marked {
		t.Fatal("event should not be marked after simulated crash")
	}

	// Second batch: event reclaimed, republished (duplicate), mark succeeds.
	p.publishBatch(context.Background())
	if producer.count() != 2 {
		t.Fatalf("expected duplicate publish (2) after reclaim, got %d", producer.count())
	}
	repo.mu.Lock()
	marked = repo.marked["e1"]
	repo.mu.Unlock()
	if !marked {
		t.Fatal("event should be marked published after reclaim")
	}
}

// TestChaosRetryExhaustion verifies that persistent Kafka failures cause the
// publisher to record errors (which drive retry scheduling / DLQ) rather than
// silently dropping events.
func TestChaosRetryExhaustion(t *testing.T) {
	repo := newFakeRepo(sampleEvent("e1"))
	producer := &fakeProducer{failTimes: 1000} // always fails
	cfg := chaosConfig()

	p := NewPublisher(repo, producer, cfg)

	for i := 0; i < 3; i++ {
		p.publishBatch(context.Background())
	}

	repo.mu.Lock()
	errs := repo.errors["e1"]
	marked := repo.marked["e1"]
	repo.mu.Unlock()

	if errs == 0 {
		t.Fatal("expected recorded errors on persistent kafka failure")
	}
	if marked {
		t.Fatal("event must not be marked published when kafka always fails")
	}
}

// TestChaosTransientKafkaThenRecovery verifies recovery: after N transient
// failures the event is successfully published exactly once (marked).
func TestChaosTransientKafkaThenRecovery(t *testing.T) {
	repo := newFakeRepo(sampleEvent("e1"))
	producer := &fakeProducer{failTimes: 2}
	cfg := chaosConfig()

	p := NewPublisher(repo, producer, cfg)

	// Each failing batch records an error and releases the claim.
	for i := 0; i < 3; i++ {
		p.publishBatch(context.Background())
	}

	if producer.count() != 1 {
		t.Fatalf("expected exactly 1 successful publish after recovery, got %d", producer.count())
	}
	repo.mu.Lock()
	marked := repo.marked["e1"]
	repo.mu.Unlock()
	if !marked {
		t.Fatal("event should be marked after successful recovery")
	}
}
