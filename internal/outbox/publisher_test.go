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

// fakeProducer records published messages and can be configured to fail.
type fakeProducer struct {
	mu          sync.Mutex
	published   [][]byte
	lastHeaders []kafka.Header
	failTimes   int
	calls       int
}

func (f *fakeProducer) Publish(_ context.Context, _ string, _ string, value []byte, headers ...kafka.Header) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failTimes > 0 {
		f.failTimes--
		return fmt.Errorf("simulated kafka failure")
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	f.published = append(f.published, cp)
	f.lastHeaders = headers
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func (f *fakeProducer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

// fakeRepo is an in-memory implementation of the repository surface the
// Publisher depends on, used to test claim/publish/mark/retry behavior.
type fakeRepo struct {
	mu      sync.Mutex
	events  map[string]model.OutboxEvent
	claimed map[string]string
	marked  map[string]bool
	errors  map[string]int
}

func newFakeRepo(events ...model.OutboxEvent) *fakeRepo {
	m := &fakeRepo{
		events:  make(map[string]model.OutboxEvent),
		claimed: make(map[string]string),
		marked:  make(map[string]bool),
		errors:  make(map[string]int),
	}
	for _, e := range events {
		m.events[e.ID] = e
	}
	return m
}

func (r *fakeRepo) ClaimPendingOutboxEvents(_ context.Context, publisherID string, batchSize int, _ time.Duration, _ time.Time) ([]model.OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []model.OutboxEvent
	for _, e := range r.events {
		if r.marked[e.ID] || r.claimed[e.ID] != "" {
			continue
		}
		r.claimed[e.ID] = publisherID
		out = append(out, e)
		if len(out) >= batchSize {
			break
		}
	}
	return out, nil
}

func (r *fakeRepo) MarkOutboxPublished(_ context.Context, eventID, publisherID string, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claimed[eventID] != publisherID {
		return fmt.Errorf("not claimed by %s", publisherID)
	}
	r.marked[eventID] = true
	r.claimed[eventID] = ""
	return nil
}

func (r *fakeRepo) RecordOutboxError(_ context.Context, eventID, _ string, _ string, _ int, _ time.Duration, _ int, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors[eventID]++
	r.claimed[eventID] = ""
	return nil
}

func (r *fakeRepo) CleanupPublishedOutboxEvents(_ context.Context, _ time.Time) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var removed int64
	for id, m := range r.marked {
		if m {
			delete(r.marked, id)
			removed++
		}
	}
	return removed, nil
}

func TestPublisherPublishesPendingEvents(t *testing.T) {
	repo := newFakeRepo(
		model.OutboxEvent{ID: "e1", WorkflowRunID: "w1", EventType: model.EventWorkflowStarted, Attempts: 0},
		model.OutboxEvent{ID: "e2", WorkflowRunID: "w1", EventType: model.EventTaskCompleted, Attempts: 0},
	)
	producer := &fakeProducer{}
	cfg := &config.Config{KafkaTopic: "t", KafkaClientID: "pub", OutboxPollInterval: time.Hour, OutboxBatchSize: 10, OutboxClaimTimeout: 30 * time.Second, OutboxMaxRetries: 5, OutboxRetryBaseDelay: time.Second}
	pub := NewPublisher(repo, producer, cfg)

	pub.publishBatch(context.Background())

	if producer.count() != 2 {
		t.Fatalf("expected 2 published events, got %d", producer.count())
	}
	if !repo.marked["e1"] || !repo.marked["e2"] {
		t.Fatalf("expected both events marked published")
	}
}

func TestPublisherRetriesOnKafkaFailure(t *testing.T) {
	repo := newFakeRepo(model.OutboxEvent{ID: "e1", WorkflowRunID: "w1", EventType: model.EventWorkflowStarted, Attempts: 0})
	producer := &fakeProducer{failTimes: 1}
	cfg := &config.Config{KafkaTopic: "t", KafkaClientID: "pub", OutboxPollInterval: time.Hour, OutboxBatchSize: 10, OutboxClaimTimeout: 30 * time.Second, OutboxMaxRetries: 5, OutboxRetryBaseDelay: time.Second}
	pub := NewPublisher(repo, producer, cfg)

	pub.publishBatch(context.Background())
	if producer.count() != 0 {
		t.Fatalf("expected no successful publish on first attempt, got %d", producer.count())
	}
	if repo.errors["e1"] != 1 {
		t.Fatalf("expected 1 recorded error, got %d", repo.errors["e1"])
	}
	if repo.marked["e1"] {
		t.Fatalf("event must not be marked published after kafka failure")
	}
}

func TestEnvelopeFromEvent(t *testing.T) {
	taskID := "task-1"
	e := model.OutboxEvent{
		ID:            "evt-1",
		EventType:     model.EventTaskFailed,
		EventVersion:  1,
		AggregateType: "task_run",
		AggregateID:   "agg-1",
		WorkflowRunID: "w-1",
		TaskRunID:     &taskID,
		Sequence:      3,
		Payload:       []byte(`{"x":1}`),
		CreatedAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	env := envelopeFromEvent(e)
	if env.EventID != e.ID || env.Sequence != 3 || env.WorkflowRunID != "w-1" {
		t.Fatalf("envelope mapping incorrect: %+v", env)
	}
	if env.TaskRunID == nil || *env.TaskRunID != taskID {
		t.Fatalf("expected task run id preserved")
	}
}

func TestPublisherRecordsMetrics(t *testing.T) {
	repo := newFakeRepo(
		model.OutboxEvent{ID: "e1", WorkflowRunID: "w1", EventType: model.EventWorkflowStarted, Attempts: 0},
	)
	producer := &fakeProducer{}
	metrics := &Metrics{}
	cfg := &config.Config{KafkaTopic: "t", KafkaClientID: "pub", OutboxPollInterval: time.Hour, OutboxBatchSize: 10, OutboxClaimTimeout: 30 * time.Second, OutboxMaxRetries: 5, OutboxRetryBaseDelay: time.Second, OutboxRetention: 24 * time.Hour}
	pub := NewPublisherWithMetrics(repo, producer, cfg, metrics)

	pub.publishBatch(context.Background())

	if got := metrics.Snapshot().Published; got != 1 {
		t.Fatalf("expected 1 published metric, got %d", got)
	}
	if got := metrics.Snapshot().Failed; got != 0 {
		t.Fatalf("expected 0 failed metric, got %d", got)
	}
}

func TestPublisherRecordsFailureMetric(t *testing.T) {
	repo := newFakeRepo(model.OutboxEvent{ID: "e1", WorkflowRunID: "w1", EventType: model.EventWorkflowStarted, Attempts: 0})
	producer := &fakeProducer{failTimes: 1}
	metrics := &Metrics{}
	cfg := &config.Config{KafkaTopic: "t", KafkaClientID: "pub", OutboxPollInterval: time.Hour, OutboxBatchSize: 10, OutboxClaimTimeout: 30 * time.Second, OutboxMaxRetries: 5, OutboxRetryBaseDelay: time.Second, OutboxRetention: 24 * time.Hour}
	pub := NewPublisherWithMetrics(repo, producer, cfg, metrics)

	pub.publishBatch(context.Background())

	if got := metrics.Snapshot().Failed; got != 1 {
		t.Fatalf("expected 1 failed metric, got %d", got)
	}
	if got := metrics.Snapshot().Published; got != 0 {
		t.Fatalf("expected 0 published metric, got %d", got)
	}
}
