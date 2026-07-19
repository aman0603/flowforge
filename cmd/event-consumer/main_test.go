package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/model"
	"github.com/segmentio/kafka-go"
)

// stubConfig returns a minimal config so newEventConsumer can be constructed
// without a live Kafka connection in unit tests.
func stubConfig() *config.Config {
	return &config.Config{
		KafkaBrokers:  []string{"localhost:9092"},
		KafkaTopic:    "flowforge.workflow-events.v1",
		KafkaClientID: "test-consumer-group",
	}
}

func TestProcessDeduplicatesByEventID(t *testing.T) {
	var mu sync.Mutex
	var handled []string
	c := newEventConsumer(stubConfig())
	c.handler = func(_ context.Context, env model.EventEnvelope) error {
		mu.Lock()
		handled = append(handled, env.EventID)
		mu.Unlock()
		return nil
	}

	env := model.EventEnvelope{
		EventID:   "evt-1",
		EventType: model.EventTaskCompleted,
		Sequence:  1,
	}

	msg := kafka.Message{Value: mustJSON(t, env)}
	if err := c.process(context.Background(), msg); err != nil {
		t.Fatalf("first process failed: %v", err)
	}
	if err := c.process(context.Background(), msg); err != nil {
		t.Fatalf("duplicate process failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(handled) != 1 {
		t.Fatalf("expected handler called once due to dedup, got %d", len(handled))
	}
}

func TestProcessSkipsMalformedPayload(t *testing.T) {
	c := newEventConsumer(stubConfig())
	called := false
	c.handler = func(_ context.Context, _ model.EventEnvelope) error {
		called = true
		return nil
	}

	msg := kafka.Message{Value: []byte("not-json")}
	if err := c.process(context.Background(), msg); err != nil {
		t.Fatalf("malformed payload should be skipped: %v", err)
	}
	if called {
		t.Fatalf("handler must not be called for malformed payload")
	}
}

func TestProcessSkipsUnknownEventTypeAndMissingID(t *testing.T) {
	c := newEventConsumer(stubConfig())
	called := false
	c.handler = func(_ context.Context, _ model.EventEnvelope) error {
		called = true
		return nil
	}

	// Missing EventID: must skip without calling handler.
	noID := kafka.Message{Value: mustJSON(t, model.EventEnvelope{EventType: model.EventTaskCompleted})}
	if err := c.process(context.Background(), noID); err != nil {
		t.Fatalf("missing id should be skipped: %v", err)
	}

	// Missing EventType: must skip without calling handler.
	noType := kafka.Message{Value: mustJSON(t, model.EventEnvelope{EventID: "x"})}
	if err := c.process(context.Background(), noType); err != nil {
		t.Fatalf("missing type should be skipped: %v", err)
	}

	if called {
		t.Fatalf("handler must not be called for incomplete envelope")
	}
}

func TestProcessPropagatesHandlerError(t *testing.T) {
	c := newEventConsumer(stubConfig())
	c.handler = func(_ context.Context, _ model.EventEnvelope) error {
		return errors.New("boom")
	}

	env := model.EventEnvelope{EventID: "evt-err", EventType: model.EventTaskStarted}
	msg := kafka.Message{Value: mustJSON(t, env)}
	if err := c.process(context.Background(), msg); err == nil {
		t.Fatalf("expected handler error to propagate")
	}
}

func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return b
}
