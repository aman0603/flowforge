package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/outbox"
	"github.com/aman0603/flowforge/internal/telemetry"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// eventConsumer is a minimal, idempotent example consumer. It subscribes to the
// workflow-events topic using a consumer group, parses the versioned envelope,
// deduplicates by event ID, and commits offsets only after successful
// processing. Unknown event types and malformed payloads are skipped safely.
type eventConsumer struct {
	reader  *kafka.Reader
	groupID string
	seen    sync.Map
	handler func(ctx context.Context, env model.EventEnvelope) error
}

func newEventConsumer(cfg *config.Config) *eventConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaClientID,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: 0, // manual commits after processing
		StartOffset:    kafka.FirstOffset,
	})
	return &eventConsumer{
		reader:  reader,
		groupID: cfg.KafkaClientID,
		handler: defaultHandler,
	}
}

// Run consumes messages until the context is cancelled.
func (c *eventConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Transient Kafka error: back off briefly and continue.
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := c.process(ctx, msg); err != nil {
			// Processing failed: do not commit. Kafka will redeliver, and the
			// idempotent handler tolerates duplicates.
			fmt.Fprintf(os.Stderr, "[consumer] failed to process message at offset %d: %v\n", msg.Offset, err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			fmt.Fprintf(os.Stderr, "[consumer] failed to commit offset %d: %v\n", msg.Offset, err)
		}
	}
}

// process deduplicates by event ID and dispatches to the handler.
func (c *eventConsumer) process(ctx context.Context, msg kafka.Message) error {
	// Extract upstream trace context + correlation ID from Kafka headers so the
	// consume span joins the producing trace.
	headers := msg.Headers
	ctx = telemetry.ExtractContext(ctx, outbox.NewKafkaHeaderCarrier(&headers))
	for _, h := range msg.Headers {
		if h.Key == "x-request-id" && len(h.Value) > 0 {
			ctx = telemetry.WithCorrelationID(ctx, string(h.Value))
		}
	}
	ctx, span := telemetry.StartSpan(ctx, "kafka.consume "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.source.name", msg.Topic),
		),
	)
	defer span.End()

	var env model.EventEnvelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		// Malformed payload: skip to avoid poison messages, but commit so we
		// do not block the partition. Operators should alert on these.
		fmt.Fprintf(os.Stderr, "[consumer] skipping malformed payload: %v\n", err)
		return nil
	}

	if env.EventID == "" {
		fmt.Fprintln(os.Stderr, "[consumer] skipping envelope without event_id")
		return nil
	}

	if _, exists := c.seen.LoadOrStore(env.EventID, struct{}{}); exists {
		// Duplicate delivery: already processed, treat as success.
		return nil
	}

	if env.EventType == "" {
		fmt.Fprintln(os.Stderr, "[consumer] skipping envelope without event_type")
		return nil
	}

	return c.handler(ctx, env)
}

// defaultHandler is an example handler that logs the event. Real consumers
// would dispatch on EventType and apply idempotent side effects.
func defaultHandler(_ context.Context, env model.EventEnvelope) error {
	fmt.Printf("[consumer] %s event %s (run=%s seq=%d version=%d)\n",
		env.EventType, env.EventID, env.WorkflowRunID, env.Sequence, env.EventVersion)
	return nil
}

func main() {
	cfg := config.Load()

	if len(cfg.KafkaBrokers) == 0 {
		fmt.Fprintln(os.Stderr, "[consumer] KAFKA_BROKERS must be set")
		os.Exit(1)
	}
	if cfg.KafkaClientID == "" {
		fmt.Fprintln(os.Stderr, "[consumer] KAFKA_CLIENT_ID (consumer group) must be set")
		os.Exit(1)
	}

	tel, err := telemetry.Init(telemetry.Config{
		ServiceName:      cfg.OTelServiceName,
		OTelDisabled:     cfg.OTelDisabled,
		ExporterEndpoint: cfg.OTelExporterEndpoint,
		MetricsAddr:      cfg.MetricsAddr,
		LogLevel:         cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[consumer] failed to initialize telemetry: %v\n", err)
		os.Exit(1)
	}
	if _, err := telemetry.InitMetrics(); err != nil {
		fmt.Fprintf(os.Stderr, "[consumer] failed to initialize metrics: %v\n", err)
		os.Exit(1)
	}
	defer telemetry.Shutdown(context.Background())

	consumer := newEventConsumer(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		fmt.Printf("[consumer] serving metrics on %s/metrics\n", cfg.MetricsAddr)
		if err := tel.ServeMetrics(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "[consumer] metrics server error: %v\n", err)
		}
	}()

	fmt.Printf("[consumer] subscribing to topic %s as group %s\n", cfg.KafkaTopic, cfg.KafkaClientID)

	if err := consumer.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[consumer] exited with error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("[consumer] shutdown complete.")
}
