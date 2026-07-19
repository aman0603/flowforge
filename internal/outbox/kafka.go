package outbox

import (
	"context"
	"fmt"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/segmentio/kafka-go"
)

// Producer publishes a serialized event payload to a Kafka topic under a key.
// Implementations must be safe for concurrent use and must not publish inside a
// PostgreSQL transaction.
type Producer interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
	Close() error
}

// KafkaProducer is the kafka-go backed implementation of Producer.
type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer creates a Producer that writes to the configured Kafka
// brokers using the transactional-outbox topic and client ID.
func NewKafkaProducer(cfg *config.Config) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		Logger:       nil,
		ErrorLogger:  nil,
	}
	return &KafkaProducer{writer: writer}
}

// Publish writes a message to Kafka with the workflow_run_id as the key to
// preserve per-workflow ordering.
func (p *KafkaProducer) Publish(ctx context.Context, topic, key string, value []byte) error {
	if topic == "" {
		topic = p.writer.Topic
	}
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write kafka message: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying Kafka writer.
func (p *KafkaProducer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("failed to close kafka writer: %w", err)
	}
	return nil
}
