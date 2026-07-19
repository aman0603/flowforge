package outbox

import (
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
)

// KafkaHeaderCarrier adapts a slice of kafka.Header to the OpenTelemetry
// TextMapCarrier interface so trace context and correlation IDs can be injected
// into and extracted from Kafka messages. The pointer receiver lets Set append
// new headers to the backing slice.
type KafkaHeaderCarrier struct {
	Headers *[]kafka.Header
}

// NewKafkaHeaderCarrier wraps an existing header slice pointer.
func NewKafkaHeaderCarrier(h *[]kafka.Header) KafkaHeaderCarrier {
	return KafkaHeaderCarrier{Headers: h}
}

// Get returns the value of the header with the given key, or "".
func (c KafkaHeaderCarrier) Get(key string) string {
	if c.Headers == nil {
		return ""
	}
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces or appends the header with the given key.
func (c KafkaHeaderCarrier) Set(key, value string) {
	if c.Headers == nil {
		return
	}
	for i, h := range *c.Headers {
		if h.Key == key {
			(*c.Headers)[i].Value = []byte(value)
			return
		}
	}
	*c.Headers = append(*c.Headers, kafka.Header{Key: key, Value: []byte(value)})
}

// Keys returns all header keys.
func (c KafkaHeaderCarrier) Keys() []string {
	if c.Headers == nil {
		return nil
	}
	keys := make([]string, 0, len(*c.Headers))
	for _, h := range *c.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

var _ propagation.TextMapCarrier = KafkaHeaderCarrier{}
