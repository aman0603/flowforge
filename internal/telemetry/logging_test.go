package telemetry

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestRedactDBURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"with password", "postgres://user:secret@localhost:5432/flowforge?sslmode=disable", "postgres://user:xxxxx@localhost:5432/flowforge?sslmode=disable"},
		{"no password", "postgres://user@localhost:5432/flowforge", "postgres://user@localhost:5432/flowforge"},
		{"no user", "postgres://localhost:5432/flowforge", "postgres://localhost:5432/flowforge"},
		{"kv dsn", "host=localhost user=u password=p dbname=d", "[redacted]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := RedactDBURL(c.in); got != c.want {
				t.Fatalf("RedactDBURL(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestLoggerWithContextAddsFields(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)

	mu.Lock()
	prev := globalTL
	globalTL = &Telemetry{logger: zap.New(core)}
	mu.Unlock()
	defer func() {
		mu.Lock()
		globalTL = prev
		mu.Unlock()
	}()

	ctx := WithCorrelationID(context.Background(), "req-42")
	Info(ctx, "hello")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["request_id"] != "req-42" {
		t.Fatalf("expected request_id=req-42 in log, got %v", fields)
	}
	if entries[0].Message != "hello" {
		t.Fatalf("unexpected message %q", entries[0].Message)
	}
}
