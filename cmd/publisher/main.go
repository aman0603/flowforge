package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/outbox"
	"github.com/aman0603/flowforge/internal/repository"
)

func main() {
	cfg := config.Load()

	logf := func(format string, args ...interface{}) {
		fmt.Printf("[publisher] "+format+"\n", args...)
	}

	logf("Initializing FlowForge Outbox Publisher...")

	repo, err := repository.New(cfg.DBURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[publisher] Failed to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer repo.Close()

	producer := outbox.NewKafkaProducer(cfg)
	defer producer.Close()

	publisher := outbox.NewPublisher(repo, producer, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logf("Publisher started. Polling outbox every %s, retention %s.", cfg.OutboxPollInterval, cfg.OutboxRetention)

	// Periodic observability snapshot so operators can watch throughput and
	// backlog without external tooling.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m := publisher.Metrics()
				logf("metrics published=%d failed=%d retried=%d cleaned=%d",
					m.Published, m.Failed, m.Retried, m.CleanedUp)
			}
		}
	}()

	publisher.Run(ctx)

	logf("Publisher shutdown complete.")
}
