package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	logf("Publisher started. Polling outbox every %s.", cfg.OutboxPollInterval)

	publisher.Run(ctx)

	logf("Publisher shutdown complete.")
}
