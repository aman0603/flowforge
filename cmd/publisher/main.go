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
	"github.com/aman0603/flowforge/internal/telemetry"
)

func main() {
	cfg := config.Load()

	logf := func(format string, args ...interface{}) {
		fmt.Printf("[publisher] "+format+"\n", args...)
	}

	logf("Initializing FlowForge Outbox Publisher...")

	tel, err := telemetry.Init(telemetry.Config{
		ServiceName:      cfg.OTelServiceName,
		OTelDisabled:     cfg.OTelDisabled,
		ExporterEndpoint: cfg.OTelExporterEndpoint,
		MetricsAddr:      cfg.MetricsAddr,
		LogLevel:         cfg.LogLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[publisher] Failed to initialize telemetry: %v\n", err)
		os.Exit(1)
	}
	if _, err := telemetry.InitMetrics(); err != nil {
		fmt.Fprintf(os.Stderr, "[publisher] Failed to initialize metrics: %v\n", err)
		os.Exit(1)
	}
	defer telemetry.Shutdown(context.Background())

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

	if err := telemetry.RegisterOutboxMetrics(func() (published, failed, retried, cleaned int64) {
		m := publisher.Metrics()
		return m.Published, m.Failed, m.Retried, m.CleanedUp
	}); err != nil {
		logf("Failed to register outbox metrics: %v", err)
	}
	go func() {
		logf("Serving metrics on %s/metrics", cfg.MetricsAddr)
		if err := tel.ServeMetrics(ctx); err != nil {
			logf("metrics server error: %v", err)
		}
	}()

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
