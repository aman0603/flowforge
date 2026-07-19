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
	"go.uber.org/zap"
)

func main() {
	cfg := config.Load()

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

	logger := tel.Logger()
	logger.Info("initializing outbox publisher", zap.String("db", telemetry.RedactDBURL(cfg.DBURL)))

	repo, err := repository.NewWithPool(cfg.DBURL, repository.PoolConfig{
		MaxOpenConns:    cfg.DBMaxOpenConns,
		MaxIdleConns:    cfg.DBMaxIdleConns,
		ConnMaxLifetime: cfg.DBConnMaxLifetime,
		ConnMaxIdleTime: cfg.DBConnMaxIdleTime,
	})
	if err != nil {
		logger.Fatal("failed to connect to database", zap.Error(err))
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
		logger.Error("failed to register outbox metrics", zap.Error(err))
	}
	go func() {
		logger.Info("serving metrics", zap.String("addr", cfg.MetricsAddr))
		if err := tel.ServeMetrics(ctx); err != nil {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	logger.Info("publisher started",
		zap.Duration("poll_interval", cfg.OutboxPollInterval),
		zap.Duration("retention", cfg.OutboxRetention))

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
				logger.Info("outbox metrics",
					zap.Int64("published", m.Published),
					zap.Int64("failed", m.Failed),
					zap.Int64("retried", m.Retried),
					zap.Int64("cleaned", m.CleanedUp))
			}
		}
	}()

	publisher.Run(ctx)

	logger.Info("publisher shutdown complete")
}
