package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aman0603/flowforge/internal/api"
	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/repository"
	"github.com/aman0603/flowforge/internal/telemetry"
	"go.uber.org/zap"
)

func main() {
	// Setup context that listens for SIGINT or SIGTERM signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load configuration
	cfg := config.Load()

	// Initialize observability (tracing/metrics/logging).
	tel, err := telemetry.Init(telemetry.Config{
		ServiceName:      cfg.OTelServiceName,
		OTelDisabled:     cfg.OTelDisabled,
		ExporterEndpoint: cfg.OTelExporterEndpoint,
		MetricsAddr:      cfg.MetricsAddr,
		LogLevel:         cfg.LogLevel,
	})
	if err != nil {
		log.Fatalf("Failed to initialize telemetry: %v", err)
	}
	if _, err := telemetry.InitMetrics(); err != nil {
		log.Fatalf("Failed to initialize metrics: %v", err)
	}
	defer telemetry.Shutdown(context.Background())

	logger := tel.Logger()
	logger.Info("initializing api service", zap.String("db", telemetry.RedactDBURL(cfg.DBURL)))

	// Initialize database repository
	repo, err := repository.New(cfg.DBURL)
	if err != nil {
		logger.Fatal("failed to initialize database", zap.Error(err))
	}
	defer func() {
		logger.Info("closing database connection")
		if err := repo.Close(); err != nil {
			logger.Error("error closing database", zap.Error(err))
		}
	}()

	// Run schema creation
	logger.Info("initializing database schema", zap.String("schema_path", cfg.SchemaPath))
	if err := repo.InitializeSchema(cfg.SchemaPath); err != nil {
		logger.Fatal("failed to initialize database schema", zap.Error(err))
	}
	logger.Info("database schema initialized")

	// Initialize API server with repo dependency
	server := api.NewServer(cfg, repo)

	// Start server with context for graceful shutdown
	if err := server.Start(ctx); err != nil {
		logger.Fatal("server stopped with error", zap.Error(err))
	}

	logger.Info("server exited successfully")
}
