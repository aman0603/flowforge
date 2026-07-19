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
)

func main() {
	// Setup context that listens for SIGINT or SIGTERM signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load configuration
	cfg := config.Load()

	// Initialize observability (tracing/metrics/logging).
	if _, err := telemetry.Init(telemetry.Config{
		ServiceName:      cfg.OTelServiceName,
		OTelDisabled:     cfg.OTelDisabled,
		ExporterEndpoint: cfg.OTelExporterEndpoint,
		MetricsAddr:      cfg.MetricsAddr,
		LogLevel:         cfg.LogLevel,
	}); err != nil {
		log.Fatalf("Failed to initialize telemetry: %v", err)
	}
	if _, err := telemetry.InitMetrics(); err != nil {
		log.Fatalf("Failed to initialize metrics: %v", err)
	}
	defer telemetry.Shutdown(context.Background())

	// Initialize database repository
	repo, err := repository.New(cfg.DBURL)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer func() {
		log.Println("Closing database connection...")
		if err := repo.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	// Run schema creation
	log.Printf("Initializing database schema from %s...", cfg.SchemaPath)
	if err := repo.InitializeSchema(cfg.SchemaPath); err != nil {
		log.Fatalf("Failed to initialize database schema: %v", err)
	}
	log.Println("Database schema initialized successfully.")

	// Initialize API server with repo dependency
	server := api.NewServer(cfg, repo)

	// Start server with context for graceful shutdown
	if err := server.Start(ctx); err != nil {
		log.Fatalf("Server stopped with error: %v", err)
	}

	log.Println("Server exited successfully")
}
