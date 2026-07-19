package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/grpcutil"
	"github.com/aman0603/flowforge/internal/proto/health"
	pbsched "github.com/aman0603/flowforge/internal/proto/scheduler"
	"github.com/aman0603/flowforge/internal/repository"
	"github.com/aman0603/flowforge/internal/scheduler"
	"github.com/aman0603/flowforge/internal/telemetry"
)

// Scheduler service: hosts the SchedulerService gRPC server (claiming + retry
// promotion) over the repository. It does not execute tasks.
func main() {
	cfg := config.Load()

	log.Println("[scheduler] Initializing FlowForge Scheduler service...")

	tel, err := telemetry.Init(telemetry.Config{
		ServiceName:      cfg.OTelServiceName,
		OTelDisabled:     cfg.OTelDisabled,
		ExporterEndpoint: cfg.OTelExporterEndpoint,
		MetricsAddr:      cfg.MetricsAddr,
		LogLevel:         cfg.LogLevel,
	})
	if err != nil {
		log.Fatalf("[scheduler] Failed to initialize telemetry: %v", err)
	}
	if _, err := telemetry.InitMetrics(); err != nil {
		log.Fatalf("[scheduler] Failed to initialize metrics: %v", err)
	}
	defer telemetry.Shutdown(context.Background())

	repo, err := repository.New(cfg.DBURL)
	if err != nil {
		log.Fatalf("[scheduler] Failed to connect to database: %v", err)
	}
	defer repo.Close()

	grpcSrv := grpcutil.NewServer(cfg.GRPCAddr)
	pbsched.RegisterSchedulerServiceServer(grpcSrv.Server(), scheduler.NewGRPCServer(repo))
	health.RegisterHealthServiceServer(grpcSrv.Server(), grpcutil.NewHealthServer(grpcutil.NewDBHealthChecker(repo)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[scheduler] Serving metrics on %s/metrics", cfg.MetricsAddr)
		if err := tel.ServeMetrics(ctx); err != nil {
			log.Printf("[scheduler] metrics server error: %v", err)
		}
	}()

	log.Printf("[scheduler] Serving gRPC on %s", cfg.GRPCAddr)
	if err := grpcSrv.Start(); err != nil {
		log.Fatalf("[scheduler] gRPC server exited with error: %v", err)
	}
	<-ctx.Done()
	log.Println("[scheduler] Scheduler service shutdown complete.")
}
