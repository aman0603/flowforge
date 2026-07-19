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
	"go.uber.org/zap"
)

// Scheduler service: hosts the SchedulerService gRPC server (claiming + retry
// promotion) over the repository. It does not execute tasks.
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
		log.Fatalf("[scheduler] Failed to initialize telemetry: %v", err)
	}
	if _, err := telemetry.InitMetrics(); err != nil {
		log.Fatalf("[scheduler] Failed to initialize metrics: %v", err)
	}
	defer telemetry.Shutdown(context.Background())

	logger := tel.Logger()
	logger.Info("initializing scheduler service", zap.String("db", telemetry.RedactDBURL(cfg.DBURL)))

	repo, err := repository.New(cfg.DBURL)
	if err != nil {
		logger.Fatal("failed to connect to database", zap.Error(err))
	}
	defer repo.Close()

	grpcSrv := grpcutil.NewServer(cfg.GRPCAddr)
	pbsched.RegisterSchedulerServiceServer(grpcSrv.Server(), scheduler.NewGRPCServer(repo))
	health.RegisterHealthServiceServer(grpcSrv.Server(), grpcutil.NewHealthServer(grpcutil.NewDBHealthChecker(repo)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("serving metrics", zap.String("addr", cfg.MetricsAddr))
		if err := tel.ServeMetrics(ctx); err != nil {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	logger.Info("serving grpc", zap.String("addr", cfg.GRPCAddr))
	if err := grpcSrv.Start(); err != nil {
		logger.Fatal("grpc server exited with error", zap.Error(err))
	}
	<-ctx.Done()
	logger.Info("scheduler service shutdown complete")
}
