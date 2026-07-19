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
	pbrecov "github.com/aman0603/flowforge/internal/proto/recovery"
	"github.com/aman0603/flowforge/internal/recovery"
	"github.com/aman0603/flowforge/internal/repository"
	"github.com/aman0603/flowforge/internal/telemetry"
	"go.uber.org/zap"
)

// Recovery service: hosts the RecoveryService gRPC server (guarded stale-task
// reclaim transitions) over the repository. It does not execute tasks.
func main() {
	cfg := config.Load()

	tel, err := telemetry.Init(telemetry.Config{
		ServiceName:      cfg.OTelServiceName,
		OTelDisabled:     cfg.OTelDisabled,
		ExporterEndpoint: cfg.OTelExporterEndpoint,
		MetricsAddr:      cfg.MetricsAddr,
		PProfEnabled:     cfg.PProfEnabled,
		LogLevel:         cfg.LogLevel,
	})
	if err != nil {
		log.Fatalf("[recovery] Failed to initialize telemetry: %v", err)
	}
	if _, err := telemetry.InitMetrics(); err != nil {
		log.Fatalf("[recovery] Failed to initialize metrics: %v", err)
	}
	defer telemetry.Shutdown(context.Background())

	logger := tel.Logger()
	logger.Info("initializing recovery service", zap.String("db", telemetry.RedactDBURL(cfg.DBURL)))

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

	grpcSrv, err := grpcutil.NewServerTLS(cfg.GRPCAddr, grpcutil.TLSConfig{
		Enabled:  cfg.GRPCTLSEnabled,
		CertFile: cfg.GRPCTLSCertFile,
		KeyFile:  cfg.GRPCTLSKeyFile,
		CAFile:   cfg.GRPCTLSCAFile,
	})
	if err != nil {
		logger.Fatal("failed to create gRPC server", zap.Error(err))
	}
	pbrecov.RegisterRecoveryServiceServer(grpcSrv.Server(), recovery.NewGRPCServer(repo))
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
	logger.Info("recovery service shutdown complete")
}
