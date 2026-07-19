package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/grpcutil"
	"github.com/aman0603/flowforge/internal/recovery"
	"github.com/aman0603/flowforge/internal/repository"
	"github.com/aman0603/flowforge/internal/scheduler"
	"github.com/aman0603/flowforge/internal/telemetry"
	"github.com/aman0603/flowforge/internal/worker"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Load()

	// 1. Generate/Determine Worker ID
	workerID := getWorkerID()

	// 1.5 Initialize observability.
	tel, err := telemetry.Init(telemetry.Config{
		ServiceName:      cfg.OTelServiceName,
		OTelDisabled:     cfg.OTelDisabled,
		ExporterEndpoint: cfg.OTelExporterEndpoint,
		MetricsAddr:      cfg.MetricsAddr,
		PProfEnabled:     cfg.PProfEnabled,
		LogLevel:         cfg.LogLevel,
	})
	if err != nil {
		log.Fatalf("[worker-%s] Failed to initialize telemetry: %v", workerID, err)
	}
	if _, err := telemetry.InitMetrics(); err != nil {
		log.Fatalf("[worker-%s] Failed to initialize metrics: %v", workerID, err)
	}
	defer telemetry.Shutdown(context.Background())

	logger := tel.Logger().With(zap.String("worker_id", workerID))
	logger.Info("initializing worker process", zap.String("db", telemetry.RedactDBURL(cfg.DBURL)))

	// 2. Connect to Database
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

	// 3. Parse Poll Interval
	pollInterval := 1 * time.Second
	if intervalStr := os.Getenv("WORKER_POLL_INTERVAL"); intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil {
			pollInterval = d
		} else {
			logger.Warn("failed to parse WORKER_POLL_INTERVAL, using default 1s", zap.String("value", intervalStr))
		}
	}

	// 4. Parse Recovery Configuration
	claimedStaleTimeout := 30 * time.Second
	if val := os.Getenv("CLAIMED_STALE_TIMEOUT"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			claimedStaleTimeout = d
		} else {
			logger.Warn("failed to parse CLAIMED_STALE_TIMEOUT, using default 30s", zap.String("value", val))
		}
	}

	runningStaleTimeout := 5 * time.Minute
	if val := os.Getenv("RUNNING_STALE_TIMEOUT"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			runningStaleTimeout = d
		} else {
			logger.Warn("failed to parse RUNNING_STALE_TIMEOUT, using default 5m", zap.String("value", val))
		}
	}

	recoveryInterval := 30 * time.Second
	if val := os.Getenv("RECOVERY_INTERVAL"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			recoveryInterval = d
		} else {
			logger.Warn("failed to parse RECOVERY_INTERVAL, using default 30s", zap.String("value", val))
		}
	}

	// 5. Connect to Redis Coordination
	coord, err := worker.NewRedisCoordinator(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		logger.Fatal("failed to connect to redis coordination", zap.Error(err))
	}
	defer coord.Close()

	// 5.5 Register Executors
	executors := map[string]worker.Executor{
		"SLEEP": worker.NewSleepExecutor(),
	}

	// 5.6 Choose scheduler client: gRPC when SCHEDULER_ADDR is set, else local.
	var sched scheduler.Client
	if cfg.SchedulerAddr != "" {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		grpcSched, err := scheduler.NewGRPCClient(sctx, cfg.SchedulerAddr, &grpcutil.CallOptions{
			MaxAttempts:    cfg.GRPCRetryMaxAttempts,
			BaseDelay:      cfg.GRPCRetryBaseDelay,
			RequestTimeout: cfg.GRPCRequestTimeout,
		})
		scancel()
		if err != nil {
			logger.Fatal("failed to connect to scheduler", zap.String("addr", cfg.SchedulerAddr), zap.Error(err))
		}
		defer grpcSched.Close()
		sched = grpcSched
		logger.Info("using remote scheduler", zap.String("addr", cfg.SchedulerAddr))
	} else {
		sched = scheduler.NewLocalClient(repo)
		logger.Info("using local (in-process) scheduler")
	}

	// 5.7 Choose recovery client: gRPC when RECOVERY_ADDR is set, else local.
	var recov recovery.Client
	if cfg.RecoveryAddr != "" {
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		grpcRecov, err := recovery.NewGRPCClient(rctx, cfg.RecoveryAddr, &grpcutil.CallOptions{
			MaxAttempts:    cfg.GRPCRetryMaxAttempts,
			BaseDelay:      cfg.GRPCRetryBaseDelay,
			RequestTimeout: cfg.GRPCRequestTimeout,
		})
		rcancel()
		if err != nil {
			logger.Fatal("failed to connect to recovery", zap.String("addr", cfg.RecoveryAddr), zap.Error(err))
		}
		defer grpcRecov.Close()
		recov = grpcRecov
		logger.Info("using remote recovery", zap.String("addr", cfg.RecoveryAddr))
	} else {
		recov = recovery.NewLocalClient(repo)
		logger.Info("using local (in-process) recovery")
	}

	// 6. Construct Worker
	w := worker.New(
		workerID,
		repo,
		executors,
		pollInterval,
		claimedStaleTimeout,
		runningStaleTimeout,
		recoveryInterval,
		coord,
		cfg.WorkerHeartbeatInterval,
		cfg.WorkerHeartbeatTTL,
		cfg.TaskLeaseTTL,
		cfg.TaskLeaseRenewInterval,
		cfg.WorkerPoolSize,
		cfg.WorkerQueueCapacity,
		cfg.WorkerClaimBatchSize,
		cfg.WorkerShutdownGrace,
		sched,
		recov,
	)

	// 6. Signal-aware context for Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 6.5 Bridge worker counters to Prometheus and serve /metrics.
	if err := telemetry.RegisterWorkerMetrics(func() telemetry.WorkerCounters {
		c := w.GetCounters()
		return telemetry.WorkerCounters{
			ActiveExecutions: c.ActiveExecutions,
			QueuedTasks:      c.QueuedTasks,
			TotalClaimed:     c.TotalClaimed,
			TotalStarted:     c.TotalStarted,
			TotalCompleted:   c.TotalCompleted,
			TotalFailed:      c.TotalFailed,
			TotalTimedOut:    c.TotalTimedOut,
			TotalPanics:      c.TotalPanics,
			TotalLeaseLosses: c.TotalLeaseLosses,
		}
	}); err != nil {
		logger.Error("failed to register worker metrics", zap.Error(err))
	}
	go func() {
		logger.Info("serving metrics", zap.String("addr", cfg.MetricsAddr))
		if err := tel.ServeMetrics(ctx); err != nil {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	// 7. Run Worker Loop
	if err := w.Run(ctx); err != nil {
		logger.Fatal("worker exited with error", zap.Error(err))
	}

	logger.Info("worker process shutdown complete")
}

func getWorkerID() string {
	if id := os.Getenv("WORKER_ID"); id != "" {
		return id
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	u := newUUID()
	return fmt.Sprintf("%s_%s", hostname, u)
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
