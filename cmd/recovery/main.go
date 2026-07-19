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
)

// Recovery service: hosts the RecoveryService gRPC server (guarded stale-task
// reclaim transitions) over the repository. It does not execute tasks.
func main() {
	cfg := config.Load()

	log.Println("[recovery] Initializing FlowForge Recovery service...")

	repo, err := repository.New(cfg.DBURL)
	if err != nil {
		log.Fatalf("[recovery] Failed to connect to database: %v", err)
	}
	defer repo.Close()

	grpcSrv := grpcutil.NewServer(cfg.GRPCAddr)
	pbrecov.RegisterRecoveryServiceServer(grpcSrv.Server(), recovery.NewGRPCServer(repo))
	health.RegisterHealthServiceServer(grpcSrv.Server(), grpcutil.NewHealthServer(grpcutil.NewDBHealthChecker(repo)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("[recovery] Serving gRPC on %s", cfg.GRPCAddr)
	if err := grpcSrv.Start(); err != nil {
		log.Fatalf("[recovery] gRPC server exited with error: %v", err)
	}
	<-ctx.Done()
	log.Println("[recovery] Recovery service shutdown complete.")
}
