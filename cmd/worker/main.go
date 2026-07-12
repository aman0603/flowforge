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
	"github.com/aman0603/flowforge/internal/repository"
	"github.com/aman0603/flowforge/internal/worker"
)

func main() {
	cfg := config.Load()

	// 1. Generate/Determine Worker ID
	workerID := getWorkerID()
	log.Printf("[worker-%s] Initializing FlowForge Worker process...", workerID)

	// 2. Connect to Database
	repo, err := repository.New(cfg.DBURL)
	if err != nil {
		log.Fatalf("[worker-%s] Failed to connect to database: %v", workerID, err)
	}
	defer repo.Close()

	// 3. Parse Poll Interval
	pollInterval := 1 * time.Second
	if intervalStr := os.Getenv("WORKER_POLL_INTERVAL"); intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil {
			pollInterval = d
		} else {
			log.Printf("[worker-%s] Warning: failed to parse WORKER_POLL_INTERVAL %q, using default 1s", workerID, intervalStr)
		}
	}

	// 4. Register Executors
	executors := map[string]worker.Executor{
		"SLEEP": worker.NewSleepExecutor(),
	}

	// 5. Construct Worker
	w := worker.New(workerID, repo, executors, pollInterval)

	// 6. Signal-aware context for Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 7. Run Worker Loop
	if err := w.Run(ctx); err != nil {
		log.Fatalf("[worker-%s] Worker exited with error: %v", workerID, err)
	}

	log.Printf("[worker-%s] Worker process shutdown complete.", workerID)
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
