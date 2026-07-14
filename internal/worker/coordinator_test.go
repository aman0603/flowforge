package worker

import (
	"context"
	"testing"
	"time"
)

func TestRedisCoordinatorHeartbeat(t *testing.T) {
	// Connect to local Redis
	coordinator, err := NewRedisCoordinator("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available on localhost:6379, skipping: %v", err)
	}
	defer coordinator.Close()

	ctx := context.Background()
	workerID := "test-worker-heartbeat"

	// 1. Register worker
	err = coordinator.RegisterWorker(ctx, workerID, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// Verify alive
	alive, err := coordinator.IsWorkerAlive(ctx, workerID)
	if err != nil || !alive {
		t.Errorf("expected worker to be alive, got alive=%t, err=%v", alive, err)
	}

	// 2. Heartbeat worker
	err = coordinator.HeartbeatWorker(ctx, workerID, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to send heartbeat: %v", err)
	}

	// 3. Expiry check (optional, but let's register another with very short TTL)
	shortWorker := "test-worker-short"
	_ = coordinator.RegisterWorker(ctx, shortWorker, 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	alive, _ = coordinator.IsWorkerAlive(ctx, shortWorker)
	if alive {
		t.Errorf("expected short worker to expire, but still alive")
	}
}

func TestRedisCoordinatorLease(t *testing.T) {
	coordinator, err := NewRedisCoordinator("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available on localhost:6379, skipping: %v", err)
	}
	defer coordinator.Close()

	ctx := context.Background()
	taskRunID := "test-task-1"

	// 1. Acquire lease
	acquired, err := coordinator.AcquireTaskLease(ctx, taskRunID, "worker-1", 42, 2*time.Second)
	if err != nil || !acquired {
		t.Fatalf("expected to acquire lease, got acquired=%t, err=%v", acquired, err)
	}

	// 2. Second worker tries to acquire (must fail)
	acquired, err = coordinator.AcquireTaskLease(ctx, taskRunID, "worker-2", 43, 2*time.Second)
	if err != nil || acquired {
		t.Errorf("expected second worker to fail lease acquisition, got acquired=%t, err=%v", acquired, err)
	}

	// Verify current lease details
	currWorker, currToken, err := coordinator.GetTaskLease(ctx, taskRunID)
	if err != nil || currWorker != "worker-1" || currToken != 42 {
		t.Errorf("unexpected current lease details: worker=%s, token=%d, err=%v", currWorker, currToken, err)
	}

	// 3. Renew lease (correct owner and token)
	renewed, err := coordinator.RenewTaskLease(ctx, taskRunID, "worker-1", 42, 5*time.Second)
	if err != nil || !renewed {
		t.Errorf("expected to renew lease, got renewed=%t, err=%v", renewed, err)
	}

	// 4. Renew lease with wrong owner
	renewed, err = coordinator.RenewTaskLease(ctx, taskRunID, "worker-2", 42, 5*time.Second)
	if err != nil || renewed {
		t.Errorf("expected wrong owner renewal to fail, got renewed=%t, err=%v", renewed, err)
	}

	// 5. Renew lease with wrong token
	renewed, err = coordinator.RenewTaskLease(ctx, taskRunID, "worker-1", 99, 5*time.Second)
	if err != nil || renewed {
		t.Errorf("expected wrong token renewal to fail, got renewed=%t, err=%v", renewed, err)
	}

	// 6. Release lease with wrong owner/token (must not release)
	err = coordinator.ReleaseTaskLease(ctx, taskRunID, "worker-2", 42)
	if err != nil {
		t.Errorf("ReleaseTaskLease returned unexpected error: %v", err)
	}
	currWorker, _, _ = coordinator.GetTaskLease(ctx, taskRunID)
	if currWorker != "worker-1" {
		t.Errorf("expected lease to still belong to worker-1, got %s", currWorker)
	}

	// 7. Release lease with correct owner/token
	err = coordinator.ReleaseTaskLease(ctx, taskRunID, "worker-1", 42)
	if err != nil {
		t.Errorf("failed to release lease: %v", err)
	}

	// Verify lease is now free
	currWorker, _, _ = coordinator.GetTaskLease(ctx, taskRunID)
	if currWorker != "" {
		t.Errorf("expected lease to be empty after release, got %s", currWorker)
	}
}
