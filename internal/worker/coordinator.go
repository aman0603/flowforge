package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type LeaseValue struct {
	WorkerID     string `json:"worker_id"`
	FencingToken int64  `json:"fencing_token"`
}

type Coordinator interface {
	RegisterWorker(ctx context.Context, workerID string, ttl time.Duration) error
	DeregisterWorker(ctx context.Context, workerID string) error
	HeartbeatWorker(ctx context.Context, workerID string, ttl time.Duration) error
	IsWorkerAlive(ctx context.Context, workerID string) (bool, error)
	AcquireTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64, ttl time.Duration) (bool, error)
	RenewTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64, ttl time.Duration) (bool, error)
	ReleaseTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64) error
	GetTaskLease(ctx context.Context, taskRunID string) (string, int64, error)
	Close() error
}

type RedisCoordinator struct {
	client *redis.Client
}

func NewRedisCoordinator(addr string, password string, db int) (*RedisCoordinator, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Startup connectivity check
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to ping Redis: %w", err)
	}

	return &RedisCoordinator{client: client}, nil
}

func (rc *RedisCoordinator) RegisterWorker(ctx context.Context, workerID string, ttl time.Duration) error {
	key := fmt.Sprintf("flowforge:worker:%s:heartbeat", workerID)
	return rc.client.Set(ctx, key, workerID, ttl).Err()
}

func (rc *RedisCoordinator) DeregisterWorker(ctx context.Context, workerID string) error {
	key := fmt.Sprintf("flowforge:worker:%s:heartbeat", workerID)
	return rc.client.Del(ctx, key).Err()
}

func (rc *RedisCoordinator) HeartbeatWorker(ctx context.Context, workerID string, ttl time.Duration) error {
	key := fmt.Sprintf("flowforge:worker:%s:heartbeat", workerID)
	res, err := rc.client.PExpire(ctx, key, ttl).Result()
	if err != nil {
		return err
	}
	if !res {
		// If key was expired or absent, recreate it
		return rc.client.Set(ctx, key, workerID, ttl).Err()
	}
	return nil
}

func (rc *RedisCoordinator) IsWorkerAlive(ctx context.Context, workerID string) (bool, error) {
	key := fmt.Sprintf("flowforge:worker:%s:heartbeat", workerID)
	exists, err := rc.client.Exists(ctx, key).Result()
	return exists > 0, err
}

func (rc *RedisCoordinator) AcquireTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64, ttl time.Duration) (bool, error) {
	key := fmt.Sprintf("flowforge:task:%s:lease", taskRunID)
	val := LeaseValue{WorkerID: workerID, FencingToken: fencingToken}
	b, err := json.Marshal(val)
	if err != nil {
		return false, err
	}

	res, err := rc.client.SetNX(ctx, key, string(b), ttl).Result()
	return res, err
}

const renewScript = `
if redis.call('get', KEYS[1]) == ARGV[1] then
    redis.call('pexpire', KEYS[1], ARGV[2])
    return 1
else
    return 0
end
`

func (rc *RedisCoordinator) RenewTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64, ttl time.Duration) (bool, error) {
	key := fmt.Sprintf("flowforge:task:%s:lease", taskRunID)
	val := LeaseValue{WorkerID: workerID, FencingToken: fencingToken}
	b, err := json.Marshal(val)
	if err != nil {
		return false, err
	}

	res, err := rc.client.Eval(ctx, renewScript, []string{key}, string(b), int64(ttl/time.Millisecond)).Result()
	if err != nil {
		return false, err
	}
	return res.(int64) == 1, nil
}

const releaseScript = `
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
`

func (rc *RedisCoordinator) ReleaseTaskLease(ctx context.Context, taskRunID string, workerID string, fencingToken int64) error {
	key := fmt.Sprintf("flowforge:task:%s:lease", taskRunID)
	val := LeaseValue{WorkerID: workerID, FencingToken: fencingToken}
	b, err := json.Marshal(val)
	if err != nil {
		return err
	}

	_, err = rc.client.Eval(ctx, releaseScript, []string{key}, string(b)).Result()
	return err
}

func (rc *RedisCoordinator) GetTaskLease(ctx context.Context, taskRunID string) (string, int64, error) {
	key := fmt.Sprintf("flowforge:task:%s:lease", taskRunID)
	res, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", 0, nil
	} else if err != nil {
		return "", 0, err
	}

	var val LeaseValue
	if err := json.Unmarshal([]byte(res), &val); err != nil {
		return "", 0, err
	}

	return val.WorkerID, val.FencingToken, nil
}

func (rc *RedisCoordinator) Close() error {
	return rc.client.Close()
}
