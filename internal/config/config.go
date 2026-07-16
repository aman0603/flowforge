package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the application configuration.
type Config struct {
	Port                    string
	DBURL                   string
	Env                     string
	SchemaPath              string
	RedisAddr               string
	RedisPassword           string
	RedisDB                 int
	WorkerHeartbeatInterval time.Duration
	WorkerHeartbeatTTL      time.Duration
	TaskLeaseTTL            time.Duration
	TaskLeaseRenewInterval  time.Duration
	WorkerPoolSize          int
	WorkerQueueCapacity     int
	WorkerClaimBatchSize    int
	WorkerShutdownGrace     time.Duration
}

// Load reads configuration from environment variables with fallback defaults.
func Load() *Config {
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))

	hbIntervalMs, _ := strconv.Atoi(getEnv("WORKER_HEARTBEAT_INTERVAL_MS", "1000"))
	hbTTLMs, _ := strconv.Atoi(getEnv("WORKER_HEARTBEAT_TTL_MS", "3000"))
	leaseTTLMs, _ := strconv.Atoi(getEnv("TASK_LEASE_TTL_MS", "5000"))
	leaseRenewMs, _ := strconv.Atoi(getEnv("TASK_LEASE_RENEW_INTERVAL_MS", "1500"))

	poolSize, _ := strconv.Atoi(getEnv("WORKER_POOL_SIZE", "16"))
	queueCapacity, _ := strconv.Atoi(getEnv("WORKER_QUEUE_CAPACITY", "32"))
	batchSize, _ := strconv.Atoi(getEnv("WORKER_CLAIM_BATCH_SIZE", "8"))
	graceMs, _ := strconv.Atoi(getEnv("WORKER_SHUTDOWN_GRACE_PERIOD_MS", "10000"))

	if hbIntervalMs >= hbTTLMs {
		panic("invalid configuration: heartbeat interval must be less than heartbeat TTL")
	}
	if leaseRenewMs >= leaseTTLMs {
		panic("invalid configuration: lease renewal interval must be less than lease TTL")
	}
	if poolSize <= 0 {
		panic("invalid configuration: WORKER_POOL_SIZE must be greater than 0")
	}
	if queueCapacity < poolSize {
		panic("invalid configuration: WORKER_QUEUE_CAPACITY must be greater than or equal to WORKER_POOL_SIZE")
	}
	if batchSize <= 0 {
		panic("invalid configuration: WORKER_CLAIM_BATCH_SIZE must be greater than 0")
	}

	return &Config{
		Port:                    getEnv("PORT", "8080"),
		DBURL:                   getEnv("DB_URL", "postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable"),
		Env:                     getEnv("ENV", "development"),
		SchemaPath:              getEnv("SCHEMA_PATH", "schema.sql"),
		RedisAddr:               getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:           getEnv("REDIS_PASSWORD", ""),
		RedisDB:                 redisDB,
		WorkerHeartbeatInterval: time.Duration(hbIntervalMs) * time.Millisecond,
		WorkerHeartbeatTTL:      time.Duration(hbTTLMs) * time.Millisecond,
		TaskLeaseTTL:            time.Duration(leaseTTLMs) * time.Millisecond,
		TaskLeaseRenewInterval:  time.Duration(leaseRenewMs) * time.Millisecond,
		WorkerPoolSize:          poolSize,
		WorkerQueueCapacity:     queueCapacity,
		WorkerClaimBatchSize:    batchSize,
		WorkerShutdownGrace:     time.Duration(graceMs) * time.Millisecond,
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
