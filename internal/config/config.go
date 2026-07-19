package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the application configuration.
type Config struct {
	Port                    string
	GRPCAddr                string
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
	KafkaBrokers            []string
	KafkaTopic              string
	KafkaClientID           string
	SchedulerAddr           string
	RecoveryAddr            string
	GRPCRetryMaxAttempts    int
	GRPCRetryBaseDelay      time.Duration
	GRPCRequestTimeout      time.Duration
	OutboxPollInterval      time.Duration
	OutboxBatchSize         int
	OutboxClaimTimeout      time.Duration
	OutboxMaxRetries        int
	OutboxRetryBaseDelay    time.Duration
	OutboxRetention         time.Duration
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

	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	var kafkaBrokers []string
	if kafkaBrokersStr != "" {
		kafkaBrokers = strings.Split(kafkaBrokersStr, ",")
	}

	batchSizeOutbox, _ := strconv.Atoi(getEnv("OUTBOX_BATCH_SIZE", "100"))
	if batchSizeOutbox <= 0 {
		batchSizeOutbox = 100
	}

	maxRetriesOutbox, _ := strconv.Atoi(getEnv("OUTBOX_MAX_RETRIES", "5"))
	if maxRetriesOutbox < 0 {
		maxRetriesOutbox = 5
	}

	pollInterval, err := time.ParseDuration(getEnv("OUTBOX_POLL_INTERVAL", "500ms"))
	if err != nil {
		pollInterval = 500 * time.Millisecond
	}

	claimTimeout, err := time.ParseDuration(getEnv("OUTBOX_CLAIM_TIMEOUT", "30s"))
	if err != nil {
		claimTimeout = 30 * time.Second
	}

	retryBaseDelay, err := time.ParseDuration(getEnv("OUTBOX_RETRY_BASE_DELAY", "1s"))
	if err != nil {
		retryBaseDelay = 1 * time.Second
	}

	retention, err := time.ParseDuration(getEnv("OUTBOX_RETENTION", "24h"))
	if err != nil {
		retention = 24 * time.Hour
	}

	grpcRetryMaxAttempts := 3
	if v, err := strconv.Atoi(getEnv("GRPC_RETRY_MAX_ATTEMPTS", "3")); err == nil && v >= 0 {
		grpcRetryMaxAttempts = v
	}

	grpcRetryBaseDelay, err := time.ParseDuration(getEnv("GRPC_RETRY_BASE_DELAY", "50ms"))
	if err != nil {
		grpcRetryBaseDelay = 50 * time.Millisecond
	}

	grpcRequestTimeout, err := time.ParseDuration(getEnv("GRPC_REQUEST_TIMEOUT", "5s"))
	if err != nil {
		grpcRequestTimeout = 5 * time.Second
	}

	return &Config{
		Port:                    getEnv("PORT", "8080"),
		GRPCAddr:                getEnv("GRPC_ADDR", "0.0.0.0:9090"),
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
		KafkaBrokers:            kafkaBrokers,
		KafkaTopic:              getEnv("KAFKA_TOPIC", "flowforge.workflow-events.v1"),
		KafkaClientID:           getEnv("KAFKA_CLIENT_ID", "flowforge-publisher"),
		SchedulerAddr:           getEnv("SCHEDULER_ADDR", ""),
		RecoveryAddr:            getEnv("RECOVERY_ADDR", ""),
		GRPCRetryMaxAttempts:    grpcRetryMaxAttempts,
		GRPCRetryBaseDelay:      grpcRetryBaseDelay,
		GRPCRequestTimeout:      grpcRequestTimeout,
		OutboxPollInterval:      pollInterval,
		OutboxBatchSize:         batchSizeOutbox,
		OutboxClaimTimeout:      claimTimeout,
		OutboxMaxRetries:        maxRetriesOutbox,
		OutboxRetryBaseDelay:    retryBaseDelay,
		OutboxRetention:         retention,
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
