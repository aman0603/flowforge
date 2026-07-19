package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// Empty values force the fallback defaults in Load.
	t.Setenv("DB_MAX_OPEN_CONNS", "")
	t.Setenv("DB_MAX_IDLE_CONNS", "")
	t.Setenv("DB_CONN_MAX_LIFETIME", "")
	t.Setenv("DB_CONN_MAX_IDLE_TIME", "")

	cfg := Load()

	if cfg.DBMaxOpenConns != 25 {
		t.Errorf("DBMaxOpenConns default = %d, want 25", cfg.DBMaxOpenConns)
	}
	if cfg.DBMaxIdleConns != 10 {
		t.Errorf("DBMaxIdleConns default = %d, want 10", cfg.DBMaxIdleConns)
	}
	if cfg.DBConnMaxLifetime != 30*time.Minute {
		t.Errorf("DBConnMaxLifetime default = %s, want 30m", cfg.DBConnMaxLifetime)
	}
	if cfg.DBConnMaxIdleTime != 5*time.Minute {
		t.Errorf("DBConnMaxIdleTime default = %s, want 5m", cfg.DBConnMaxIdleTime)
	}
}

func TestLoadDBPoolOverrides(t *testing.T) {
	t.Setenv("DB_MAX_OPEN_CONNS", "50")
	t.Setenv("DB_MAX_IDLE_CONNS", "20")
	t.Setenv("DB_CONN_MAX_LIFETIME", "1h")
	t.Setenv("DB_CONN_MAX_IDLE_TIME", "2m")

	cfg := Load()

	if cfg.DBMaxOpenConns != 50 {
		t.Errorf("DBMaxOpenConns = %d, want 50", cfg.DBMaxOpenConns)
	}
	if cfg.DBMaxIdleConns != 20 {
		t.Errorf("DBMaxIdleConns = %d, want 20", cfg.DBMaxIdleConns)
	}
	if cfg.DBConnMaxLifetime != time.Hour {
		t.Errorf("DBConnMaxLifetime = %s, want 1h", cfg.DBConnMaxLifetime)
	}
	if cfg.DBConnMaxIdleTime != 2*time.Minute {
		t.Errorf("DBConnMaxIdleTime = %s, want 2m", cfg.DBConnMaxIdleTime)
	}
}

func TestLoadIdleClampedToOpen(t *testing.T) {
	t.Setenv("DB_MAX_OPEN_CONNS", "5")
	t.Setenv("DB_MAX_IDLE_CONNS", "100")

	cfg := Load()

	if cfg.DBMaxIdleConns != 5 {
		t.Errorf("DBMaxIdleConns = %d, want clamped to 5", cfg.DBMaxIdleConns)
	}
}

func TestValidate(t *testing.T) {
	base := func() *Config {
		return &Config{
			WorkerHeartbeatInterval: 1 * time.Second,
			WorkerHeartbeatTTL:      3 * time.Second,
			TaskLeaseRenewInterval:  1 * time.Second,
			TaskLeaseTTL:            5 * time.Second,
			WorkerPoolSize:          16,
			WorkerQueueCapacity:     32,
			WorkerClaimBatchSize:    8,
			DBMaxOpenConns:          25,
			DBMaxIdleConns:          10,
		}
	}

	if err := base().Validate(); err != nil {
		t.Fatalf("valid config returned error: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"heartbeat interval >= ttl", func(c *Config) { c.WorkerHeartbeatInterval = c.WorkerHeartbeatTTL }},
		{"lease renew >= ttl", func(c *Config) { c.TaskLeaseRenewInterval = c.TaskLeaseTTL }},
		{"pool size zero", func(c *Config) { c.WorkerPoolSize = 0 }},
		{"queue < pool", func(c *Config) { c.WorkerQueueCapacity = c.WorkerPoolSize - 1 }},
		{"batch size zero", func(c *Config) { c.WorkerClaimBatchSize = 0 }},
		{"db open zero", func(c *Config) { c.DBMaxOpenConns = 0 }},
		{"db idle > open", func(c *Config) { c.DBMaxIdleConns = c.DBMaxOpenConns + 1 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mutate(c)
			if err := c.Validate(); err == nil {
				t.Errorf("expected validation error for %s", tc.name)
			}
		})
	}
}
