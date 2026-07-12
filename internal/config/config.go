package config

import (
	"os"
)

// Config holds the application configuration.
type Config struct {
	Port       string
	DBURL      string
	Env        string
	SchemaPath string
}

// Load reads configuration from environment variables with fallback defaults.
func Load() *Config {
	return &Config{
		Port:       getEnv("PORT", "8080"),
		DBURL:      getEnv("DB_URL", "postgres://postgres:postgres@localhost:5432/flowforge?sslmode=disable"),
		Env:        getEnv("ENV", "development"),
		SchemaPath: getEnv("SCHEMA_PATH", "schema.sql"),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
