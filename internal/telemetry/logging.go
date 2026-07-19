package telemetry

import (
	"context"
	"net/url"

	"go.uber.org/zap"
)

// RedactDBURL returns a database connection string safe for logging by removing
// any password. It accepts URL-style DSNs (postgres://user:pass@host/db). If the
// input cannot be parsed as a URL it returns "[redacted]" to avoid leaking a
// key/value DSN that may embed a password.
func RedactDBURL(dsn string) string {
	if dsn == "" {
		return ""
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return "[redacted]"
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), "xxxxx")
		}
	}
	return u.String()
}

// Info logs an info-level message using the context-aware global logger so that
// trace_id, span_id, and request_id are attached automatically when present.
func Info(ctx context.Context, msg string, fields ...zap.Field) {
	LoggerWithContext(ctx).Info(msg, fields...)
}

// Error logs an error-level message using the context-aware global logger.
func Error(ctx context.Context, msg string, fields ...zap.Field) {
	LoggerWithContext(ctx).Error(msg, fields...)
}

// Warn logs a warn-level message using the context-aware global logger.
func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	LoggerWithContext(ctx).Warn(msg, fields...)
}
