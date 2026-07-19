package httpmw

import (
	"net/http"
	"sync"
	"time"
)

// tokenBucket is a minimal thread-safe token-bucket rate limiter using only the
// standard library, so FlowForge avoids an extra dependency. Tokens refill
// continuously at `rate` per second up to `burst` capacity.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	burst    float64
	rate     float64 // tokens per second
	lastFill time.Time
	now      func() time.Time
}

func newTokenBucket(rate, burst float64, now func() time.Time) *tokenBucket {
	if now == nil {
		now = time.Now
	}
	return &tokenBucket{
		tokens:   burst,
		burst:    burst,
		rate:     rate,
		lastFill: now(),
		now:      now,
	}
}

// allow reports whether a request may proceed, consuming one token if so.
func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	elapsed := now.Sub(b.lastFill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.lastFill = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// RateLimit wraps a handler with a global token-bucket limiter. When rps <= 0 it
// is disabled and returns the handler unchanged (default behavior). Rejected
// requests receive HTTP 429 with a Retry-After hint.
func RateLimit(next http.Handler, rps, burst int) http.Handler {
	if rps <= 0 {
		return next
	}
	if burst <= 0 {
		burst = rps
	}
	bucket := newTokenBucket(float64(rps), float64(burst), time.Now)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !bucket.allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
