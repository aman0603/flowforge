package httpmw

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTokenBucketAllowsBurstThenBlocks(t *testing.T) {
	now := time.Unix(0, 0)
	b := newTokenBucket(1, 3, func() time.Time { return now })

	// Burst of 3 should succeed with no time advance.
	for i := 0; i < 3; i++ {
		if !b.allow() {
			t.Fatalf("request %d within burst should be allowed", i)
		}
	}
	// 4th within the same instant is blocked.
	if b.allow() {
		t.Fatal("request beyond burst should be blocked")
	}
	// After 1 second, one token refills.
	now = now.Add(time.Second)
	if !b.allow() {
		t.Fatal("request after refill should be allowed")
	}
	if b.allow() {
		t.Fatal("second request after single refill should be blocked")
	}
}

func TestRateLimitDisabledPassthrough(t *testing.T) {
	called := 0
	h := RateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	}), 0, 0)

	for i := 0; i < 100; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("disabled limiter returned %d", rec.Code)
		}
	}
	if called != 100 {
		t.Fatalf("handler called %d times, want 100", called)
	}
}

func TestRateLimitReturns429(t *testing.T) {
	h := RateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), 1, 1)

	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec1.Code != http.StatusOK {
		t.Fatalf("first request = %d, want 200", rec1.Code)
	}

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", rec2.Code)
	}
	if rec2.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header on 429")
	}
}
