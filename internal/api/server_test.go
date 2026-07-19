package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aman0603/flowforge/internal/config"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(_ context.Context) error { return f.err }

func TestHandleHealth(t *testing.T) {
	cfg := &config.Config{Port: "8080", Env: "test"}
	server := NewServer(cfg, nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK, got %v", resp.Status)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %v", contentType)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if status, ok := body["status"]; !ok || status != "ok" {
		t.Errorf("expected status 'ok', got %v", body)
	}
}

func TestHandleHealthMethodNotAllowed(t *testing.T) {
	cfg := &config.Config{Port: "8080", Env: "test"}
	server := NewServer(cfg, nil)

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	// Go 1.22+ ServeMux returns 405 (Method Not Allowed) when path matches but method doesn't.
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected status Method Not Allowed (405), got %v", resp.StatusCode)
	}
}

func TestHandleLiveness(t *testing.T) {
	server := NewServer(&config.Config{Env: "test"}, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("liveness expected 200, got %d", w.Code)
	}
}

func TestHandleReadinessReady(t *testing.T) {
	server := NewServer(&config.Config{Env: "test"}, nil)
	server.ready = fakePinger{err: nil}

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("readiness expected 200, got %d", w.Code)
	}
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["status"] != "ready" {
		t.Fatalf("expected status ready, got %v", body)
	}
}

func TestHandleReadinessUnavailable(t *testing.T) {
	server := NewServer(&config.Config{Env: "test"}, nil)
	server.ready = fakePinger{err: errors.New("connection refused")}

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness expected 503, got %d", w.Code)
	}
}
