package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aman0603/flowforge/internal/config"
)

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
