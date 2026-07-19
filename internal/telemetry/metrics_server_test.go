package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterPProf(t *testing.T) {
	mux := http.NewServeMux()
	registerPProf(mux)

	// The pprof index handler should respond on /debug/pprof/.
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ = %d, want 200", rec.Code)
	}
}

func TestPProfNotRegisteredByDefault(t *testing.T) {
	// A mux without registerPProf should not serve pprof endpoints.
	mux := http.NewServeMux()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /debug/pprof/ without registration = %d, want 404", rec.Code)
	}
}
