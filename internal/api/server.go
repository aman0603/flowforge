package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/dag"
	"github.com/aman0603/flowforge/internal/grpcutil"
	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/proto/common"
	health "github.com/aman0603/flowforge/internal/proto/health"
	"github.com/aman0603/flowforge/internal/repository"
	"github.com/aman0603/flowforge/internal/telemetry"
	"github.com/aman0603/flowforge/internal/telemetry/httpmw"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// pinger is the minimal readiness dependency: something that can verify its
// backing store is reachable. *repository.Repository satisfies it, and tests can
// substitute a fake to exercise the /readyz probe without a real database.
type pinger interface {
	Ping(ctx context.Context) error
}

// Server represents the HTTP server.
type Server struct {
	cfg    *config.Config
	repo   *repository.Repository
	ready  pinger
	router *http.ServeMux
}

// NewServer initializes a new Server instance.
func NewServer(cfg *config.Config, repo *repository.Repository) *Server {
	s := &Server{
		cfg:    cfg,
		repo:   repo,
		router: http.NewServeMux(),
	}
	if repo != nil {
		s.ready = repo
	}
	s.registerRoutes()
	return s
}

// registerRoutes sets up the endpoints.
func (s *Server) registerRoutes() {
	s.router.HandleFunc("GET /health", s.handleHealth)
	s.router.HandleFunc("GET /healthz", s.handleLiveness)
	s.router.HandleFunc("GET /readyz", s.handleReadiness)
	s.router.HandleFunc("POST /api/v1/workflows", s.handleCreateDefinition)
	s.router.HandleFunc("POST /runs", s.handleCreateRun)
	s.router.HandleFunc("GET /runs/{id}", s.handleGetRunDetails)
	s.router.HandleFunc("GET /api/v1/runs/{run_id}/history", s.handleGetWorkflowRunHistory)
	s.router.HandleFunc("GET /api/v1/tasks/{task_run_id}/attempts", s.handleGetTaskAttempts)
	s.router.HandleFunc("GET /api/v1/dead-letter", s.handleGetDeadLetterTasks)

	// Prometheus scrape endpoint (served from the telemetry registry).
	if reg := telemetry.GetMetricsRegistry(); reg != nil {
		s.router.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	}
}

// maxBodyBytes returns the configured request body size limit, falling back to
// 1 MiB when unset (e.g. in tests constructing a bare config).
func (s *Server) maxBodyBytes() int64 {
	if s.cfg != nil && s.cfg.MaxRequestBodyBytes > 0 {
		return s.cfg.MaxRequestBodyBytes
	}
	return 1 << 20
}

// handleHealth responds with a JSON status OK.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleLiveness is the Kubernetes-style liveness probe: it returns 200 as long
// as the process is running and able to serve HTTP. It does not check
// dependencies so a transient DB outage does not cause pod restarts.
func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// handleReadiness is the readiness probe: it returns 200 only when the database
// is reachable, otherwise 503 so traffic is not routed to an unready instance.
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	if s.ready == nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unready",
			"reason": "no readiness dependency configured",
		})
		return
	}
	pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.ready.Ping(pingCtx); err != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unready",
			"reason": "database unreachable",
		})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// handleCreateDefinition handles POST /definitions.
func (s *Server) handleCreateDefinition(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes())
	var req model.CreateDefinitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}

	// Validate the DAG
	if err := dag.Validate(&req); err != nil {
		s.writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf("DAG validation failed: %v", err), nil)
		return
	}

	// Persist to Postgres
	def, err := s.repo.CreateWorkflowDefinition(r.Context(), &req)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to store workflow definition", err)
		return
	}

	s.writeJSON(w, http.StatusCreated, def)
}

// handleCreateRun handles POST /runs.
func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes())
	var req model.CreateRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", err)
		return
	}

	if req.WorkflowDefinitionID == "" {
		s.writeError(w, http.StatusBadRequest, "workflow_definition_id is required", nil)
		return
	}

	// Instantiate the run in Postgres
	run, err := s.repo.CreateWorkflowRun(r.Context(), req.WorkflowDefinitionID, req.Input)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to start workflow run", err)
		return
	}

	s.writeJSON(w, http.StatusCreated, run)
}

// handleGetRunDetails handles GET /runs/{id}.
func (s *Server) handleGetRunDetails(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		s.writeError(w, http.StatusBadRequest, "missing run id", nil)
		return
	}

	details, err := s.repo.GetWorkflowRunDetails(r.Context(), runID)
	if err != nil {
		// In a real application, check for ErrNoRows to return 404
		s.writeError(w, http.StatusInternalServerError, "failed to fetch run details", err)
		return
	}

	s.writeJSON(w, http.StatusOK, details)
}

// writeJSON helper to write JSON responses.
func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("failed to write JSON response: %v", err)
	}
}

// writeError helper to write standard error JSON responses.
func (s *Server) writeError(w http.ResponseWriter, status int, message string, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	payload := map[string]string{"error": message}
	if err != nil && s.cfg.Env == "development" {
		payload["details"] = err.Error()
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("failed to write JSON error response: %v", err)
	}
}

// Start runs the HTTP server.
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf(":%s", s.cfg.Port)
	handler := httpmw.RateLimit(httpmw.Middleware(s.router), s.cfg.RateLimitRPS, s.cfg.RateLimitBurst)
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MiB, mitigates slow-header / oversized-header DoS
	}

	log.Printf("Starting HTTP server on %s (env: %s)", addr, s.cfg.Env)

	// Start the gRPC server on its own address, exposing internal RPC
	// contracts (Phase 11) alongside the unchanged REST API.
	grpcErrChan := make(chan error, 1)
	grpcSrv, err := grpcutil.NewServerTLS(s.cfg.GRPCAddr, grpcutil.TLSConfig{
		Enabled:  s.cfg.GRPCTLSEnabled,
		CertFile: s.cfg.GRPCTLSCertFile,
		KeyFile:  s.cfg.GRPCTLSKeyFile,
		CAFile:   s.cfg.GRPCTLSCAFile,
	})
	if err != nil {
		return fmt.Errorf("failed to create gRPC server: %w", err)
	}
	health.RegisterHealthServiceServer(grpcSrv.Server(), grpcutil.NewHealthServer(&apiHealthChecker{repo: s.repo}))
	go func() {
		log.Printf("Starting gRPC server on %s", s.cfg.GRPCAddr)
		if err := grpcSrv.Start(); err != nil {
			grpcErrChan <- err
		}
	}()

	errChan := make(chan error, 1)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down HTTP server gracefully...")
		grpcSrv.Stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-grpcErrChan:
		return err
	case err := <-errChan:
		return err
	}
}

// apiHealthChecker reports the API service's health based on database
// connectivity. It implements grpcutil.HealthChecker.
type apiHealthChecker struct {
	repo *repository.Repository
}

// Status returns HEALTHY if the database can be pinged, otherwise UNHEALTHY.
func (c *apiHealthChecker) Status(ctx context.Context, readiness bool) (common.ServiceStatus, string) {
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.repo.Ping(pingCtx); err != nil {
		return common.ServiceStatus_SERVICE_STATUS_UNHEALTHY, fmt.Sprintf("database unreachable: %v", err)
	}
	return common.ServiceStatus_SERVICE_STATUS_HEALTHY, ""
}

// handleGetWorkflowRunHistory handles GET /api/v1/runs/{run_id}/history.
func (s *Server) handleGetWorkflowRunHistory(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	if runID == "" {
		s.writeError(w, http.StatusBadRequest, "missing run id", nil)
		return
	}

	history, err := s.repo.GetWorkflowRunHistory(r.Context(), runID)
	if err == sql.ErrNoRows {
		s.writeError(w, http.StatusNotFound, "workflow run not found", nil)
		return
	} else if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to fetch workflow history", err)
		return
	}

	s.writeJSON(w, http.StatusOK, history)
}

// handleGetTaskAttempts handles GET /api/v1/tasks/{task_run_id}/attempts.
func (s *Server) handleGetTaskAttempts(w http.ResponseWriter, r *http.Request) {
	taskRunID := r.PathValue("task_run_id")
	if taskRunID == "" {
		s.writeError(w, http.StatusBadRequest, "missing task run id", nil)
		return
	}

	attempts, err := s.repo.GetTaskAttempts(r.Context(), taskRunID)
	if err == sql.ErrNoRows {
		s.writeError(w, http.StatusNotFound, "task run not found", nil)
		return
	} else if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to fetch task attempts", err)
		return
	}

	s.writeJSON(w, http.StatusOK, attempts)
}

// handleGetDeadLetterTasks handles GET /api/v1/dead-letter.
func (s *Server) handleGetDeadLetterTasks(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 50
	if limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed < 0 {
			s.writeError(w, http.StatusBadRequest, "invalid limit parameter", nil)
			return
		}
		limit = parsed
	}

	if limit > 100 {
		limit = 100
	}

	offset := 0
	if offsetStr != "" {
		parsed, err := strconv.Atoi(offsetStr)
		if err != nil || parsed < 0 {
			s.writeError(w, http.StatusBadRequest, "invalid offset parameter", nil)
			return
		}
		offset = parsed
	}

	dlqs, err := s.repo.GetDeadLetterTasks(r.Context(), limit, offset)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to fetch dead letter tasks", err)
		return
	}

	// Always return an empty array if nil
	if dlqs == nil {
		dlqs = []model.DeadLetterTask{}
	}

	s.writeJSON(w, http.StatusOK, dlqs)
}
