package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aman0603/flowforge/internal/config"
	"github.com/aman0603/flowforge/internal/dag"
	"github.com/aman0603/flowforge/internal/model"
	"github.com/aman0603/flowforge/internal/repository"
)

// Server represents the HTTP server.
type Server struct {
	cfg    *config.Config
	repo   *repository.Repository
	router *http.ServeMux
}

// NewServer initializes a new Server instance.
func NewServer(cfg *config.Config, repo *repository.Repository) *Server {
	s := &Server{
		cfg:    cfg,
		repo:   repo,
		router: http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

// registerRoutes sets up the endpoints.
func (s *Server) registerRoutes() {
	s.router.HandleFunc("GET /health", s.handleHealth)
	s.router.HandleFunc("POST /definitions", s.handleCreateDefinition)
	s.router.HandleFunc("POST /runs", s.handleCreateRun)
	s.router.HandleFunc("GET /runs/{id}", s.handleGetRunDetails)
}

// handleHealth responds with a JSON status OK.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleCreateDefinition handles POST /definitions.
func (s *Server) handleCreateDefinition(w http.ResponseWriter, r *http.Request) {
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
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("Starting HTTP server on %s (env: %s)", addr, s.cfg.Env)

	errChan := make(chan error, 1)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down HTTP server gracefully...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}
