package grpcutil

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/aman0603/flowforge/internal/telemetry/grpcmw"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server is a minimal gRPC server wrapper used by FlowForge services. It owns a
// grpc.Server and exposes a single Start/Stop lifecycle with graceful shutdown.
type Server struct {
	server *grpc.Server
	addr   string
}

// NewServer constructs a gRPC Server listening on addr. Interceptors can be
// supplied via opts; a sensible default uses plaintext (insecure) credentials
// for internal, same-network communication. TLS is a future hardening step.
func NewServer(addr string, opts ...grpc.ServerOption) *Server {
	base := []grpc.ServerOption{
		grpc.Creds(insecure.NewCredentials()),
		grpc.ChainUnaryInterceptor(grpcmw.UnaryServerInterceptor()),
	}
	opts = append(base, opts...)
	return &Server{
		server: grpc.NewServer(opts...),
		addr:   addr,
	}
}

// Server returns the underlying *grpc.Server so callers can register services.
func (s *Server) Server() *grpc.Server {
	return s.server
}

// Start begins serving on the configured address. It blocks until the server
// stops; call from a goroutine and use Stop for graceful shutdown.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc server failed to listen on %s: %w", s.addr, err)
	}
	if err := s.server.Serve(lis); err != nil {
		return fmt.Errorf("grpc server stopped: %w", err)
	}
	return nil
}

// Stop gracefully drains the server.
func (s *Server) Stop() {
	s.server.GracefulStop()
}

// Dial connects to a FlowForge gRPC service at addr using insecure credentials.
// Callers are responsible for closing the returned *grpc.ClientConn.
func Dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial gRPC service at %s: %w", addr, err)
	}
	return conn, nil
}
