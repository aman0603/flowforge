package grpcutil

import (
	"testing"

	"google.golang.org/grpc/credentials/insecure"
)

func TestTLSDisabledReturnsInsecure(t *testing.T) {
	cfg := TLSConfig{Enabled: false}

	sc, err := cfg.ServerCredentials()
	if err != nil {
		t.Fatalf("ServerCredentials error: %v", err)
	}
	if sc.Info().SecurityProtocol != insecure.NewCredentials().Info().SecurityProtocol {
		t.Errorf("expected insecure server credentials when disabled")
	}

	cc, err := cfg.ClientCredentials()
	if err != nil {
		t.Fatalf("ClientCredentials error: %v", err)
	}
	if cc.Info().SecurityProtocol != insecure.NewCredentials().Info().SecurityProtocol {
		t.Errorf("expected insecure client credentials when disabled")
	}
}

func TestTLSEnabledWithoutCertErrors(t *testing.T) {
	cfg := TLSConfig{Enabled: true}
	if _, err := cfg.ServerCredentials(); err == nil {
		t.Error("expected error when TLS enabled but cert/key missing")
	}
}

func TestTLSEnabledBadCAErrors(t *testing.T) {
	cfg := TLSConfig{Enabled: true, CAFile: "/nonexistent/ca.pem"}
	if _, err := cfg.ClientCredentials(); err == nil {
		t.Error("expected error when CA file cannot be read")
	}
}

func TestTLSConfigFromEnv(t *testing.T) {
	t.Setenv("GRPC_TLS_ENABLED", "true")
	t.Setenv("GRPC_TLS_CERT_FILE", "/tmp/cert.pem")
	t.Setenv("GRPC_TLS_KEY_FILE", "/tmp/key.pem")
	t.Setenv("GRPC_TLS_CA_FILE", "/tmp/ca.pem")

	cfg := TLSConfigFromEnv()
	if !cfg.Enabled || cfg.CertFile != "/tmp/cert.pem" || cfg.KeyFile != "/tmp/key.pem" || cfg.CAFile != "/tmp/ca.pem" {
		t.Errorf("unexpected TLSConfig from env: %+v", cfg)
	}
}
