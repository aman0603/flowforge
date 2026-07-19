package grpcutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TLSConfig describes optional transport security for internal gRPC. It is
// sourced from environment variables so the security posture is deployment- and
// not code-controlled. When Enabled is false (the default) FlowForge uses
// insecure credentials for same-network development, preserving prior behavior.
type TLSConfig struct {
	Enabled  bool
	CertFile string // server certificate (PEM)
	KeyFile  string // server private key (PEM)
	CAFile   string // CA bundle used to verify the peer (PEM)
}

// TLSConfigFromEnv reads GRPC_TLS_* variables. Defaults keep TLS disabled.
//
//	GRPC_TLS_ENABLED   = "true" to require TLS
//	GRPC_TLS_CERT_FILE = path to server cert (server side)
//	GRPC_TLS_KEY_FILE  = path to server key  (server side)
//	GRPC_TLS_CA_FILE   = path to CA bundle   (client side, and mutual TLS)
func TLSConfigFromEnv() TLSConfig {
	return TLSConfig{
		Enabled:  os.Getenv("GRPC_TLS_ENABLED") == "true",
		CertFile: os.Getenv("GRPC_TLS_CERT_FILE"),
		KeyFile:  os.Getenv("GRPC_TLS_KEY_FILE"),
		CAFile:   os.Getenv("GRPC_TLS_CA_FILE"),
	}
}

// ServerCredentials returns transport credentials for a gRPC server. With TLS
// disabled it returns insecure credentials. With TLS enabled it loads the
// server key pair (and, if a CA is supplied, requires and verifies client
// certificates for mutual TLS).
func (c TLSConfig) ServerCredentials() (credentials.TransportCredentials, error) {
	if !c.Enabled {
		return insecure.NewCredentials(), nil
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return nil, fmt.Errorf("grpc tls enabled but GRPC_TLS_CERT_FILE/GRPC_TLS_KEY_FILE not set")
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if c.CAFile != "" {
		pool, err := loadCertPool(c.CAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), nil
}

// ClientCredentials returns transport credentials for a gRPC client. With TLS
// disabled it returns insecure credentials. With TLS enabled it verifies the
// server against the supplied CA (and presents a client cert for mutual TLS if
// a key pair is configured).
func (c TLSConfig) ClientCredentials() (credentials.TransportCredentials, error) {
	if !c.Enabled {
		return insecure.NewCredentials(), nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pool, err := loadCertPool(c.CAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
	}
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsCfg), nil
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid certificates found in CA file %s", caFile)
	}
	return pool, nil
}
