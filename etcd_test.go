package etcd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// writeTestCerts generates a self signed certificate and writes it out as the
// CA, client certificate and key, which is enough to exercise the loading in
// etcdConfig without needing a real PKI.
func writeTestCerts(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()

	dir := t.TempDir()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("could not generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "coredns-etcd-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("could not create certificate: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("could not marshal key: %v", err)
	}

	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	for path, content := range map[string][]byte{
		caFile:   certPEM,
		certFile: certPEM,
		keyFile:  keyPEM,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("could not write %s: %v", path, err)
		}
	}

	return caFile, certFile, keyFile
}

func TestEtcdConfig(t *testing.T) {
	caFile, certFile, keyFile := writeTestCerts(t)
	endpoints := []string{"https://192.168.3.42:2379", "https://192.168.3.43:2379"}

	cfg, err := etcdConfig(endpoints, caFile, certFile, keyFile)
	if err != nil {
		t.Fatalf("etcdConfig() error = %v", err)
	}

	if !reflect.DeepEqual(cfg.Endpoints, endpoints) {
		t.Errorf("Endpoints = %v, want %v", cfg.Endpoints, endpoints)
	}
	if cfg.TLS == nil {
		t.Fatal("TLS is nil, the connection would not be mutually authenticated")
	}
	if len(cfg.TLS.Certificates) != 1 {
		t.Errorf("got %d client certificates, want 1", len(cfg.TLS.Certificates))
	}
	if cfg.TLS.RootCAs == nil {
		t.Error("RootCAs is nil, the server certificate would not be verified")
	}
}

// Every one of these is a plausible deployment mistake, and each has to fail
// loudly rather than yield a config that silently skips verification.
func TestEtcdConfigRejectsUnusableFiles(t *testing.T) {
	caFile, certFile, keyFile := writeTestCerts(t)
	missing := filepath.Join(t.TempDir(), "does-not-exist.pem")

	garbage := filepath.Join(t.TempDir(), "garbage.pem")
	if err := os.WriteFile(garbage, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("could not write garbage file: %v", err)
	}

	tests := []struct {
		name                      string
		caFile, certFile, keyFile string
	}{
		{"missing CA", missing, certFile, keyFile},
		{"missing certificate", caFile, missing, keyFile},
		{"missing key", caFile, certFile, missing},
		{"certificate that will not parse", caFile, garbage, keyFile},
		{"key that will not parse", caFile, certFile, garbage},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := etcdConfig(nil, test.caFile, test.certFile, test.keyFile); err == nil {
				t.Error("etcdConfig() error = nil, want an error")
			}
		})
	}
}

// A CA file that reads but holds no certificate leaves an empty pool, which
// would fail every connection at handshake time rather than at startup. This
// records that today it is accepted, so a change in that behaviour is visible.
func TestEtcdConfigAcceptsCAWithoutCertificates(t *testing.T) {
	_, certFile, keyFile := writeTestCerts(t)

	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatalf("could not write empty file: %v", err)
	}

	cfg, err := etcdConfig(nil, empty, certFile, keyFile)
	if err != nil {
		t.Fatalf("etcdConfig() error = %v", err)
	}
	if cfg.TLS == nil || cfg.TLS.RootCAs == nil {
		t.Error("expected a TLS config with a (empty) CA pool")
	}
}
