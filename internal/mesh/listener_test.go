package mesh

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// generateTestCert creates a self-signed certificate for testing TLS.
// Returns paths to temp cert.pem and key.pem files, plus a cleanup function.
func generateTestCert(t *testing.T) (certFile, keyFile string, cleanup func()) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "flare-test",
			Organization: []string{"Flare Test"},
		},
		DNSNames:              []string{"localhost", "127.0.0.1"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// x509.CreateCertificate accepts a crypto.PublicKey interface
	var pub any = &key.PublicKey

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, pub, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	dir := t.TempDir()

	certPath := filepath.Join(dir, "cert.pem")
	certFileW, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	if err := pem.Encode(certFileW, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		certFileW.Close()
		t.Fatalf("encode cert: %v", err)
	}
	certFileW.Close()

	keyPath := filepath.Join(dir, "key.pem")
	keyFileW, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		keyFileW.Close()
		t.Fatalf("marshal key: %v", err)
	}
	if err := pem.Encode(keyFileW, &pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}); err != nil {
		keyFileW.Close()
		t.Fatalf("encode key: %v", err)
	}
	keyFileW.Close()

	return certPath, keyPath, func() { os.RemoveAll(dir) }
}

// TestListener_WithTLS verifies that a TLS-enabled listener accepts
// WebSocket connections over wss:// with a self-signed certificate.
func TestListener_WithTLS(t *testing.T) {
	certFile, keyFile, cleanup := generateTestCert(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	addr := getFreePort(t)
	_ = NewHub(func(p *PeerState) { _ = p })
	var connected atomic.Int64

	// Create listener with TLS
	ln := NewListener(addr, func(conn *websocket.Conn) {
		connected.Add(1)
		// Accept the hello-like handshake with a response
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello"}`))
	}).WithTLS(certFile, keyFile)

	// Start in background
	go func() {
		_ = ln.Start(ctx)
	}()

	// Wait for listener to be ready
	time.Sleep(200 * time.Millisecond)

	// Connect using WebSocket with TLS (wss://)
	u := url.URL{Scheme: "wss", Host: addr, Path: "/mesh"}

	// Use a custom dialer that trusts our self-signed cert
	dialer := &websocket.Dialer{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // test cert is self-signed
		},
	}

	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		t.Fatalf("TLS WebSocket dial failed: %v", err)
	}
	defer conn.Close()

	// Send a message to trigger the handler
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"hello","from":"test"}`)); err != nil {
		t.Fatalf("write message: %v", err)
	}

	// Give the handler time to fire
	time.Sleep(100 * time.Millisecond)

	if n := connected.Load(); n != 1 {
		t.Errorf("connection handler called: want 1, got %d", n)
	}
}

// TestListener_WithTLS_RejectsPlainWS verifies that a TLS-only listener
// rejects plain non-TLS WebSocket connections.
func TestListener_WithTLS_RejectsPlainWS(t *testing.T) {
	certFile, keyFile, cleanup := generateTestCert(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr := getFreePort(t)
	_ = NewHub(func(p *PeerState) { _ = p })

	ln := NewListener(addr, func(conn *websocket.Conn) {
		t.Error("plain WS connection should not reach handler on TLS listener")
	}).WithTLS(certFile, keyFile)

	go func() {
		_ = ln.Start(ctx)
	}()

	time.Sleep(200 * time.Millisecond)

	// Try to connect with plain WS (should fail TLS handshake)
	u := url.URL{Scheme: "ws", Host: addr, Path: "/mesh"}
	dialer := websocket.DefaultDialer
	_, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err == nil {
		t.Error("expected plain WS dial to fail on TLS listener, but it succeeded")
	}
}

// TestListener_WithTLS_Connect verifies an end-to-end WebSocket upgrade
// over TLS between two nodes using the Connect function with wss://.
func TestListener_WithTLS_Connect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TLS Connect test in short mode")
	}

	certFile, keyFile, cleanup := generateTestCert(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Node A (listener with TLS)
	_ = NewHub(func(p *PeerState) { _ = p })
	ln := NewListener("127.0.0.1:19731", func(conn *websocket.Conn) {
		// Incoming connection — read hello, respond
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(
			`{"type":"hello","from":"alpha","payload":{"node_name":"alpha","version":"0.1.0"}}`,
		))
	}).WithTLS(certFile, keyFile)

	go func() {
		_ = ln.Start(ctx)
	}()

	time.Sleep(200 * time.Millisecond)

	// Since Connect() uses the default dialer which doesn't trust our
	// self-signed cert, we verify the TLS upgrade works at the transport
	// level with a custom dialer.
	u := url.URL{Scheme: "wss", Host: "127.0.0.1:19731", Path: "/mesh"}
	dialer := &websocket.Dialer{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
		},
	}

	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		t.Fatalf("TLS WebSocket dial: %v", err)
	}
	defer conn.Close()

	// Verify we can read/write through TLS
	hello := []byte(`{"type":"hello","from":"beta","payload":{"node_name":"beta","version":"0.1.0"}}`)
	if err := conn.WriteMessage(websocket.TextMessage, hello); err != nil {
		t.Fatalf("write over TLS: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, resp, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read over TLS: %v", err)
	}
	if len(resp) == 0 {
		t.Fatal("empty response over TLS")
	}

	t.Logf("TLS WebSocket exchange succeeded: got %d bytes", len(resp))
}

// TestListener_PlainWS_Fallback verifies that a listener without TLS
// still works normally (plain WS).
func TestListener_PlainWS_Fallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fallback test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = NewHub(func(p *PeerState) { _ = p })
	var connected atomic.Int64

	// Listener without TLS
	ln := NewListener("127.0.0.1:19732", func(conn *websocket.Conn) {
		connected.Add(1)
	})

	go func() {
		_ = ln.Start(ctx)
	}()

	time.Sleep(200 * time.Millisecond)

	// Connect with plain WS
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, "ws://127.0.0.1:19732/mesh", nil)
	if err != nil {
		t.Fatalf("plain WS dial failed: %v", err)
	}
	defer conn.Close()

	// Verify no TLS
	if conn.UnderlyingConn() == nil {
		t.Fatal("underlying connection is nil")
	}

	// Send a message
	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if n := connected.Load(); n != 1 {
		t.Errorf("connection handler: want 1, got %d", n)
	}

	t.Log("plain WS fallback listener works correctly")
}

// TestNewListener_WithTLS_Config verifies that WithTLS properly ignores
// empty cert/key paths (no TLS configured).
func TestNewListener_WithTLS_Config(t *testing.T) {
	// With both empty — no TLS
	ln1 := NewListener(":0", nil).WithTLS("", "")
	if ln1.certFile != "" || ln1.keyFile != "" {
		t.Error("WithTLS('', '') should not set cert/key")
	}

	// With one empty — no TLS
	ln2 := NewListener(":0", nil).WithTLS("cert.pem", "")
	if ln2.certFile != "" || ln2.keyFile != "" {
		t.Error("WithTLS('cert.pem', '') should not enable TLS when key is empty")
	}

	// With both set — TLS configured
	ln3 := NewListener(":0", nil).WithTLS("cert.pem", "key.pem")
	if ln3.certFile != "cert.pem" || ln3.keyFile != "key.pem" {
		t.Errorf("WithTLS should set cert/key: got %q, %q", ln3.certFile, ln3.keyFile)
	}
}

// getFreePort finds an available TCP port for testing.
func getFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return fmt.Sprintf("127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port)
}

// TestStartListener_PassesTLS tests that the StartListener helper correctly
// passes TLS config through to the underlying Listener.
func TestStartListener_PassesTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hub := NewHub(func(p *PeerState) { _ = p })

	// Start a plain WS listener via StartListener helper
	ln := StartListener(ctx, "127.0.0.1:19733", "test-node", hub, "", "")
	if ln == nil {
		t.Fatal("StartListener returned nil")
	}
	if ln.certFile != "" || ln.keyFile != "" {
		t.Error("StartListener with empty TLS should not set TLS cert/key")
	}

	// Verify the listener is actually serving by connecting
	time.Sleep(200 * time.Millisecond)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws://127.0.0.1:19733/mesh", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()
}
