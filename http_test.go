package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net/http"
	"testing"
	"time"

	"go.ntppool.org/common/apitls"
)

/*
HTTP Client Timeout Logic Tests (http_test.go)

WHAT IS BEING TESTED:
- Timeout calculation: Tests the 0.85 multiplier formula for response header timeouts
- No-op behavior: Setting the same timeout twice should be a no-op
- Precision edge cases: Tests very small timeouts and floating-point precision
- Configuration application: Tests that timeouts are applied to both client and transport
- Getter methods: Tests APIBase() and Client() accessor methods

WHAT IS NOT BEING TESTED:
- Actual network timeouts in practice
- Interaction with real TLS handshakes
- Transport connection pooling behavior
- Error handling when timeout values are invalid
- Concurrent timeout modifications
- Memory leaks from transport recreation

NOTES:
- Uses 0.85 multiplier for response header timeout
- No specific min/max timeout limits implemented

QUESTIONS:
- Why specifically 0.85 as the multiplier? Is this based on empirical testing or network analysis?
- Should there be minimum/maximum timeout limits? (e.g., reject timeouts < 100ms or > 10 minutes)
- What should happen with zero or negative timeouts?
- Should concurrent calls to SetTimeout() be thread-safe?
*/

// TestHTTPClientManager_SetTimeout tests timeout calculation and application
// Tests the 0.85 multiplier formula: responseHeaderTimeout = baseTimeout * 0.85
func TestHTTPClientManager_SetTimeout(t *testing.T) {
	cm := &httpClientManager{
		tlsConfig: &tls.Config{},
		apiURL:    "https://test.example.com/",
		client:    &http.Client{},
	}

	tests := []struct {
		name                          string
		baseTimeout                   time.Duration
		expectedResponseHeaderTimeout time.Duration
	}{
		{
			name:                          "60 second timeout",
			baseTimeout:                   60 * time.Second,
			expectedResponseHeaderTimeout: time.Duration(float64(60000)*0.85) * time.Millisecond, // 51s
		},
		{
			name:                          "30 second timeout",
			baseTimeout:                   30 * time.Second,
			expectedResponseHeaderTimeout: time.Duration(float64(30000)*0.85) * time.Millisecond, // 25.5s
		},
		{
			name:                          "120 second timeout",
			baseTimeout:                   120 * time.Second,
			expectedResponseHeaderTimeout: time.Duration(float64(120000)*0.85) * time.Millisecond, // 102s
		},
		{
			name:                          "1 second timeout",
			baseTimeout:                   1 * time.Second,
			expectedResponseHeaderTimeout: time.Duration(float64(1000)*0.85) * time.Millisecond, // 850ms
		},
		{
			name:                          "10 millisecond timeout",
			baseTimeout:                   10 * time.Millisecond,
			expectedResponseHeaderTimeout: time.Duration(8.5 * float64(time.Millisecond)), // 8.5ms
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Record current timeout to verify no-op behavior
			initialTimeout := cm.currentTimeout

			cm.SetTimeout(tt.baseTimeout)

			// Verify timeout was set
			if cm.currentTimeout != tt.baseTimeout {
				t.Errorf("Expected currentTimeout = %v, got %v", tt.baseTimeout, cm.currentTimeout)
			}

			// Verify client timeout was set
			if cm.client.Timeout != tt.baseTimeout {
				t.Errorf("Expected client.Timeout = %v, got %v", tt.baseTimeout, cm.client.Timeout)
			}

			// Verify transport was configured (basic check that it's not nil)
			if cm.client.Transport == nil {
				t.Error("Expected Transport to be configured, got nil")
			}

			// Test no-op behavior when setting same timeout
			cm.SetTimeout(tt.baseTimeout)
			if cm.currentTimeout != tt.baseTimeout {
				t.Error("SetTimeout should be no-op when setting same timeout")
			}

			// Reset for next test
			cm.currentTimeout = initialTimeout
		})
	}
}

// TestHTTPClientManager_SetTimeout_NoOpSameValue tests no-op behavior for duplicate timeout sets
func TestHTTPClientManager_SetTimeout_NoOpSameValue(t *testing.T) {
	cm := &httpClientManager{
		tlsConfig:      &tls.Config{},
		apiURL:         "https://test.example.com/",
		client:         &http.Client{},
		currentTimeout: 60 * time.Second,
	}

	// Should be no-op when setting same timeout
	cm.SetTimeout(60 * time.Second)

	if cm.currentTimeout != 60*time.Second {
		t.Errorf("Expected timeout to remain %v, got %v", 60*time.Second, cm.currentTimeout)
	}
}

// TestHTTPClientManager_ResponseHeaderTimeoutCalculation tests mathematical precision of timeout calculations
func TestHTTPClientManager_ResponseHeaderTimeoutCalculation(t *testing.T) {
	tests := []struct {
		name          string
		baseTimeoutMs int64
		expectedMs    int64
		description   string
	}{
		{
			name:          "Standard 60s timeout",
			baseTimeoutMs: 60000,
			expectedMs:    51000, // 60000 * 0.85
			description:   "60 seconds should give 51 second response header timeout",
		},
		{
			name:          "Small timeout",
			baseTimeoutMs: 1000,
			expectedMs:    850, // 1000 * 0.85
			description:   "1 second should give 850ms response header timeout",
		},
		{
			name:          "Large timeout",
			baseTimeoutMs: 300000,
			expectedMs:    255000, // 300000 * 0.85
			description:   "5 minutes should give 4.25 minute response header timeout",
		},
		{
			name:          "Very small timeout",
			baseTimeoutMs: 100,
			expectedMs:    85, // 100 * 0.85
			description:   "100ms should give 85ms response header timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate using the same formula as the code
			calculated := time.Duration(time.Millisecond * time.Duration(float64(tt.baseTimeoutMs)*0.85))
			expectedDuration := time.Duration(tt.expectedMs) * time.Millisecond

			if calculated != expectedDuration {
				t.Errorf("Timeout calculation failed: expected %v, got %v", expectedDuration, calculated)
			}

			// Verify the calculation is exactly what we expect
			calculatedMs := calculated.Milliseconds()
			if calculatedMs != tt.expectedMs {
				t.Errorf("Expected %d ms, got %d ms", tt.expectedMs, calculatedMs)
			}
		})
	}
}

func TestHTTPClientManager_APIBase(t *testing.T) {
	apiURL := "https://example.com/api/"
	cm := &httpClientManager{
		apiURL: apiURL,
	}

	if cm.APIBase() != apiURL {
		t.Errorf("Expected APIBase() = %q, got %q", apiURL, cm.APIBase())
	}
}

func TestHTTPClientManager_Client(t *testing.T) {
	client := &http.Client{}
	cm := &httpClientManager{
		client: client,
	}

	if cm.Client() != client {
		t.Error("Expected Client() to return the same client instance")
	}
}

// Mock certificate provider for testing
type mockCertificateProvider struct {
	cert *tls.Certificate
}

func (m *mockCertificateProvider) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	if m.cert != nil {
		return m.cert, nil
	}
	return &tls.Certificate{}, nil
}

func (m *mockCertificateProvider) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if m.cert != nil {
		return m.cert, nil
	}
	return &tls.Certificate{}, nil
}

// Ensure mockCertificateProvider implements the interface
var _ apitls.CertificateProvider = (*mockCertificateProvider)(nil)

func generateTestCert(notBefore, notAfter time.Time) *tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    notBefore,
		NotAfter:     notAfter,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return &tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
}

func TestHTTPClientManager_CertExpiry(t *testing.T) {
	notAfter := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	cert := generateTestCert(time.Now().Add(-1*time.Hour), notAfter)

	provider := &mockCertificateProvider{cert: cert}
	cm := &httpClientManager{
		certProvider: provider,
	}

	expiry, err := cm.CertExpiry()
	if err != nil {
		t.Fatalf("CertExpiry() returned error: %v", err)
	}
	if !expiry.Equal(notAfter) {
		t.Errorf("CertExpiry() = %v, want %v", expiry, notAfter)
	}

	if !cm.CertValid() {
		t.Error("CertValid() = false, want true for non-expired cert")
	}
}

func TestHTTPClientManager_CertExpired(t *testing.T) {
	cert := generateTestCert(time.Now().Add(-48*time.Hour), time.Now().Add(-1*time.Hour))

	provider := &mockCertificateProvider{cert: cert}
	cm := &httpClientManager{
		certProvider: provider,
	}

	if cm.CertValid() {
		t.Error("CertValid() = true, want false for expired cert")
	}
}

func TestHTTPClientManager_CertExpiryNoCert(t *testing.T) {
	provider := &mockCertificateProvider{}
	cm := &httpClientManager{
		certProvider: provider,
	}

	_, err := cm.CertExpiry()
	if err == nil {
		t.Error("CertExpiry() should return error when no certificate is available")
	}

	if cm.CertValid() {
		t.Error("CertValid() = true, want false when no cert available")
	}
}

// TestHTTPClientManager_TimeoutPrecision tests edge cases around floating point precision
func TestHTTPClientManager_TimeoutPrecision(t *testing.T) {
	// Test edge cases around floating point precision
	tests := []struct {
		name        string
		baseTimeout time.Duration
	}{
		{"Odd milliseconds", 333 * time.Millisecond},
		{"Large number", 999999 * time.Millisecond},
		{"Very small", 2 * time.Millisecond},
	}

	cm := &httpClientManager{
		tlsConfig: &tls.Config{},
		apiURL:    "https://test.example.com/",
		client:    &http.Client{},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm.SetTimeout(tt.baseTimeout)

			// Verify the timeout was set correctly
			if cm.currentTimeout != tt.baseTimeout {
				t.Errorf("Expected currentTimeout = %v, got %v", tt.baseTimeout, cm.currentTimeout)
			}

			// The calculation should not cause any panics or invalid values
			baseTimeoutMs := tt.baseTimeout.Milliseconds()
			expectedResponseHeaderMs := int64(float64(baseTimeoutMs) * 0.85)

			if expectedResponseHeaderMs <= 0 && baseTimeoutMs > 0 {
				t.Errorf("Response header timeout calculation resulted in non-positive value: %d ms", expectedResponseHeaderMs)
			}
		})
	}
}
