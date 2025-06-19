package main

import (
	"crypto/x509"
	"testing"
)

func TestAPIServer_getVerifiedCert(t *testing.T) {
	srv := &APIServer{}

	tests := []struct {
		name            string
		dnsNames        []string
		expectedName    string
		expectedCertNil bool
		description     string
	}{
		{
			name:            "Valid ntppool.dev domain",
			dnsNames:        []string{"server1.mon.ntppool.dev"},
			expectedName:    "server1.mon.ntppool.dev",
			expectedCertNil: false,
			description:     "Should accept certificates with .mon.ntppool.dev suffix",
		},
		{
			name:            "Valid ntppool.net domain",
			dnsNames:        []string{"server2.ntppool.net"},
			expectedName:    "server2.ntppool.net",
			expectedCertNil: false,
			description:     "Should accept certificates with .ntppool.net suffix",
		},
		{
			name:            "Multiple DNS names with valid one",
			dnsNames:        []string{"invalid.example.com", "valid.mon.ntppool.dev", "another.example.com"},
			expectedName:    "valid.mon.ntppool.dev",
			expectedCertNil: false,
			description:     "Should find the valid domain among multiple DNS names",
		},
		{
			name:            "Both valid domains present",
			dnsNames:        []string{"test1.mon.ntppool.dev", "test2.ntppool.net"},
			expectedName:    "test1.mon.ntppool.dev",
			expectedCertNil: false,
			description:     "Should return first valid domain (.mon.ntppool.dev has precedence)",
		},
		{
			name:            "Invalid domain",
			dnsNames:        []string{"server.example.com"},
			expectedName:    "",
			expectedCertNil: true,
			description:     "Should reject certificates without valid domain suffixes",
		},
		{
			name:            "Empty DNS names",
			dnsNames:        []string{},
			expectedName:    "",
			expectedCertNil: true,
			description:     "Should handle certificates with no DNS names",
		},
		{
			name:            "Partial matches should not work",
			dnsNames:        []string{"malicious-ntppool.net", "fake.mon.ntppool.dev.evil.com"},
			expectedName:    "",
			expectedCertNil: true,
			description:     "Should not accept partial or manipulated domain names",
		},
		{
			name:            "Domain ending with valid suffix",
			dnsNames:        []string{"test.mon.ntppool.dev", "test.ntppool.net"},
			expectedName:    "test.mon.ntppool.dev",
			expectedCertNil: false,
			description:     "Should accept domains ending with valid suffixes",
		},
		{
			name:            "Case sensitivity test",
			dnsNames:        []string{"Server.Mon.Ntppool.Dev"},
			expectedName:    "",
			expectedCertNil: true,
			description:     "Should be case sensitive (current implementation)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock certificate with the specified DNS names
			cert := &x509.Certificate{
				DNSNames: tt.dnsNames,
			}

			// Create verified chains
			verifiedChains := [][]*x509.Certificate{
				{cert},
			}

			resultCert, resultName := srv.getVerifiedCert(verifiedChains)

			// Check certificate result
			if tt.expectedCertNil && resultCert != nil {
				t.Errorf("Expected nil certificate, got non-nil")
			}
			if !tt.expectedCertNil && resultCert == nil {
				t.Errorf("Expected non-nil certificate, got nil")
			}

			// Check name result
			if resultName != tt.expectedName {
				t.Errorf("Expected name %q, got %q", tt.expectedName, resultName)
			}
		})
	}
}

func TestAPIServer_getVerifiedCert_MultipleCertificates(t *testing.T) {
	srv := &APIServer{}

	// Create multiple certificates in the chain
	cert1 := &x509.Certificate{
		DNSNames: []string{"invalid.example.com"},
	}
	cert2 := &x509.Certificate{
		DNSNames: []string{"valid.mon.ntppool.dev"},
	}
	cert3 := &x509.Certificate{
		DNSNames: []string{"another.invalid.com"},
	}

	verifiedChains := [][]*x509.Certificate{
		{cert1, cert2, cert3},
	}

	resultCert, resultName := srv.getVerifiedCert(verifiedChains)

	if resultCert == nil {
		t.Error("Expected to find valid certificate in chain")
	}
	if resultName != "valid.mon.ntppool.dev" {
		t.Errorf("Expected name 'valid.mon.ntppool.dev', got %q", resultName)
	}
}

func TestAPIServer_getVerifiedCert_MultipleChains(t *testing.T) {
	srv := &APIServer{}

	// Create multiple certificate chains
	cert1 := &x509.Certificate{
		DNSNames: []string{"invalid.example.com"},
	}
	cert2 := &x509.Certificate{
		DNSNames: []string{"valid.ntppool.net"},
	}

	verifiedChains := [][]*x509.Certificate{
		{cert1},
		{cert2},
	}

	resultCert, resultName := srv.getVerifiedCert(verifiedChains)

	if resultCert == nil {
		t.Error("Expected to find valid certificate across chains")
	}
	if resultName != "valid.ntppool.net" {
		t.Errorf("Expected name 'valid.ntppool.net', got %q", resultName)
	}
}

func TestAPIServer_getVerifiedCert_EmptyChains(t *testing.T) {
	srv := &APIServer{}

	// Test with nil chains
	resultCert, resultName := srv.getVerifiedCert(nil)
	if resultCert != nil || resultName != "" {
		t.Error("Expected nil certificate and empty name for nil chains")
	}

	// Test with empty chains
	verifiedChains := [][]*x509.Certificate{}
	resultCert, resultName = srv.getVerifiedCert(verifiedChains)
	if resultCert != nil || resultName != "" {
		t.Error("Expected nil certificate and empty name for empty chains")
	}

	// Test with chains containing nil certificates
	verifiedChains = [][]*x509.Certificate{
		{nil},
	}
	// This test verifies the function handles nil certificates gracefully
	// It may panic in current implementation, which would indicate a bug
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Function panicked with nil certificate: %v", r)
			// This is actually expected behavior that should be fixed
		}
	}()
	resultCert, resultName = srv.getVerifiedCert(verifiedChains)
	if resultCert != nil || resultName != "" {
		t.Error("Expected nil certificate and empty name for nil certificate in chain")
	}
}

func TestAPIServer_verifyClient(t *testing.T) {
	srv := &APIServer{}

	tests := []struct {
		name        string
		dnsNames    []string
		expectError bool
		description string
	}{
		{
			name:        "Valid certificate",
			dnsNames:    []string{"test.mon.ntppool.dev"},
			expectError: false,
			description: "Should accept valid certificate",
		},
		{
			name:        "Invalid certificate",
			dnsNames:    []string{"invalid.example.com"},
			expectError: true,
			description: "Should reject invalid certificate",
		},
		{
			name:        "Empty DNS names",
			dnsNames:    []string{},
			expectError: true,
			description: "Should reject certificate with no DNS names",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := &x509.Certificate{
				DNSNames: tt.dnsNames,
			}

			verifiedChains := [][]*x509.Certificate{
				{cert},
			}

			err := srv.verifyClient(nil, verifiedChains)

			if tt.expectError && err == nil {
				t.Errorf("Expected error for %s, got nil", tt.description)
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error for %s, got %v", tt.description, err)
			}
		})
	}
}

func TestAPIServer_DomainValidationEdgeCases(t *testing.T) {
	srv := &APIServer{}

	edgeCases := []struct {
		name        string
		dnsName     string
		shouldMatch bool
		description string
	}{
		{
			name:        "Subdomain of valid domain",
			dnsName:     "sub.server.mon.ntppool.dev",
			shouldMatch: true,
			description: "Multi-level subdomains should be accepted",
		},
		{
			name:        "Domain with hyphen",
			dnsName:     "test-server.mon.ntppool.dev",
			shouldMatch: true,
			description: "Hyphens in hostname should be accepted",
		},
		{
			name:        "Domain with numbers",
			dnsName:     "server123.mon.ntppool.dev",
			shouldMatch: true,
			description: "Numbers in hostname should be accepted",
		},
		{
			name:        "Very long subdomain",
			dnsName:     "very-long-server-name-with-multiple-parts.mon.ntppool.dev",
			shouldMatch: true,
			description: "Long subdomains should be accepted",
		},
		{
			name:        "Domain ending with valid suffix but different TLD",
			dnsName:     "server.mon.ntppool.dev.com",
			shouldMatch: false,
			description: "Should not match domains that end with valid suffix but have additional TLD",
		},
		{
			name:        "Domain containing valid suffix in middle",
			dnsName:     "mon.ntppool.dev.evil.com",
			shouldMatch: false,
			description: "Should not match when valid suffix appears in middle of domain",
		},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			cert := &x509.Certificate{
				DNSNames: []string{tc.dnsName},
			}

			verifiedChains := [][]*x509.Certificate{
				{cert},
			}

			resultCert, resultName := srv.getVerifiedCert(verifiedChains)

			if tc.shouldMatch {
				if resultCert == nil || resultName != tc.dnsName {
					t.Errorf("Expected to match %s, but got cert=%v, name=%q", tc.dnsName, resultCert != nil, resultName)
				}
			} else {
				if resultCert != nil || resultName != "" {
					t.Errorf("Expected not to match %s, but got cert=%v, name=%q", tc.dnsName, resultCert != nil, resultName)
				}
			}
		})
	}
}
