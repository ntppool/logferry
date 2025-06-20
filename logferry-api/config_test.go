package main

import (
	"os"
	"testing"
)

/*
Configuration Loading Tests (logferry-api/config_test.go)

WHAT IS BEING TESTED:
- Default behavior: Kafka defaults but empty TLS values when no env vars set
- Environment override: Environment variables properly override defaults
- Partial configuration: Mixed environment/default behavior works correctly
- Empty string handling: Empty strings fall back to defaults for Kafka but remain empty for TLS
- Struct integrity: All expected configuration fields exist and are accessible

WHAT IS NOT BEING TESTED:
- Invalid file paths in configuration
- Configuration validation (checking if cert files exist)
- Configuration reload/hot-reload functionality
- Command-line flag integration
- JSON file configuration support (commented out in code)
- Configuration precedence order beyond env vars and defaults

NOTES:
- TLS fields have no defaults while Kafka fields do - intentional security behavior
- Empty strings in env vars fall back to defaults for Kafka but remain empty for TLS

QUESTIONS:
- Should the system validate that certificate files exist and are readable?
- What should happen with invalid file paths in configuration? (e.g., "/nonexistent/path.crt")
- Should there be validation that TLS_CERT and TLS_KEY are both set or both empty?
*/

// TestLoadConfig_DefaultValues validates default behavior when no environment variables are set
func TestLoadConfig_DefaultValues(t *testing.T) {
	// Clear any existing environment variables
	os.Unsetenv("TLS_CERT")
	os.Unsetenv("TLS_KEY")
	os.Unsetenv("KAFKA_CA")
	os.Unsetenv("KAFKA_CERT")
	os.Unsetenv("KAFKA_KEY")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() failed: %v", err)
	}

	// TLS values should be empty (no defaults)
	if cfg.TLS.Cert != "" {
		t.Errorf("Expected empty TLS.Cert, got %q", cfg.TLS.Cert)
	}
	if cfg.TLS.Key != "" {
		t.Errorf("Expected empty TLS.Key, got %q", cfg.TLS.Key)
	}

	// Kafka values should have defaults
	expectedDefaults := map[string]string{
		"KAFKA_CA":   "/etc/kafka-ca/tls/ca.crt",
		"KAFKA_CERT": "/etc/kafka/tls/user.crt",
		"KAFKA_KEY":  "/etc/kafka/tls/key.crt",
	}

	if cfg.Kafka.CA != expectedDefaults["KAFKA_CA"] {
		t.Errorf("Expected Kafka.CA = %q, got %q", expectedDefaults["KAFKA_CA"], cfg.Kafka.CA)
	}
	if cfg.Kafka.Cert != expectedDefaults["KAFKA_CERT"] {
		t.Errorf("Expected Kafka.Cert = %q, got %q", expectedDefaults["KAFKA_CERT"], cfg.Kafka.Cert)
	}
	if cfg.Kafka.Key != expectedDefaults["KAFKA_KEY"] {
		t.Errorf("Expected Kafka.Key = %q, got %q", expectedDefaults["KAFKA_KEY"], cfg.Kafka.Key)
	}
}

// TestLoadConfig_EnvironmentVariables tests that environment variables properly override defaults
func TestLoadConfig_EnvironmentVariables(t *testing.T) {
	// Set up test environment variables
	testValues := map[string]string{
		"TLS_CERT":   "/custom/tls/cert.pem",
		"TLS_KEY":    "/custom/tls/key.pem",
		"KAFKA_CA":   "/custom/kafka/ca.crt",
		"KAFKA_CERT": "/custom/kafka/cert.crt",
		"KAFKA_KEY":  "/custom/kafka/key.crt",
	}

	// Set environment variables
	for key, value := range testValues {
		os.Setenv(key, value)
	}

	// Clean up after test
	defer func() {
		for key := range testValues {
			os.Unsetenv(key)
		}
	}()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() failed: %v", err)
	}

	// Verify all values were loaded from environment
	if cfg.TLS.Cert != testValues["TLS_CERT"] {
		t.Errorf("Expected TLS.Cert = %q, got %q", testValues["TLS_CERT"], cfg.TLS.Cert)
	}
	if cfg.TLS.Key != testValues["TLS_KEY"] {
		t.Errorf("Expected TLS.Key = %q, got %q", testValues["TLS_KEY"], cfg.TLS.Key)
	}
	if cfg.Kafka.CA != testValues["KAFKA_CA"] {
		t.Errorf("Expected Kafka.CA = %q, got %q", testValues["KAFKA_CA"], cfg.Kafka.CA)
	}
	if cfg.Kafka.Cert != testValues["KAFKA_CERT"] {
		t.Errorf("Expected Kafka.Cert = %q, got %q", testValues["KAFKA_CERT"], cfg.Kafka.Cert)
	}
	if cfg.Kafka.Key != testValues["KAFKA_KEY"] {
		t.Errorf("Expected Kafka.Key = %q, got %q", testValues["KAFKA_KEY"], cfg.Kafka.Key)
	}
}

// TestLoadConfig_PartialEnvironmentVariables tests mixed environment/default behavior
func TestLoadConfig_PartialEnvironmentVariables(t *testing.T) {
	// Clear all variables first
	envVars := []string{"TLS_CERT", "TLS_KEY", "KAFKA_CA", "KAFKA_CERT", "KAFKA_KEY"}
	for _, env := range envVars {
		os.Unsetenv(env)
	}

	// Set only some environment variables
	os.Setenv("TLS_CERT", "/partial/tls/cert.pem")
	os.Setenv("KAFKA_CA", "/partial/kafka/ca.crt")

	defer func() {
		os.Unsetenv("TLS_CERT")
		os.Unsetenv("KAFKA_CA")
	}()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() failed: %v", err)
	}

	// Verify set values
	if cfg.TLS.Cert != "/partial/tls/cert.pem" {
		t.Errorf("Expected TLS.Cert = %q, got %q", "/partial/tls/cert.pem", cfg.TLS.Cert)
	}
	if cfg.Kafka.CA != "/partial/kafka/ca.crt" {
		t.Errorf("Expected Kafka.CA = %q, got %q", "/partial/kafka/ca.crt", cfg.Kafka.CA)
	}

	// Verify unset TLS values are empty
	if cfg.TLS.Key != "" {
		t.Errorf("Expected empty TLS.Key, got %q", cfg.TLS.Key)
	}

	// Verify unset Kafka values use defaults
	if cfg.Kafka.Cert != "/etc/kafka/tls/user.crt" {
		t.Errorf("Expected default Kafka.Cert, got %q", cfg.Kafka.Cert)
	}
	if cfg.Kafka.Key != "/etc/kafka/tls/key.crt" {
		t.Errorf("Expected default Kafka.Key, got %q", cfg.Kafka.Key)
	}
}

// TestLoadConfig_EmptyEnvironmentVariables tests behavior when env vars are set to empty strings
// This tests the configuration library behavior: empty strings fall back to defaults for Kafka
func TestLoadConfig_EmptyEnvironmentVariables(t *testing.T) {
	// Set environment variables to empty strings
	envVars := []string{"TLS_CERT", "TLS_KEY", "KAFKA_CA", "KAFKA_CERT", "KAFKA_KEY"}
	for _, env := range envVars {
		os.Setenv(env, "")
	}

	defer func() {
		for _, env := range envVars {
			os.Unsetenv(env)
		}
	}()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() failed: %v", err)
	}

	// TLS values should be empty (no defaults defined)
	if cfg.TLS.Cert != "" {
		t.Errorf("Expected empty TLS.Cert, got %q", cfg.TLS.Cert)
	}
	if cfg.TLS.Key != "" {
		t.Errorf("Expected empty TLS.Key, got %q", cfg.TLS.Key)
	}

	// Kafka values fall back to defaults when empty string is provided
	// This is the behavior of the configuration library
	expectedDefaults := map[string]string{
		"KAFKA_CA":   "/etc/kafka-ca/tls/ca.crt",
		"KAFKA_CERT": "/etc/kafka/tls/user.crt",
		"KAFKA_KEY":  "/etc/kafka/tls/key.crt",
	}

	if cfg.Kafka.CA != expectedDefaults["KAFKA_CA"] {
		t.Errorf("Expected Kafka.CA = %q, got %q", expectedDefaults["KAFKA_CA"], cfg.Kafka.CA)
	}
	if cfg.Kafka.Cert != expectedDefaults["KAFKA_CERT"] {
		t.Errorf("Expected Kafka.Cert = %q, got %q", expectedDefaults["KAFKA_CERT"], cfg.Kafka.Cert)
	}
	if cfg.Kafka.Key != expectedDefaults["KAFKA_KEY"] {
		t.Errorf("Expected Kafka.Key = %q, got %q", expectedDefaults["KAFKA_KEY"], cfg.Kafka.Key)
	}
}

// TestConfig_StructFields ensures all expected configuration fields exist (compilation test)
func TestConfig_StructFields(t *testing.T) {
	cfg := Config{}

	// Test that struct has expected nested structure
	_ = cfg.TLS.Cert
	_ = cfg.TLS.Key
	_ = cfg.Kafka.CA
	_ = cfg.Kafka.Cert
	_ = cfg.Kafka.Key

	// This test passes if the code compiles (fields exist)
}
