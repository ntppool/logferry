package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"
)

/*
Avro File Processing Tests (logferry-api/avro_test.go)

WHAT IS BEING TESTED:
- Constructor validation: NewAvroHandler() proper initialization including timeout calculation (+5s)
- Configuration endpoint: Config() JSON response format
- SHA256 validation: Tests that incorrect SHA256 headers result in BadRequest
- File size validation: Tests that mismatched file sizes result in BadRequest
- Size limits: readPostedFile() enforces 20MB limit 
- Rate limiting: Tests that exceeding request limiter capacity returns 429
- Authorization: Tests that missing hostname results in 401
- Empty data handling: Tests SHA256 calculation for empty files
- Temporary file handling: Tests file creation and cleanup

WHAT IS NOT BEING TESTED:
- Actual Avro schema validation and parsing (requires full setup)
- Kafka message production (would need Kafka integration)
- Complete request flow with valid Avro data
- Memory usage with large files
- Cleanup of temporary files in error scenarios
- Concurrent request handling beyond rate limiting

NOTES:
- Client timeout = timeoutStick + 5s - So client times out after the server
- Rate limiter capacity = 20 - To prevent overloading the Kafka server
- 20MB limit is in the server - Located in avro.go line 248

QUESTIONS:
- Should there be different rate limits for different certificate types/hostnames?
- What should happen if temporary file creation fails due to disk space?
- Should the 20MB limit be configurable?
*/

// TestNewAvroHandler validates constructor creates properly initialized handler
func TestNewAvroHandler(t *testing.T) {
	writer := &kafka.Writer{}
	timeoutStick := 60 * time.Second

	handler, err := NewAvroHandler(writer, timeoutStick)
	if err != nil {
		t.Fatalf("NewAvroHandler() failed: %v", err)
	}

	if handler == nil {
		t.Fatal("NewAvroHandler() returned nil handler")
	}

	expectedTimeout := timeoutStick + 5*time.Second
	if handler.clientTimeout != expectedTimeout {
		t.Errorf("Expected clientTimeout = %v, got %v", expectedTimeout, handler.clientTimeout)
	}

	if handler.writer != writer {
		t.Error("Expected writer to be set correctly")
	}

	if handler.schema == nil {
		t.Error("Expected schema to be initialized")
	}

	if cap(handler.requestLimiter) != 20 {
		t.Errorf("Expected requestLimiter capacity = 20, got %d", cap(handler.requestLimiter))
	}
}

// TestAvroHandler_Config tests the configuration endpoint JSON response
func TestAvroHandler_Config(t *testing.T) {
	handler := &AvroHandler{
		clientTimeout: 65 * time.Second,
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Set required context value
	ctx := context.WithValue(c.Request().Context(), CertificateKey, "test.example.com")
	c.SetRequest(c.Request().WithContext(ctx))

	err := handler.Config(c)
	if err != nil {
		t.Fatalf("Config() failed: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rec.Code)
	}

	expectedBody := `{"Timeout":65000000000}`
	body := strings.TrimSpace(rec.Body.String())
	if body != expectedBody {
		t.Errorf("Expected body %q, got %q", expectedBody, body)
	}
}

// TestAvroHandler_readPostedFile_SHA256Calculation tests SHA256 hash calculation and file handling
func TestAvroHandler_readPostedFile_SHA256Calculation(t *testing.T) {
	handler := &AvroHandler{}

	// Test data
	testData := []byte("test data for sha256 calculation")
	expectedSHA256 := sha256.Sum256(testData)
	expectedSHA256Base64 := base64.StdEncoding.EncodeToString(expectedSHA256[:])

	// Create request with test data
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(testData))

	ctx := context.Background()
	fh, sha, size, err := handler.readPostedFile(ctx, req)
	if err != nil {
		t.Fatalf("readPostedFile() failed: %v", err)
	}
	defer func() {
		if fh != nil {
			fh.Close()
			os.Remove(fh.Name())
		}
	}()

	// Verify SHA256
	if sha != expectedSHA256Base64 {
		t.Errorf("Expected SHA256 = %q, got %q", expectedSHA256Base64, sha)
	}

	// Verify size
	if size != int64(len(testData)) {
		t.Errorf("Expected size = %d, got %d", len(testData), size)
	}

	// Verify file content
	fh.Seek(0, io.SeekStart)
	readData, err := io.ReadAll(fh)
	if err != nil {
		t.Fatalf("Failed to read temp file: %v", err)
	}
	if !bytes.Equal(readData, testData) {
		t.Error("File content doesn't match original data")
	}
}

// TestAvroHandler_readPostedFile_SizeLimit tests the 20MB file size limit enforcement
func TestAvroHandler_readPostedFile_SizeLimit(t *testing.T) {
	handler := &AvroHandler{}

	// Create data larger than 20MB limit
	largeData := make([]byte, 21*1024*1024) // 21MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(largeData))

	ctx := context.Background()
	fh, _, size, err := handler.readPostedFile(ctx, req)
	if err != nil {
		t.Fatalf("readPostedFile() failed: %v", err)
	}
	defer func() {
		if fh != nil {
			fh.Close()
			os.Remove(fh.Name())
		}
	}()

	// Should be limited to 20MB
	expectedMaxSize := int64(20 * 1024 * 1024)
	if size != expectedMaxSize {
		t.Errorf("Expected size to be limited to %d, got %d", expectedMaxSize, size)
	}
}

func TestAvroHandler_readPostedFile_EmptyData(t *testing.T) {
	handler := &AvroHandler{}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))

	ctx := context.Background()
	fh, sha, size, err := handler.readPostedFile(ctx, req)
	if err != nil {
		t.Fatalf("readPostedFile() failed: %v", err)
	}
	defer func() {
		if fh != nil {
			fh.Close()
			os.Remove(fh.Name())
		}
	}()

	// Empty data should have size 0
	if size != 0 {
		t.Errorf("Expected size = 0, got %d", size)
	}

	// SHA256 of empty string
	emptyStringSHA256 := sha256.Sum256([]byte{})
	expectedSHA256 := base64.StdEncoding.EncodeToString(emptyStringSHA256[:])
	if sha != expectedSHA256 {
		t.Errorf("Expected SHA256 of empty data = %q, got %q", expectedSHA256, sha)
	}
}

// TestAvroHandler_Post_SizeValidation tests file size validation logic at HTTP layer
func TestAvroHandler_Post_SizeValidation(t *testing.T) {
	// Test size validation logic by testing mismatched sizes
	handler := &AvroHandler{
		requestLimiter: make(chan bool, 20),
	}

	testData := []byte("test data")
	wrongSize := len(testData) + 10 // Intentionally wrong size

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(testData))
	req.Header.Set("file-size", strconv.Itoa(wrongSize))
	req.Header.Set("Request-ID", "test-123")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Set required context value
	ctx := context.WithValue(c.Request().Context(), CertificateKey, "test.example.com")
	c.SetRequest(c.Request().WithContext(ctx))

	// This should fail at size validation stage
	_ = handler.Post(c)

	// Should get BadRequest due to size mismatch
	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected BadRequest for size mismatch, got status %d", rec.Code)
	}
}

// TestAvroHandler_Post_SHA256Validation tests SHA256 validation at HTTP layer
func TestAvroHandler_Post_SHA256Validation(t *testing.T) {
	handler := &AvroHandler{
		requestLimiter: make(chan bool, 20),
	}

	testData := []byte("test data for sha validation")

	// Test incorrect SHA256 (should fail at validation stage)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(testData))
	req.Header.Set("Request-ID", "test-123")
	req.Header.Set("content-sha256", "incorrect-sha256-value")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Set required context value
	ctx := context.WithValue(c.Request().Context(), CertificateKey, "test.example.com")
	c.SetRequest(c.Request().WithContext(ctx))

	_ = handler.Post(c)

	// Should get BadRequest due to SHA256 mismatch
	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected BadRequest for SHA256 mismatch, got status %d", rec.Code)
	}
}

// TestAvroHandler_Post_RateLimiting tests the rate limiting functionality (capacity = 20)
func TestAvroHandler_Post_RateLimiting(t *testing.T) {
	// Create handler with small limiter for testing
	handler := &AvroHandler{
		requestLimiter: make(chan bool, 2), // Only allow 2 concurrent requests
	}

	// Fill up the limiter
	handler.requestLimiter <- true
	handler.requestLimiter <- true

	testData := []byte("test data")
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(testData))
	req.Header.Set("Request-ID", "test-rate-limit")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Set required context value
	ctx := context.WithValue(c.Request().Context(), CertificateKey, "test.example.com")
	c.SetRequest(c.Request().WithContext(ctx))

	err := handler.Post(c)
	if err != nil {
		// The error from Post() might be a rate limit error (429) which is expected
		if rec.Code != http.StatusTooManyRequests {
			t.Errorf("Expected rate limiting (429), got status %d", rec.Code)
		}
	} else if rec.Code != http.StatusTooManyRequests {
		t.Errorf("Expected rate limiting (429), got status %d", rec.Code)
	}

	// Verify rate limit headers
	if rec.Header().Get("Retry-After") != "5" {
		t.Errorf("Expected Retry-After header = '5', got %q", rec.Header().Get("Retry-After"))
	}
	if rec.Header().Get("Connection") != "Close" {
		t.Errorf("Expected Connection header = 'Close', got %q", rec.Header().Get("Connection"))
	}
}

// TestAvroHandler_Post_MissingHostname tests authorization via certificate hostname validation
func TestAvroHandler_Post_MissingHostname(t *testing.T) {
	handler := &AvroHandler{
		requestLimiter: make(chan bool, 20),
	}

	testData := []byte("test data")
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(testData))

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Don't set hostname in context (or set empty string)
	ctx := context.WithValue(c.Request().Context(), CertificateKey, "")
	c.SetRequest(c.Request().WithContext(ctx))

	err := handler.Post(c)
	if err != nil {
		t.Fatalf("Post() failed: %v", err)
	}

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected Unauthorized (401) for missing hostname, got status %d", rec.Code)
	}
}
