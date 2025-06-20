# Test Documentation

This document comprehensively describes what each test is testing and what it is NOT testing, along with any ambiguities that need clarification.

## Phase 1: Critical Core Functions

### File Locking System (`filelock_test.go`)

#### What is being tested:
- **Constructor**: `TestNewFileLock()` validates that `NewFileLock()` returns a non-nil instance with initialized map
- **Basic locking**: `TestFileLock_Lock()` tests that new files can be locked successfully, including edge cases (empty strings, filenames with spaces)
- **Double locking prevention**: `TestFileLock_DoubleLock()` ensures the same file cannot be locked twice
- **Lock status checking**: `TestFileLock_IsLocked()` validates lock state queries and isolation between different filenames
- **Unlocking**: `TestFileLock_Unlock()` tests that unlocked files can be locked again
- **Graceful error handling**: `TestFileLock_UnlockNonExistent()` ensures unlocking non-existent locks doesn't panic
- **Concurrency safety**: `TestFileLock_ConcurrentAccess()` tests that only one goroutine can acquire the same lock
- **Parallel locking**: `TestFileLock_ConcurrentDifferentFiles()` tests that different files can be locked simultaneously
- **Lock lifecycle**: `TestFileLock_LockUnlockCycle()` tests repeated lock/unlock cycles

#### What is NOT being tested:
- Memory cleanup after many lock/unlock cycles
- Behavior with extremely long filenames (>1000 chars)
- Performance under high contention
- Lock timeouts or expiration
- Persistence across process restarts

#### Questions/Ambiguities:
1. ✅ **RESOLVED: Empty string filenames are NOT allowed** - `Lock("")` and `IsLocked("")` return `false`
2. **What's the maximum supported filename length?** Should there be validation or limits?
3. **Should there be any filename sanitization** (e.g., path traversal protection)?

### File Validation (`uploader_test.go`)

#### What is being tested:
- **Basic validation**: `TestIsAvroFileName()` tests the core logic that files must end with ".avro"
- **Path handling**: Tests that files with paths (e.g., "path/to/data.avro") are accepted
- **Case sensitivity**: Validates that only lowercase ".avro" is accepted
- **Extension isolation**: Tests that ".avro" in the middle or start of filename doesn't count
- **Special characters**: Tests hyphens, underscores, numbers in filenames
- **Edge cases**: Empty strings, very long filenames, multiple dots
- **Whitespace handling**: Tests that filenames with leading/trailing whitespace are rejected

#### What is NOT being tested:
- File existence on disk
- File readability/permissions
- File content validation
- Unicode filename support
- Network path handling (UNC paths, etc.)

#### Questions/Ambiguities:
1. ✅ **RESOLVED: `.avro` alone is NOT a valid filename** - requires at least one character before the extension
2. **Should Unicode characters in filenames be supported?** (e.g., "データ.avro")
3. **What about case-insensitive filesystems?** Should "DATA.AVRO" be accepted on Windows?
4. **Should there be any path traversal validation** (e.g., reject "../../../etc/passwd.avro")?

### Configuration Loading (`logferry-api/config_test.go`)

#### What is being tested:
- **Default behavior**: `TestLoadConfig_DefaultValues()` validates Kafka defaults but empty TLS values
- **Environment override**: `TestLoadConfig_EnvironmentVariables()` tests that env vars override defaults
- **Partial configuration**: `TestLoadConfig_PartialEnvironmentVariables()` tests mixed env/default behavior
- **Empty string handling**: `TestLoadConfig_EmptyEnvironmentVariables()` tests that empty strings fall back to defaults for Kafka but remain empty for TLS
- **Struct integrity**: `TestConfig_StructFields()` ensures all expected fields exist

#### What is NOT being tested:
- Invalid file paths in configuration
- Configuration validation (e.g., checking if cert files exist)
- Configuration reload/hot-reload
- Command-line flag integration
- JSON file configuration support (commented out in code)
- Configuration precedence order beyond env vars and defaults

#### Questions/Ambiguities:
1. **Why do TLS fields have no defaults while Kafka fields do?** Is this intentional security behavior?
2. **Should the system validate that certificate files exist and are readable?**
3. **What should happen with invalid file paths in configuration?** (e.g., "/nonexistent/path.crt")
4. **Should there be validation that TLS_CERT and TLS_KEY are both set or both empty?**

## Phase 2: Data Processing & Validation

### HTTP Client Timeout Logic (`http_test.go`)

#### What is being tested:
- **Timeout calculation**: Tests the 0.85 multiplier formula for response header timeouts
- **No-op behavior**: `TestHTTPClientManager_SetTimeout_NoOpSameValue()` tests that setting the same timeout twice is a no-op
- **Precision edge cases**: Tests very small timeouts and floating-point precision
- **Configuration application**: Tests that timeouts are applied to both client and transport
- **Getter methods**: Tests `APIBase()` and `Client()` methods

#### What is NOT being tested:
- Actual network timeouts in practice
- Interaction with real TLS handshakes
- Transport connection pooling behavior
- Error handling when timeout values are invalid
- Concurrent timeout modifications
- Memory leaks from transport recreation

#### Questions/Ambiguities:
1. **Why specifically 0.85 as the multiplier?** Is this based on empirical testing or network analysis?
2. **Should there be minimum/maximum timeout limits?** (e.g., reject timeouts < 100ms or > 10 minutes)
3. **What should happen with zero or negative timeouts?**
4. **Should concurrent calls to SetTimeout() be thread-safe?**

### Avro File Processing (`logferry-api/avro_test.go`)

#### What is being tested:
- **Constructor validation**: `TestNewAvroHandler()` tests proper initialization including timeout calculation (+5s)
- **Configuration endpoint**: `TestAvroHandler_Config()` tests JSON response format
- **SHA256 validation**: Tests that incorrect SHA256 headers result in BadRequest
- **File size validation**: Tests that mismatched file sizes result in BadRequest
- **Size limits**: `TestAvroHandler_readPostedFile_SizeLimit()` tests 20MB limit enforcement
- **Rate limiting**: Tests that exceeding request limiter capacity returns 429
- **Authorization**: Tests that missing hostname results in 401
- **Empty data handling**: Tests SHA256 calculation for empty files

#### What is NOT being tested:
- Actual Avro schema validation and parsing (requires full setup)
- Kafka message production (would need Kafka integration)
- Complete request flow with valid Avro data
- Memory usage with large files
- Cleanup of temporary files in error scenarios
- Concurrent request handling beyond rate limiting

#### Questions/Ambiguities:
1. ✅ **RESOLVED: Client timeout = timeoutStick + 5s** - So client times out after the server
2. ✅ **RESOLVED: Rate limiter capacity = 20** - To prevent overloading the Kafka server
3. **Should there be different rate limits for different certificate types/hostnames?**
4. **What should happen if temporary file creation fails due to disk space?**
5. ✅ **RESOLVED: 20MB limit is in the server** - Located in `logferry-api/avro.go` line 248. Should this be configurable?

### TLS Certificate Validation (`logferry-api/tls_test.go`)

#### What is being tested:
- **Domain suffix validation**: Tests `.mon.ntppool.dev` and `.ntppool.net` suffixes
- **Priority order**: Tests that `.mon.ntppool.dev` has precedence over `.ntppool.net`
- **Security edge cases**: Tests against domain manipulation attempts
- **Certificate chain handling**: Tests multiple certificates and chains
- **DNS name processing**: Tests multiple DNS names per certificate
- **Case sensitivity**: Tests that domain matching is case-sensitive
- **Empty input handling**: Tests behavior with empty/nil certificate chains

#### What is NOT being tested:
- Certificate expiration validation
- Certificate revocation checking
- Certificate signature validation
- Certificate authority validation
- Certificate key usage validation
- Client certificate mutual TLS handshake

#### Questions/Ambiguities:
1. ✅ **RESOLVED: `.mon.ntppool.dev` has priority** - Because it's used most frequently
2. **Should domain matching be case-insensitive** to be more robust against certificate variations?
3. **What about subdomain depth limits?** Should `very.deep.sub.domain.mon.ntppool.dev` be accepted?
4. **Should there be wildcard certificate support** (e.g., `*.mon.ntppool.dev`)?
5. **What happens if a certificate has both valid and invalid DNS names?** Currently returns the first valid one - is this correct?
6. **Should there be additional certificate validation** beyond DNS name checking?

## Overall Testing Gaps

### Security Testing
- **Input sanitization**: No tests for malicious inputs (SQL injection, XSS, etc.)
- **Denial of service**: No tests for resource exhaustion attacks
- **Path traversal**: Limited testing of file path validation

### Error Handling
- **Network failures**: Limited testing of network error scenarios
- **Disk space**: No testing of disk full scenarios
- **Memory pressure**: No testing under low memory conditions

### Performance Testing
- **Load testing**: No tests under high load
- **Memory leaks**: No long-running tests to detect leaks
- **Garbage collection**: No tests of GC behavior under load

### Integration Testing
- **End-to-end flows**: Tests are mostly unit tests, limited integration
- **External dependencies**: No testing with real Kafka, file systems, etc.
- **Configuration interaction**: Limited testing of configuration edge cases

## Resolved Clarifications

Based on feedback, the following behaviors have been clarified and implemented:

1. ✅ **File locking with empty strings**: NOT allowed - returns `false`
2. ✅ **Avro filename validation**: `.avro` alone is NOT valid - requires filename before extension
3. ✅ **TLS certificate domain precedence**: `.mon.ntppool.dev` has priority because it's used most
4. ✅ **Timeout calculations**: +5s buffer ensures client times out after server; 0.85 multiplier still needs investigation
5. ✅ **Rate limiting strategy**: Global limit of 20 to prevent Kafka overload
6. ✅ **20MB file limit**: Located in server, may need to be configurable

## Remaining Questions for Clarification

1. **Timeout calculations**: What's the rationale for the 0.85 multiplier for response headers?
2. **Rate limiting strategy**: Should limits be per-hostname or remain global?
3. **Configuration validation**: Should the system validate file existence?
4. **Unicode filename support**: Should filenames like "データ.avro" be supported?
5. **Case sensitivity**: Should domain matching be case-insensitive for robustness?
6. **Error handling**: What's the preferred behavior for edge cases like disk full, invalid certificates, etc.?

These remaining clarifications would help improve test coverage and ensure the tests match intended system behavior.